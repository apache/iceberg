/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source

import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.iceberg.{ContentFile, FileFormat, PartitionKey, Table}
import org.apache.iceberg.io.{ClusteredDataWriter, ClusteredEqualityDeleteWriter, OutputFileFactory}
import org.apache.iceberg.spark.SparkSchemaUtil

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.{InternalRow, ProjectingInternalRow}
import org.apache.spark.sql.types.StructType

case class EqDeltaWriter(
    tableBroadcast: Broadcast[Table],
    rowType: StructType,
    eqCols: Seq[String]) extends Serializable {

  private final val FILE_FORMAT = FileFormat.PARQUET
  private final val FILE_SIZE = Long.MaxValue

  def write(rows: Iterator[InternalRow]): Seq[ContentFile[_]] = {
    val table = tableBroadcast.value
    val tableSchema = table.schema
    val spec = table.spec
    val partitionId = Random.nextInt(10000)
    val taskId = Random.nextInt(10000)
    val operationId = UUID.randomUUID().toString
    val isUpdateColOrdinal = rowType.fieldIndex("is_update")

    val dataFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
      .format(FILE_FORMAT)
      .operationId(operationId)
      .build()

    val deleteFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
      .format(FILE_FORMAT)
      .operationId(operationId)
      .suffix("deletes")
      .build()

    val dataSparkType = SparkSchemaUtil.convert(tableSchema)
    val dataColOrdinals = dataSparkType.map(col => rowType.fieldIndex(col.name))
    val eqDeleteSchema = tableSchema.select(eqCols.asJava)
    val eqDeleteSparkType = SparkSchemaUtil.convert(eqDeleteSchema)
    val eqColOrdinals = eqDeleteSparkType.map(col => rowType.fieldIndex(col.name))

    val writerFactory = SparkFileWriterFactory.builderFor(table)
      .dataFileFormat(FILE_FORMAT)
      .dataSchema(tableSchema)
      .dataSparkType(dataSparkType)
      .deleteFileFormat(FILE_FORMAT)
      .equalityFieldIds(eqDeleteSchema.columns.asScala.map(column => column.fieldId).toArray)
      .equalityDeleteRowSchema(eqDeleteSchema)
      .equalityDeleteSparkType(eqDeleteSparkType)
      .build()

    val partitionKey = new PartitionKey(spec, tableSchema)
    val internalRowWrapper = new InternalRowWrapper(dataSparkType)
    val dataProj = ProjectingInternalRow(dataSparkType, dataColOrdinals)
    val eqDeleteProj = ProjectingInternalRow(eqDeleteSparkType, eqColOrdinals)
    val dataWriter = new ClusteredDataWriter(writerFactory, dataFileFactory, table.io, FILE_SIZE)
    val eqDeleteWriter = new ClusteredEqualityDeleteWriter(writerFactory, deleteFileFactory, table.io, FILE_SIZE)

    try {
      for (row <- rows) {
        dataProj.project(row)
        partitionKey.partition(internalRowWrapper.wrap(dataProj))

        val isUpdate = row.getBoolean(isUpdateColOrdinal)
        if (isUpdate) {
          eqDeleteProj.project(row)
          eqDeleteWriter.write(eqDeleteProj, spec, partitionKey)
        }

        dataWriter.write(row, spec, partitionKey)
      }
    } finally {
      dataWriter.close()
      eqDeleteWriter.close()
    }

    dataWriter.result.dataFiles.asScala ++ eqDeleteWriter.result.deleteFiles.asScala
  }
}