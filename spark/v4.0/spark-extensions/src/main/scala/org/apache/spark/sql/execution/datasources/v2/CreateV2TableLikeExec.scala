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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.SortOrder
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.expressions.Transform
import scala.jdk.CollectionConverters._

case class CreateV2TableLikeExec(
    catalog: TableCatalog,
    ident: Identifier,
    sourceCatalog: TableCatalog,
    sourceIdent: Identifier,
    tableProps: Map[String, String],
    ignoreIfExists: Boolean) extends LeafV2CommandExec {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    if (ignoreIfExists && catalog.tableExists(ident)) {
      return Nil
    }

    val sourceTable = sourceCatalog.loadTable(sourceIdent) match {
      case iceberg: SparkTable => iceberg
      case other =>
        throw new UnsupportedOperationException(
          s"CREATE TABLE LIKE is only supported for Iceberg tables, got: ${other.getClass}")
    }

    val schema: Schema = sourceTable.table().schema()
    val partitionSpec: PartitionSpec = sourceTable.table().spec()
    val sortOrder: SortOrder = sourceTable.table().sortOrder()

    val partitioning: Array[Transform] = Spark3Util.toTransforms(partitionSpec)

    val sourceProps = sourceTable.table().properties().asScala.toMap
    val mergedProps = sourceProps ++ tableProps

    catalog.createTable(
      ident,
      SparkSchemaUtil.convert(schema),
      partitioning,
      mergedProps.asJava
    )

    if (sortOrder.isSorted) {
      catalog.loadTable(ident) match {
        case newIceberg: SparkTable =>
          val replaceSortOrder = newIceberg.table().replaceSortOrder()
          sortOrder.fields().asScala.foreach { field =>
            val fieldName = schema.findColumnName(field.sourceId())
            field.direction() match {
              case org.apache.iceberg.SortDirection.ASC =>
                replaceSortOrder.asc(fieldName)
              case org.apache.iceberg.SortDirection.DESC =>
                replaceSortOrder.desc(fieldName)
            }
          }
          replaceSortOrder.commit()
        case _ =>
      }
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateTableLike ${catalog.name}.${ident.quoted} LIKE ${sourceCatalog.name}.${sourceIdent.quoted}"
  }
}
