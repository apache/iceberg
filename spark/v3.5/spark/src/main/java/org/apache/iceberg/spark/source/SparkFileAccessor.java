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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_ROW_FIELD_NAME;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.avro.AvroFileAccessFactory;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.FileAccessor;
import org.apache.iceberg.orc.ORCFileAccessFactory;
import org.apache.iceberg.parquet.ParquetFileAccessFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.data.SparkAvroWriter;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkOrcWriter;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.spark.data.SparkPlannedAvroReader;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkFileAccessor {
  public static final FileAccessor<StructType, InternalRow, ColumnarBatch> INSTANCE =
      new FileAccessor<>(
          ImmutableMap.of(
              FileFormat.PARQUET,
              new ParquetFileAccessFactory<
                  StructType, InternalRow, ColumnarBatch, DeleteFilter<InternalRow>>(
                  SparkParquetReaders::buildReader,
                  VectorizedSparkParquetReaders::buildReader,
                  (engineSchema, icebergSchema, messageType) ->
                      SparkParquetWriters.buildWriter(engineSchema, messageType),
                  path -> UTF8String.fromString(path.toString())),
              FileFormat.AVRO,
              new AvroFileAccessFactory<>(
                  SparkPlannedAvroReader::create,
                  (unused, rowType) -> new SparkAvroWriter(rowType),
                  (unused, rowType) ->
                      new SparkAvroWriter(
                          (StructType) rowType.apply(DELETE_FILE_ROW_FIELD_NAME).dataType())),
              FileFormat.ORC,
              new ORCFileAccessFactory<>(
                  SparkOrcReader::new,
                  VectorizedSparkOrcReaders::buildReader,
                  (schema, messageType, engineSchema) -> new SparkOrcWriter(schema, messageType),
                  path -> UTF8String.fromString(path.toString()))));

  private SparkFileAccessor() {}
}
