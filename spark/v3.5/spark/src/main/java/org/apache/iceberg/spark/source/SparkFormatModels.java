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

import org.apache.iceberg.avro.AvroFormatModel;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.FormatModelRegistry;
import org.apache.iceberg.orc.ORCFormatModel;
import org.apache.iceberg.parquet.ParquetFormatModel;
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

public class SparkFormatModels {
  public static final String MODEL_NAME = "spark";
  public static final String VECTORIZED_MODEL_NAME = "spark-vectorized";

  public static void register() {
    FormatModelRegistry.registerFormatModel(
        new AvroFormatModel<StructType, InternalRow>(
            MODEL_NAME,
            SparkPlannedAvroReader::create,
            (schema, engineSchema) -> new SparkAvroWriter(engineSchema),
            (schema, engineSchema) ->
                new SparkAvroWriter(
                    (StructType) engineSchema.apply(DELETE_FILE_ROW_FIELD_NAME).dataType())));

    FormatModelRegistry.registerFormatModel(
        new ParquetFormatModel<InternalRow, DeleteFilter<InternalRow>, StructType>(
            MODEL_NAME,
            SparkParquetReaders::buildReader,
            (engineSchema, icebergSchema, messageType) ->
                SparkParquetWriters.buildWriter(engineSchema, messageType),
            path -> UTF8String.fromString(path.toString())));

    FormatModelRegistry.registerFormatModel(
        new ParquetFormatModel<ColumnarBatch, DeleteFilter<InternalRow>, StructType>(
            VECTORIZED_MODEL_NAME, VectorizedSparkParquetReaders::buildReader));

    FormatModelRegistry.registerFormatModel(
        new ORCFormatModel<StructType, InternalRow>(
            MODEL_NAME,
            SparkOrcReader::new,
            (schema, messageType, engineSchema) -> new SparkOrcWriter(schema, messageType),
            path -> UTF8String.fromString(path.toString())));

    FormatModelRegistry.registerFormatModel(
        new ORCFormatModel<StructType, ColumnarBatch>(
            VECTORIZED_MODEL_NAME, VectorizedSparkOrcReaders::buildReader));
  }

  private SparkFormatModels() {}
}
