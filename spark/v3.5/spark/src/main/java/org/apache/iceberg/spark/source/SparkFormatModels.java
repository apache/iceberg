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

import org.apache.iceberg.avro.AvroFormatModel;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.FormatModelRegistry;
import org.apache.iceberg.orc.ORCFormatModel;
import org.apache.iceberg.parquet.ParquetFormatModel;
import org.apache.iceberg.spark.InternalRowTransformerUtil;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkAvroWriter;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkOrcWriter;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.spark.data.SparkPlannedAvroReader;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class SparkFormatModels {
  public static final String MODEL_NAME = "spark";
  public static final String VECTORIZED_MODEL_NAME = "spark-vectorized";

  public static void register() {
    FormatModelRegistry.register(
        new AvroFormatModel<>(
            MODEL_NAME,
            SparkPlannedAvroReader::create,
            (schema, avroSchema) -> new SparkAvroWriter(SparkSchemaUtil.convert(schema)),
            InternalRowTransformerUtil::deleteTransformer));

    FormatModelRegistry.register(
        new ParquetFormatModel<InternalRow, DeleteFilter<InternalRow>>(
            MODEL_NAME,
            SparkParquetReaders::buildReader,
            (icebergSchema, messageType) ->
                SparkParquetWriters.buildWriter(
                    SparkSchemaUtil.convert(icebergSchema), messageType),
            InternalRowTransformerUtil::deleteTransformer));

    FormatModelRegistry.register(
        new ParquetFormatModel<ColumnarBatch, DeleteFilter<InternalRow>>(
            VECTORIZED_MODEL_NAME, VectorizedSparkParquetReaders::buildReader));

    FormatModelRegistry.register(
        new ORCFormatModel<>(
            MODEL_NAME,
            SparkOrcReader::new,
            SparkOrcWriter::new,
            InternalRowTransformerUtil::deleteTransformer));

    FormatModelRegistry.register(
        new ORCFormatModel<>(VECTORIZED_MODEL_NAME, VectorizedSparkOrcReaders::buildReader));
  }

  private SparkFormatModels() {}
}
