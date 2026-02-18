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
import org.apache.iceberg.formats.FormatModelRegistry;
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

public class SparkFormatModels {
  public static void register() {
    FormatModelRegistry.register(
        AvroFormatModel.create(
            InternalRow.class,
            StructType.class,
            (icebergSchema, fileSchema, engineSchema) -> new SparkAvroWriter(engineSchema),
            (icebergSchema, fileSchema, engineSchema, idToConstant) ->
                SparkPlannedAvroReader.create(icebergSchema, idToConstant)));

    FormatModelRegistry.register(
        ParquetFormatModel.create(
            InternalRow.class,
            StructType.class,
            (icebergSchema, fileSchema, engineSchema) ->
                SparkParquetWriters.buildWriter(icebergSchema, engineSchema, fileSchema),
            (icebergSchema, fileSchema, engineSchema, idToConstant) ->
                SparkParquetReaders.buildReader(icebergSchema, fileSchema, idToConstant)));

    FormatModelRegistry.register(
        ParquetFormatModel.create(
            ColumnarBatch.class,
            StructType.class,
            (icebergSchema, fileSchema, engineSchema, idToConstant) ->
                VectorizedSparkParquetReaders.buildReader(
                    icebergSchema, fileSchema, idToConstant)));

    FormatModelRegistry.register(
        ParquetFormatModel.create(
            VectorizedSparkParquetReaders.CometColumnarBatch.class,
            StructType.class,
            (icebergSchema, fileSchema, engineSchema, idToConstant) ->
                VectorizedSparkParquetReaders.buildCometReader(
                    icebergSchema, fileSchema, idToConstant)));

    FormatModelRegistry.register(
        ORCFormatModel.create(
            InternalRow.class,
            StructType.class,
            (icebergSchema, fileSchema, engineSchema) ->
                new SparkOrcWriter(icebergSchema, fileSchema),
            (icebergSchema, fileSchema, engineSchema, idToConstant) ->
                new SparkOrcReader(icebergSchema, fileSchema, idToConstant)));

    FormatModelRegistry.register(
        ORCFormatModel.create(
            ColumnarBatch.class,
            StructType.class,
            (icebergSchema, fileSchema, engineSchema, idToConstant) ->
                VectorizedSparkOrcReaders.buildReader(icebergSchema, fileSchema, idToConstant)));
  }

  private SparkFormatModels() {}
}
