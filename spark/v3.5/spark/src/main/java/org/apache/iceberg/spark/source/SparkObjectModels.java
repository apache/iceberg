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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.ObjectModelRegistry;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
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
import org.apache.spark.unsafe.types.UTF8String;

public class SparkObjectModels {
  public static final String SPARK_OBJECT_MODEL = "spark";
  public static final String SPARK_VECTORIZED_OBJECT_MODEL = "spark-vectorized";

  public static void register() {
    // Base readers
    ObjectModelRegistry.registerReader(
        FileFormat.PARQUET,
        SPARK_OBJECT_MODEL,
        inputFile -> Parquet.read(inputFile).readerFunction(SparkParquetReaders::buildReader));

    ObjectModelRegistry.registerReader(
        FileFormat.AVRO,
        SPARK_OBJECT_MODEL,
        inputFile -> Avro.read(inputFile).readerFunction(SparkPlannedAvroReader::create));

    ObjectModelRegistry.registerReader(
        FileFormat.ORC,
        SPARK_OBJECT_MODEL,
        inputFile -> ORC.read(inputFile).readerFunction(SparkOrcReader::new));

    // Vectorized readers
    ObjectModelRegistry.registerReader(
        FileFormat.PARQUET,
        SPARK_VECTORIZED_OBJECT_MODEL,
        inputFile ->
            Parquet.<DeleteFilter<InternalRow>>readWithFilter(inputFile)
                .batchReaderFunction(VectorizedSparkParquetReaders::buildReader));

    ObjectModelRegistry.registerReader(
        FileFormat.ORC,
        SPARK_VECTORIZED_OBJECT_MODEL,
        inputFile ->
            ORC.read(inputFile).batchReaderFunction(VectorizedSparkOrcReaders::buildReader));

    // Appenders
    ObjectModelRegistry.registerAppender(
        FileFormat.AVRO,
        SPARK_OBJECT_MODEL,
        outputFile ->
            Avro.<StructType>appender(outputFile)
                .writerFunction((schema, engineSchema) -> new SparkAvroWriter(engineSchema))
                .deleteRowWriterFunction(
                    (schema, engineSchema) ->
                        new SparkAvroWriter(
                            (StructType)
                                engineSchema.apply(DELETE_FILE_ROW_FIELD_NAME).dataType())));

    ObjectModelRegistry.registerAppender(
        FileFormat.PARQUET,
        SPARK_OBJECT_MODEL,
        outputFile ->
            Parquet.<StructType>appender(outputFile)
                .writerFunction(
                    (engineSchema, icebergSchema, messageType) ->
                        SparkParquetWriters.buildWriter(engineSchema, messageType))
                .pathTransformFunc(path -> UTF8String.fromString(path.toString())));

    ObjectModelRegistry.registerAppender(
        FileFormat.ORC,
        SPARK_OBJECT_MODEL,
        outputFile ->
            ORC.appender(outputFile)
                .writerFunction(
                    (schema, messageType, engineSchema) -> new SparkOrcWriter(schema, messageType))
                .pathTransformFunc(path -> UTF8String.fromString(path.toString())));
  }

  private SparkObjectModels() {}
}
