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
import org.apache.iceberg.io.datafile.DataFileServiceRegistry;
import org.apache.iceberg.io.datafile.DeleteFilter;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.ParquetReaderType;
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

public class DataFileServices {
  public static void register() {
    // Base readers
    DataFileServiceRegistry.registerReader(
        FileFormat.PARQUET,
        InternalRow.class.getName(),
        inputFile -> Parquet.read(inputFile).readerFunction(SparkParquetReaders::buildReader));

    DataFileServiceRegistry.registerReader(
        FileFormat.AVRO,
        InternalRow.class.getName(),
        inputFile -> Avro.read(inputFile).readerFunction(SparkPlannedAvroReader::create));

    DataFileServiceRegistry.registerReader(
        FileFormat.ORC,
        InternalRow.class.getName(),
        inputFile -> ORC.read(inputFile).readerFunction(SparkOrcReader::new));

    // Vectorized readers
    DataFileServiceRegistry.registerReader(
        FileFormat.PARQUET,
        ColumnarBatch.class.getName(),
        ParquetReaderType.ICEBERG.name(),
        inputFile ->
            Parquet.read(inputFile)
                .batchReaderFunction(
                    (schema, messageType, idToConstant, deleteFilter) ->
                        VectorizedSparkParquetReaders.buildReader(
                            schema,
                            messageType,
                            idToConstant,
                            (DeleteFilter<InternalRow>) deleteFilter)));

    DataFileServiceRegistry.registerReader(
        FileFormat.PARQUET,
        ColumnarBatch.class.getName(),
        ParquetReaderType.COMET.name(),
        inputFile ->
            Parquet.read(inputFile)
                .batchReaderFunction(
                    (schema, messageType, idToConstant, deleteFilter) ->
                        VectorizedSparkParquetReaders.buildCometReader(
                            schema,
                            messageType,
                            idToConstant,
                            (DeleteFilter<InternalRow>) deleteFilter)));

    DataFileServiceRegistry.registerReader(
        FileFormat.ORC,
        ColumnarBatch.class.getName(),
        inputFile ->
            ORC.read(inputFile).batchReaderFunction(VectorizedSparkOrcReaders::buildReader));

    DataFileServiceRegistry.registerAppender(
        FileFormat.AVRO,
        InternalRow.class.getName(),
        outputFile ->
            Avro.write(outputFile)
                .writerFunction(
                    (schema, engineSchema) -> new SparkAvroWriter((StructType) engineSchema))
                .deleteRowWriterFunction(
                    (schema, engineSchema) ->
                        new SparkAvroWriter(
                            (StructType)
                                ((StructType) engineSchema)
                                    .apply(DELETE_FILE_ROW_FIELD_NAME)
                                    .dataType())));

    DataFileServiceRegistry.registerAppender(
        FileFormat.PARQUET,
        InternalRow.class.getName(),
        outputFile ->
            Parquet.write(outputFile)
                .writerFunction(
                    (engineSchema, messageType) ->
                        SparkParquetWriters.buildWriter((StructType) engineSchema, messageType))
                .pathTransformFunc(path -> UTF8String.fromString(path.toString())));

    DataFileServiceRegistry.registerAppender(
        FileFormat.ORC,
        InternalRow.class.getName(),
        outputFile ->
            ORC.write(outputFile)
                .writerFunction(
                    (schema, messageType, engineSchema) -> new SparkOrcWriter(schema, messageType))
                .pathTransformFunc(path -> UTF8String.fromString(path.toString())));
  }

  private DataFileServices() {}
}
