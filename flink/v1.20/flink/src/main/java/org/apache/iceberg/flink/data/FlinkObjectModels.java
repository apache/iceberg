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
package org.apache.iceberg.flink.data;

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_ROW_FIELD_NAME;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.ObjectModelRegistry;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

public class FlinkObjectModels {
  public static final String FLINK_OBJECT_MODEL = "flink";

  public static void register() {
    ObjectModelRegistry.registerReader(
        FileFormat.PARQUET,
        FLINK_OBJECT_MODEL,
        inputFile -> Parquet.read(inputFile).readerFunction(FlinkParquetReaders::buildReader));

    ObjectModelRegistry.registerReader(
        FileFormat.AVRO,
        FLINK_OBJECT_MODEL,
        inputFile -> Avro.read(inputFile).readerFunction(FlinkPlannedAvroReader::create));

    ObjectModelRegistry.registerReader(
        FileFormat.ORC,
        FLINK_OBJECT_MODEL,
        inputFile -> ORC.read(inputFile).readerFunction(FlinkOrcReader::new));

    ObjectModelRegistry.registerAppender(
        FileFormat.AVRO,
        FLINK_OBJECT_MODEL,
        outputFile ->
            Avro.<RowType>appender(outputFile)
                .writerFunction((unused, rowType) -> new FlinkAvroWriter(rowType))
                .deleteRowWriterFunction(
                    (unused, rowType) ->
                        new FlinkAvroWriter(
                            (RowType)
                                rowType.getTypeAt(
                                    rowType.getFieldIndex(DELETE_FILE_ROW_FIELD_NAME)))));

    ObjectModelRegistry.registerAppender(
        FileFormat.PARQUET,
        FLINK_OBJECT_MODEL,
        outputFile ->
            Parquet.<RowType>appender(outputFile)
                .writerFunction(
                    (engineType, icebergSchema, messageType) ->
                        FlinkParquetWriters.buildWriter(engineType, messageType))
                .pathTransformFunc(path -> StringData.fromString(path.toString())));

    ObjectModelRegistry.registerAppender(
        FileFormat.ORC,
        FLINK_OBJECT_MODEL,
        outputFile ->
            ORC.<RowType>appender(outputFile)
                .writerFunction(
                    (schema, messageType, nativeSchema) ->
                        FlinkOrcWriter.buildWriter(nativeSchema, schema))
                .pathTransformFunc(path -> StringData.fromString(path.toString())));
  }

  private FlinkObjectModels() {}
}
