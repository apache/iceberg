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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.datafile.DataFileToObjectModelRegistry;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

public class FlinkObjectModels {
  public static void register() {
    DataFileToObjectModelRegistry.registerReader(
        FileFormat.PARQUET,
        RowData.class.getName(),
        inputFile -> Parquet.read(inputFile).readerFunction(FlinkParquetReaders::buildReader));

    DataFileToObjectModelRegistry.registerReader(
        FileFormat.AVRO,
        RowData.class.getName(),
        inputFile -> Avro.read(inputFile).readerFunction(FlinkPlannedAvroReader::create));

    DataFileToObjectModelRegistry.registerReader(
        FileFormat.ORC,
        RowData.class.getName(),
        inputFile -> ORC.read(inputFile).readerFunction(FlinkOrcReader::new));

    DataFileToObjectModelRegistry.registerAppender(
        FileFormat.AVRO,
        RowData.class.getName(),
        outputFile ->
            Avro.write(outputFile)
                .writerFunction((unused, rowType) -> new FlinkAvroWriter((RowType) rowType))
                .deleteRowWriterFunction(
                    (unused, rowType) ->
                        new FlinkAvroWriter(
                            (RowType)
                                ((RowType) rowType)
                                    .getTypeAt(
                                        ((RowType) rowType)
                                            .getFieldIndex(DELETE_FILE_ROW_FIELD_NAME)))));

    DataFileToObjectModelRegistry.registerAppender(
        FileFormat.PARQUET,
        RowData.class.getName(),
        outputFile ->
            Parquet.write(outputFile)
                .writerFunction(
                    (engineType, icebergSchema, messageType) ->
                        FlinkParquetWriters.buildWriter((LogicalType) engineType, messageType))
                .pathTransformFunc(path -> StringData.fromString(path.toString())));

    DataFileToObjectModelRegistry.registerAppender(
        FileFormat.ORC,
        RowData.class.getName(),
        outputFile ->
            ORC.write(outputFile)
                .writerFunction(
                    (schema, messageType, nativeSchema) ->
                        FlinkOrcWriter.buildWriter((RowType) nativeSchema, schema))
                .pathTransformFunc(path -> StringData.fromString(path.toString())));
  }

  private FlinkObjectModels() {}
}
