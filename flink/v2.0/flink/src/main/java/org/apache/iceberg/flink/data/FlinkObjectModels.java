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
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.avro.AvroFileAccessFactory;
import org.apache.iceberg.data.FileAccessFactoryRegistry;
import org.apache.iceberg.orc.ORCFileAccessFactory;
import org.apache.iceberg.parquet.ParquetFileAccessFactory;

public class FlinkObjectModels {
  public static final String FLINK_OBJECT_MODEL = "flink";

  public static void register() {
    FileAccessFactoryRegistry.registerFileAccessFactory(
        new ParquetFileAccessFactory<RowData, Object, RowType>(
            FLINK_OBJECT_MODEL,
            FlinkParquetReaders::buildReader,
            (engineType, unused, messageType) ->
                FlinkParquetWriters.buildWriter(engineType, messageType),
            path -> StringData.fromString(path.toString())));

    FileAccessFactoryRegistry.registerFileAccessFactory(
        new AvroFileAccessFactory<RowType, RowData>(
            FLINK_OBJECT_MODEL,
            FlinkPlannedAvroReader::create,
            (unused, rowType) -> new FlinkAvroWriter(rowType),
            (unused, rowType) ->
                new FlinkAvroWriter(
                    (RowType)
                        rowType.getTypeAt(rowType.getFieldIndex(DELETE_FILE_ROW_FIELD_NAME)))));

    FileAccessFactoryRegistry.registerFileAccessFactory(
        new ORCFileAccessFactory<RowType, RowData>(
            FLINK_OBJECT_MODEL,
            FlinkOrcReader::new,
            (schema, messageType, nativeSchema) -> FlinkOrcWriter.buildWriter(nativeSchema, schema),
            path -> StringData.fromString(path.toString())));
  }

  private FlinkObjectModels() {}
}
