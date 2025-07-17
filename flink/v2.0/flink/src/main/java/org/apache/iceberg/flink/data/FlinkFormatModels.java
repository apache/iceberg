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
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroFormatModel;
import org.apache.iceberg.data.FormatModelRegistry;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.orc.ORCFormatModel;
import org.apache.iceberg.parquet.ParquetFormatModel;

public class FlinkFormatModels {
  public static final String MODEL_NAME = "flink";

  public static void register() {
    FormatModelRegistry.registerFormatModel(
        new ParquetFormatModel<>(
            MODEL_NAME,
            FlinkParquetReaders::buildReader,
            (schema, messageType) ->
                FlinkParquetWriters.buildWriter(FlinkSchemaUtil.convert(schema), messageType),
            path -> StringData.fromString(path.toString())));

    FormatModelRegistry.registerFormatModel(
        new AvroFormatModel<>(
            MODEL_NAME,
            FlinkPlannedAvroReader::create,
            (schema, avroSchema) -> new FlinkAvroWriter(FlinkSchemaUtil.convert(schema)),
            (schema, avroSchema) ->
                new FlinkAvroWriter(
                    FlinkSchemaUtil.convert(
                        new Schema(
                            schema
                                .findField(DELETE_FILE_ROW_FIELD_NAME)
                                .type()
                                .asStructType()
                                .fields())))));

    FormatModelRegistry.registerFormatModel(
        new ORCFormatModel<>(
            MODEL_NAME,
            FlinkOrcReader::new,
            (schema, messageType) ->
                FlinkOrcWriter.buildWriter(FlinkSchemaUtil.convert(schema), schema),
            path -> StringData.fromString(path.toString())));
  }

  private FlinkFormatModels() {}
}
