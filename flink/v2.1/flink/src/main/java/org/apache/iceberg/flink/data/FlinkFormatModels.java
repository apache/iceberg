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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.avro.AvroFormatModel;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.orc.ORCFormatModel;
import org.apache.iceberg.parquet.ParquetFormatModel;

public class FlinkFormatModels {
  public static void register() {
    FormatModelRegistry.register(
        new ParquetFormatModel<>(
            RowData.class,
            RowType.class,
            FlinkParquetReaders::buildReader,
            (unused, messageType, rowType) ->
                FlinkParquetWriters.buildWriter(rowType, messageType)));

    FormatModelRegistry.register(
        new AvroFormatModel<>(
            RowData.class,
            RowType.class,
            FlinkPlannedAvroReader::create,
            (unused, rowType) -> new FlinkAvroWriter(rowType)));

    FormatModelRegistry.register(
        new ORCFormatModel<>(
            RowData.class,
            RowType.class,
            FlinkOrcReader::new,
            (schema, unused, rowType) -> FlinkOrcWriter.buildWriter(rowType, schema)));
  }

  private FlinkFormatModels() {}
}
