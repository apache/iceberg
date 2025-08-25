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

import java.util.function.Function;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroFormatModel;
import org.apache.iceberg.data.FormatModelRegistry;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.orc.ORCFormatModel;
import org.apache.iceberg.parquet.ParquetFormatModel;

public class FlinkFormatModels {
  private static final DeleteTransformer DELETE_TRANSFORMER = new DeleteTransformer();

  public static final String MODEL_NAME = "flink";

  public static void register() {
    FormatModelRegistry.register(
        new ParquetFormatModel<RowData, RowType, Object>(
            MODEL_NAME,
            FlinkParquetReaders::buildReader,
            (unused, messageType, rowType) -> FlinkParquetWriters.buildWriter(rowType, messageType),
            DELETE_TRANSFORMER));

    FormatModelRegistry.register(
        new AvroFormatModel<RowData, RowType>(
            MODEL_NAME,
            FlinkPlannedAvroReader::create,
            (unused, rowType) -> new FlinkAvroWriter(rowType),
            DELETE_TRANSFORMER));

    FormatModelRegistry.register(
        new ORCFormatModel<RowData, RowType>(
            MODEL_NAME,
            FlinkOrcReader::new,
            (schema, unused, rowType) -> FlinkOrcWriter.buildWriter(rowType, schema),
            DELETE_TRANSFORMER));
  }

  private FlinkFormatModels() {}

  private static class DeleteTransformer
      implements Function<Schema, Function<PositionDelete<RowData>, RowData>> {
    @Override
    public Function<PositionDelete<RowData>, RowData> apply(Schema schema) {
      GenericRowData deleteRecord = new GenericRowData(RowKind.INSERT, 3);
      return delete -> {
        deleteRecord.setField(0, StringData.fromString(delete.path().toString()));
        deleteRecord.setField(1, delete.pos());
        deleteRecord.setField(2, delete.row());
        return deleteRecord;
      };
    }
  }
}
