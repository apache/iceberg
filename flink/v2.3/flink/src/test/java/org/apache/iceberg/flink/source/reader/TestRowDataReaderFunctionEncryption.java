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
package org.apache.iceberg.flink.source.reader;

import java.io.IOException;
import java.util.Collections;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.EncryptedTableReadTestBase;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.StructLikeSet;

class TestRowDataReaderFunctionEncryption extends EncryptedTableReadTestBase {

  @Override
  protected StructLikeSet read(Table encryptedTable) throws IOException {
    Schema schema = encryptedTable.schema();
    RowDataReaderFunction readerFunction =
        new RowDataReaderFunction(
            new Configuration(),
            schema,
            schema,
            null /* name mapping */,
            true /* case sensitive */,
            encryptedTable.io(),
            encryptedTable.encryption(),
            Collections.emptyList());
    RowDataWrapper wrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());

    StructLikeSet actual = StructLikeSet.create(schema.asStruct());
    try (CloseableIterable<CombinedScanTask> tasks = encryptedTable.newScan().planTasks()) {
      for (CombinedScanTask task : tasks) {
        try (CloseableIterator<RecordsWithSplitIds<RecordAndPosition<RowData>>> batches =
            readerFunction.apply(IcebergSourceSplit.fromCombinedScanTask(task))) {
          while (batches.hasNext()) {
            RecordsWithSplitIds<RecordAndPosition<RowData>> batch = batches.next();
            while (batch.nextSplit() != null) {
              RecordAndPosition<RowData> row;
              while ((row = batch.nextRecordFromSplit()) != null) {
                actual.add(toRecord(schema, wrapper.wrap(row.record())));
              }
            }

            batch.recycle();
          }
        }
      }
    }

    return actual;
  }

  private static Record toRecord(Schema schema, RowDataWrapper row) {
    Record record = GenericRecord.create(schema);
    for (int pos = 0; pos < row.size(); pos += 1) {
      record.set(pos, row.get(pos, Object.class));
    }

    return record;
  }
}
