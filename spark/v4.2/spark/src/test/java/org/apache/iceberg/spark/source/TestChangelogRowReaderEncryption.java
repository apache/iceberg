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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.EncryptedTableReadTestBase;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.TestTemplate;

class TestChangelogRowReaderEncryption extends EncryptedTableReadTestBase {

  @Override
  protected boolean supportsDeletes() {
    return false;
  }

  @Override
  protected StructLikeSet read(Table encryptedTable) throws IOException {
    return readChangelog(
        encryptedTable, encryptedTable.newIncrementalChangelogScan(), encryptedTable.schema());
  }

  @TestTemplate
  void readDeletedEncryptedDataFile() throws IOException {
    Record first = record(1L, "a");
    Record second = record(2L, "b");
    Record third = record(3L, "c");
    DataFile firstFile =
        FileHelpers.writeDataFile(table, outputFile("first"), List.of(first, second));
    DataFile secondFile = FileHelpers.writeDataFile(table, outputFile("second"), List.of(third));
    table.newAppend().appendFile(firstFile).appendFile(secondFile).commit();
    long appendSnapshotId = table.currentSnapshot().snapshotId();

    table.newDelete().deleteFile(firstFile).commit();

    assertThat(firstFile.keyMetadata()).isNotNull();

    Schema projection = TypeUtil.join(table.schema(), new Schema(MetadataColumns.CHANGE_TYPE));
    StructLikeSet expected = StructLikeSet.create(projection.asStruct());
    expected.add(deleted(projection, first));
    expected.add(deleted(projection, second));

    assertThat(
            readChangelog(
                table,
                table.newIncrementalChangelogScan().fromSnapshotExclusive(appendSnapshotId),
                projection))
        .isEqualTo(expected);
  }

  private static Record deleted(Schema projection, Record row) {
    Record record = GenericRecord.create(projection);
    for (Types.NestedField field : row.struct().fields()) {
      record.setField(field.name(), row.getField(field.name()));
    }

    record.setField(MetadataColumns.CHANGE_TYPE.name(), ChangelogOperation.DELETE.name());
    return record;
  }

  private static StructLikeSet readChangelog(
      Table encryptedTable, IncrementalChangelogScan scan, Schema projection) throws IOException {
    StructType sparkType = SparkSchemaUtil.convert(projection);
    StructLikeSet actual = StructLikeSet.create(projection.asStruct());
    try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> taskGroups = scan.planTasks()) {
      for (ScanTaskGroup<ChangelogScanTask> taskGroup : taskGroups) {
        try (ChangelogRowReader reader =
            new ChangelogRowReader(
                encryptedTable,
                encryptedTable.io(),
                taskGroup,
                projection,
                false /* case sensitive */,
                false /* cache delete files on executors */)) {
          while (reader.next()) {
            actual.add(
                new InternalRowWrapper(sparkType, projection.asStruct()).wrap(reader.get().copy()));
          }
        }
      }
    }

    return actual;
  }
}
