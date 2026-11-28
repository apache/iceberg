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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
class TestPositionDeletesRowReaderEncryption {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
  private static final Schema PROJECTION =
      new Schema(MetadataColumns.DELETE_FILE_PATH, MetadataColumns.DELETE_FILE_POS);

  @Parameter private int formatVersion;

  @TempDir private File tableDir;

  private Table table;

  @Parameters(name = "formatVersion = {0}")
  private static List<Integer> parameters() {
    return List.of(2, 3);
  }

  @BeforeEach
  void createTable() {
    this.table =
        TestTables.createEncrypted(
            tableDir,
            "encrypted",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            formatVersion,
            ImmutableMap.of(),
            EncryptionTestHelpers.createEncryptionManager());
  }

  @AfterEach
  void dropTable() {
    TestTables.clearTables();
  }

  @TestTemplate
  void readEncryptedPositionDeletes() throws Exception {
    DataFile dataFile =
        FileHelpers.writeDataFile(
            table, outputFile("data"), List.of(record(1L, "a"), record(2L, "b"), record(3L, "c")));
    table.newAppend().appendFile(dataFile).commit();

    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
                table,
                outputFile("deletes"),
                List.of(Pair.of(dataFile.location(), 0L), Pair.of(dataFile.location(), 2L)),
                formatVersion)
            .first();
    table.newRowDelta().addDeletes(deleteFile).commit();

    assertThat(dataFile.keyMetadata()).isNotNull();
    assertThat(deleteFile.keyMetadata()).isNotNull();
    assertThat(deleteFile.format())
        .isEqualTo(formatVersion >= 3 ? FileFormat.PUFFIN : FileFormat.PARQUET);

    assertThat(readPositionDeletes())
        .containsExactlyInAnyOrder(
            Pair.of(dataFile.location(), 0L), Pair.of(dataFile.location(), 2L));
  }

  private List<Pair<String, Long>> readPositionDeletes() throws IOException {
    List<Pair<String, Long>> deletes = Lists.newArrayList();
    try (PositionDeletesRowReader reader =
        new PositionDeletesRowReader(
            table,
            table.io(),
            new BaseScanTaskGroup<>(planPositionDeletes()),
            PROJECTION,
            false /* case sensitive */,
            false /* cache delete files on executors */)) {
      while (reader.next()) {
        InternalRow row = reader.get();
        deletes.add(Pair.of(row.getString(0), row.getLong(1)));
      }
    }

    return deletes;
  }

  private List<PositionDeletesScanTask> planPositionDeletes() throws IOException {
    Table positionDeletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);

    List<PositionDeletesScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<ScanTask> scanTasks =
        positionDeletesTable.newBatchScan().project(PROJECTION).planFiles()) {
      scanTasks.forEach(task -> tasks.add((PositionDeletesScanTask) task));
    }

    return tasks;
  }

  private Record record(long id, String data) {
    Record record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private OutputFile outputFile(String name) {
    return Files.localOutput(new File(tableDir, FileFormat.PARQUET.addExtension(name)));
  }
}
