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
package org.apache.iceberg.flink.maintenance.operator;

import static org.apache.iceberg.flink.SimpleDataUtil.createRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestEqualityConvertReader extends OperatorTestBase {

  @TempDir private Path tempDir;

  @Test
  void emitsAddStagingDataRowForDataFile() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    DataFile dataFile = getFirstDataFile(table);
    PartitionSpec spec = table.specs().get(dataFile.specId());
    List<Integer> fieldIds = Lists.newArrayList(1);

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      ReadCommand cmd =
          ReadCommand.stagingDataFile(new FlinkAddedRowsScanTask(dataFile, spec), 0L, 0L, 0L);
      harness.processElement(cmd, 0);

      List<IndexCommand> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      // stagingDataFile marks a staging snapshot's new data, so the reader emits
      // ADD_STAGING_DATA_ROW.
      assertThat(output.get(0).type()).isEqualTo(IndexCommand.Type.ADD_STAGING_DATA_ROW);
      assertThat(output.get(0).rowPosition().dataFilePath()).isEqualTo(dataFile.location());
      assertThat(output.get(0).rowPosition().position()).isZero();
    }
  }

  @Test
  void emitsAddDataRowWhenNotStaging() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    DataFile dataFile = getFirstDataFile(table);
    PartitionSpec spec = table.specs().get(dataFile.specId());
    List<Integer> fieldIds = Lists.newArrayList(1);

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      // Same task type as the staging test: the command's staging flag, not the task type, decides
      // that main reindex data is indexed immediately as ADD_DATA_ROW.
      ReadCommand cmd =
          ReadCommand.dataFile(new FlinkAddedRowsScanTask(dataFile, spec), 0L, 0L, 0L);
      harness.processElement(cmd, 0);

      List<IndexCommand> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).type()).isEqualTo(IndexCommand.Type.ADD_DATA_ROW);
    }
  }

  @Test
  void emitsResolveDeleteForEqDeleteFile() throws Exception {
    Table table = createTableWithDelete(3);
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    PartitionSpec spec = table.specs().get(eqDelete.specId());
    List<Integer> fieldIds = eqDelete.equalityFieldIds();

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      ReadCommand cmd = ReadCommand.eqDeleteFile(eqDelete, spec, 0L, 0L, 0L);
      harness.processElement(cmd, 0);

      List<IndexCommand> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).type()).isEqualTo(IndexCommand.Type.RESOLVE_DELETE);
      assertThat(output.get(0).rowPosition()).isNull();
    }
  }

  @Test
  void tracksPositionsAcrossRecords() throws Exception {
    Table table = createTableWithDelete(3);

    DataFile dataFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(createRecord(1, "a"), createRecord(2, "b")));
    table.newAppend().appendFile(dataFile).commit();
    table.refresh();

    PartitionSpec spec = table.specs().get(dataFile.specId());
    List<Integer> fieldIds = Lists.newArrayList(1);

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      ReadCommand cmd =
          ReadCommand.stagingDataFile(new FlinkAddedRowsScanTask(dataFile, spec), 0L, 0L, 0L);
      harness.processElement(cmd, 0);

      List<IndexCommand> output = harness.extractOutputValues();
      assertThat(output).hasSize(2);
      assertThat(output.get(0).rowPosition().position()).isZero();
      assertThat(output.get(1).rowPosition().position()).isEqualTo(1);
    }
  }

  @Test
  void skipsDeletedPositionsWhenCreatingIndexFromMain() throws Exception {
    Table table = createTableWithDelete(3);

    DataFile dataFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(
                Lists.newArrayList(
                    createRecord(1, "a"), createRecord(2, "b"), createRecord(3, "c")));
    table.newAppend().appendFile(dataFile).commit();
    table.refresh();

    // DV marks position 1 (id=2) as deleted on main. When the reader creates the worker's index
    // from this data file, it must skip position 1 and emit rows only for positions 0 and 2.
    DeleteFile dv = writeDV(table, dataFile.location(), 1L);

    PartitionSpec spec = table.specs().get(dataFile.specId());
    List<Integer> fieldIds = Lists.newArrayList(1);

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      ReadCommand cmd =
          ReadCommand.stagingDataFile(
              new FlinkAddedRowsScanTask(dataFile, spec, Lists.newArrayList(dv)), 0L, 0L, 0L);
      harness.processElement(cmd, 0);

      List<IndexCommand> output = harness.extractOutputValues();
      assertThat(output).hasSize(2);
      assertThat(output.get(0).rowPosition().position()).isZero();
      assertThat(output.get(1).rowPosition().position()).isEqualTo(2);
    }
  }

  @Test
  void sameKeyFromDataAndDeleteProducesEqualSerializedValues() throws Exception {
    Table table = createTableWithDelete(3);

    DataFile dataFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(createRecord(1, "a")));
    table.newAppend().appendFile(dataFile).commit();
    table.refresh();

    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    PartitionSpec dataSpec = table.specs().get(dataFile.specId());
    PartitionSpec deleteSpec = table.specs().get(eqDelete.specId());
    List<Integer> fieldIds = eqDelete.equalityFieldIds();

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      harness.processElement(
          ReadCommand.stagingDataFile(new FlinkAddedRowsScanTask(dataFile, dataSpec), 0L, 0L, 0L),
          0);
      harness.processElement(ReadCommand.eqDeleteFile(eqDelete, deleteSpec, 0L, 0L, 0L), 1);

      List<IndexCommand> output = harness.extractOutputValues();
      assertThat(output).hasSize(2);
      assertThat(output.get(0).type()).isEqualTo(IndexCommand.Type.ADD_STAGING_DATA_ROW);
      assertThat(output.get(1).type()).isEqualTo(IndexCommand.Type.RESOLVE_DELETE);
      assertThat(output.get(0).key()).isEqualTo(output.get(1).key());
    }
  }

  @Test
  void propagatesMainSnapshotId() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    DataFile dataFile = getFirstDataFile(table);
    PartitionSpec spec = table.specs().get(dataFile.specId());
    List<Integer> fieldIds = Lists.newArrayList(1);
    long mainSnapshotId = 42L;
    long mainSequenceNumber = 7L;
    long dataSequenceNumber = 9L;

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      ReadCommand cmd =
          ReadCommand.stagingDataFile(
              new FlinkAddedRowsScanTask(dataFile, spec),
              mainSnapshotId,
              mainSequenceNumber,
              dataSequenceNumber);
      harness.processElement(cmd, 0);

      List<IndexCommand> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).mainSnapshotId()).isEqualTo(mainSnapshotId);
      assertThat(output.get(0).mainSequenceNumber()).isEqualTo(mainSequenceNumber);
      assertThat(output.get(0).rowPosition().dataSequenceNumber()).isEqualTo(dataSequenceNumber);
    }
  }

  @Test
  void routesExceptionToErrorStream() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    DataFile dataFile = getFirstDataFile(table);
    DataFile missing =
        DataFiles.builder(table.spec())
            .copy(dataFile)
            .withPath(dataFile.location() + ".missing")
            .build();
    PartitionSpec spec = table.specs().get(dataFile.specId());
    List<Integer> fieldIds = Lists.newArrayList(1);

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      // Non-existent file path causes the reader to throw.
      ReadCommand cmd =
          ReadCommand.stagingDataFile(new FlinkAddedRowsScanTask(missing, spec), 0L, 0L, 0L);
      harness.processElement(cmd, 0);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(harness.getSideOutput(EqualityConvertReader.READER_ABORT_STREAM)).hasSize(1);
    }
  }

  @Test
  void failsWhenEqualityFieldMissingFromCurrentSchema() throws Exception {
    // Field ids from SimpleDataUtil.SCHEMA: 1=id, 2=data.
    Table table = createTableWithDelete(3);

    DataFile dataFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(createRecord(1, "a")));
    table.newAppend().appendFile(dataFile).commit();

    // Drop `data`. Static equality fields resolve against the current schema at build time, so an
    // equality field id missing from the current schema is a misconfiguration.
    table.updateSchema().deleteColumn("data").commit();
    table.refresh();

    List<Integer> fieldIds = Lists.newArrayList(1, 2);

    assertThatThrownBy(
            () -> {
              try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
                  createHarness(fieldIds)) {
                harness.open();
              }
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not present in table schema");
  }

  @Test
  void throwsOnEqualityDeleteAttachedToMainDataFile() throws Exception {
    Table table = createTableWithDelete(3);

    DataFile dataFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(createRecord(1, "a")));
    table.newAppend().appendFile(dataFile).commit();
    table.refresh();

    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    PartitionSpec spec = table.specs().get(dataFile.specId());
    List<Integer> fieldIds = Lists.newArrayList(1);

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      // An equality delete attached to a main data file is unexpected on a separate target branch
      // (stagingOnTargetBranch=false): the reader must route the error rather than silently ignore.
      ReadCommand cmd =
          ReadCommand.stagingDataFile(
              new FlinkAddedRowsScanTask(dataFile, spec, Lists.newArrayList(eqDelete)), 0L, 0L, 0L);
      harness.processElement(cmd, 0);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(harness.getSideOutput(EqualityConvertReader.READER_ABORT_STREAM)).hasSize(1);
    }
  }

  @Test
  void throwsOnV2PositionalDeleteAttachedToMainDataFile() throws Exception {
    Table table = createTableWithDelete(3);

    DataFile dataFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(createRecord(1, "a")));
    table.newAppend().appendFile(dataFile).commit();
    table.refresh();

    DeleteFile posDelete = writePosDeleteFile(table, dataFile.location(), 0L);
    PartitionSpec spec = table.specs().get(dataFile.specId());
    List<Integer> fieldIds = Lists.newArrayList(1);

    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        createHarness(fieldIds)) {
      harness.open();

      // A V2 positional delete attached to a main data file means the target is not a DV-only V3
      // target, the reader must route to the error output.
      ReadCommand cmd =
          ReadCommand.dataFile(
              new FlinkAddedRowsScanTask(dataFile, spec, Lists.newArrayList(posDelete)),
              0L,
              0L,
              0L);
      harness.processElement(cmd, 0);

      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(harness.getSideOutput(EqualityConvertReader.READER_ABORT_STREAM)).hasSize(1);
    }
  }

  private OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> createHarness(
      List<Integer> fieldIds) throws Exception {
    return ProcessFunctionTestHarnesses.forProcessFunction(
        new EqualityConvertReader(tableLoader(), Sets.newHashSet(fieldIds), false));
  }

  private static DataFile getFirstDataFile(Table table) {
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        for (DataFile file : reader) {
          return file.copy();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    throw new IllegalStateException("No data files found");
  }

  private DeleteFile writeEqualityDelete(Table table, Integer id, String data) throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        new PartitionData(PartitionSpec.unpartitioned().partitionType()),
        Lists.newArrayList(createRecord(id, data)),
        table.schema());
  }

  private static DeleteFile writeDV(Table table, String dataFilePath, long... positions)
      throws IOException {
    List<PositionDelete<?>> deletes = Lists.newArrayList();
    GenericRecord nested = GenericRecord.create(table.schema());
    for (long pos : positions) {
      PositionDelete<GenericRecord> delete = PositionDelete.create();
      delete.set(dataFilePath, pos, nested);
      deletes.add(delete);
    }

    return FileHelpers.writePosDeleteFile(table, null, null, deletes, 3);
  }
}
