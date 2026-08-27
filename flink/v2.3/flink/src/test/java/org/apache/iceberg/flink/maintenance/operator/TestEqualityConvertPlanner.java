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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestEqualityConvertPlanner extends OperatorTestBase {

  private static final String STAGING_BRANCH = "__flink_staging_test";

  @TempDir private Path tempDir;

  @Test
  void bootstrapsIndexFromBuilderEqualityFieldIds() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    // Staging branch exists but contains no eq-delete files yet. The planner still populates the
    // worker index from main using the builder-configured equality field set so the index is ready
    // when the first delete arrives.
    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH, Lists.newArrayList(1, 2))) {
      harness.open();
      sendTrigger(harness);

      List<ReadCommand> commands = harness.extractOutputValues();
      assertThat(countDataFileTasks(commands)).isEqualTo(2);
      assertThat(countEqDeleteTasks(commands)).isZero();

      assertThat(harness.getSideOutput(EqualityConvertPlanner.METADATA_STREAM)).hasSize(1);
    }
  }

  @Test
  void failsWhenStagingEqDeleteFieldIdsMismatchBuilder() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Builder configured for [1, 2], but writer produces an eq delete with [1] only.
    DeleteFile mismatched = writeIdOnlyEqualityDelete(table, 1);
    table.newRowDelta().addDeletes(mismatched).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH, Lists.newArrayList(1, 2))) {
      harness.open();
      sendTrigger(harness);

      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
    }
  }

  @Test
  void failsWhenStagingEqDeleteSpecPartitionsByNonEqualityColumn() throws Exception {
    Table table = createPartitionedTableWithDelete(3);
    insertPartitioned(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete = writeIdOnlyPartitionedEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH, Lists.newArrayList(1))) {
      harness.open();
      sendTrigger(harness);

      List<StreamRecord<Exception>> errOutput =
          Lists.newArrayList(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM));

      assertThat(errOutput).hasSize(1);
      assertThat(errOutput.get(0).getValue())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Partition columns must be a subset of the equality fields.");
    }
  }

  @Test
  void doesNotDuplicateNewDataFilesWhenStagingEqualsTarget() throws Exception {
    // When stagingBranch == targetBranch, the writer commits new data files directly to main.
    // Bootstrap scans the main snapshot (which already includes those files) and indexes them.
    // The planner must NOT also emit the staging-data phase for the same files — that would
    // duplicate ADD_DATA_ROW commands for the same (PK, file, position) in the worker's index.
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    // Writer commits (new-data id=3) + (eq-delete id=1) in one RowDelta directly to main.
    DataFile newDataFile = writeDataFile(table, createRecord(3, "c"));
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addRows(newDataFile).addDeletes(eqDelete).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(SnapshotRef.MAIN_BRANCH)) {
      harness.open();
      sendTrigger(harness);

      List<ReadCommand> commands = harness.extractOutputValues();

      // Bootstrap from main emits one data-file command per file (id=1, id=2, id=3) = 3.
      // Without the same-branch guard, emitSnapshotDataPhase would also emit newDataFile → 4.
      assertThat(countDataFileTasks(commands)).isEqualTo(3);
      assertThat(countEqDeleteTasks(commands)).isEqualTo(1);

      long newDataFileCount =
          commands.stream()
              .filter(c -> c.task() instanceof FileScanTask)
              .filter(c -> c.task().file().location().equals(newDataFile.location()))
              .count();
      assertThat(newDataFileCount).isEqualTo(1);
    }
  }

  @Test
  void emitsReadCommandsForEqualityDeletes() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete = writeEqualityDelete(table, 2, "b");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      sendTrigger(harness);

      List<ReadCommand> commands = harness.extractOutputValues();
      // 3 DATA_FILE (from main) + 1 EQ_DELETE_FILE
      assertThat(countDataFileTasks(commands)).isEqualTo(3);
      assertThat(countEqDeleteTasks(commands)).isEqualTo(1);
      assertThat(planner(harness).processedStagingSnapshotNum()).isEqualTo(1);
      assertThat(planner(harness).processedEqDeleteFileNum()).isEqualTo(1);

      List<StreamRecord<EqualityConvertPlan>> metadata =
          Lists.newArrayList(harness.getSideOutput(EqualityConvertPlanner.METADATA_STREAM));
      assertThat(metadata).hasSize(1);
      assertThat(metadata.get(0).getValue().dataFiles()).isEmpty();
    }
  }

  @Test
  void emitsDataFileFromInsertOnlyStagingSnapshot() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // S1: eq delete targeting main data
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();
    long s1SnapshotId = table.snapshot(STAGING_BRANCH).snapshotId();

    // S2: insert-only (no eq deletes), but its data must still be indexed for the configured
    // equality fields
    DataFile insertS2 = writeDataFile(table, createRecord(2, "b"));
    table.newAppend().appendFile(insertS2).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      // Trigger 1: processes S1 (eq delete)
      sendTrigger(harness);
      int afterFirst = harness.extractOutputValues().size();
      assertThat(countEqDeleteTasks(harness.extractOutputValues())).isEqualTo(1);

      // Record S1's conversion on main, so the planner walks past S1 before processing S2.
      simulateConvertCommit(table, s1SnapshotId);

      // Trigger 2: processes S2 (insert-only). Must emit its data file so the configured equality
      // fields stay indexed.
      sendTrigger(harness);
      List<ReadCommand> allCommands = harness.extractOutputValues();
      List<ReadCommand> trigger2Commands = allCommands.subList(afterFirst, allCommands.size());

      Set<String> dataFilePaths =
          trigger2Commands.stream()
              .filter(c -> c.task() instanceof FileScanTask)
              .map(TestEqualityConvertPlanner::filePath)
              .collect(Collectors.toSet());

      assertThat(dataFilePaths).contains(insertS2.location());
    }
  }

  @Test
  void includesNewDataFilesFromStaging() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DataFile newDataFile = writeDataFile(table, createRecord(2, "b"));
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addRows(newDataFile).addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      sendTrigger(harness);

      List<ReadCommand> commands = harness.extractOutputValues();
      // 1 main data file + 1 eq delete + 1 staging data file
      assertThat(countDataFileTasks(commands)).isEqualTo(2);
      assertThat(countEqDeleteTasks(commands)).isEqualTo(1);

      List<StreamRecord<EqualityConvertPlan>> metadata =
          Lists.newArrayList(harness.getSideOutput(EqualityConvertPlanner.METADATA_STREAM));
      assertThat(metadata).hasSize(1);
      assertThat(metadata.get(0).getValue().dataFiles()).hasSize(1);
    }
  }

  @Test
  void findsIntersectionWhenMainAdvancedAfterStagingFork() throws Exception {
    Table table = createTableWithDelete(3);
    // Pre-fork main: two snapshots.
    insert(table, 1, "a");
    insert(table, 2, "b");

    // Fork staging from current main head.
    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Main advances with two more snapshots after the fork. These are on main's
    // lineage but not on staging's.
    insert(table, 3, "c");
    insert(table, 4, "d");

    // Staging gets one new snapshot containing an equality delete.
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      sendTrigger(harness);

      // Planner identifies the fork point and processes only the staging-only
      // commit (the eq delete), and emits all four current main data files
      // for the index refresh.
      List<ReadCommand> commands = harness.extractOutputValues();
      assertThat(countEqDeleteTasks(commands)).isEqualTo(1);
      assertThat(countDataFileTasks(commands)).isEqualTo(4);
    }
  }

  @Test
  void skipsAlreadyProcessedStagingSnapshot() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    long stagingSnapshotId = table.snapshot(STAGING_BRANCH).snapshotId();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      sendTrigger(harness);
      int firstTriggerCount = harness.extractOutputValues().size();
      assertThat(firstTriggerCount).isGreaterThan(0);
      assertThat(planner(harness).skippedNoOpCycles()).isZero();

      // Simulate the committer committing to main with the staging snapshot property
      // (the planner promotes pending only after confirming the commit landed on main).
      simulateConvertCommit(table, stagingSnapshotId);

      sendTrigger(harness);
      assertThat(harness.extractOutputValues()).hasSize(firstTriggerCount);
      assertThat(planner(harness).skippedNoOpCycles()).isEqualTo(1);
    }
  }

  @Test
  void noOutputWhenStagingBranchEmpty() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      sendTrigger(harness);

      // Bootstrap from main for the configured field set: 1 main DATA_FILE; no-op metadata still
      // emitted because there's nothing on staging to convert.
      assertThat(countDataFileTasks(harness.extractOutputValues())).isEqualTo(1);
      assertThat(countEqDeleteTasks(harness.extractOutputValues())).isZero();
      assertThat(harness.getSideOutput(EqualityConvertPlanner.METADATA_STREAM)).hasSize(1);
      assertThat(planner(harness).skippedNoOpCycles()).isEqualTo(1);
      assertThat(planner(harness).reindexCount()).isZero();
    }
  }

  @Test
  void propagatesStagingOnlyPositionalDeletes() throws Exception {
    Table table = createTableWithDelete(3);
    DataFile mainData = writeDataFile(table, createRecord(1, "a"));
    table.newAppend().appendFile(mainData).commit();
    table.refresh();

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Staging snapshot has only a DV (no eq deletes, no new data files). Without the
    // planner forwarding stagingDVFiles in this case, the committer would never
    // see the DV and it would be lost.
    DeleteFile stagingDV = writeStagingDV(table, mainData.location(), 0L);
    table.newRowDelta().addDeletes(stagingDV).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      sendTrigger(harness);

      List<ReadCommand> commands = harness.extractOutputValues();
      assertThat(countDataFileTasks(commands)).isEqualTo(1);
      assertThat(countEqDeleteTasks(commands)).isZero();

      List<StreamRecord<EqualityConvertPlan>> metadata =
          Lists.newArrayList(harness.getSideOutput(EqualityConvertPlanner.METADATA_STREAM));
      assertThat(metadata).hasSize(1);
      EqualityConvertPlan result = metadata.get(0).getValue();
      assertThat(result.dataFiles()).isEmpty();
      assertThat(result.stagingDVFiles()).hasSize(1);
      assertThat(result.stagingDVFiles().get(0).location()).isEqualTo(stagingDV.location());
    }
  }

  @Test
  void phaseTimestampsAreMonotonicallyIncreasing() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Staging: eq delete + new data file (triggers 3 phases: main data, eq delete, staging data)
    DataFile newDataFile = writeDataFile(table, createRecord(2, "b"));
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addRows(newDataFile).addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      long triggerTs = 100L;
      sendTrigger(harness, triggerTs);

      List<Object> output = Lists.newArrayList(harness.getOutput());
      assertThat(((StreamRecord<ReadCommand>) output.get(0)).getValue().task())
          .isInstanceOf(FileScanTask.class);
      assertThat(output.get(1)).isEqualTo(new Watermark(triggerTs));

      assertThat(((StreamRecord<ReadCommand>) output.get(2)).getValue().task())
          .isInstanceOf(EqualityDeleteFileScanTask.class);
      assertThat(output.get(3)).isEqualTo(new Watermark(triggerTs + 1));

      assertThat(((StreamRecord<ReadCommand>) output.get(4)).getValue().task())
          .isInstanceOf(FileScanTask.class);
      assertThat(output.get(5)).isEqualTo(new Watermark(triggerTs + 2));
    }
  }

  @Test
  void routesExceptionToErrorStream() throws Exception {
    createTableWithDelete(3);

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      dropTable();

      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      sendTrigger(harness);
      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
    }
  }

  @Test
  void failsOnRemovedDataFilesOnStagingBranch() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    Set<DataFile> oldDataFiles = Sets.newHashSet();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        for (DataFile df : reader) {
          oldDataFiles.add(df.copy());
        }
      }
    }

    assertThat(oldDataFiles).hasSize(2);

    // Rewrite on the staging branch (not main). Equality delete conversion does not support
    // rewrites on staging; the planner must fail the cycle instead of silently dropping the
    // removed files.
    DataFile compactedFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(createRecord(1, "a"), createRecord(2, "b")));
    RewriteFiles rewrite = table.newRewrite();
    for (DataFile old : oldDataFiles) {
      rewrite.deleteFile(old);
    }

    rewrite.addFile(compactedFile);
    rewrite.toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      sendTrigger(harness);

      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      // Bootstrap runs before processCycle's failure, so the main data commands are already on
      // the wire. Only the cycle itself fails.
      assertThat(countDataFileTasks(harness.extractOutputValues())).isEqualTo(2);
      assertThat(countEqDeleteTasks(harness.extractOutputValues())).isZero();
    }
  }

  @Test
  void reEmitsMainDataAfterCompaction() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      sendTrigger(harness);
      int firstTriggerCount = harness.extractOutputValues().size();
      // 2 DATA_FILE (main) + 1 EQ_DELETE_FILE
      assertThat(firstTriggerCount).isEqualTo(3);

      // Compact data files on main: rewrite 2 files into 1
      Set<DataFile> oldDataFiles = Sets.newHashSet();
      for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
        try (ManifestReader<DataFile> reader =
            ManifestFiles.read(manifest, table.io(), table.specs())) {
          for (DataFile df : reader) {
            oldDataFiles.add(df.copy());
          }
        }
      }

      assertThat(oldDataFiles).hasSize(2);

      DataFile compactedFile =
          new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
              .writeFile(Lists.newArrayList(createRecord(1, "a"), createRecord(2, "b")));
      RewriteFiles rewrite = table.newRewrite();
      for (DataFile old : oldDataFiles) {
        rewrite.deleteFile(old);
      }

      rewrite.addFile(compactedFile);
      rewrite.commit();
      table.refresh();

      DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
      table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      sendTrigger(harness);
      List<ReadCommand> allCommands = harness.extractOutputValues();
      List<ReadCommand> trigger2Commands =
          allCommands.subList(firstTriggerCount, allCommands.size());

      // 1 DATA_FILE (compacted main) + 1 EQ_DELETE_FILE
      assertThat(countDataFileTasks(trigger2Commands)).isEqualTo(1);
      assertThat(countEqDeleteTasks(trigger2Commands)).isEqualTo(1);

      // DATA_FILE should reference the compacted file
      List<ReadCommand> dataCmds =
          trigger2Commands.stream()
              .filter(c -> c.task() instanceof FileScanTask)
              .collect(Collectors.toList());
      for (ReadCommand cmd : dataCmds) {
        assertThat(cmd.task().file().location()).isEqualTo(compactedFile.location());
      }
    }
  }

  @Test
  void refreshesIndexBeforeFirstCycleProcessesEqDeletes() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Stage MULTIPLE eq-delete snapshots before the first trigger arrives. The first trigger
    // bootstraps the index from main for the configured equality fields before processing any eq
    // deletes, then processes one snapshot per trigger (oldest first).
    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
    table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
    table.refresh();
    long s2SnapshotId = table.snapshot(STAGING_BRANCH).snapshotId();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      sendTrigger(harness);
      int afterFirst = harness.extractOutputValues().size();

      // First trigger processes the OLDER staging snapshot (eqDelete1). Bootstrap built the index
      // from main once: 2 main DATA_FILE + 1 EQ_DELETE_FILE = 3 commands.
      assertThat(afterFirst).isEqualTo(3);
      assertThat(countDataFileTasks(harness.extractOutputValues())).isEqualTo(2);
      assertThat(countEqDeleteTasks(harness.extractOutputValues())).isEqualTo(1);

      simulateConvertCommit(table, table.snapshot(STAGING_BRANCH).parentId());

      // Second trigger processes the newer staging snapshot. The index is already up-to-date.
      sendTrigger(harness);
      List<ReadCommand> allCommands = harness.extractOutputValues();
      List<ReadCommand> trigger2Commands = allCommands.subList(afterFirst, allCommands.size());
      assertThat(countDataFileTasks(trigger2Commands)).isZero();
      assertThat(countEqDeleteTasks(trigger2Commands)).isEqualTo(1);

      simulateConvertCommit(table, s2SnapshotId);
    }
  }

  @Test
  void noMainReEmitWhenUnchanged() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      sendTrigger(harness);
      int firstTriggerCount = harness.extractOutputValues().size();
      // 2 DATA_FILE (main) + 1 EQ_DELETE_FILE
      assertThat(firstTriggerCount).isEqualTo(3);
      assertThat(harness.getSideOutput(EqualityConvertPlanner.METADATA_STREAM)).hasSize(1);

      DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
      table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      sendTrigger(harness);
      List<ReadCommand> allCommands = harness.extractOutputValues();
      List<ReadCommand> trigger2Commands =
          allCommands.subList(firstTriggerCount, allCommands.size());

      // Only 1 EQ_DELETE_FILE, no main re-emission
      assertThat(countDataFileTasks(trigger2Commands)).isEqualTo(0);
      assertThat(countEqDeleteTasks(trigger2Commands)).isEqualTo(1);

      assertThat(harness.getSideOutput(EqualityConvertPlanner.METADATA_STREAM)).hasSize(2);
    }
  }

  @Test
  void noMainReEmitAfterOwnCommit() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    long indexedStagingSnapshotId = table.snapshot(STAGING_BRANCH).snapshotId();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      sendTrigger(harness);
      int firstTriggerCount = harness.extractOutputValues().size();

      // Record the converter's own commit (COMMITTED_STAGING_SNAPSHOT property) for the staging
      // snapshot the first trigger processed. The next trigger reads the property off main and
      // advances lastStagingSnapshotId. No reindex should be triggered.
      simulateConvertCommit(table, indexedStagingSnapshotId);

      DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
      table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      sendTrigger(harness);
      List<ReadCommand> allCommands = harness.extractOutputValues();
      List<ReadCommand> trigger2Commands =
          allCommands.subList(firstTriggerCount, allCommands.size());

      // Own commits don't trigger index rebuild.
      assertThat(countDataFileTasks(trigger2Commands)).isEqualTo(0);
      assertThat(countEqDeleteTasks(trigger2Commands)).isEqualTo(1);
    }
  }

  @Test
  void reIndexesAfterExternalCommit() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      sendTrigger(harness);
      int firstTriggerCount = harness.extractOutputValues().size();
      // 1 DATA_FILE (main) + 1 EQ_DELETE_FILE
      assertThat(firstTriggerCount).isEqualTo(2);
      assertThat(planner(harness).reindexCount()).isZero();
      assertThat(planner(harness).processedStagingSnapshotNum()).isEqualTo(1);
      assertThat(planner(harness).processedEqDeleteFileNum()).isEqualTo(1);

      // External commit: no COMMITTED_STAGING_SNAPSHOT_PROPERTY
      DataFile externalFile =
          new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
              .writeFile(Lists.newArrayList(createRecord(10, "z")));
      table.newAppend().appendFile(externalFile).commit();
      table.refresh();

      DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
      table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      sendTrigger(harness);
      List<ReadCommand> allCommands = harness.extractOutputValues();
      List<ReadCommand> trigger2Commands =
          allCommands.subList(firstTriggerCount, allCommands.size());

      // External commit triggers re-index: main data files are re-emitted
      assertThat(countDataFileTasks(trigger2Commands)).isGreaterThan(0);
      assertThat(countEqDeleteTasks(trigger2Commands)).isEqualTo(1);
      assertThat(planner(harness).reindexCount()).isEqualTo(1);
      assertThat(planner(harness).processedStagingSnapshotNum()).isEqualTo(2);
      assertThat(planner(harness).processedEqDeleteFileNum()).isEqualTo(2);
    }
  }

  @Test
  void emitsClearIndexBroadcastOnReindex() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      // Trigger 1 bootstraps the index. Bootstrap does not broadcast CLEAR_INDEX.
      sendTrigger(harness);
      assertThat(harness.getSideOutput(EqualityConvertPlanner.CLEAR_BROADCAST_STREAM))
          .isNullOrEmpty();

      // External commit advances main. The next trigger reindexes and must broadcast CLEAR_INDEX
      // so workers evict ghost keys (e.g. a PK whose data file was removed by external CoW).
      DataFile externalFile =
          new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
              .writeFile(Lists.newArrayList(createRecord(10, "z")));
      table.newAppend().appendFile(externalFile).commit();
      table.refresh();
      long mainAfterExternal = table.snapshot(SnapshotRef.MAIN_BRANCH).snapshotId();
      long mainSeqAfterExternal = table.snapshot(SnapshotRef.MAIN_BRANCH).sequenceNumber();

      DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
      table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      sendTrigger(harness);

      List<IndexCommand> clears =
          harness.getSideOutput(EqualityConvertPlanner.CLEAR_BROADCAST_STREAM).stream()
              .map(StreamRecord::getValue)
              .collect(Collectors.toList());
      assertThat(clears).hasSize(1);
      assertThat(clears.get(0).type()).isEqualTo(IndexCommand.Type.CLEAR_INDEX);
      assertThat(clears.get(0).mainSnapshotId()).isEqualTo(mainAfterExternal);
      assertThat(clears.get(0).mainSequenceNumber()).isEqualTo(mainSeqAfterExternal);
    }
  }

  @Test
  void detectsMainBranchChangeWithoutNewStagingSnapshots() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      // Trigger 1: build the index and process the existing eq delete.
      sendTrigger(harness);
      int afterFirst = harness.extractOutputValues().size();
      assertThat(afterFirst).isGreaterThan(0);

      // External commit on main (no COMMITTED_STAGING_SNAPSHOT_PROPERTY).
      DataFile externalFile =
          new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
              .writeFile(Lists.newArrayList(createRecord(10, "z")));
      table.newAppend().appendFile(externalFile).commit();
      table.refresh();

      // Trigger 2: nothing new on staging, but the planner must still notice the
      // external main change and immediately re-emit main data for index rebuild.
      sendTrigger(harness);
      List<ReadCommand> allAfterTrigger2 = harness.extractOutputValues();
      List<ReadCommand> trigger2Commands =
          allAfterTrigger2.subList(afterFirst, allAfterTrigger2.size());
      assertThat(countDataFileTasks(trigger2Commands)).isGreaterThan(0);

      // A subsequent staging eq delete should NOT re-emit main data because the index
      // was already rebuilt in trigger 2.
      int afterSecond = allAfterTrigger2.size();
      DeleteFile eqDelete2 = writeEqualityDelete(table, 10, "z");
      table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      sendTrigger(harness);
      List<ReadCommand> all = harness.extractOutputValues();
      List<ReadCommand> trigger3 = all.subList(afterSecond, all.size());
      assertThat(countDataFileTasks(trigger3)).isEqualTo(0);
      assertThat(countEqDeleteTasks(trigger3)).isEqualTo(1);
    }
  }

  @Test
  void stateRestoredAfterCheckpoint() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    long stagingSnapshotId = table.snapshot(STAGING_BRANCH).snapshotId();

    OperatorSubtaskState state;
    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();

      // First trigger bootstraps the index and processes the staging snapshot.
      sendTrigger(harness);
      int commandCount = harness.extractOutputValues().size();
      assertThat(commandCount).isGreaterThan(0);

      // Record S1's conversion on main (committer marker) so the planner advances
      // lastStagingSnapshotId on the second trigger.
      simulateConvertCommit(table, stagingSnapshotId);

      // Second trigger reads the marker and advances lastStagingSnapshotId.
      sendTrigger(harness);
      assertThat(harness.extractOutputValues()).hasSize(commandCount);

      state = harness.snapshot(1, System.currentTimeMillis());
    }

    // Restore from checkpoint: the restored index is not re-indexed because its state matches the
    // snapshot COMMITTED_STAGING_SNAPSHOT_PROPERTY property.
    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.initializeState(state);
      harness.open();

      sendTrigger(harness);
      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void failsOnV2PosDeleteOnStagingBranch() throws Exception {
    Table table = createTableWithDelete(2);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Flink-written staging branches are V3 and only produce DVs for deletes. A V2 positional
    // delete on staging is a writer-side misconfiguration and should fail the cycle rather than
    // be silently absorbed.
    DataFile dataFile = writeDataFile(table, createRecord(2, "b"));
    DeleteFile posDelete = writePosDeleteFile(table, dataFile.location(), 0);
    table.newRowDelta().addRows(dataFile).addDeletes(posDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      sendTrigger(harness);

      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
    }
  }

  @Test
  void skipsCommittedSnapshotAfterCommitLandsButCheckpointDoesNot() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();
    long stagingSnapshotId = table.snapshot(STAGING_BRANCH).snapshotId();

    // Capture the planner's checkpoint before any cycle has run.
    OperatorSubtaskState state;
    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      state = harness.snapshot(1, System.currentTimeMillis());
    }

    // The staging commit landed on main but lastStagingSnapshotId was not checkpointed.
    simulateConvertCommit(table, stagingSnapshotId);

    // On restore, the planner walks main, finds the COMMITTED_STAGING_SNAPSHOT_PROPERTY,
    // advances lastStagingSnapshotId, and skips re-emitting the already-committed snapshot.
    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.initializeState(state);
      harness.open();

      sendTrigger(harness);

      List<ReadCommand> commands = harness.extractOutputValues();
      assertThat(countEqDeleteTasks(commands)).isEqualTo(0);
      // Bootstrap from main creates the index from the main data files on the first trigger even
      // though the staging snapshot itself is already committed and skipped. Main now has the
      // original insert plus simulateConvertCommit's marker file = 2 data files.
      assertThat(countDataFileTasks(commands)).isEqualTo(2);
    }
  }

  @Test
  void failsWhenCommitMarkerDisappears() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    long startSnapshotId = table.currentSnapshot().snapshotId();

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();
    long stagingSnapshotId = table.snapshot(STAGING_BRANCH).snapshotId();

    simulateConvertCommit(table, stagingSnapshotId);

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH)) {
      harness.open();
      sendTrigger(harness);

      table.manageSnapshots().rollbackTo(startSnapshotId).commit();
      table.refresh();

      sendTrigger(harness);

      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(
              harness
                  .getSideOutput(TaskResultAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("No COMMITTED_STAGING_SNAPSHOT marker reachable");
    }
  }

  @Test
  void failsOnEqFieldIdsChangeAcrossRestart() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    OperatorSubtaskState state;
    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH, Lists.newArrayList(1, 2))) {
      harness.open();
      state = harness.snapshot(1, System.currentTimeMillis());
    }

    try (OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness =
        createHarness(STAGING_BRANCH, Lists.newArrayList(1))) {
      assertThatThrownBy(() -> harness.initializeState(state))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Equality field IDs changed across restart");
    }
  }

  private OneInputStreamOperatorTestHarness<Trigger, ReadCommand> createHarness(
      String stagingBranch) throws Exception {
    // Default matches the [1, 2] equality-field-id list produced by writeEqualityDelete.
    return createHarness(stagingBranch, Lists.newArrayList(1, 2));
  }

  private OneInputStreamOperatorTestHarness<Trigger, ReadCommand> createHarness(
      String stagingBranch, List<Integer> eqFieldIds) throws Exception {
    return new OneInputStreamOperatorTestHarness<>(
        new EqualityConvertPlanner(
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            tableLoader(),
            stagingBranch,
            SnapshotRef.MAIN_BRANCH,
            Sets.newHashSet(eqFieldIds)));
  }

  private static void sendTrigger(OneInputStreamOperatorTestHarness<Trigger, ?> harness)
      throws Exception {
    sendTrigger(harness, System.currentTimeMillis());
  }

  private static void sendTrigger(OneInputStreamOperatorTestHarness<Trigger, ?> harness, long time)
      throws Exception {
    harness.processElement(new StreamRecord<>(Trigger.create(time, 0), time));
  }

  private DataFile writeDataFile(Table table, Record record) throws IOException {
    return new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
        .writeFile(Lists.newArrayList(record));
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

  private DeleteFile writeIdOnlyEqualityDelete(Table table, int id) throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();
    Schema idOnly = table.schema().select("id");
    Record record = GenericRecord.create(idOnly);
    record.setField("id", id);
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        new PartitionData(PartitionSpec.unpartitioned().partitionType()),
        Lists.newArrayList(record),
        idOnly);
  }

  private DeleteFile writeIdOnlyPartitionedEqualityDelete(Table table, int id, String data)
      throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();
    Schema idOnly = table.schema().select("id");
    Record record = GenericRecord.create(idOnly);
    record.setField("id", id);
    PartitionData partition = new PartitionData(table.spec().partitionType());
    partition.set(0, data);
    return FileHelpers.writeDeleteFile(
        table, Files.localOutput(file), partition, Lists.newArrayList(record), idOnly);
  }

  private DeleteFile writeStagingDV(Table table, String dataFilePath, long position)
      throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();

    PositionDelete<Record> delete = PositionDelete.create();
    delete.set(dataFilePath, position, null);
    return FileHelpers.writePosDeleteFile(
        table,
        Files.localOutput(file),
        new PartitionData(PartitionSpec.unpartitioned().partitionType()),
        Lists.newArrayList(delete),
        3);
  }

  private static long countDataFileTasks(List<ReadCommand> commands) {
    return commands.stream().filter(c -> c.task() instanceof FileScanTask).count();
  }

  private static long countEqDeleteTasks(List<ReadCommand> commands) {
    return commands.stream().filter(TestEqualityConvertPlanner::isEqDelete).count();
  }

  private static EqualityConvertPlanner planner(
      OneInputStreamOperatorTestHarness<Trigger, ReadCommand> harness) {
    return (EqualityConvertPlanner) harness.getOperator();
  }

  private static boolean isEqDelete(ReadCommand cmd) {
    return cmd.task() instanceof EqualityDeleteFileScanTask;
  }

  private static String filePath(ReadCommand cmd) {
    return cmd.task().file().location();
  }

  private void simulateConvertCommit(Table table, long stagingSnapshotId) throws IOException {
    DataFile dummy =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(createRecord(-1, "marker" + stagingSnapshotId)));
    table
        .newRowDelta()
        .addRows(dummy)
        .set(
            EqualityConvertCommitter.COMMITTED_STAGING_SNAPSHOT_PROPERTY,
            String.valueOf(stagingSnapshotId))
        .commit();
    table.refresh();
  }
}
