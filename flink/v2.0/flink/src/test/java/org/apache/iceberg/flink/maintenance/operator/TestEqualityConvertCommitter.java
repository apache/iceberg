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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestEqualityConvertCommitter extends OperatorTestBase {

  @TempDir private Path tempDir;

  @Test
  void commitsDataFilesToMainBranch() throws Exception {
    Table table = createTable(3, FileFormat.PARQUET);
    insert(table, 1, "a");

    DataFile stagingDataFile = getFirstDataFile(table);
    long snapshotIdBefore = table.currentSnapshot().snapshotId();
    long stagingSnapshotId = 42L;

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        createHarness()) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(stagingDataFile),
              Lists.newArrayList(),
              Lists.newArrayList(),
              stagingSnapshotId,
              snapshotIdBefore,
              doneTs - 1,
              doneTs);

      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));
      harness.processBothWatermarks(new Watermark(doneTs));

      assertThat(harness.extractOutputValues()).hasSize(1);
      table.refresh();
      assertThat(table.currentSnapshot().snapshotId()).isNotEqualTo(snapshotIdBefore);
      assertThat(
              table
                  .currentSnapshot()
                  .summary()
                  .get(EqualityConvertCommitter.COMMITTED_STAGING_SNAPSHOT_PROPERTY))
          .isEqualTo(String.valueOf(stagingSnapshotId));
    }
  }

  @Test
  void holdsBackWatermarkUntilCommit() throws Exception {
    Table table = createTable(3, FileFormat.PARQUET);
    insert(table, 1, "a");

    DataFile stagingDataFile = getFirstDataFile(table);
    long snapshotIdBefore = table.currentSnapshot().snapshotId();

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        createHarness()) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(stagingDataFile),
              Lists.newArrayList(),
              Lists.newArrayList(),
              42L,
              snapshotIdBefore,
              doneTs - 2,
              doneTs);

      harness.processElement2(new StreamRecord<>(planResult, doneTs - 2));

      // A phase watermark before the plan's done timestamp must not be forwarded: it would let the
      // LockRemover release the maintenance lock before this cycle commits.
      harness.processBothWatermarks(new Watermark(doneTs - 1));
      assertThat(harness.extractOutputValues()).isEmpty();
      assertThat(watermarks(harness)).isEmpty();

      // The done-timestamp watermark commits the cycle; the watermark is forwarded only now.
      harness.processBothWatermarks(new Watermark(doneTs));
      assertThat(harness.extractOutputValues()).hasSize(1);
      assertThat(watermarks(harness)).containsExactly(new Watermark(doneTs));
    }
  }

  @Test
  void skipsCommitForEmptyCycle() throws Exception {
    Table table = createTable(3, FileFormat.PARQUET);

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        createHarness()) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult = EqualityConvertPlan.noOp(null, doneTs - 1, doneTs);

      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));
      harness.processBothWatermarks(new Watermark(doneTs));

      assertThat(harness.extractOutputValues()).hasSize(1);
      table.refresh();
      assertThat(table.currentSnapshot()).isNull();
    }
  }

  @Test
  void abortsCommitWhenDVWriterFailed() throws Exception {
    Table table = createTable(3, FileFormat.PARQUET);
    insert(table, 1, "a");

    DataFile stagingDataFile = getFirstDataFile(table);
    long snapshotIdBefore = table.currentSnapshot().snapshotId();

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        createHarness()) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(stagingDataFile),
              Lists.newArrayList(),
              Lists.newArrayList(),
              42L,
              snapshotIdBefore,
              doneTs - 1,
              doneTs);

      // Receive an abort signal from the DVWriter followed by the planning planResult.
      harness.processElement1(new StreamRecord<>(DVWriteResult.ABORT, doneTs));
      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));

      harness.processBothWatermarks(new Watermark(doneTs));

      // The cycle must complete (Trigger emitted) but nothing must be committed to the table.
      assertThat(harness.extractOutputValues()).hasSize(1);
      table.refresh();
      assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotIdBefore);
    }
  }

  @Test
  void failsCommitWhenExternalCommitLandsAfterPlan() throws Exception {
    Table table = createTable(3, FileFormat.PARQUET);
    insert(table, 1, "a");

    DataFile stagingDataFile = getFirstDataFile(table);
    long mainSnapshotIdAtPlan = table.currentSnapshot().snapshotId();
    long stagingSnapshotId = 555L;

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        createHarness()) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(stagingDataFile),
              Lists.newArrayList(),
              Lists.newArrayList(),
              stagingSnapshotId,
              mainSnapshotIdAtPlan,
              doneTs - 1,
              doneTs);

      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));

      // External writer appends to main between plan and commit.
      insert(table, 2, "b");
      table.refresh();
      long mainSnapshotIdAfterExternal = table.currentSnapshot().snapshotId();
      assertThat(mainSnapshotIdAfterExternal).isNotEqualTo(mainSnapshotIdAtPlan);

      harness.processBothWatermarks(new Watermark(doneTs));

      // Trigger still fires (so the aggregator records the cycle), but the commit
      // must have been rejected by validateNoConflictingDataFiles.
      assertThat(harness.extractOutputValues()).hasSize(1);

      List<StreamRecord<Exception>> errors =
          Lists.newArrayList(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM));
      assertThat(errors).hasSize(1);
      Exception failure = errors.get(0).getValue();
      assertThat(failure).isInstanceOf(ValidationException.class);
      assertThat(failure)
          .hasMessageContaining("Found conflicting files that can contain records matching");

      // Target head is still the external commit, not our cycle's commit.
      table.refresh();
      assertThat(table.currentSnapshot().snapshotId()).isEqualTo(mainSnapshotIdAfterExternal);
      assertThat(
              table
                  .currentSnapshot()
                  .summary()
                  .get(EqualityConvertCommitter.COMMITTED_STAGING_SNAPSHOT_PROPERTY))
          .isNull();
    }
  }

  @Test
  void failsReplayOfCommittedPlanFromOlderState() throws Exception {
    Table table = createTable(3, FileFormat.PARQUET);
    insert(table, 1, "a");

    DataFile stagingDataFile = getFirstDataFile(table);
    long mainSnapshotIdAtPlan = table.currentSnapshot().snapshotId();
    long stagingSnapshotId = 909L;

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        createHarness()) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(stagingDataFile),
              Lists.newArrayList(),
              Lists.newArrayList(),
              stagingSnapshotId,
              mainSnapshotIdAtPlan,
              doneTs - 1,
              doneTs);

      // Cycle 1 commits, advancing the target past mainSnapshotIdAtPlan and writing the marker.
      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));
      harness.processBothWatermarks(new Watermark(doneTs));
      assertThat(harness.extractOutputValues()).hasSize(1);

      table.refresh();
      long committedSnapshotId = table.currentSnapshot().snapshotId();
      assertThat(committedSnapshotId).isNotEqualTo(mainSnapshotIdAtPlan);

      // Restart from older state: the identical plan with its stale mainSnapshotId is replayed.
      // The committer is stateless, so validateFromSnapshot is the only guard against a duplicate
      // commit. It must reject the replay.
      long doneTs2 = doneTs + 1;
      EqualityConvertPlan replay =
          new EqualityConvertPlan(
              Lists.newArrayList(stagingDataFile),
              Lists.newArrayList(),
              Lists.newArrayList(),
              stagingSnapshotId,
              mainSnapshotIdAtPlan,
              doneTs2 - 1,
              doneTs2);

      harness.processElement2(new StreamRecord<>(replay, doneTs2 - 1));
      harness.processBothWatermarks(new Watermark(doneTs2));

      // Trigger still fires (so the aggregator records the cycle), but the commit was rejected.
      assertThat(harness.extractOutputValues()).hasSize(2);

      List<StreamRecord<Exception>> errors =
          Lists.newArrayList(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM));
      assertThat(errors).hasSize(1);
      Exception failure = errors.get(0).getValue();
      assertThat(failure).isInstanceOf(ValidationException.class);
      assertThat(failure)
          .hasMessageContaining("Found conflicting files that can contain records matching");

      // No duplicate commit: target head is still cycle 1's commit.
      table.refresh();
      assertThat(table.currentSnapshot().snapshotId()).isEqualTo(committedSnapshotId);
    }
  }

  @Test
  void deletesWrittenDvsWhenCommitFails() throws Exception {
    Table table = createTable(2, FileFormat.PARQUET);
    insert(table, 1, "a");

    DataFile stagingDataFile = getFirstDataFile(table);
    long mainSnapshotIdAtPlan = table.currentSnapshot().snapshotId();

    DeleteFile writtenDv = writePosDeleteFile(table, stagingDataFile.location(), 0L);
    assertThat(table.io().newInputFile(writtenDv.location()).exists()).isTrue();

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        createHarness()) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(stagingDataFile),
              Lists.newArrayList(),
              Lists.newArrayList(),
              777L,
              mainSnapshotIdAtPlan,
              doneTs - 1,
              doneTs);

      harness.processElement1(
          new StreamRecord<>(
              new DVWriteResult(Lists.newArrayList(writtenDv), Lists.newArrayList()), doneTs));
      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));

      // External writer appends to main between plan and commit, so the commit is rejected.
      insert(table, 2, "b");
      table.refresh();

      harness.processBothWatermarks(new Watermark(doneTs));

      assertThat(harness.extractOutputValues()).hasSize(1);

      List<StreamRecord<Exception>> errors =
          Lists.newArrayList(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM));
      assertThat(errors).hasSize(1);
      assertThat(errors.get(0).getValue()).isInstanceOf(ValidationException.class);

      // The DV written this cycle is unreferenced after the failed commit and must be deleted.
      assertThat(table.io().newInputFile(writtenDv.location()).exists()).isFalse();
    }
  }

  @Test
  void deletesSiblingDvsOnAbortButKeepsRewrittenDvs() throws Exception {
    Table table = createTable(2, FileFormat.PARQUET);
    insert(table, 1, "a");

    DataFile stagingDataFile = getFirstDataFile(table);
    long snapshotIdBefore = table.currentSnapshot().snapshotId();

    DeleteFile writtenDv = writePosDeleteFile(table, stagingDataFile.location(), 0L);
    DeleteFile rewrittenDv = writePosDeleteFile(table, stagingDataFile.location(), 1L);
    assertThat(table.io().newInputFile(writtenDv.location()).exists()).isTrue();
    assertThat(table.io().newInputFile(rewrittenDv.location()).exists()).isTrue();

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        createHarness()) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(stagingDataFile),
              Lists.newArrayList(),
              Lists.newArrayList(),
              42L,
              snapshotIdBefore,
              doneTs - 1,
              doneTs);

      // One writer task wrote a DV (rewriting an existing one); a sibling task aborted.
      harness.processElement1(
          new StreamRecord<>(
              new DVWriteResult(Lists.newArrayList(writtenDv), Lists.newArrayList(rewrittenDv)),
              doneTs));
      harness.processElement1(new StreamRecord<>(DVWriteResult.ABORT, doneTs));
      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));
      harness.processBothWatermarks(new Watermark(doneTs));

      assertThat(harness.extractOutputValues()).hasSize(1);
      table.refresh();
      assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotIdBefore);

      // The freshly written DV is removed; the rewritten DV is still live on target and stays.
      assertThat(table.io().newInputFile(writtenDv.location()).exists()).isFalse();
      assertThat(table.io().newInputFile(rewrittenDv.location()).exists()).isTrue();
    }
  }

  @Test
  void retainsWrittenDvsWhenCommitStateUnknown() throws Exception {
    Table table = createTable(2, FileFormat.PARQUET);
    insert(table, 1, "a");

    DataFile stagingDataFile = getFirstDataFile(table);
    long mainSnapshotIdAtPlan = table.currentSnapshot().snapshotId();

    DeleteFile writtenDv = writePosDeleteFile(table, stagingDataFile.location(), 0L);
    assertThat(table.io().newInputFile(writtenDv.location()).exists()).isTrue();

    EqualityConvertCommitter committer =
        new EqualityConvertCommitter(
            DUMMY_TABLE_NAME, DUMMY_TASK_NAME, tableLoader(), "staging", SnapshotRef.MAIN_BRANCH) {
          @Override
          void commit(RowDelta rowDelta) {
            throw new CommitStateUnknownException(
                new RuntimeException("simulated unknown commit state"));
          }
        };

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        new TwoInputStreamOperatorTestHarness<>(committer)) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(stagingDataFile),
              Lists.newArrayList(),
              Lists.newArrayList(),
              888L,
              mainSnapshotIdAtPlan,
              doneTs - 1,
              doneTs);

      harness.processElement1(
          new StreamRecord<>(
              new DVWriteResult(Lists.newArrayList(writtenDv), Lists.newArrayList()), doneTs));
      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));
      harness.processBothWatermarks(new Watermark(doneTs));

      assertThat(harness.extractOutputValues()).hasSize(1);

      List<StreamRecord<Exception>> errors =
          Lists.newArrayList(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM));
      assertThat(errors).hasSize(1);
      assertThat(errors.get(0).getValue()).isInstanceOf(CommitStateUnknownException.class);

      // Commit outcome unknown: the DV may be live, so it must not be deleted.
      assertThat(table.io().newInputFile(writtenDv.location()).exists()).isTrue();
    }
  }

  @Test
  void removesRewrittenStagingDvOnSharedBranch() throws Exception {
    // Shared branch: stagingBranch == targetBranch == main, so the writer's DVs are already on
    // target. A staging DV folded into a merged conversion DV must be removed, else the data file
    // would carry two DVs (V3 allows one per data file).
    Table table = createTable(3, FileFormat.PARQUET);
    insert(table, 1, "a");

    DataFile dataFile = getFirstDataFile(table);

    // Staging DV already committed on the shared target branch; the writer rewrites it by folding
    // it into a merged DV for the same data file.
    DeleteFile stagingDv = writeDV(table, dataFile.location(), 0L);
    table.newRowDelta().addDeletes(stagingDv).commit();
    table.refresh();
    long mainSnapshotId = table.currentSnapshot().snapshotId();

    DeleteFile mergedDv = writeDV(table, dataFile.location(), 0L);

    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        sharedBranchHarness()) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(),
              Lists.newArrayList(stagingDv),
              Lists.newArrayList(),
              123L,
              mainSnapshotId,
              doneTs - 1,
              doneTs);

      harness.processElement1(
          new StreamRecord<>(
              new DVWriteResult(Lists.newArrayList(mergedDv), Lists.newArrayList(stagingDv)),
              doneTs));
      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));
      harness.processBothWatermarks(new Watermark(doneTs));

      assertThat(harness.extractOutputValues()).hasSize(1);

      // Exactly one DV references the data file: the merged DV, with the rewritten staging DV
      // removed rather than left as a second DV on the same data file.
      table.refresh();
      List<DeleteFile> dvs = deletesForDataFile(table, dataFile.location());
      assertThat(dvs).hasSize(1);
      assertThat(dvs.get(0).location()).isEqualTo(mergedDv.location());
    }
  }

  @Test
  void removesConvertedEqualityDeletesOnSharedBranch() throws Exception {
    // Shared branch: the writer's equality deletes are already on the target branch. Once the cycle
    // resolves them to DVs, the committer removes the equality delete files in the same commit so
    // readers stop applying them.
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    DataFile dataFile = getFirstDataFile(table);

    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).commit();
    table.refresh();
    long mainSnapshotId = table.currentSnapshot().snapshotId();
    assertThat(equalityDeletes(table)).hasSize(1);

    // The cycle resolved the eq delete to a DV covering row position 0 of the data file.
    DeleteFile convertedDv = writeDV(table, dataFile.location(), 0L);

    EqualityConvertCommitter committer =
        new EqualityConvertCommitter(
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            tableLoader(),
            SnapshotRef.MAIN_BRANCH,
            SnapshotRef.MAIN_BRANCH);
    try (TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger> harness =
        new TwoInputStreamOperatorTestHarness<>(committer)) {
      harness.open();

      long doneTs = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(),
              Lists.newArrayList(),
              Lists.newArrayList(eqDelete),
              123L,
              mainSnapshotId,
              doneTs - 1,
              doneTs);

      harness.processElement1(
          new StreamRecord<>(
              new DVWriteResult(Lists.newArrayList(convertedDv), Lists.newArrayList()), doneTs));
      harness.processElement2(new StreamRecord<>(planResult, doneTs - 1));
      harness.processBothWatermarks(new Watermark(doneTs));

      assertThat(harness.extractOutputValues()).hasSize(1);

      table.refresh();
      // The equality delete file is gone; only the converted DV remains for the data file.
      assertThat(equalityDeletes(table)).isEmpty();
      List<DeleteFile> dvs = deletesForDataFile(table, dataFile.location());
      assertThat(dvs).hasSize(1);
      assertThat(dvs.get(0).location()).isEqualTo(convertedDv.location());

      assertThat(committer.removedEqDeleteNum()).isEqualTo(1);
    }
  }

  private TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger>
      createHarness() throws Exception {
    return new TwoInputStreamOperatorTestHarness<>(
        new EqualityConvertCommitter(
            DUMMY_TABLE_NAME, DUMMY_TASK_NAME, tableLoader(), "staging", SnapshotRef.MAIN_BRANCH));
  }

  private TwoInputStreamOperatorTestHarness<DVWriteResult, EqualityConvertPlan, Trigger>
      sharedBranchHarness() throws Exception {
    return new TwoInputStreamOperatorTestHarness<>(
        new EqualityConvertCommitter(
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            tableLoader(),
            SnapshotRef.MAIN_BRANCH,
            SnapshotRef.MAIN_BRANCH));
  }

  private static List<Watermark> watermarks(TwoInputStreamOperatorTestHarness<?, ?, ?> harness) {
    return harness.getOutput().stream()
        .filter(Watermark.class::isInstance)
        .map(Watermark.class::cast)
        .collect(Collectors.toList());
  }

  private static List<DeleteFile> deletesForDataFile(Table table, String dataFilePath) {
    List<DeleteFile> deletes = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          if (dataFilePath.equals(file.referencedDataFile())) {
            deletes.add(file.copy());
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return deletes;
  }

  private static List<DeleteFile> equalityDeletes(Table table) {
    List<DeleteFile> deletes = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          if (file.content() == FileContent.EQUALITY_DELETES) {
            deletes.add(file.copy());
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return deletes;
  }

  private DeleteFile writeEqualityDelete(Table table, Integer id, String data) throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        new PartitionData(PartitionSpec.unpartitioned().partitionType()),
        Lists.newArrayList(SimpleDataUtil.createRecord(id, data)),
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
}
