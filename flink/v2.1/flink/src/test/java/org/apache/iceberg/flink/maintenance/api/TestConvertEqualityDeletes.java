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
package org.apache.iceberg.flink.maintenance.api;

import static org.apache.iceberg.flink.SimpleDataUtil.createRecord;
import static org.apache.iceberg.flink.maintenance.api.ConvertEqualityDeletes.COMMIT_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.ConvertEqualityDeletes.PLANNER_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.operator.EqualityConvertCommitter.COMMITTED_STAGING_SNAPSHOT_PROPERTY;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ADDED_DATA_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ADDED_DATA_FILE_SIZE_METRIC;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ERROR_COUNTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
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
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestConvertEqualityDeletes extends MaintenanceTaskTestBase {

  private static final String STAGING_BRANCH = "__flink_staging_test";

  @TempDir private Path tempDir;

  @Test
  void testRejectsFormatVersion2() {
    createTableWithDelete(2);

    assertThatThrownBy(
            () ->
                ConvertEqualityDeletes.builder()
                    .stagingBranch(STAGING_BRANCH)
                    .equalityFieldColumns(ImmutableList.of("id", "data"))
                    .append(
                        infra.triggerStream(),
                        DUMMY_TABLE_NAME,
                        DUMMY_TASK_NAME,
                        0,
                        tableLoader(),
                        UID_SUFFIX,
                        StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
                        1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("format version >= 3");
  }

  @Test
  void testRejectsUnknownEqualityFieldColumns() {
    createTableWithDelete(3);

    assertThatThrownBy(
            () ->
                ConvertEqualityDeletes.builder()
                    .stagingBranch(STAGING_BRANCH)
                    .equalityFieldColumns(ImmutableList.of("nonexistent"))
                    .append(
                        infra.triggerStream(),
                        DUMMY_TABLE_NAME,
                        DUMMY_TASK_NAME,
                        0,
                        tableLoader(),
                        UID_SUFFIX,
                        StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
                        1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Equality field column 'nonexistent' not found in table schema");
  }

  @Test
  void testConvertEqualityDeletesToDVs() throws Exception {
    Table table = createTableWithDelete(3);

    // Insert initial data to main
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");

    assertThat(dataFileCount(table)).isEqualTo(3);

    // Create staging branch from current main state
    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Write a new data file (simulating insert of id=4)
    DataFile newDataFile = writeDataFile(table, createRecord(4, "d"));

    // Write an equality delete for id=2 (simulating delete of row "b")
    DeleteFile eqDelete = writeEqualityDelete(table, 2, "b");

    // Commit both to the staging branch
    table.newRowDelta().addRows(newDataFile).addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Verify staging branch has the eq delete
    long stagingEqDeleteCount =
        table.snapshot(STAGING_BRANCH).deleteManifests(table.io()).stream()
            .flatMap(
                m ->
                    StreamSupport.stream(
                        ManifestFiles.readDeleteManifest(m, table.io(), table.specs())
                            .spliterator(),
                        false))
            .filter(f -> f.content() == FileContent.EQUALITY_DELETES)
            .count();
    assertThat(stagingEqDeleteCount).isEqualTo(1);

    // Wire the ConvertEqualityDeletes maintenance task
    appendConvertTask();

    // Run the maintenance task
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    // Verify main branch now has 4 data files (3 original + 1 new)
    table.refresh();
    assertThat(dataFileCount(table)).isEqualTo(4);

    // Verify main branch has exactly one DV (id=2 deleted from its single-row data file)
    long mainDvCount =
        table.currentSnapshot().deleteManifests(table.io()).stream()
            .flatMap(
                m ->
                    StreamSupport.stream(
                        ManifestFiles.readDeleteManifest(m, table.io(), table.specs())
                            .spliterator(),
                        false))
            .filter(f -> f.content() == FileContent.POSITION_DELETES)
            .count();
    assertThat(mainDvCount).isEqualTo(1);

    // Verify no equality deletes on main
    long mainEqDeleteCount =
        table.currentSnapshot().deleteManifests(table.io()).stream()
            .flatMap(
                m ->
                    StreamSupport.stream(
                        ManifestFiles.readDeleteManifest(m, table.io(), table.specs())
                            .spliterator(),
                        false))
            .filter(f -> f.content() == FileContent.EQUALITY_DELETES)
            .count();
    assertThat(mainEqDeleteCount).isEqualTo(0);

    // Verify data correctness: id=2 should be deleted, id=4 should be added
    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(createRecord(1, "a"), createRecord(3, "c"), createRecord(4, "d")));
  }

  @Test
  void testMetrics() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // One staging snapshot: a new data file (id=3) plus an eq-delete (id=1). The conversion commits
    // exactly one data file and one DV to main.
    DataFile newDataFile = writeDataFile(table, createRecord(3, "c"));
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addRows(newDataFile).addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    // Only metrics named on TableMaintenanceMetrics flow through the test reporter. Among the
    // converter operators only the planner and committer own an ERROR_COUNTER; the parallel reader,
    // PK index, and DV writer report failures through ERROR_STREAM instead. The committer also
    // counts the data files it adds. Operator-specific counters (reindexCount, addedDvNum, ...) are
    // asserted by the operator unit tests. A -1 expected value means "present, value not checked".
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(errorKey(PLANNER_TASK_NAME), 0L)
            .put(errorKey(COMMIT_TASK_NAME), 0L)
            .put(metricKey(COMMIT_TASK_NAME, ADDED_DATA_FILE_NUM_METRIC), 1L)
            .put(metricKey(COMMIT_TASK_NAME, ADDED_DATA_FILE_SIZE_METRIC), -1L)
            .build());
  }

  private static List<String> errorKey(String taskName) {
    return metricKey(taskName, ERROR_COUNTER);
  }

  private static List<String> metricKey(String taskName, String metric) {
    return ImmutableList.of(taskName + "[0]", DUMMY_TABLE_NAME, DUMMY_TASK_NAME, "0", metric);
  }

  @Test
  void testNoOpWhenStagingBranchEmpty() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    // Create staging branch with no new files
    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();

    // Should complete successfully with no changes
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    assertThat(dataFileCount(table)).isEqualTo(1);
    SimpleDataUtil.assertTableRecords(table, ImmutableList.of(createRecord(1, "a")));
  }

  @Test
  void testMultipleEqualityDeletes() throws Exception {
    Table table = createTableWithDelete(3);

    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");
    insert(table, 5, "e");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Delete id=1 and id=4 via equality deletes on staging
    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    DeleteFile eqDelete2 = writeEqualityDelete(table, 4, "d");

    table
        .newRowDelta()
        .addDeletes(eqDelete1)
        .addDeletes(eqDelete2)
        .toBranch(STAGING_BRANCH)
        .commit();
    table.refresh();

    appendConvertTask();

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(createRecord(2, "b"), createRecord(3, "c"), createRecord(5, "e")));
  }

  @Test
  void testDuplicateKeyAcrossDataFiles() throws Exception {
    Table table = createTableWithDelete(3);

    // Two data files with the same key (id=1, data="a")
    insert(table, 1, "a");
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Eq delete for id=1 should produce DVs for both data files containing id=1
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    // Only id=2 should remain
    SimpleDataUtil.assertTableRecords(table, ImmutableList.of(createRecord(2, "b")));
  }

  @Test
  void testMultiSnapshotStagingWithPerSnapshotScoping() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Staging snapshot 1: delete id=1 from main
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Staging snapshot 2: re-insert id=1 (new data file)
    DataFile newDataFile = writeDataFile(table, createRecord(1, "a"));
    table.newAppend().appendFile(newDataFile).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // Cycle 1: processes S1 (delete id=1)
      long time1 = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result1.success()).isTrue();

      // Cycle 2: processes S2 (re-insert id=1)
      long time2 = time1 + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result2.success()).isTrue();

      table.refresh();
      // id=1 should still exist: the delete from S1 removed the original,
      // but S2 re-inserted it. Per-snapshot scoping ensures S1's delete
      // doesn't affect S2's data.
      assertRecords(table, ImmutableList.of(createRecord(1, "a")));
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testInsertThenDeleteAcrossCycles() throws Exception {
    Table table = createTableWithDelete(3);

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Staging snapshot 1: insert id=1 (data-only, no eq deletes)
    DataFile insertS1 = writeDataFile(table, createRecord(1, "a"));
    table.newAppend().appendFile(insertS1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Staging snapshot 2: eq delete the row written in S1
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // Cycle 1: processes S1 (insert id=1), commits data file to main
      long time1 = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result1.exceptions()).isEmpty();
      assertThat(result1.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(1, "a")));

      // Cycle 2: processes S2 (eq delete id=1), must find id=1 on main
      long time2 = time1 + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result2.exceptions()).isEmpty();
      assertThat(result2.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of());
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testInsertUpdateDeleteInsertUpdateChain() throws Exception {
    Table table = createTableWithDelete(3);

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // S1: insert K=1, V=A
    DataFile s1 = writeDataFile(table, createRecord(1, "a"));
    table.newAppend().appendFile(s1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    // S2: update K=1 to V=B (eq delete + insert in same commit)
    DataFile s2 = writeDataFile(table, createRecord(1, "b"));
    DeleteFile e2 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addRows(s2).addDeletes(e2).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    // S3: delete K=1
    DeleteFile e3 = writeEqualityDelete(table, 1, "b");
    table.newRowDelta().addDeletes(e3).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    // S4: insert K=1, V=C
    DataFile s4 = writeDataFile(table, createRecord(1, "c"));
    table.newAppend().appendFile(s4).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    // S5: update K=1 to V=D (eq delete + insert in same commit)
    DataFile s5 = writeDataFile(table, createRecord(1, "d"));
    DeleteFile e5 = writeEqualityDelete(table, 1, "c");
    table.newRowDelta().addRows(s5).addDeletes(e5).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      long time = System.currentTimeMillis();

      // Cycle 1: S1 inserts K=1, V=A
      long time1 = time;
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result1.exceptions()).isEmpty();
      assertThat(result1.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(1, "a")));

      // Cycle 2: S2 updates K=1 from V=A to V=B (eq delete + insert)
      long time2 = time + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result2.exceptions()).isEmpty();
      assertThat(result2.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(1, "b")));

      // Cycle 3: S3 deletes K=1
      long time3 = time + 2;
      infra.source().sendRecord(Trigger.create(time3, 0), time3);
      TaskResult result3 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result3.exceptions()).isEmpty();
      assertThat(result3.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of());

      // Cycle 4: S4 inserts K=1, V=C
      long time4 = time + 3;
      infra.source().sendRecord(Trigger.create(time4, 0), time4);
      TaskResult result4 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result4.exceptions()).isEmpty();
      assertThat(result4.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(1, "c")));

      // Cycle 5: S5 updates K=1 from V=C to V=D (eq delete + insert)
      long time5 = time + 4;
      infra.source().sendRecord(Trigger.create(time5, 0), time5);
      TaskResult result5 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result5.exceptions()).isEmpty();
      assertThat(result5.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(1, "d")));
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testParallelInsertOfToBeDeletedKeySurvives() throws Exception {
    Table table = createTableWithDelete(3);

    // Main holds the original (1, "a"); the staging eq-delete below removes this copy.
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // One staging snapshot updates id=1 in place: re-insert (1, "a") plus an eq-delete (1, "a") in
    // the same commit. The re-insert shares the equality key with the delete and carries the
    // delete's sequence, so it must survive: the delete only removes the lower-sequence main copy.
    // At parallelism > 1 the staging-data ADD can reach the index before the eq-delete resolves.
    // Event-time phase ordering is what keeps the re-insert from being accidentally deleted.
    DataFile reinsert = writeDataFile(table, createRecord(1, "a"));
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addRows(reinsert).addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask(STAGING_BRANCH);
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    assertRecords(table, ImmutableList.of(createRecord(1, "a")));
  }

  @Test
  void testDVMergeAcrossConversionCycles() throws Exception {
    Table table = createTableWithDelete(3);

    // Single data file with 3 rows so DV merge applies to the same file
    insert(
        table, ImmutableList.of(createRecord(1, "a"), createRecord(2, "b"), createRecord(3, "c")));

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Cycle 1 setup: eq delete for id=1
    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // Cycle 1: convert eq delete for id=1
      long time1 = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result1.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(2, "b"), createRecord(3, "c")));

      // Cycle 2 setup: eq delete for id=2 (committed while job is running)
      DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
      table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      // Cycle 2: convert eq delete for id=2, should merge DV with existing
      long time2 = time1 + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result2.exceptions()).isEmpty();
      assertThat(result2.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(3, "c")));

      // Verify: main has DVs, no equality deletes
      assertNoEqualityDeletesOnMain(table, 0);
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testConversionCorrectAfterCompaction() throws Exception {
    Table table = createTableWithDelete(3);

    // Three separate data files
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Cycle 1: delete id=1
    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      long time1 = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result1.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(2, "b"), createRecord(3, "c")));

      // Compact file2 and file3 on main into one file (leave file1 + its DV untouched)
      Set<DataFile> allDataFiles = Sets.newHashSet();
      for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
        try (ManifestReader<DataFile> reader =
            ManifestFiles.read(manifest, table.io(), table.specs())) {
          for (DataFile df : reader) {
            allDataFiles.add(df.copy());
          }
        }
      }

      // Find file1 (contains id=1) by checking which file has a DV against it
      Set<String> dvReferencedFiles = Sets.newHashSet();
      for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
          for (DeleteFile df : reader) {
            if (df.referencedDataFile() != null) {
              dvReferencedFiles.add(df.referencedDataFile());
            }
          }
        }
      }

      Set<DataFile> filesToCompact = Sets.newHashSet();
      for (DataFile df : allDataFiles) {
        if (!dvReferencedFiles.contains(df.location())) {
          filesToCompact.add(df);
        }
      }

      assertThat(filesToCompact).hasSize(2);

      DataFile compactedFile =
          new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
              .writeFile(ImmutableList.of(createRecord(2, "b"), createRecord(3, "c")));
      RewriteFiles rewrite = table.newRewrite();
      for (DataFile old : filesToCompact) {
        rewrite.deleteFile(old);
      }

      rewrite.addFile(compactedFile);
      rewrite.commit();
      table.refresh();

      assertRecords(table, ImmutableList.of(createRecord(2, "b"), createRecord(3, "c")));

      // Cycle 2: delete id=2 (should target the compacted file)
      DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
      table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      long time2 = time1 + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result2.exceptions()).isEmpty();
      assertThat(result2.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(3, "c")));
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testConvertEqualityDeletesPartitionedTable() throws Exception {
    Table table = createPartitionedTableWithDelete(3);

    // Insert data into two partitions
    insertPartitioned(table, 1, "a");
    insertPartitioned(table, 2, "b");
    insertPartitioned(table, 3, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Partition-scoped equality delete for id=1 in partition data="a"
    DeleteFile eqDelete = writePartitionedEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    // id=1 deleted from partition "a", id=2 in partition "b" and id=3 in partition "a" remain
    assertRecords(table, ImmutableList.of(createRecord(2, "b"), createRecord(3, "a")));
  }

  @Test
  void testEqualityDeleteIsScopedToItsPartition() throws Exception {
    Table table = createPartitionedTableWithDelete(3);

    // Same PK (id=1) exists in two partitions. An eq delete in one partition must not delete
    // rows in the other.
    insertPartitioned(table, 1, "a");
    insertPartitioned(table, 1, "b");
    insertPartitioned(table, 2, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete = writePartitionedEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    // Only (1, "a") is deleted; (1, "b") remains because the eq delete was scoped to partition
    // "a", and (2, "a") remains because its equality field values don't match.
    assertRecords(table, ImmutableList.of(createRecord(1, "b"), createRecord(2, "a")));
  }

  @Test
  void testUnpartitionedDeleteAppliesAcrossPartitionEvolution() throws Exception {
    // Data is written under a partitioned spec, then the table evolves to unpartitioned. A delete
    // written under the unpartitioned spec is a global delete and must remove the data row from the
    // older spec. This only works because the index key is spec-independent (spec scoping happens
    // at resolve time, where a global delete matches every spec).
    Table table = createPartitionedTableWithDelete(3);
    insertPartitioned(table, 1, "a");

    table.updateSpec().removeField("data").commit();
    table.refresh();

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // writeEqualityDelete writes with the current (now unpartitioned) spec, so the delete is
    // global.
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    assertRecords(table, ImmutableList.of());
  }

  @Test
  void testPartitionedDeleteDoesNotApplyAcrossPartitionEvolution() throws Exception {
    // Inverse of the global case: a partitioned delete under a new spec must NOT remove a data row
    // written under an older spec. The row survives because resolution is scoped to the delete's
    // spec.
    Table table = createPartitionedTableWithDelete(3);
    insertPartitioned(table, 1, "a");

    // Evolve to a different partition spec so the delete below is written under a new spec id.
    table.updateSpec().removeField("data").addField("id").commit();
    table.refresh();

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    DeleteFile eqDelete = writeIdPartitionedEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    // The spec-0 row survives: the delete's spec differs and it is not a global (unpartitioned)
    // delete.
    assertRecords(table, ImmutableList.of(createRecord(1, "a")));
  }

  @Test
  void testStagingPositionDeleteMergedIntoConversionDV() throws Exception {
    Table table = createTableWithDelete(3);

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // S1: write a data file with two rows (id=1 at pos 0, id=2 at pos 1).
    DataFile data =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(createRecord(1, "a"), createRecord(2, "b")));
    table.newAppend().appendFile(data).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    // S2: eq delete matches row 0 (will produce a conversion DV at pos 0) + a position
    // delete DV referencing the same data file at pos 1. Both DVs target the same data
    // file and must be merged into a single DV (V3 invariant).
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    DeleteFile stagingDV = writeStagingDV(table, data.location(), 1L);
    table
        .newRowDelta()
        .addDeletes(eqDelete)
        .addDeletes(stagingDV)
        .toBranch(STAGING_BRANCH)
        .commit();
    table.refresh();

    appendConvertTask();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // Cycle 1: commits S1's data file to main
      long time1 = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result1.success()).isTrue();

      // Cycle 2: converts S2's eq delete to DV, merges with staging DV
      long time2 = time1 + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result2.exceptions()).isEmpty();
      assertThat(result2.success()).isTrue();

      table.refresh();
      // Both rows from S1 must be masked: pos 0 by the conversion DV, pos 1 by the staging DV.
      assertRecords(table, ImmutableList.of());

      // Exactly one DV per data file (V3 invariant): the resolver must have folded the
      // staging DV's positions into the conversion DV.
      long dvCount =
          table.currentSnapshot().deleteManifests(table.io()).stream()
              .flatMap(
                  m ->
                      StreamSupport.stream(
                          ManifestFiles.readDeleteManifest(m, table.io(), table.specs())
                              .spliterator(),
                          false))
              .filter(f -> f.content() == FileContent.POSITION_DELETES)
              .filter(f -> data.location().equals(f.referencedDataFile()))
              .count();
      assertThat(dvCount).isEqualTo(1);
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testStagingDataFilesOnlyNoEqDeletes() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Staging has only a new data file, no equality deletes
    DataFile newDataFile = writeDataFile(table, createRecord(2, "b"));
    table.newAppend().appendFile(newDataFile).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    assertThat(dataFileCount(table)).isEqualTo(2);
    SimpleDataUtil.assertTableRecords(
        table, ImmutableList.of(createRecord(1, "a"), createRecord(2, "b")));
  }

  @Test
  void testReindexAfterExternalCommit() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Cycle 1: delete id=1
    DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      long time1 = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result1.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(2, "b")));

      // External commit: insert id=3 directly to main (not via staging)
      insert(table, 3, "c");

      // Cycle 2: delete id=2 (should reindex because of external commit)
      DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
      table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      long time2 = time1 + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result2.exceptions()).isEmpty();
      assertThat(result2.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(3, "c")));
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testReindexEvictsGhostKeyAfterExternalDataFileRemoval() throws Exception {
    // CoW removal case: an external commit removes a data file, leaving a stale
    // PK in the worker's index. A later staging eq-delete for that PK must NOT produce a DV
    // referencing the removed file. The external commit advances main, so the next cycle reindexes
    // and the worker clears the ghost key before resolving the delete.
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    table.refresh();
    DataFile file1 = table.currentSnapshot().addedDataFiles(table.io()).iterator().next().copy();
    insert(table, 2, "b");

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Cycle 1: delete id=2. Bootstraps the worker index from main (id=1 -> file1, id=2 -> file2).
    DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
    table.newRowDelta().addDeletes(eqDelete2).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    appendConvertTask();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      long time1 = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result1.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(1, "a")));

      // External CoW-style removal: drop file1 (id=1) from main without re-adding the row. The
      // worker's index still holds the ghost id=1 -> file1 until the next reindex clears it.
      table.newDelete().deleteFile(file1).commit();
      table.refresh();
      assertRecords(table, ImmutableList.of());

      // Cycle 2: stage an eq-delete for the removed key id=1. Without ghost eviction the worker
      // would emit a DV position against the now-absent file1.
      DeleteFile eqDelete1 = writeEqualityDelete(table, 1, "a");
      table.newRowDelta().addDeletes(eqDelete1).toBranch(STAGING_BRANCH).commit();
      table.refresh();

      long time2 = time1 + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result2.exceptions()).isEmpty();
      assertThat(result2.success()).isTrue();

      // No deletion vector may reference the removed file1.
      table.refresh();
      assertThat(dvReferencedDataFiles(table)).doesNotContain(file1.location());
      assertRecords(table, ImmutableList.of());
    } finally {
      closeJobClient(jobClient);
    }
  }

  private static Set<String> dvReferencedDataFiles(Table table) {
    Set<String> referenced = Sets.newHashSet();
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile df : reader) {
          if (df.referencedDataFile() != null) {
            referenced.add(df.referencedDataFile());
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return referenced;
  }

  private DataFile writeDataFile(Table table, Record record) throws IOException {
    return new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
        .writeFile(Lists.newArrayList(record));
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

  private DeleteFile writeEqualityDelete(Table table, Integer id, String data) throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();

    Schema eqDeleteSchema = table.schema();
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        new PartitionData(PartitionSpec.unpartitioned().partitionType()),
        Lists.newArrayList(createRecord(id, data)),
        eqDeleteSchema);
  }

  private DeleteFile writePartitionedEqualityDelete(Table table, Integer id, String data)
      throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();

    PartitionData partition = new PartitionData(table.spec().partitionType());
    partition.set(0, data);
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        partition,
        Lists.newArrayList(createRecord(id, data)),
        table.schema());
  }

  private DeleteFile writeIdPartitionedEqualityDelete(Table table, Integer id, String data)
      throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();

    PartitionData partition = new PartitionData(table.spec().partitionType());
    partition.set(0, id);
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        partition,
        Lists.newArrayList(createRecord(id, data)),
        table.schema());
  }

  private static long dataFileCount(Table table) {
    table.refresh();
    long count = 0;
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        for (DataFile ignored : reader) {
          count++;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return count;
  }

  @Test
  void testStagingEqualsTargetBranch() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    // Write eq delete directly to main (no separate staging branch)
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    DataFile newData = writeDataFile(table, createRecord(3, "c"));
    table.newRowDelta().addRows(newData).addDeletes(eqDelete).commit();
    table.refresh();

    appendConvertTask(SnapshotRef.MAIN_BRANCH);

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // Cycle 1: process the eq delete for id=1
      long time1 = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result1.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(2, "b"), createRecord(3, "c")));
      long dataFilesAfterCycle1 = dataFileCount(table);
      // Expect 3: two from insert() + one from the writer's rowDelta.addRows(newData).
      // When stagingBranch == targetBranch, the committer must NOT re-add newData via
      // rowDelta.addRows(...) — that would duplicate (count=4).
      assertThat(dataFilesAfterCycle1).isEqualTo(3);

      // Cycle 2: no-op (converter's own commit must be skipped)
      long time2 = time1 + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result2.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(2, "b"), createRecord(3, "c")));
      assertThat(dataFileCount(table)).isEqualTo(dataFilesAfterCycle1);

      // New eq delete for id=2 committed directly to main between cycles
      DeleteFile eqDelete2 = writeEqualityDelete(table, 2, "b");
      DataFile newData2 = writeDataFile(table, createRecord(4, "d"));
      table.newRowDelta().addRows(newData2).addDeletes(eqDelete2).commit();
      table.refresh();

      // Cycle 3: process the new eq delete
      long time3 = time2 + 1;
      infra.source().sendRecord(Trigger.create(time3, 0), time3);
      TaskResult result3 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result3.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(3, "c"), createRecord(4, "d")));
      long dataFilesAfterCycle3 = dataFileCount(table);

      // Cycle 4: no-op again
      long time4 = time3 + 1;
      infra.source().sendRecord(Trigger.create(time4, 0), time4);
      TaskResult result4 = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result4.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(3, "c"), createRecord(4, "d")));
      assertThat(dataFileCount(table)).isEqualTo(dataFilesAfterCycle3);
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testStagingEqualsTargetBranchColdStartCatchUp() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");

    // Writer commits three eq-deletes to main BEFORE the converter starts.
    // Cold start must pick up the unconverted history, not just the head snapshot.
    table.newRowDelta().addDeletes(writeEqualityDelete(table, 1, "a")).commit();
    table.newRowDelta().addDeletes(writeEqualityDelete(table, 2, "b")).commit();
    table.newRowDelta().addDeletes(writeEqualityDelete(table, 3, "c")).commit();
    table.refresh();

    appendConvertTask(SnapshotRef.MAIN_BRANCH);

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // One unconverted snapshot per cycle, oldest first. After three cycles every eq-delete
      // commit has its own committer commit carrying the marker.
      for (int cycle = 1; cycle <= 3; cycle++) {
        long ts = System.currentTimeMillis() + cycle;
        infra.source().sendRecord(Trigger.create(ts, 0), ts);
        TaskResult result = infra.sink().poll(Duration.ofSeconds(10));
        assertThat(result.success()).isTrue();
      }

      table.refresh();
      long convertedCount =
          StreamSupport.stream(table.snapshots().spliterator(), false)
              .filter(s -> s.summary().containsKey(COMMITTED_STAGING_SNAPSHOT_PROPERTY))
              .count();
      assertThat(convertedCount).isEqualTo(3);
      assertRecords(table, ImmutableList.of());
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testStagingEqualsTargetBranchReinsertAfterDeleteSurvives() throws Exception {
    Table table = createTableWithDelete(3);

    // Shared branch: insert id=1, eq-delete id=1, then re-insert id=1. The re-insert has a higher
    // sequence than the delete and must survive the conversion (sequence-aware resolution).
    insert(table, 1, "a");
    table.newRowDelta().addDeletes(writeEqualityDelete(table, 1, "a")).commit();
    table.newAppend().appendFile(writeDataFile(table, createRecord(1, "a"))).commit();
    table.refresh();

    appendConvertTask(SnapshotRef.MAIN_BRANCH);

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // One cycle converts the eq-delete: the original row is deleted, the newer re-insert is
      // not (its sequence is at or above the delete's).
      long ts = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(ts, 0), ts);
      TaskResult result = infra.sink().poll(Duration.ofSeconds(10));
      assertThat(result.exceptions()).isEmpty();
      assertThat(result.success()).isTrue();

      table.refresh();
      assertRecords(table, ImmutableList.of(createRecord(1, "a")));
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testStagingEqualsTargetBranchMergesStagingDvIntoSingleDv() throws Exception {
    Table table = createTableWithDelete(3);

    // One data file with two rows: id=1 at pos 0, id=2 at pos 1, committed to main.
    DataFile data =
        new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
            .writeFile(Lists.newArrayList(createRecord(1, "a"), createRecord(2, "b")));
    table.newAppend().appendFile(data).commit();
    table.refresh();

    // Same-branch commit: an eq-delete for id=1 (resolves to a conversion DV at pos 0) plus a
    // writer DV at pos 1 on the same data file. The resolver folds the staging DV into the
    // conversion DV; on a shared branch the committer must remove the superseded staging DV.
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    DeleteFile stagingDV = writeStagingDV(table, data.location(), 1L);
    table.newRowDelta().addDeletes(eqDelete).addDeletes(stagingDV).commit();
    table.refresh();

    appendConvertTask(SnapshotRef.MAIN_BRANCH);
    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    // Both rows masked: pos 0 by the conversion DV, pos 1 by the merged-in staging DV.
    assertRecords(table, ImmutableList.of());

    // Exactly one DV per data file (V3 invariant). Without removing the rewritten staging DV on a
    // shared branch, the data file would carry two DVs.
    long dvCount =
        table.currentSnapshot().deleteManifests(table.io()).stream()
            .flatMap(
                m ->
                    StreamSupport.stream(
                        ManifestFiles.readDeleteManifest(m, table.io(), table.specs())
                            .spliterator(),
                        false))
            .filter(f -> f.content() == FileContent.POSITION_DELETES)
            .filter(f -> data.location().equals(f.referencedDataFile()))
            .count();
    assertThat(dvCount).isEqualTo(1);
  }

  @Test
  void testReaderErrorSkipsCommit() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    long mainSnapshotBeforeStaging = table.currentSnapshot().snapshotId();

    table.manageSnapshots().createBranch(STAGING_BRANCH).commit();
    table.refresh();

    // Staging data file + eq delete file, both referenced by the staging commit.
    DataFile newDataFile = writeDataFile(table, createRecord(2, "b"));
    DeleteFile eqDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addRows(newDataFile).addDeletes(eqDelete).toBranch(STAGING_BRANCH).commit();
    table.refresh();

    File eqDeleteLocalFile = new File(eqDelete.location().replace("file:", ""));

    // Delete the eq delete file; the committer must abort rather than committing data without its
    // DV.
    assertThat(eqDeleteLocalFile.delete()).isTrue();

    appendConvertTask();

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      long time1 = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time1, 0), time1);
      TaskResult result1 = infra.sink().poll(Duration.ofSeconds(10));

      assertThat(result1.success()).isFalse();
      assertThat(result1.exceptions()).isNotEmpty();

      table.refresh();
      // Main must not have advanced (no commit happened).
      assertThat(table.currentSnapshot().snapshotId()).isEqualTo(mainSnapshotBeforeStaging);
      SimpleDataUtil.assertTableRecords(table, ImmutableList.of(createRecord(1, "a")));

      // Restore the eq delete file content by rewriting an identical delete, and retry:
      // the planner must re-process the same staging snapshot (cursor didn't advance on failure).
      DeleteFile recreated =
          FileHelpers.writeDeleteFile(
              table,
              Files.localOutput(eqDeleteLocalFile),
              new PartitionData(PartitionSpec.unpartitioned().partitionType()),
              Lists.newArrayList(createRecord(1, "a")),
              table.schema());
      assertThat(recreated.location()).isEqualTo(eqDelete.location());

      long time2 = time1 + 1;
      infra.source().sendRecord(Trigger.create(time2, 0), time2);
      TaskResult result2 = infra.sink().poll(Duration.ofSeconds(10));

      assertThat(result2.exceptions()).isEmpty();
      assertThat(result2.success()).isTrue();

      table.refresh();
      // Staging data file committed with DV for id=1: should see id=2 only.
      SimpleDataUtil.assertTableRecords(table, ImmutableList.of(createRecord(2, "b")));
    } finally {
      closeJobClient(jobClient);
    }
  }

  private void appendConvertTask() {
    appendConvertTask(STAGING_BRANCH);
  }

  private void appendConvertTask(String stagingBranch) {
    ConvertEqualityDeletes.builder()
        .stagingBranch(stagingBranch)
        .equalityFieldColumns(ImmutableList.of("id", "data"))
        .parallelism(2)
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            UID_SUFFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());
  }

  private static void assertRecords(Table table, List<Record> expected) throws IOException {
    table.refresh();
    Types.StructType type = SimpleDataUtil.SCHEMA.asStruct();

    StructLikeSet expectedSet = StructLikeSet.create(type);
    expectedSet.addAll(expected);

    try (CloseableIterable<Record> iterable =
        IcebergGenerics.read(table)
            .useSnapshot(table.currentSnapshot().snapshotId())
            .project(SimpleDataUtil.SCHEMA)
            .build()) {
      StructLikeSet actualSet = StructLikeSet.create(type);
      for (Record record : iterable) {
        actualSet.add(record);
      }

      assertThat(actualSet).isEqualTo(expectedSet);
    }
  }

  private static void assertNoEqualityDeletesOnMain(Table table, long expectedEqDeleteCount) {
    long mainEqDeleteCount = 0;
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile f : reader) {
          if (f.content() == FileContent.EQUALITY_DELETES) {
            mainEqDeleteCount++;
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    assertThat(mainEqDeleteCount).isEqualTo(expectedEqDeleteCount);
  }
}
