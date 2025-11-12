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
package org.apache.iceberg.flink.sink.dynamic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.sink.CommitSummary;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestDynamicCommitter {

  static final String DB = "db";
  static final String TABLE1 = "table";
  static final String TABLE2 = "table2";

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION = new HadoopCatalogExtension(DB, TABLE1);

  Catalog catalog;

  final int cacheMaximumSize = 10;

  private static final DataFile DATA_FILE =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(0)
          .withMetrics(
              new Metrics(
                  42L,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L), // value count
                  ImmutableMap.of(1, 0L), // null count
                  null,
                  ImmutableMap.of(1, ByteBuffer.allocate(1)), // lower bounds
                  ImmutableMap.of(1, ByteBuffer.allocate(1)) // upper bounds
                  ))
          .build();

  private static final DataFile DATA_FILE_2 =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-2.parquet")
          .withFileSizeInBytes(0)
          .withMetrics(
              new Metrics(
                  24L,
                  null, // no column sizes
                  ImmutableMap.of(1, 3L), // value count
                  ImmutableMap.of(1, 0L), // null count
                  null,
                  ImmutableMap.of(1, ByteBuffer.allocate(1)), // lower bounds
                  ImmutableMap.of(1, ByteBuffer.allocate(1)) // upper bounds
                  ))
          .build();

  private static final DeleteFile DELETE_FILE =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-3.parquet")
          .withFileSizeInBytes(0)
          .withMetrics(
              new Metrics(
                  24L,
                  null, // no column sizes
                  ImmutableMap.of(1, 3L), // value count
                  ImmutableMap.of(1, 0L), // null count
                  null,
                  ImmutableMap.of(1, ByteBuffer.allocate(1)), // lower bounds
                  ImmutableMap.of(1, ByteBuffer.allocate(1)) // upper bounds
                  ))
          .ofPositionDeletes()
          .build();

  @BeforeEach
  void before() {
    catalog = CATALOG_EXTENSION.catalog();
    Schema schema1 = new Schema(42);
    Schema schema2 = new Schema(43);
    catalog.createTable(TableIdentifier.of(TABLE1), schema1);
    catalog.createTable(TableIdentifier.of(TABLE2), schema2);
  }

  @Test
  void testCommit() throws Exception {
    Table table1 = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table1.snapshots()).isEmpty();
    Table table2 = catalog.loadTable(TableIdentifier.of(TABLE2));
    assertThat(table2.snapshots()).isEmpty();

    boolean overwriteMode = false;
    int workerPoolSize = 1;
    String sinkId = "sinkId";
    UnregisteredMetricsGroup metricGroup = new UnregisteredMetricsGroup();
    DynamicCommitterMetrics committerMetrics = new DynamicCommitterMetrics(metricGroup);
    DynamicCommitter dynamicCommitter =
        new DynamicCommitter(
            CATALOG_EXTENSION.catalog(),
            Maps.newHashMap(),
            overwriteMode,
            workerPoolSize,
            sinkId,
            committerMetrics);

    WriteTarget writeTarget1 =
        new WriteTarget(TABLE1, "branch", 42, 0, true, Sets.newHashSet(1, 2));
    WriteTarget writeTarget2 =
        new WriteTarget(TABLE1, "branch2", 43, 0, true, Sets.newHashSet(1, 2));
    WriteTarget writeTarget3 =
        new WriteTarget(TABLE2, "branch2", 43, 0, true, Sets.newHashSet(1, 2));

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    byte[] deltaManifest1 =
        aggregator.writeToManifest(
            writeTarget1,
            Sets.newHashSet(
                new DynamicWriteResult(
                    writeTarget1, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            0);
    byte[] deltaManifest2 =
        aggregator.writeToManifest(
            writeTarget2,
            Sets.newHashSet(
                new DynamicWriteResult(
                    writeTarget2, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            0);
    byte[] deltaManifest3 =
        aggregator.writeToManifest(
            writeTarget3,
            Sets.newHashSet(
                new DynamicWriteResult(
                    writeTarget3, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            0);

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    CommitRequest<DynamicCommittable> commitRequest1 =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget1, deltaManifest1, jobId, operatorId, checkpointId));

    CommitRequest<DynamicCommittable> commitRequest2 =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget2, deltaManifest2, jobId, operatorId, checkpointId));

    CommitRequest<DynamicCommittable> commitRequest3 =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget3, deltaManifest3, jobId, operatorId, checkpointId));

    dynamicCommitter.commit(Sets.newHashSet(commitRequest1, commitRequest2, commitRequest3));

    table1.refresh();
    assertThat(table1.snapshots()).hasSize(2);
    Snapshot first = Iterables.getFirst(table1.snapshots(), null);
    assertThat(first.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "1")
                .put("added-records", "42")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "1")
                .put("total-delete-files", "0")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "0")
                .put("total-records", "42")
                .build());
    Snapshot second = Iterables.get(table1.snapshots(), 1, null);
    assertThat(second.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "1")
                .put("added-records", "42")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "1")
                .put("total-delete-files", "0")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "0")
                .put("total-records", "42")
                .build());

    table2.refresh();
    assertThat(table2.snapshots()).hasSize(1);
    Snapshot third = Iterables.getFirst(table2.snapshots(), null);
    assertThat(third.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "1")
                .put("added-records", "42")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "1")
                .put("total-delete-files", "0")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "0")
                .put("total-records", "42")
                .build());
  }

  @Test
  void testAlreadyCommitted() throws Exception {
    Table table1 = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table1.snapshots()).isEmpty();

    boolean overwriteMode = false;
    int workerPoolSize = 1;
    String sinkId = "sinkId";
    UnregisteredMetricsGroup metricGroup = new UnregisteredMetricsGroup();
    DynamicCommitterMetrics committerMetrics = new DynamicCommitterMetrics(metricGroup);
    DynamicCommitter dynamicCommitter =
        new DynamicCommitter(
            CATALOG_EXTENSION.catalog(),
            Maps.newHashMap(),
            overwriteMode,
            workerPoolSize,
            sinkId,
            committerMetrics);

    WriteTarget writeTarget =
        new WriteTarget(TABLE1, "branch", 42, 0, false, Sets.newHashSet(1, 2));

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    byte[] deltaManifest =
        aggregator.writeToManifest(
            writeTarget,
            Sets.newHashSet(
                new DynamicWriteResult(
                    writeTarget, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            checkpointId);

    CommitRequest<DynamicCommittable> commitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget, deltaManifest, jobId, operatorId, checkpointId));

    dynamicCommitter.commit(Sets.newHashSet(commitRequest));

    CommitRequest<DynamicCommittable> oldCommitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(
                writeTarget, deltaManifest, jobId, operatorId, checkpointId - 1));

    // Old commits requests shouldn't affect the result
    dynamicCommitter.commit(Sets.newHashSet(oldCommitRequest));

    table1.refresh();
    assertThat(table1.snapshots()).hasSize(1);
    Snapshot first = Iterables.getFirst(table1.snapshots(), null);
    assertThat(first.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "1")
                .put("added-records", "42")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "1")
                .put("total-delete-files", "0")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "0")
                .put("total-records", "42")
                .build());
  }

  @Test
  void testTableBranchAtomicCommitForAppendOnlyData() throws Exception {
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table.snapshots()).isEmpty();

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    WriteTarget writeTarget1 =
        new WriteTarget(TABLE1, "branch1", 42, 0, true, Sets.newHashSet(1, 2));
    // writeTarget2 has a different schema
    WriteTarget writeTarget2 = new WriteTarget(TABLE1, "branch1", 23, 0, true, Sets.newHashSet());
    // Different branch for writeTarget3
    WriteTarget writeTarget3 = new WriteTarget(TABLE1, "branch2", 23, 0, true, Sets.newHashSet());

    WriteResult writeResult1 = WriteResult.builder().addDataFiles(DATA_FILE).build();
    WriteResult writeResult2 = WriteResult.builder().addDataFiles(DATA_FILE_2).build();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId1 = 1;
    final int checkpointId2 = 2;

    byte[] deltaManifest1 =
        aggregator.writeToManifest(
            writeTarget1,
            Sets.newHashSet(new DynamicWriteResult(writeTarget1, writeResult1)),
            checkpointId1);

    CommitRequest<DynamicCommittable> commitRequest1 =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget1, deltaManifest1, jobId, operatorId, checkpointId1));

    byte[] deltaManifest2 =
        aggregator.writeToManifest(
            writeTarget2,
            Sets.newHashSet(new DynamicWriteResult(writeTarget2, writeResult2)),
            checkpointId1);

    CommitRequest<DynamicCommittable> commitRequest2 =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget2, deltaManifest2, jobId, operatorId, checkpointId1));

    byte[] deltaManifest3 =
        aggregator.writeToManifest(
            writeTarget3,
            Sets.newHashSet(new DynamicWriteResult(writeTarget3, writeResult2)),
            checkpointId2);

    CommitRequest<DynamicCommittable> commitRequest3 =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget3, deltaManifest3, jobId, operatorId, checkpointId2));

    boolean overwriteMode = false;
    int workerPoolSize = 1;
    String sinkId = "sinkId";
    UnregisteredMetricsGroup metricGroup = new UnregisteredMetricsGroup();
    DynamicCommitterMetrics committerMetrics = new DynamicCommitterMetrics(metricGroup);
    DynamicCommitter dynamicCommitter =
        new DynamicCommitter(
            CATALOG_EXTENSION.catalog(),
            Maps.newHashMap(),
            overwriteMode,
            workerPoolSize,
            sinkId,
            committerMetrics);

    dynamicCommitter.commit(Sets.newHashSet(commitRequest1, commitRequest2, commitRequest3));

    table.refresh();
    // Two committables, one for each snapshot / table / branch.
    assertThat(table.snapshots()).hasSize(2);

    Snapshot snapshot1 = Iterables.getFirst(table.snapshots(), null);
    assertThat(snapshot1.snapshotId()).isEqualTo(table.refs().get("branch1").snapshotId());
    assertThat(snapshot1.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "2")
                .put("added-records", "66")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId1)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "2")
                .put("total-delete-files", "0")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "0")
                .put("total-records", "66")
                .build());

    Snapshot snapshot2 = Iterables.get(table.snapshots(), 1);
    assertThat(snapshot2.snapshotId()).isEqualTo(table.refs().get("branch2").snapshotId());
    assertThat(snapshot2.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "1")
                .put("added-records", "24")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId2)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "1")
                .put("total-delete-files", "0")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "0")
                .put("total-records", "24")
                .build());
  }

  @Test
  void testTableBranchAtomicCommitWithFailures() throws Exception {
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table.snapshots()).isEmpty();

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    WriteTarget writeTarget1 = new WriteTarget(TABLE1, "branch", 42, 0, false, Sets.newHashSet());
    // writeTarget2 has a different schema
    WriteTarget writeTarget2 = new WriteTarget(TABLE1, "branch", 23, 0, false, Sets.newHashSet());
    WriteTarget writeTarget3 = new WriteTarget(TABLE1, "branch", 23, 0, false, Sets.newHashSet());

    WriteResult writeResult1 = WriteResult.builder().addDataFiles(DATA_FILE).build();
    WriteResult writeResult2 = WriteResult.builder().addDeleteFiles(DELETE_FILE).build();
    WriteResult writeResult3 = WriteResult.builder().addDataFiles(DATA_FILE).build();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId1 = 1;
    final int checkpointId2 = 2;

    byte[] deltaManifest1 =
        aggregator.writeToManifest(
            writeTarget1,
            Sets.newHashSet(new DynamicWriteResult(writeTarget1, writeResult1)),
            checkpointId1);

    CommitRequest<DynamicCommittable> commitRequest1 =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget1, deltaManifest1, jobId, operatorId, checkpointId1));

    byte[] deltaManifest2 =
        aggregator.writeToManifest(
            writeTarget2,
            Sets.newHashSet(new DynamicWriteResult(writeTarget2, writeResult2)),
            checkpointId2);

    CommitRequest<DynamicCommittable> commitRequest2 =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget2, deltaManifest2, jobId, operatorId, checkpointId2));

    byte[] deltaManifest3 =
        aggregator.writeToManifest(
            writeTarget3,
            Sets.newHashSet(new DynamicWriteResult(writeTarget3, writeResult3)),
            checkpointId2);

    CommitRequest<DynamicCommittable> commitRequest3 =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget3, deltaManifest3, jobId, operatorId, checkpointId2));

    boolean overwriteMode = false;
    int workerPoolSize = 1;
    String sinkId = "sinkId";
    UnregisteredMetricsGroup metricGroup = new UnregisteredMetricsGroup();
    DynamicCommitterMetrics committerMetrics = new DynamicCommitterMetrics(metricGroup);

    // Use special hook to fail during various states of the commit operation
    CommitHook commitHook = new FailBeforeAndAfterCommit();
    DynamicCommitter dynamicCommitter =
        new CommitHookEnabledDynamicCommitter(
            commitHook,
            CATALOG_EXTENSION.catalog(),
            Maps.newHashMap(),
            overwriteMode,
            workerPoolSize,
            sinkId,
            committerMetrics);

    ThrowingCallable commitExecutable =
        () ->
            dynamicCommitter.commit(
                Sets.newHashSet(commitRequest1, commitRequest2, commitRequest3));

    // First fail pre-commit
    assertThatThrownBy(commitExecutable);
    assertThat(FailBeforeAndAfterCommit.failedBeforeCommit).isTrue();

    // Second fail during commit
    assertThatThrownBy(commitExecutable);
    assertThat(FailBeforeAndAfterCommit.failedDuringCommit).isTrue();

    // Third fail after commit
    assertThatThrownBy(commitExecutable);
    assertThat(FailBeforeAndAfterCommit.failedAfterCommit).isTrue();

    // Finally commit must go through, although it is a NOOP because the third failure is directly
    // after the commit finished.
    try {
      commitExecutable.call();
    } catch (Throwable e) {
      fail("Should not have thrown an exception");
    }

    table.refresh();
    // Three committables, but only two snapshots! WriteResults from different checkpoints are not
    // getting
    // combined due to one writeResult2 containing a delete file.
    assertThat(table.snapshots()).hasSize(2);

    Snapshot snapshot1 = Iterables.getFirst(table.snapshots(), null);
    assertThat(snapshot1.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "1")
                .put("added-records", "42")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId1)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "1")
                .put("total-delete-files", "0")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "0")
                .put("total-records", "42")
                .build());

    Snapshot snapshot2 = Iterables.get(table.snapshots(), 1);
    assertThat(snapshot2.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "1")
                .put("added-records", "42")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId2)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "2")
                .put("total-delete-files", "1")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "24")
                .put("total-records", "84")
                .build());
  }

  @Test
  void testReplacePartitions() throws Exception {
    Table table1 = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table1.snapshots()).isEmpty();

    // Overwrite mode is active
    boolean overwriteMode = true;
    int workerPoolSize = 1;
    String sinkId = "sinkId";
    UnregisteredMetricsGroup metricGroup = new UnregisteredMetricsGroup();
    DynamicCommitterMetrics committerMetrics = new DynamicCommitterMetrics(metricGroup);
    DynamicCommitter dynamicCommitter =
        new DynamicCommitter(
            CATALOG_EXTENSION.catalog(),
            Maps.newHashMap(),
            overwriteMode,
            workerPoolSize,
            sinkId,
            committerMetrics);

    WriteTarget writeTarget =
        new WriteTarget(TABLE1, "branch", 42, 0, false, Sets.newHashSet(1, 2));

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    byte[] deltaManifest =
        aggregator.writeToManifest(
            writeTarget,
            Sets.newHashSet(
                new DynamicWriteResult(
                    writeTarget, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            checkpointId);

    CommitRequest<DynamicCommittable> commitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget, deltaManifest, jobId, operatorId, checkpointId));

    dynamicCommitter.commit(Sets.newHashSet(commitRequest));

    byte[] overwriteManifest =
        aggregator.writeToManifest(
            writeTarget,
            Sets.newHashSet(
                new DynamicWriteResult(
                    writeTarget, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            checkpointId + 1);

    CommitRequest<DynamicCommittable> overwriteRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(
                writeTarget, overwriteManifest, jobId, operatorId, checkpointId + 1));

    dynamicCommitter.commit(Sets.newHashSet(overwriteRequest));

    table1.refresh();
    assertThat(table1.snapshots()).hasSize(2);
    Snapshot latestSnapshot = Iterables.getLast(table1.snapshots());
    assertThat(latestSnapshot.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("replace-partitions", "true")
                .put("added-data-files", "1")
                .put("added-records", "42")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", String.valueOf(checkpointId + 1))
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "1")
                .put("total-delete-files", "0")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "0")
                .put("total-records", "42")
                .build());
  }

  interface CommitHook extends Serializable {
    void beforeCommit();

    void duringCommit();

    void afterCommit();
  }

  static class FailBeforeAndAfterCommit implements CommitHook {

    static boolean failedBeforeCommit;
    static boolean failedDuringCommit;
    static boolean failedAfterCommit;

    FailBeforeAndAfterCommit() {
      reset();
    }

    @Override
    public void beforeCommit() {
      if (!failedBeforeCommit) {
        failedBeforeCommit = true;
        throw new RuntimeException("Failing before commit");
      }
    }

    @Override
    public void duringCommit() {
      if (!failedDuringCommit) {
        failedDuringCommit = true;
        throw new RuntimeException("Failing during commit");
      }
    }

    @Override
    public void afterCommit() {
      if (!failedAfterCommit) {
        failedAfterCommit = true;
        throw new RuntimeException("Failing before commit");
      }
    }

    static void reset() {
      failedBeforeCommit = false;
      failedDuringCommit = false;
      failedAfterCommit = false;
    }
  }

  static class CommitHookEnabledDynamicCommitter extends DynamicCommitter {
    private final CommitHook commitHook;

    CommitHookEnabledDynamicCommitter(
        CommitHook commitHook,
        Catalog catalog,
        Map<String, String> snapshotProperties,
        boolean replacePartitions,
        int workerPoolSize,
        String sinkId,
        DynamicCommitterMetrics committerMetrics) {
      super(
          catalog, snapshotProperties, replacePartitions, workerPoolSize, sinkId, committerMetrics);
      this.commitHook = commitHook;
    }

    @Override
    public void commit(Collection<CommitRequest<DynamicCommittable>> commitRequests)
        throws IOException, InterruptedException {
      commitHook.beforeCommit();
      super.commit(commitRequests);
      commitHook.afterCommit();
    }

    @Override
    void commitOperation(
        Table table,
        String branch,
        SnapshotUpdate<?> operation,
        CommitSummary summary,
        String description,
        String newFlinkJobId,
        String operatorId,
        long checkpointId) {
      super.commitOperation(
          table, branch, operation, summary, description, newFlinkJobId, operatorId, checkpointId);
      commitHook.duringCommit();
    }
  }
}
