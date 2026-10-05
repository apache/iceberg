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
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.sink.CommitSummary;
import org.apache.iceberg.flink.sink.FlinkCommitMarkers;
import org.apache.iceberg.flink.sink.InterceptingCatalog;
import org.apache.iceberg.flink.sink.SinkTestUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDynamicCommitter {

  static final String DB = "db";
  static final String TABLE1 = "table";
  static final String TABLE2 = "table2";
  private static final String MAIN = SnapshotRef.MAIN_BRANCH;
  private static final TableKey MAIN_KEY = new TableKey(TABLE1, MAIN);

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION = new HadoopCatalogExtension(DB, TABLE1);

  Catalog catalog;

  final int cacheMaximumSize = 10;
  private final Writer writerA = Writer.generate();
  private final Writer writerB = Writer.generate();

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

  private static final Map<Integer, Collection<WriteResult>> WRITE_RESULT_BY_SPEC =
      Map.of(
          DATA_FILE.specId(),
          Lists.newArrayList(WriteResult.builder().addDataFiles(DATA_FILE).build()));
  private static final Map<Integer, Collection<WriteResult>> WRITE_RESULT_BY_SPEC_2 =
      Map.of(
          DATA_FILE_2.specId(),
          Lists.newArrayList(WriteResult.builder().addDataFiles(DATA_FILE_2).build()));

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

    TableKey tableKey1 = new TableKey(TABLE1, "branch");
    TableKey tableKey2 = new TableKey(TABLE1, "branch2");
    TableKey tableKey3 = new TableKey(TABLE2, "branch2");

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    byte[][] deltaManifests1 =
        aggregator.writeToManifests(tableKey1.tableName(), WRITE_RESULT_BY_SPEC, 0);
    byte[][] deltaManifests2 =
        aggregator.writeToManifests(tableKey2.tableName(), WRITE_RESULT_BY_SPEC, 0);
    byte[][] deltaManifests3 =
        aggregator.writeToManifests(tableKey3.tableName(), WRITE_RESULT_BY_SPEC, 0);

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    CommitRequest<DynamicCommittable> commitRequest1 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey1, deltaManifests1, jobId, operatorId, checkpointId));

    CommitRequest<DynamicCommittable> commitRequest2 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey2, deltaManifests2, jobId, operatorId, checkpointId));

    CommitRequest<DynamicCommittable> commitRequest3 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey3, deltaManifests3, jobId, operatorId, checkpointId));

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
  void testSkipsCommitRequestsForPreviousCheckpoints() throws Exception {
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

    TableKey tableKey = new TableKey(TABLE1, "branch");

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    byte[][] deltaManifests =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC, 0);

    CommitRequest<DynamicCommittable> commitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifests, jobId, operatorId, checkpointId));

    dynamicCommitter.commit(Sets.newHashSet(commitRequest));

    CommitRequest<DynamicCommittable> oldCommitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifests, jobId, operatorId, checkpointId - 1));

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
  void testSkipsAlreadyCommittedDataAfterJobIdChanges() throws Exception {
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table.snapshots()).isEmpty();

    boolean overwriteMode = false;
    int workerPoolSize = 1;
    String uidPrefix = "uidPrefix";
    UnregisteredMetricsGroup metricGroup = new UnregisteredMetricsGroup();
    JobID previousJobId = JobID.generate();
    DynamicCommitterMetrics previousCommitterMetrics = new DynamicCommitterMetrics(metricGroup);
    DynamicCommitter previousCommitter =
        new DynamicCommitter(
            CATALOG_EXTENSION.catalog(),
            Maps.newHashMap(),
            overwriteMode,
            workerPoolSize,
            uidPrefix,
            previousCommitterMetrics);

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    TableKey tableKey = new TableKey(TABLE1, "branch");
    // Operator id is stable across Flink job restarts, jobId is not.
    final String operatorId = new OperatorID().toHexString();
    final String previousJobIdStr = previousJobId.toHexString();
    final int previousCheckpointId = 10;

    byte[][] previousManifests =
        aggregator.writeToManifests(
            tableKey.tableName(), WRITE_RESULT_BY_SPEC, previousCheckpointId);

    DynamicCommittable previousCommittable =
        new DynamicCommittable(
            tableKey, previousManifests, previousJobIdStr, operatorId, previousCheckpointId);
    previousCommitter.commit(Sets.newHashSet(new MockCommitRequest<>(previousCommittable)));

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);

    JobID newJobId = JobID.generate();
    DynamicCommitterMetrics newCommitterMetrics = new DynamicCommitterMetrics(metricGroup);
    DynamicCommitter newCommitter =
        new DynamicCommitter(
            CATALOG_EXTENSION.catalog(),
            Maps.newHashMap(),
            overwriteMode,
            workerPoolSize,
            uidPrefix,
            newCommitterMetrics);

    final String newJobIdStr = newJobId.toHexString();
    final int newCheckpointId = previousCheckpointId + 1;

    byte[][] previousManifestsNew =
        aggregator.writeToManifests(
            tableKey.tableName(), WRITE_RESULT_BY_SPEC, previousCheckpointId);
    byte[][] newManifests =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC_2, newCheckpointId);

    CommitRequest<DynamicCommittable> replayedPreviousCommitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(
                tableKey,
                previousManifestsNew,
                previousJobIdStr,
                operatorId,
                previousCheckpointId));
    CommitRequest<DynamicCommittable> newCommitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(
                tableKey, newManifests, newJobIdStr, operatorId, newCheckpointId));

    newCommitter.commit(Sets.newHashSet(replayedPreviousCommitRequest, newCommitRequest));

    table.refresh();
    assertThat(table.snapshots()).hasSize(2);

    Snapshot first = Iterables.get(table.snapshots(), 0);
    assertThat(first.summary())
        .containsEntry("flink.job-id", previousJobIdStr)
        .containsEntry("flink.max-committed-checkpoint-id", String.valueOf(previousCheckpointId))
        .containsEntry("flink.operator-id", operatorId);

    Snapshot second = Iterables.get(table.snapshots(), 1);
    assertThat(second.summary())
        .containsEntry("flink.job-id", newJobIdStr)
        .containsEntry("flink.max-committed-checkpoint-id", String.valueOf(newCheckpointId))
        .containsEntry("flink.operator-id", operatorId);
  }

  @Test
  void testCommitsLandInCheckpointOrderAcrossJobIds() throws Exception {
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table.snapshots()).isEmpty();

    boolean overwriteMode = false;
    int workerPoolSize = 1;
    String sinkId = "sinkId";
    UnregisteredMetricsGroup metricGroup = new UnregisteredMetricsGroup();
    DynamicCommitterMetrics committerMetrics = new DynamicCommitterMetrics(metricGroup);
    DynamicCommitter committer =
        new DynamicCommitter(
            CATALOG_EXTENSION.catalog(),
            Maps.newHashMap(),
            overwriteMode,
            workerPoolSize,
            sinkId,
            committerMetrics);

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    TableKey tableKey = new TableKey(TABLE1, "branch");
    final String oldJobId = JobID.generate().toHexString();
    final String newJobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int oldCheckpointId = 1;
    final int newCheckpointId = 2;

    byte[][] oldManifests =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC, oldCheckpointId);
    byte[][] newManifests =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC_2, newCheckpointId);

    CommitRequest<DynamicCommittable> oldRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, oldManifests, oldJobId, operatorId, oldCheckpointId));
    CommitRequest<DynamicCommittable> newRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, newManifests, newJobId, operatorId, newCheckpointId));

    // Hand the requests in reversed order; the committer must still land them in checkpointId
    // order on the snapshot chain.
    committer.commit(Lists.newArrayList(newRequest, oldRequest));

    table.refresh();
    assertThat(table.snapshots()).hasSize(2);

    Snapshot first = Iterables.get(table.snapshots(), 0);
    assertThat(first.summary())
        .containsEntry("flink.job-id", oldJobId)
        .containsEntry("flink.max-committed-checkpoint-id", String.valueOf(oldCheckpointId))
        .containsEntry("flink.operator-id", operatorId);

    Snapshot second = Iterables.get(table.snapshots(), 1);
    assertThat(second.summary())
        .containsEntry("flink.job-id", newJobId)
        .containsEntry("flink.max-committed-checkpoint-id", String.valueOf(newCheckpointId))
        .containsEntry("flink.operator-id", operatorId);
    assertThat(second.parentId()).isEqualTo(first.snapshotId());
  }

  @Test
  void testCommitDeleteInDifferentFormatVersion() throws Exception {
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

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    TableKey tableKey = new TableKey(TABLE1, "branch");
    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    byte[][] deltaManifests =
        aggregator.writeToManifests(
            tableKey.tableName(),
            Map.of(
                DATA_FILE.specId(),
                Sets.newHashSet(
                    WriteResult.builder()
                        .addDataFiles(DATA_FILE)
                        .addDeleteFiles(DELETE_FILE)
                        .build())),
            checkpointId);

    CommitRequest<DynamicCommittable> commitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifests, jobId, operatorId, checkpointId));

    // Upgrade the table version
    UpdateProperties updateApi = table1.updateProperties();
    updateApi.set(
        TableProperties.FORMAT_VERSION, String.valueOf(TableUtil.formatVersion(table1) + 1));
    updateApi.commit();

    assertThatThrownBy(() -> dynamicCommitter.commit(Sets.newHashSet(commitRequest)))
        .hasMessage(
            "Can't add position delete file to the %s table. Concurrent table upgrade to V3 is not supported.",
            table1.name())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testCommitOnlyDataInDifferentFormatVersion() throws Exception {
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

    TableKey tableKey = new TableKey(TABLE1, "branch");

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    byte[][] deltaManifests =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC, checkpointId);

    CommitRequest<DynamicCommittable> commitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifests, jobId, operatorId, checkpointId));

    dynamicCommitter.commit(Sets.newHashSet(commitRequest));

    // Upgrade the table version
    UpdateProperties updateApi = table1.updateProperties();
    updateApi.set(
        TableProperties.FORMAT_VERSION, String.valueOf(TableUtil.formatVersion(table1) + 1));
    updateApi.commit();

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

    TableKey tableKey1 = new TableKey(TABLE1, "branch1");
    TableKey tableKey2 = new TableKey(TABLE1, "branch2");

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId1 = 1;
    final int checkpointId2 = 2;

    byte[][] deltaManifests1 =
        aggregator.writeToManifests(tableKey1.tableName(), WRITE_RESULT_BY_SPEC, checkpointId1);

    CommitRequest<DynamicCommittable> commitRequest1 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey1, deltaManifests1, jobId, operatorId, checkpointId1));

    byte[][] deltaManifests2 =
        aggregator.writeToManifests(tableKey1.tableName(), WRITE_RESULT_BY_SPEC_2, checkpointId1);

    CommitRequest<DynamicCommittable> commitRequest2 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey1, deltaManifests2, jobId, operatorId, checkpointId1));

    byte[][] deltaManifests3 =
        aggregator.writeToManifests(tableKey2.tableName(), WRITE_RESULT_BY_SPEC_2, checkpointId2);

    CommitRequest<DynamicCommittable> commitRequest3 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey2, deltaManifests3, jobId, operatorId, checkpointId2));

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

    Snapshot snapshot1 = table.snapshot(table.refs().get("branch1").snapshotId());
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

    Snapshot snapshot2 = table.snapshot(table.refs().get("branch2").snapshotId());
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

    TableKey tableKey = new TableKey(TABLE1, "branch");

    Map<Integer, Collection<WriteResult>> writeResults =
        Map.of(
            DELETE_FILE.specId(),
            Lists.newArrayList(WriteResult.builder().addDeleteFiles(DELETE_FILE).build()));

    byte[][] deltaManifests1 =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC, 0);
    byte[][] deltaManifests2 = aggregator.writeToManifests(tableKey.tableName(), writeResults, 0);
    byte[][] deltaManifests3 =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC_2, 0);

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId1 = 1;
    final int checkpointId2 = 2;
    final int checkpointId3 = 3;

    CommitRequest<DynamicCommittable> commitRequest1 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifests1, jobId, operatorId, checkpointId1));

    CommitRequest<DynamicCommittable> commitRequest2 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifests2, jobId, operatorId, checkpointId2));

    CommitRequest<DynamicCommittable> commitRequest3 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifests3, jobId, operatorId, checkpointId3));

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

    // Second fail before table update
    assertThatThrownBy(commitExecutable);
    assertThat(FailBeforeAndAfterCommit.failedBeforeCommitOperation).isTrue();

    // Third fail after table update
    assertThatThrownBy(commitExecutable);
    assertThat(FailBeforeAndAfterCommit.failedAfterCommitOperation).isTrue();

    // Fourth fail after commit
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
    assertThat(table.snapshots()).hasSize(3);

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
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId2)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "1")
                .put("total-delete-files", "1")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "24")
                .put("total-records", "42")
                .build());

    Snapshot snapshot3 = Iterables.get(table.snapshots(), 2);
    assertThat(snapshot3.summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "1")
                .put("added-records", "24")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", "" + checkpointId3)
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "2")
                .put("total-delete-files", "1")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "24")
                .put("total-records", "66")
                .build());
  }

  @Test
  void testCommitDeltaTxnWithAppendFiles() throws Exception {
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table.snapshots()).isEmpty();

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    TableKey tableKey = new TableKey(TABLE1, "branch1");
    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 1;

    byte[][] deltaManifest1 =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC, checkpointId);

    CommitRequest<DynamicCommittable> commitRequest1 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifest1, jobId, operatorId, checkpointId));

    byte[][] deltaManifest2 =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC_2, checkpointId);

    CommitRequest<DynamicCommittable> commitRequest2 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifest2, jobId, operatorId, checkpointId));

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

    dynamicCommitter.commit(Sets.newHashSet(commitRequest1, commitRequest2));

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);

    Snapshot snapshot = Iterables.getFirst(table.snapshots(), null);
    assertThat(snapshot.operation()).isEqualTo("append");
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

    TableKey tableKey = new TableKey(TABLE1, "branch");

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    byte[][] deltaManifests =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC, 0);

    CommitRequest<DynamicCommittable> commitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, deltaManifests, jobId, operatorId, checkpointId));

    dynamicCommitter.commit(Sets.newHashSet(commitRequest));

    byte[][] overwriteManifests =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC, 0);

    CommitRequest<DynamicCommittable> overwriteRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(
                tableKey, overwriteManifests, jobId, operatorId, checkpointId + 1));

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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testThrowsValidationExceptionOnDuplicateCommit(boolean overwriteMode) throws Exception {
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table.snapshots()).isEmpty();

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 1;
    final String branch = SnapshotRef.MAIN_BRANCH;

    TableKey tableKey = new TableKey(TABLE1, branch);
    byte[][] manifests =
        aggregator.writeToManifests(tableKey.tableName(), WRITE_RESULT_BY_SPEC, checkpointId);

    CommitRequest<DynamicCommittable> commitRequest1 =
        new MockCommitRequest<>(
            new DynamicCommittable(tableKey, manifests, jobId, operatorId, checkpointId));
    Collection<CommitRequest<DynamicCommittable>> commitRequests = Sets.newHashSet(commitRequest1);

    int workerPoolSize = 1;
    String sinkId = "sinkId";
    UnregisteredMetricsGroup metricGroup = new UnregisteredMetricsGroup();
    DynamicCommitterMetrics committerMetrics = new DynamicCommitterMetrics(metricGroup);

    CommitHook commitHook =
        new TestDynamicIcebergSink.DuplicateCommitHook(
            () ->
                new DynamicCommitter(
                    CATALOG_EXTENSION.catalog(),
                    Map.of(),
                    overwriteMode,
                    workerPoolSize,
                    sinkId,
                    committerMetrics));

    DynamicCommitter mainCommitter =
        new CommitHookEnabledDynamicCommitter(
            commitHook,
            CATALOG_EXTENSION.catalog(),
            Maps.newHashMap(),
            overwriteMode,
            workerPoolSize,
            sinkId,
            committerMetrics);

    mainCommitter.commit(commitRequests);

    // Only one commit should succeed
    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().summary())
        .containsAllEntriesOf(
            ImmutableMap.<String, String>builder()
                .put("added-data-files", "1")
                .put("added-records", "42")
                .put("changed-partition-count", "1")
                .put("flink.job-id", jobId)
                .put("flink.max-committed-checkpoint-id", String.valueOf(checkpointId))
                .put("flink.operator-id", operatorId)
                .put("total-data-files", "1")
                .put("total-delete-files", "0")
                .put("total-equality-deletes", "0")
                .put("total-files-size", "0")
                .put("total-position-deletes", "0")
                .put("total-records", "42")
                .build());
  }

  /**
   * Writer A goes idle while writer B keeps committing, and snapshot expiry removes every snapshot
   * between the head and A's last commit. A is then restored from a savepoint whose committable it
   * already committed. That committable must not land again.
   *
   * <p>With {@code manifestsKept}, the delta manifests survived the first commit (their deletion
   * only logs a warning on failure); otherwise they were deleted, as after a normal commit.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRestoreOfIdleWriterAfterExpiryDoesNotRecommit(boolean manifestsKept) throws Exception {
    DynamicCommittable lastOfA = null;
    DynamicCommitter committerA = newCommitter();
    for (long checkpointId = 1; checkpointId <= 3; checkpointId++) {
      lastOfA = committable(MAIN_KEY, writerA, checkpointId, "a-" + checkpointId);
      commit(committerA, lastOfA);
    }

    DynamicCommitter committerB = newCommitter();
    for (long checkpointId = 1; checkpointId <= 5; checkpointId++) {
      commit(committerB, committable(MAIN_KEY, writerB, checkpointId, "b-" + checkpointId));
    }

    expireAllButHeads();
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().summary()).containsEntry("flink.job-id", writerB.jobId());
    Set<String> filesBeforeRestore = dataFilePaths(table, MAIN);
    assertThat(filesBeforeRestore).hasSize(8);

    DynamicCommittable savepoint =
        manifestsKept ? committable(MAIN_KEY, writerA, 3, "a-3") : lastOfA;
    RecordingCommitRequest<DynamicCommittable> restored = new RecordingCommitRequest<>(savepoint);
    newCommitter().commit(Sets.newHashSet(restored));

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(dataFilePaths(table, MAIN)).isEqualTo(filesBeforeRestore);
    assertThat(restored.alreadyCommitted).isTrue();
  }

  /** The same, when a compaction replaced the idle writer's files before expiry. */
  @Test
  void testRestoreOfIdleWriterAfterCompactionAndExpiryDoesNotRecommit() throws Exception {
    DynamicCommitter committerA = newCommitter();
    for (long checkpointId = 1; checkpointId <= 3; checkpointId++) {
      commit(committerA, committable(MAIN_KEY, writerA, checkpointId, "a-" + checkpointId));
    }

    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    table
        .newRewrite()
        .deleteFile(dataFile("a-1"))
        .deleteFile(dataFile("a-2"))
        .deleteFile(dataFile("a-3"))
        .addFile(dataFile("c-1"))
        .commit();
    commit(newCommitter(), committable(MAIN_KEY, writerB, 1, "b-1"));
    expireAllButHeads();

    RecordingCommitRequest<DynamicCommittable> restored =
        new RecordingCommitRequest<>(committable(MAIN_KEY, writerA, 3, "a-3"));
    newCommitter().commit(Sets.newHashSet(restored));

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(dataFilePaths(table, MAIN)).containsExactlyInAnyOrder(path("c-1"), path("b-1"));
    assertThat(restored.alreadyCommitted).isTrue();
  }

  /**
   * A duplicate of the commit lands while it publishes, and expiry removes the duplicate's snapshot
   * before the commit retries. The validator must reject the retry from the refreshed table
   * properties, since the walk no longer reaches the duplicate.
   */
  @Test
  void testValidatorRejectsADuplicateThatLandedDuringThePublication() throws Exception {
    DynamicCommittable committable = committable(MAIN_KEY, writerA, 1, "a-1");
    InterceptingCatalog intercepting = interceptingCatalog();
    intercepting.onPublish(
        (delegate, base, metadata) -> {
          intercepting.onPublish(TableOperations::commit);
          commit(newCommitter(), committable);
          commit(newCommitter(), committable(MAIN_KEY, writerB, 1, "b-1"));
          expireAllButHeads();
          delegate.commit(base, metadata);
        });

    commit(newCommitter(intercepting), committable);

    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(intercepting.publications()).isEqualTo(1);
    assertThat(table.snapshots()).hasSize(1);
    assertThat(dataFilePaths(table, MAIN)).containsExactlyInAnyOrder(path("a-1"), path("b-1"));
  }

  /**
   * The commit is published, but the committer sees an unknown outcome. The marker landed with the
   * data, so a retry after the chain expired commits nothing.
   */
  @Test
  void testRetryAfterAnUnknownOutcomeThatPublished() throws Exception {
    DynamicCommittable committable = committable(MAIN_KEY, writerA, 1, "a-1");
    InterceptingCatalog intercepting = interceptingCatalog();
    intercepting.onPublish(
        (delegate, base, metadata) -> {
          intercepting.onPublish(TableOperations::commit);
          delegate.commit(base, metadata);
          throw new CommitStateUnknownException(new RuntimeException("Connection reset"));
        });

    assertThatThrownBy(() -> commit(newCommitter(intercepting), committable))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Connection reset");
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table.snapshots()).hasSize(1);
    assertThat(markers(table)).containsExactly(tuple(writerA.jobId(), 1L));

    commit(newCommitter(), committable(MAIN_KEY, writerB, 1, "b-1"));
    expireAllButHeads();
    RecordingCommitRequest<DynamicCommittable> retry = new RecordingCommitRequest<>(committable);
    newCommitter().commit(Sets.newHashSet(retry));

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(dataFilePaths(table, MAIN)).containsExactlyInAnyOrder(path("a-1"), path("b-1"));
    assertThat(retry.alreadyCommitted).isTrue();
  }

  /** The committer sees an unknown outcome, and nothing was published. A retry commits once. */
  @Test
  void testRetryAfterAnUnknownOutcomeThatDidNotPublish() throws Exception {
    DynamicCommittable committable = committable(MAIN_KEY, writerA, 1, "a-1");
    InterceptingCatalog intercepting = interceptingCatalog();
    intercepting.onPublish(
        (delegate, base, metadata) -> {
          intercepting.onPublish(TableOperations::commit);
          throw new CommitStateUnknownException(new RuntimeException("Timed out"));
        });

    assertThatThrownBy(() -> commit(newCommitter(intercepting), committable))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Timed out");
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(table.snapshots()).isEmpty();
    assertThat(markers(table)).isEmpty();

    commit(newCommitter(), committable);

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(dataFilePaths(table, MAIN)).containsExactly(path("a-1"));
    assertThat(markers(table)).containsExactly(tuple(writerA.jobId(), 1L));
  }

  /** Every publication fails: neither the snapshot nor the marker lands. */
  @Test
  void testFailedPublicationLeavesNoSnapshotAndNoMarker() {
    catalog
        .loadTable(TableIdentifier.of(TABLE1))
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.COMMIT_MIN_RETRY_WAIT_MS, "1")
        .set(TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "1")
        .commit();
    InterceptingCatalog intercepting = interceptingCatalog();
    intercepting.onPublish(
        (delegate, base, metadata) -> {
          throw new CommitFailedException("Conflict");
        });

    assertThatThrownBy(
            () -> commit(newCommitter(intercepting), committable(MAIN_KEY, writerA, 1, "a-1")))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Conflict");

    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(intercepting.publications()).isEqualTo(2);
    assertThat(table.snapshots()).isEmpty();
    assertThat(markers(table)).isEmpty();
  }

  /** Two writers' commits race: the retried one keeps the other's marker. */
  @Test
  void testRacingWritersKeepEachOthersMarkers() throws Exception {
    InterceptingCatalog intercepting = interceptingCatalog();
    intercepting.onPublish(
        (delegate, base, metadata) -> {
          intercepting.onPublish(TableOperations::commit);
          commit(newCommitter(), committable(MAIN_KEY, writerB, 4, "b-4"));
          delegate.commit(base, metadata);
        });

    commit(newCommitter(intercepting), committable(MAIN_KEY, writerA, 1, "a-1"));

    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    assertThat(intercepting.publications()).isEqualTo(2);
    assertThat(table.snapshots()).hasSize(2);
    assertThat(markers(table))
        .containsExactlyInAnyOrder(tuple(writerA.jobId(), 1L), tuple(writerB.jobId(), 4L));
  }

  /**
   * One writer commits the same checkpoint to main and to b1. b1 is either created before (as
   * {@code TableUpdater} does) or forked by the commit itself from main's head, which holds the
   * writer's main commit. Both land, and neither recommits after expiry.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSameCheckpointOnTwoBranches(boolean branchCreatedFirst) throws Exception {
    TableKey b1Key = new TableKey(TABLE1, "b1");
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    commit(newCommitter(), committable(MAIN_KEY, writerB, 1, "b-1"));
    if (branchCreatedFirst) {
      table.refresh();
      table.manageSnapshots().createBranch("b1").commit();
    }

    commit(newCommitter(), committable(MAIN_KEY, writerA, 7, "a-main-7"));
    commit(newCommitter(), committable(b1Key, writerA, 7, "a-b1-7"));

    table.refresh();
    assertThat(dataFilePaths(table, MAIN)).containsExactlyInAnyOrder(path("b-1"), path("a-main-7"));
    Set<String> expectedB1 = Sets.newHashSet(path("b-1"), path("a-b1-7"));
    if (!branchCreatedFirst) {
      // The commit forked b1 from main's head.
      expectedB1.add(path("a-main-7"));
    }

    assertThat(dataFilePaths(table, "b1")).isEqualTo(expectedB1);

    commit(newCommitter(), committable(MAIN_KEY, writerB, 2, "b-2"));
    commit(newCommitter(), committable(b1Key, writerB, 2, "b-b1-2"));
    expireAllButHeads();
    table.refresh();
    Set<String> mainFiles = dataFilePaths(table, MAIN);
    Set<String> b1Files = dataFilePaths(table, "b1");

    newCommitter()
        .commit(
            Sets.newHashSet(
                new MockCommitRequest<>(committable(MAIN_KEY, writerA, 7, "a-main-7")),
                new MockCommitRequest<>(committable(b1Key, writerA, 7, "a-b1-7"))));

    table.refresh();
    assertThat(dataFilePaths(table, MAIN)).isEqualTo(mainFiles);
    assertThat(dataFilePaths(table, "b1")).isEqualTo(b1Files);
  }

  /**
   * A table whose marker exists only in the summaries, as written before table-property markers:
   * the restore resolves it through the walk and records it, so it outlives the summaries.
   */
  @Test
  void testSummaryOnlyMarkerIsResolvedAndRecorded() throws Exception {
    DynamicCommittable committed = committable(MAIN_KEY, writerA, 3, "a-3");
    commit(newCommitter(), committed);
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    FlinkCommitMarkers.removeMarkers(table, marker -> true);
    assertThat(markers(table)).isEmpty();

    assertSkipped(committed);
    assertThat(markers(table)).containsExactly(tuple(writerA.jobId(), 3L));

    commit(newCommitter(), committable(MAIN_KEY, writerB, 1, "b-1"));
    expireAllButHeads();
    assertSkipped(committed);

    table.refresh();
    assertThat(dataFilePaths(table, MAIN)).containsExactlyInAnyOrder(path("a-3"), path("b-1"));
  }

  /**
   * An older committer of the same job (after a downgrade) commits a checkpoint past the table
   * property, with summaries only. The newer summary wins, and is recorded.
   */
  @Test
  void testNewerSummaryOnlyMarkerWinsOverAnOlderProperty() throws Exception {
    commit(newCommitter(), committable(MAIN_KEY, writerA, 3, "a-3"));
    Table table = catalog.loadTable(TableIdentifier.of(TABLE1));
    table
        .newAppend()
        .appendFile(dataFile("a-4"))
        .set("flink.job-id", writerA.jobId())
        .set("flink.operator-id", writerA.operatorId())
        .set("flink.max-committed-checkpoint-id", "4")
        .commit();
    assertThat(markers(table)).containsExactly(tuple(writerA.jobId(), 3L));

    assertSkipped(committable(MAIN_KEY, writerA, 4, "a-4"));
    assertThat(markers(table))
        .containsExactlyInAnyOrder(tuple(writerA.jobId(), 3L), tuple(writerA.jobId(), 4L));

    commit(newCommitter(), committable(MAIN_KEY, writerB, 1, "b-1"));
    expireAllButHeads();
    assertSkipped(committable(MAIN_KEY, writerA, 4, "a-4"));

    table.refresh();
    assertThat(dataFilePaths(table, MAIN))
        .containsExactlyInAnyOrder(path("a-3"), path("a-4"), path("b-1"));
  }

  /**
   * A batch of pending checkpoints, the last with an equality delete, fails after its first pending
   * checkpoint lands. The marker records exactly that checkpoint, and replaying the batch commits
   * only the rest, each in its own snapshot, so the delete applies to the earlier rows only.
   */
  @Test
  void testBatchFailingPartwayResumesAfterItsLastCommittedCheckpoint() throws Exception {
    String tableName = "table_v2";
    Table table =
        catalog.createTable(
            TableIdentifier.of(tableName),
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION, "2", TableProperties.COMMIT_NUM_RETRIES, "0"));
    TableKey tableKey = new TableKey(tableName, MAIN);
    commit(newCommitter(), committable(tableKey, writerA, 1, rowsOf("d-1")));
    List<DynamicCommittable> batch =
        List.of(
            committable(tableKey, writerA, 1, rowsOf("d-1")),
            committable(tableKey, writerA, 2, rowsOf("d-2")),
            committable(
                tableKey,
                writerA,
                3,
                WriteResult.builder()
                    .addDataFiles(plainDataFile("d-3"))
                    .addDeleteFiles(
                        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                            .ofEqualityDeletes(1)
                            .withPath(path("e-3"))
                            .withFileSizeInBytes(10)
                            .withRecordCount(1)
                            .build())
                    .build()));
    InterceptingCatalog intercepting = interceptingCatalog();
    intercepting.onPublish(
        (delegate, base, metadata) -> {
          if ("3"
              .equals(
                  metadata.currentSnapshot().summary().get("flink.max-committed-checkpoint-id"))) {
            throw new CommitFailedException("Conflict");
          }

          delegate.commit(base, metadata);
        });

    assertThatThrownBy(() -> newCommitter(intercepting).commit(requests(batch)))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Conflict");
    assertThat(markers(table)).containsExactly(tuple(writerA.jobId(), 2L));

    newCommitter().commit(requests(batch));

    table.refresh();
    assertThat(table.snapshots())
        .extracting(snapshot -> snapshot.summary().get("flink.max-committed-checkpoint-id"))
        .containsExactly("1", "2", "3");
    assertThat(markers(table)).containsExactly(tuple(writerA.jobId(), 3L));
    assertThat(deletesByDataFile(table))
        .isEqualTo(
            ImmutableMap.of(
                path("d-1"), Set.of(path("e-3")),
                path("d-2"), Set.of(path("e-3")),
                path("d-3"), Set.of()));
  }

  private record Writer(String jobId, String operatorId) {
    private static Writer generate() {
      return new Writer(JobID.generate().toHexString(), new OperatorID().toHexString());
    }
  }

  private DynamicCommitter newCommitter() {
    return newCommitter(CATALOG_EXTENSION.catalog());
  }

  private DynamicCommitter newCommitter(Catalog committerCatalog) {
    return new DynamicCommitter(
        committerCatalog,
        Maps.newHashMap(),
        false,
        1,
        "sinkId",
        new DynamicCommitterMetrics(new UnregisteredMetricsGroup()));
  }

  private InterceptingCatalog interceptingCatalog() {
    return new InterceptingCatalog(CATALOG_EXTENSION.warehouse());
  }

  private DynamicCommittable committable(
      TableKey tableKey, Writer writer, long checkpointId, String file) {
    return committable(
        tableKey, writer, checkpointId, WriteResult.builder().addDataFiles(dataFile(file)).build());
  }

  private DynamicCommittable committable(
      TableKey tableKey, Writer writer, long checkpointId, WriteResult writeResult) {
    try {
      DynamicWriteResultAggregator aggregator =
          new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader(), cacheMaximumSize);
      OneInputStreamOperatorTestHarness aggregatorHarness =
          new OneInputStreamOperatorTestHarness(aggregator);
      aggregatorHarness.open();
      byte[][] manifests =
          aggregator.writeToManifests(
              tableKey.tableName(),
              Map.of(PartitionSpec.unpartitioned().specId(), Lists.newArrayList(writeResult)),
              checkpointId);
      return new DynamicCommittable(
          tableKey, manifests, writer.jobId(), writer.operatorId(), checkpointId);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Commits from inside a publish hook, which can't throw checked exceptions.
  private static void commit(DynamicCommitter committer, DynamicCommittable committable) {
    try {
      committer.commit(Sets.newHashSet(new MockCommitRequest<>(committable)));
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static Set<CommitRequest<DynamicCommittable>> requests(
      List<DynamicCommittable> committables) {
    Set<CommitRequest<DynamicCommittable>> requests = Sets.newHashSet();
    committables.forEach(committable -> requests.add(new MockCommitRequest<>(committable)));
    return requests;
  }

  private void assertSkipped(DynamicCommittable committable) throws Exception {
    RecordingCommitRequest<DynamicCommittable> request = new RecordingCommitRequest<>(committable);
    newCommitter().commit(Sets.newHashSet(request));
    assertThat(request.alreadyCommitted).as("signalled as already committed").isTrue();
  }

  private static List<Tuple> markers(Table table) {
    table.refresh();
    List<Tuple> markers = Lists.newArrayList();
    FlinkCommitMarkers.markers(table)
        .forEach(marker -> markers.add(tuple(marker.jobId(), marker.checkpointId())));
    return markers;
  }

  private void expireAllButHeads() {
    SinkTestUtil.expireAllButHeads(catalog.loadTable(TableIdentifier.of(TABLE1)));
  }

  private static DataFile dataFile(String file) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .copy(DATA_FILE)
        .withPath(path(file))
        .build();
  }

  private static WriteResult rowsOf(String file) {
    return WriteResult.builder().addDataFiles(plainDataFile(file)).build();
  }

  // Without column stats, so that delete matching doesn't depend on them.
  private static DataFile plainDataFile(String file) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path(file))
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static String path(String file) {
    return "/path/to/" + file + ".parquet";
  }

  private static Set<String> dataFilePaths(Table table, String ref) throws IOException {
    Set<String> paths = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().useRef(ref).planFiles()) {
      for (FileScanTask task : tasks) {
        assertThat(paths.add(task.file().location())).as("listed once").isTrue();
      }
    }

    return paths;
  }

  private static Map<String, Set<String>> deletesByDataFile(Table table) throws IOException {
    Map<String, Set<String>> deletes = Maps.newHashMap();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        Set<String> paths = Sets.newHashSet();
        task.deletes().forEach(delete -> paths.add(delete.location()));
        assertThat(deletes.put(task.file().location(), paths)).as("listed once").isNull();
      }
    }

    return deletes;
  }

  private static class RecordingCommitRequest<T> extends MockCommitRequest<T> {
    private boolean alreadyCommitted;

    RecordingCommitRequest(T committable) {
      super(committable);
    }

    @Override
    public void signalAlreadyCommitted() {
      alreadyCommitted = true;
    }
  }

  interface CommitHook extends Serializable {
    default Collection<CommitRequest<DynamicCommittable>> beforeCommit(
        Collection<CommitRequest<DynamicCommittable>> commitRequests) {
      return commitRequests;
    }

    default void beforeCommitOperation() {}

    default void afterCommitOperation() {}

    default void afterCommit() {}
  }

  static class FailBeforeAndAfterCommit implements CommitHook {

    static boolean failedBeforeCommit;
    static boolean failedBeforeCommitOperation;
    static boolean failedAfterCommitOperation;
    static boolean failedAfterCommit;

    FailBeforeAndAfterCommit() {
      reset();
    }

    @Override
    public Collection<CommitRequest<DynamicCommittable>> beforeCommit(
        Collection<CommitRequest<DynamicCommittable>> requests) {
      if (!failedBeforeCommit) {
        failedBeforeCommit = true;
        throw new RuntimeException("Failing before commit");
      }

      return requests;
    }

    @Override
    public void beforeCommitOperation() {
      if (!failedBeforeCommitOperation) {
        failedBeforeCommitOperation = true;
        throw new RuntimeException("Failing before commit operation");
      }
    }

    @Override
    public void afterCommitOperation() {
      if (!failedAfterCommitOperation) {
        failedAfterCommitOperation = true;
        throw new RuntimeException("Failing after commit operation");
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
      failedBeforeCommitOperation = false;
      failedAfterCommitOperation = false;
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
      super.commit(commitHook.beforeCommit(commitRequests));
      commitHook.afterCommit();
    }

    @Override
    void commitOperation(
        Table table,
        Transaction transaction,
        String branch,
        SnapshotUpdate<?> operation,
        CommitSummary summary,
        String description,
        String newFlinkJobId,
        String operatorId,
        long checkpointId) {
      commitHook.beforeCommitOperation();
      super.commitOperation(
          table,
          transaction,
          branch,
          operation,
          summary,
          description,
          newFlinkJobId,
          operatorId,
          checkpointId);
      commitHook.afterCommitOperation();
    }
  }
}
