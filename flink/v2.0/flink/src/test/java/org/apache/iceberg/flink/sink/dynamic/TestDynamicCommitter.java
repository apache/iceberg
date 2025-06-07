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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
        new WriteTarget(TABLE1, "branch", 42, 0, true, Lists.newArrayList(1, 2));
    WriteTarget writeTarget2 =
        new WriteTarget(TABLE1, "branch2", 43, 0, true, Lists.newArrayList(1, 2));
    WriteTarget writeTarget3 =
        new WriteTarget(TABLE2, "branch2", 43, 0, true, Lists.newArrayList(1, 2));

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader());
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    byte[] deltaManifest1 =
        aggregator.writeToManifest(
            writeTarget1,
            Lists.newArrayList(
                new DynamicWriteResult(
                    writeTarget1, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            0);
    byte[] deltaManifest2 =
        aggregator.writeToManifest(
            writeTarget2,
            Lists.newArrayList(
                new DynamicWriteResult(
                    writeTarget2, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            0);
    byte[] deltaManifest3 =
        aggregator.writeToManifest(
            writeTarget3,
            Lists.newArrayList(
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

    dynamicCommitter.commit(Lists.newArrayList(commitRequest1, commitRequest2, commitRequest3));

    table1.refresh();
    assertThat(table1.snapshots()).hasSize(2);
    Snapshot first = Iterables.getFirst(table1.snapshots(), null);
    assertThat(first.summary())
        .containsAllEntriesOf(
            (Map)
                ImmutableMap.builder()
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
            (Map)
                ImmutableMap.builder()
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
            (Map)
                ImmutableMap.builder()
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
        new WriteTarget(TABLE1, "branch", 42, 0, false, Lists.newArrayList(1, 2));

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader());
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    byte[] deltaManifest =
        aggregator.writeToManifest(
            writeTarget,
            Lists.newArrayList(
                new DynamicWriteResult(
                    writeTarget, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            checkpointId);

    CommitRequest<DynamicCommittable> commitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget, deltaManifest, jobId, operatorId, checkpointId));

    dynamicCommitter.commit(Lists.newArrayList(commitRequest));

    CommitRequest<DynamicCommittable> oldCommitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(
                writeTarget, deltaManifest, jobId, operatorId, checkpointId - 1));

    // Old commits requests shouldn't affect the result
    dynamicCommitter.commit(Lists.newArrayList(oldCommitRequest));

    table1.refresh();
    assertThat(table1.snapshots()).hasSize(1);
    Snapshot first = Iterables.getFirst(table1.snapshots(), null);
    assertThat(first.summary())
        .containsAllEntriesOf(
            (Map)
                ImmutableMap.builder()
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
        new WriteTarget(TABLE1, "branch", 42, 0, false, Lists.newArrayList(1, 2));

    DynamicWriteResultAggregator aggregator =
        new DynamicWriteResultAggregator(CATALOG_EXTENSION.catalogLoader());
    OneInputStreamOperatorTestHarness aggregatorHarness =
        new OneInputStreamOperatorTestHarness(aggregator);
    aggregatorHarness.open();

    final String jobId = JobID.generate().toHexString();
    final String operatorId = new OperatorID().toHexString();
    final int checkpointId = 10;

    byte[] deltaManifest =
        aggregator.writeToManifest(
            writeTarget,
            Lists.newArrayList(
                new DynamicWriteResult(
                    writeTarget, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            checkpointId);

    CommitRequest<DynamicCommittable> commitRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(writeTarget, deltaManifest, jobId, operatorId, checkpointId));

    dynamicCommitter.commit(Lists.newArrayList(commitRequest));

    byte[] overwriteManifest =
        aggregator.writeToManifest(
            writeTarget,
            Lists.newArrayList(
                new DynamicWriteResult(
                    writeTarget, WriteResult.builder().addDataFiles(DATA_FILE).build())),
            checkpointId + 1);

    CommitRequest<DynamicCommittable> overwriteRequest =
        new MockCommitRequest<>(
            new DynamicCommittable(
                writeTarget, overwriteManifest, jobId, operatorId, checkpointId + 1));

    dynamicCommitter.commit(Lists.newArrayList(overwriteRequest));

    table1.refresh();
    assertThat(table1.snapshots()).hasSize(2);
    Snapshot latestSnapshot = Iterables.getLast(table1.snapshots());
    assertThat(latestSnapshot.summary())
        .containsAllEntriesOf(
            (Map)
                ImmutableMap.builder()
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
}
