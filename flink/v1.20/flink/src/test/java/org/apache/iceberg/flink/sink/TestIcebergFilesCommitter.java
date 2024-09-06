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
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.flink.sink.IcebergFilesCommitter.MAX_CONTINUOUS_EMPTY_COMMITS;
import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIcebergFilesCommitter extends TestBase {
  private static final Configuration CONF = new Configuration();

  private File flinkManifestFolder;

  @Parameter(index = 1)
  private FileFormat format;

  @Parameter(index = 2)
  private String branch;

  @Parameters(name = "formatVersion = {0}, fileFormat = {1}, branch = {2}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {1, FileFormat.AVRO, "main"},
        new Object[] {2, FileFormat.AVRO, "test-branch"},
        new Object[] {1, FileFormat.PARQUET, "main"},
        new Object[] {2, FileFormat.PARQUET, "test-branch"},
        new Object[] {1, FileFormat.ORC, "main"},
        new Object[] {2, FileFormat.ORC, "test-branch"});
  }

  @Override
  @BeforeEach
  public void setupTable() throws IOException {
    flinkManifestFolder = Files.createTempDirectory(temp, "flink").toFile();

    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
    this.metadataDir = new File(tableDir, "metadata");
    assertThat(tableDir.delete()).isTrue();

    // Construct the iceberg table.
    table = create(SimpleDataUtil.SCHEMA, PartitionSpec.unpartitioned());

    table
        .updateProperties()
        .set(DEFAULT_FILE_FORMAT, format.name())
        .set(FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath())
        .set(MAX_CONTINUOUS_EMPTY_COMMITS, "1")
        .commit();
  }

  @TestTemplate
  public void testCommitTxnWithoutDataFiles() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    JobID jobId = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      SimpleDataUtil.assertTableRows(table, Lists.newArrayList(), branch);
      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // It's better to advance the max-committed-checkpoint-id in iceberg snapshot, so that the
      // future flink job failover won't fail.
      for (int i = 1; i <= 3; i++) {
        harness.snapshot(++checkpointId, ++timestamp);
        assertFlinkManifests(0);

        harness.notifyOfCompletedCheckpoint(checkpointId);
        assertFlinkManifests(0);

        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);
      }
    }
  }

  @TestTemplate
  public void testMaxContinuousEmptyCommits() throws Exception {
    table.updateProperties().set(MAX_CONTINUOUS_EMPTY_COMMITS, "3").commit();

    JobID jobId = new JobID();
    long checkpointId = 0;
    long timestamp = 0;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);

      for (int i = 1; i <= 9; i++) {
        harness.snapshot(++checkpointId, ++timestamp);
        harness.notifyOfCompletedCheckpoint(checkpointId);

        assertSnapshotSize(i / 3);
      }
    }
  }

  private FlinkWriteResult of(long checkpointId, DataFile dataFile) {
    return new FlinkWriteResult(checkpointId, WriteResult.builder().addDataFiles(dataFile).build());
  }

  @TestTemplate
  public void testCommitTxn() throws Exception {
    // Test with 3 continues checkpoints:
    //   1. snapshotState for checkpoint#1
    //   2. notifyCheckpointComplete for checkpoint#1
    //   3. snapshotState for checkpoint#2
    //   4. notifyCheckpointComplete for checkpoint#2
    //   5. snapshotState for checkpoint#3
    //   6. notifyCheckpointComplete for checkpoint#3
    long timestamp = 0;

    JobID jobID = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobID)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertSnapshotSize(0);

      List<RowData> rows = Lists.newArrayListWithExpectedSize(3);
      for (int i = 1; i <= 3; i++) {
        RowData rowData = SimpleDataUtil.createRowData(i, "hello" + i);
        DataFile dataFile = writeDataFile("data-" + i, ImmutableList.of(rowData));
        harness.processElement(of(i, dataFile), ++timestamp);
        rows.add(rowData);

        harness.snapshot(i, ++timestamp);
        assertFlinkManifests(1);

        harness.notifyOfCompletedCheckpoint(i);
        assertFlinkManifests(0);

        SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(rows), branch);
        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(jobID, operatorId, i);
        assertThat(SimpleDataUtil.latestSnapshot(table, branch).summary())
            .containsEntry("flink.test", TestIcebergFilesCommitter.class.getName());
      }
    }
  }

  @TestTemplate
  public void testOrderedEventsBetweenCheckpoints() throws Exception {
    // It's possible that two checkpoints happen in the following orders:
    //   1. snapshotState for checkpoint#1;
    //   2. snapshotState for checkpoint#2;
    //   3. notifyCheckpointComplete for checkpoint#1;
    //   4. notifyCheckpointComplete for checkpoint#2;
    long timestamp = 0;

    JobID jobId = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      long firstCheckpointId = 1;
      harness.processElement(of(firstCheckpointId, dataFile1), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 1. snapshotState for checkpoint#1
      harness.snapshot(firstCheckpointId, ++timestamp);
      assertFlinkManifests(1);

      RowData row2 = SimpleDataUtil.createRowData(2, "world");
      DataFile dataFile2 = writeDataFile("data-2", ImmutableList.of(row2));
      long secondCheckpointId = 2;
      harness.processElement(of(secondCheckpointId, dataFile2), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 2. snapshotState for checkpoint#2
      harness.snapshot(secondCheckpointId, ++timestamp);
      assertFlinkManifests(2);

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, firstCheckpointId);
      assertFlinkManifests(1);

      // 4. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, secondCheckpointId);
      assertFlinkManifests(0);
    }
  }

  @TestTemplate
  public void testDisorderedEventsBetweenCheckpoints() throws Exception {
    // It's possible that the two checkpoints happen in the following orders:
    //   1. snapshotState for checkpoint#1;
    //   2. snapshotState for checkpoint#2;
    //   3. notifyCheckpointComplete for checkpoint#2;
    //   4. notifyCheckpointComplete for checkpoint#1;
    long timestamp = 0;

    JobID jobId = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      long firstCheckpointId = 1;
      harness.processElement(of(firstCheckpointId, dataFile1), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 1. snapshotState for checkpoint#1
      harness.snapshot(firstCheckpointId, ++timestamp);
      assertFlinkManifests(1);

      RowData row2 = SimpleDataUtil.createRowData(2, "world");
      DataFile dataFile2 = writeDataFile("data-2", ImmutableList.of(row2));
      long secondCheckpointId = 2;
      harness.processElement(of(secondCheckpointId, dataFile2), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 2. snapshotState for checkpoint#2
      harness.snapshot(secondCheckpointId, ++timestamp);
      assertFlinkManifests(2);

      // 3. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, secondCheckpointId);
      assertFlinkManifests(0);

      // 4. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, secondCheckpointId);
      assertFlinkManifests(0);
    }
  }

  @TestTemplate
  public void testRecoveryFromValidSnapshot() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> expectedRows = Lists.newArrayList();
    OperatorSubtaskState snapshot;

    JobID jobId = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      RowData row = SimpleDataUtil.createRowData(1, "hello");
      expectedRows.add(row);
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row));

      harness.processElement(of(++checkpointId, dataFile1), ++timestamp);
      snapshot = harness.snapshot(checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row), branch);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);

      RowData row = SimpleDataUtil.createRowData(2, "world");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-2", ImmutableList.of(row));
      harness.processElement(of(++checkpointId, dataFile), ++timestamp);

      harness.snapshot(checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);
    }
  }

  @TestTemplate
  public void testRecoveryFromSnapshotWithoutCompletedNotification() throws Exception {
    // We've two steps in checkpoint: 1. snapshotState(ckp); 2. notifyCheckpointComplete(ckp). It's
    // possible that we
    // flink job will restore from a checkpoint with only step#1 finished.
    long checkpointId = 0;
    long timestamp = 0;
    OperatorSubtaskState snapshot;
    List<RowData> expectedRows = Lists.newArrayList();
    JobID jobId = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      RowData row = SimpleDataUtil.createRowData(1, "hello");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-1", ImmutableList.of(row));
      harness.processElement(of(++checkpointId, dataFile), ++timestamp);

      snapshot = harness.snapshot(checkpointId, ++timestamp);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);
      assertFlinkManifests(1);
    }

    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      // All flink manifests should be cleaned because it has committed the unfinished iceberg
      // transaction.
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);

      harness.snapshot(++checkpointId, ++timestamp);
      // Did not write any new record, so it won't generate new manifest.
      assertFlinkManifests(0);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);

      RowData row = SimpleDataUtil.createRowData(2, "world");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-2", ImmutableList.of(row));
      harness.processElement(of(++checkpointId, dataFile), ++timestamp);

      snapshot = harness.snapshot(checkpointId, ++timestamp);
      assertFlinkManifests(1);
    }

    // Redeploying flink job from external checkpoint.
    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(newJobId)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      // All flink manifests should be cleaned because it has committed the unfinished iceberg
      // transaction.
      assertFlinkManifests(0);

      assertMaxCommittedCheckpointId(newJobId, operatorId, -1);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);
      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(3);

      RowData row = SimpleDataUtil.createRowData(3, "foo");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-3", ImmutableList.of(row));
      harness.processElement(of(++checkpointId, dataFile), ++timestamp);

      harness.snapshot(checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(4);
      assertMaxCommittedCheckpointId(newJobId, operatorId, checkpointId);
    }
  }

  @TestTemplate
  public void testStartAnotherJobToWriteSameTable() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();

    JobID oldJobId = new JobID();
    OperatorID oldOperatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(oldJobId)) {
      harness.setup();
      harness.open();
      oldOperatorId = harness.getOperator().getOperatorID();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(oldJobId, oldOperatorId, -1L);

      for (int i = 1; i <= 3; i++) {
        rows.add(SimpleDataUtil.createRowData(i, "hello" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);
        harness.processElement(of(++checkpointId, dataFile), ++timestamp);
        harness.snapshot(checkpointId, ++timestamp);
        assertFlinkManifests(1);

        harness.notifyOfCompletedCheckpoint(checkpointId);
        assertFlinkManifests(0);

        SimpleDataUtil.assertTableRows(table, tableRows, branch);
        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(oldJobId, oldOperatorId, checkpointId);
      }
    }

    // The new started job will start with checkpoint = 1 again.
    checkpointId = 0;
    timestamp = 0;
    JobID newJobId = new JobID();
    OperatorID newOperatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(newJobId)) {
      harness.setup();
      harness.open();
      newOperatorId = harness.getOperator().getOperatorID();

      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(oldJobId, oldOperatorId, 3);
      assertMaxCommittedCheckpointId(newJobId, newOperatorId, -1);

      rows.add(SimpleDataUtil.createRowData(2, "world"));
      tableRows.addAll(rows);

      DataFile dataFile = writeDataFile("data-new-1", rows);
      harness.processElement(of(++checkpointId, dataFile), ++timestamp);
      harness.snapshot(checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);
      SimpleDataUtil.assertTableRows(table, tableRows, branch);
      assertSnapshotSize(4);
      assertMaxCommittedCheckpointId(newJobId, newOperatorId, checkpointId);
    }
  }

  @TestTemplate
  public void testMultipleJobsWriteSameTable() throws Exception {
    long timestamp = 0;
    List<RowData> tableRows = Lists.newArrayList();

    JobID[] jobs = new JobID[] {new JobID(), new JobID(), new JobID()};
    OperatorID[] operatorIds =
        new OperatorID[] {new OperatorID(), new OperatorID(), new OperatorID()};
    for (int i = 0; i < 20; i++) {
      int jobIndex = i % 3;
      int checkpointId = i / 3;
      JobID jobId = jobs[jobIndex];
      OperatorID operatorId = operatorIds[jobIndex];
      try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
          createStreamSink(jobId)) {
        harness.getStreamConfig().setOperatorID(operatorId);
        harness.setup();
        harness.open();

        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId == 0 ? -1 : checkpointId);

        List<RowData> rows = Lists.newArrayList(SimpleDataUtil.createRowData(i, "word-" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);
        harness.processElement(of(checkpointId + 1, dataFile), ++timestamp);
        harness.snapshot(checkpointId + 1, ++timestamp);
        assertFlinkManifests(1);

        harness.notifyOfCompletedCheckpoint(checkpointId + 1);
        assertFlinkManifests(0);
        SimpleDataUtil.assertTableRows(table, tableRows, branch);
        assertSnapshotSize(i + 1);
        assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId + 1);
      }
    }
  }

  @TestTemplate
  public void testMultipleSinksRecoveryFromValidSnapshot() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> expectedRows = Lists.newArrayList();
    OperatorSubtaskState snapshot1;
    OperatorSubtaskState snapshot2;

    JobID jobId = new JobID();
    OperatorID operatorId1 = new OperatorID();
    OperatorID operatorId2 = new OperatorID();
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness1 =
            createStreamSink(jobId);
        OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness2 =
            createStreamSink(jobId)) {
      harness1.getStreamConfig().setOperatorID(operatorId1);
      harness1.setup();
      harness1.open();
      harness2.getStreamConfig().setOperatorID(operatorId2);
      harness2.setup();
      harness2.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, operatorId1, -1L);
      assertMaxCommittedCheckpointId(jobId, operatorId2, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello1");
      expectedRows.add(row1);
      DataFile dataFile1 = writeDataFile("data-1-1", ImmutableList.of(row1));

      harness1.processElement(of(++checkpointId, dataFile1), ++timestamp);
      snapshot1 = harness1.snapshot(checkpointId, ++timestamp);

      RowData row2 = SimpleDataUtil.createRowData(1, "hello2");
      expectedRows.add(row2);
      DataFile dataFile2 = writeDataFile("data-1-2", ImmutableList.of(row2));

      harness2.processElement(of(checkpointId, dataFile2), ++timestamp);
      snapshot2 = harness2.snapshot(checkpointId, ++timestamp);
      assertFlinkManifests(2);

      // Only notify one of the committers
      harness1.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(1);

      // Only the first row is committed at this point
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId, operatorId1, checkpointId);
      assertMaxCommittedCheckpointId(jobId, operatorId2, -1);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness1 =
            createStreamSink(jobId);
        OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness2 =
            createStreamSink(jobId)) {
      harness1.getStreamConfig().setOperatorID(operatorId1);
      harness1.setup();
      harness1.initializeState(snapshot1);
      harness1.open();

      harness2.getStreamConfig().setOperatorID(operatorId2);
      harness2.setup();
      harness2.initializeState(snapshot2);
      harness2.open();

      // All flink manifests should be cleaned because it has committed the unfinished iceberg
      // transaction.
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(jobId, operatorId1, checkpointId);
      assertMaxCommittedCheckpointId(jobId, operatorId2, checkpointId);

      RowData row1 = SimpleDataUtil.createRowData(2, "world1");
      expectedRows.add(row1);
      DataFile dataFile1 = writeDataFile("data-2-1", ImmutableList.of(row1));

      harness1.processElement(of(++checkpointId, dataFile1), ++timestamp);
      harness1.snapshot(checkpointId, ++timestamp);

      RowData row2 = SimpleDataUtil.createRowData(2, "world2");
      expectedRows.add(row2);
      DataFile dataFile2 = writeDataFile("data-2-2", ImmutableList.of(row2));
      harness2.processElement(of(checkpointId, dataFile2), ++timestamp);
      harness2.snapshot(checkpointId, ++timestamp);

      assertFlinkManifests(2);

      harness1.notifyOfCompletedCheckpoint(checkpointId);
      harness2.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(4);
      assertMaxCommittedCheckpointId(jobId, operatorId1, checkpointId);
      assertMaxCommittedCheckpointId(jobId, operatorId2, checkpointId);
    }
  }

  @TestTemplate
  public void testBoundedStream() throws Exception {
    JobID jobId = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertFlinkManifests(0);
      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      List<RowData> tableRows = Lists.newArrayList(SimpleDataUtil.createRowData(1, "word-1"));

      DataFile dataFile = writeDataFile("data-1", tableRows);
      harness.processElement(of(IcebergStreamWriter.END_INPUT_CHECKPOINT_ID, dataFile), 1);
      ((BoundedOneInput) harness.getOneInputOperator()).endInput();

      assertFlinkManifests(0);
      SimpleDataUtil.assertTableRows(table, tableRows, branch);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(
          jobId, operatorId, IcebergStreamWriter.END_INPUT_CHECKPOINT_ID);
      assertThat(SimpleDataUtil.latestSnapshot(table, branch).summary())
          .containsEntry("flink.test", TestIcebergFilesCommitter.class.getName());
    }
  }

  @TestTemplate
  public void testFlinkManifests() throws Exception {
    long timestamp = 0;
    final long checkpoint = 10;

    JobID jobId = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      harness.processElement(of(checkpoint, dataFile1), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 1. snapshotState for checkpoint#1
      harness.snapshot(checkpoint, ++timestamp);
      List<Path> manifestPaths = assertFlinkManifests(1);
      Path manifestPath = manifestPaths.get(0);
      assertThat(manifestPath.getFileName())
          .asString()
          .isEqualTo(
              String.format("%s-%s-%05d-%d-%d-%05d.avro", jobId, operatorId, 0, 0, checkpoint, 1));

      // 2. Read the data files from manifests and assert.
      List<DataFile> dataFiles =
          FlinkManifestUtil.readDataFiles(
              createTestingManifestFile(manifestPath), table.io(), table.specs());
      assertThat(dataFiles).hasSize(1);
      TestHelpers.assertEquals(dataFile1, dataFiles.get(0));

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      assertFlinkManifests(0);
    }
  }

  @TestTemplate
  public void testDeleteFiles() throws Exception {
    assumeThat(formatVersion)
        .as("Only support equality-delete in format v2 or later.")
        .isGreaterThan(1);

    long timestamp = 0;
    long checkpoint = 10;

    JobID jobId = new JobID();
    OperatorID operatorId;
    FileAppenderFactory<RowData> appenderFactory = createDeletableAppenderFactory();

    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      RowData row1 = SimpleDataUtil.createInsert(1, "aaa");
      DataFile dataFile1 = writeDataFile("data-file-1", ImmutableList.of(row1));
      harness.processElement(of(checkpoint, dataFile1), ++timestamp);
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 1. snapshotState for checkpoint#1
      harness.snapshot(checkpoint, ++timestamp);
      List<Path> manifestPaths = assertFlinkManifests(1);
      Path manifestPath = manifestPaths.get(0);
      assertThat(manifestPath.getFileName())
          .asString()
          .isEqualTo(
              String.format("%s-%s-%05d-%d-%d-%05d.avro", jobId, operatorId, 0, 0, checkpoint, 1));

      // 2. Read the data files from manifests and assert.
      List<DataFile> dataFiles =
          FlinkManifestUtil.readDataFiles(
              createTestingManifestFile(manifestPath), table.io(), table.specs());
      assertThat(dataFiles).hasSize(1);
      TestHelpers.assertEquals(dataFile1, dataFiles.get(0));

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      assertFlinkManifests(0);

      // 4. process both data files and delete files.
      RowData row2 = SimpleDataUtil.createInsert(2, "bbb");
      DataFile dataFile2 = writeDataFile("data-file-2", ImmutableList.of(row2));

      RowData delete1 = SimpleDataUtil.createDelete(1, "aaa");
      DeleteFile deleteFile1 =
          writeEqDeleteFile(appenderFactory, "delete-file-1", ImmutableList.of(delete1));
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      harness.processElement(
          new FlinkWriteResult(
              ++checkpoint,
              WriteResult.builder().addDataFiles(dataFile2).addDeleteFiles(deleteFile1).build()),
          ++timestamp);

      // 5. snapshotState for checkpoint#2
      harness.snapshot(checkpoint, ++timestamp);
      assertFlinkManifests(2);

      // 6. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row2), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      assertFlinkManifests(0);
    }
  }

  @TestTemplate
  public void testCommitTwoCheckpointsInSingleTxn() throws Exception {
    assumeThat(formatVersion)
        .as("Only support equality-delete in format v2 or later.")
        .isGreaterThan(1);

    long timestamp = 0;
    long checkpoint = 10;

    JobID jobId = new JobID();
    OperatorID operatorId;
    FileAppenderFactory<RowData> appenderFactory = createDeletableAppenderFactory();

    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      RowData insert1 = SimpleDataUtil.createInsert(1, "aaa");
      RowData insert2 = SimpleDataUtil.createInsert(2, "bbb");
      RowData delete3 = SimpleDataUtil.createDelete(3, "ccc");
      DataFile dataFile1 = writeDataFile("data-file-1", ImmutableList.of(insert1, insert2));
      DeleteFile deleteFile1 =
          writeEqDeleteFile(appenderFactory, "delete-file-1", ImmutableList.of(delete3));
      harness.processElement(
          new FlinkWriteResult(
              checkpoint,
              WriteResult.builder().addDataFiles(dataFile1).addDeleteFiles(deleteFile1).build()),
          ++timestamp);

      // The 1th snapshotState.
      harness.snapshot(checkpoint, ++timestamp);

      RowData insert4 = SimpleDataUtil.createInsert(4, "ddd");
      RowData delete2 = SimpleDataUtil.createDelete(2, "bbb");
      DataFile dataFile2 = writeDataFile("data-file-2", ImmutableList.of(insert4));
      DeleteFile deleteFile2 =
          writeEqDeleteFile(appenderFactory, "delete-file-2", ImmutableList.of(delete2));
      harness.processElement(
          new FlinkWriteResult(
              ++checkpoint,
              WriteResult.builder().addDataFiles(dataFile2).addDeleteFiles(deleteFile2).build()),
          ++timestamp);

      // The 2nd snapshotState.
      harness.snapshot(checkpoint, ++timestamp);

      // Notify the 2nd snapshot to complete.
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(insert1, insert4), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      assertFlinkManifests(0);
      assertThat(table.snapshots()).hasSize(2);
    }
  }

  /**
   * The testcase is to simulate upserting to an Iceberg V2 table, and facing the following
   * scenario:
   *
   * <ul>
   *   <li>A specific row is updated
   *   <li>The prepareSnapshotPreBarrier triggered
   *   <li>Checkpoint failed for reasons outside of the Iceberg connector
   *   <li>The specific row is updated again in the second checkpoint as well
   *   <li>Second snapshot is triggered, and finished
   * </ul>
   *
   * <p>Previously the files from the 2 snapshots were committed in a single Iceberg commit, as a
   * results duplicate rows were created in the table.
   *
   * @throws Exception Exception
   */
  @TestTemplate
  public void testCommitMultipleCheckpointsForV2Table() throws Exception {
    assumeThat(formatVersion)
        .as("Only support equality-delete in format v2 or later.")
        .isGreaterThan(1);

    long timestamp = 0;
    long checkpoint = 10;

    JobID jobId = new JobID();
    OperatorID operatorId;

    FileAppenderFactory<RowData> appenderFactory =
        new FlinkAppenderFactory(
            table,
            table.schema(),
            FlinkSchemaUtil.convert(table.schema()),
            table.properties(),
            table.spec(),
            new int[] {table.schema().findField("id").fieldId()},
            table.schema(),
            null);

    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      RowData insert1 = null;
      RowData insert2 = null;
      for (int i = 1; i <= 3; i++) {
        insert1 = SimpleDataUtil.createInsert(1, "aaa" + i);
        insert2 = SimpleDataUtil.createInsert(2, "bbb" + i);
        DataFile dataFile = writeDataFile("data-file-" + i, ImmutableList.of(insert1, insert2));
        DeleteFile deleteFile =
            writeEqDeleteFile(
                appenderFactory, "delete-file-" + i, ImmutableList.of(insert1, insert2));
        harness.processElement(
            new FlinkWriteResult(
                ++checkpoint,
                WriteResult.builder().addDataFiles(dataFile).addDeleteFiles(deleteFile).build()),
            ++timestamp);
      }

      harness.snapshot(checkpoint, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(insert1, insert2), branch);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      assertFlinkManifests(0);
      assertThat(table.snapshots()).hasSize(3);
    }
  }

  @TestTemplate
  public void testSpecEvolution() throws Exception {
    long timestamp = 0;
    int checkpointId = 0;
    List<RowData> rows = Lists.newArrayList();
    JobID jobId = new JobID();

    OperatorID operatorId;
    OperatorSubtaskState snapshot;
    DataFile dataFile;
    int specId;

    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertSnapshotSize(0);

      checkpointId++;
      RowData rowData = SimpleDataUtil.createRowData(checkpointId, "hello" + checkpointId);
      // table unpartitioned
      dataFile = writeDataFile("data-" + checkpointId, ImmutableList.of(rowData));
      harness.processElement(of(checkpointId, dataFile), ++timestamp);
      rows.add(rowData);
      harness.snapshot(checkpointId, ++timestamp);

      specId =
          getStagingManifestSpecId(harness.getOperator().getOperatorStateBackend(), checkpointId);
      assertThat(specId).isEqualTo(table.spec().specId());

      harness.notifyOfCompletedCheckpoint(checkpointId);

      // Change partition spec
      table.refresh();
      PartitionSpec oldSpec = table.spec();
      table.updateSpec().addField("id").commit();

      checkpointId++;
      rowData = SimpleDataUtil.createRowData(checkpointId, "hello" + checkpointId);
      // write data with old partition spec
      dataFile = writeDataFile("data-" + checkpointId, ImmutableList.of(rowData), oldSpec, null);
      harness.processElement(of(checkpointId, dataFile), ++timestamp);
      rows.add(rowData);
      snapshot = harness.snapshot(checkpointId, ++timestamp);

      specId =
          getStagingManifestSpecId(harness.getOperator().getOperatorStateBackend(), checkpointId);
      assertThat(specId).isEqualTo(oldSpec.specId());

      harness.notifyOfCompletedCheckpoint(checkpointId);

      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(rows), branch);
      assertSnapshotSize(checkpointId);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> harness =
        createStreamSink(jobId)) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      SimpleDataUtil.assertTableRows(table, rows, branch);
      assertSnapshotSize(checkpointId);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);

      checkpointId++;
      RowData row = SimpleDataUtil.createRowData(checkpointId, "world" + checkpointId);
      StructLike partition = new PartitionData(table.spec().partitionType());
      partition.set(0, checkpointId);
      dataFile =
          writeDataFile("data-" + checkpointId, ImmutableList.of(row), table.spec(), partition);
      harness.processElement(of(checkpointId, dataFile), ++timestamp);
      rows.add(row);
      harness.snapshot(checkpointId, ++timestamp);
      assertFlinkManifests(1);

      specId =
          getStagingManifestSpecId(harness.getOperator().getOperatorStateBackend(), checkpointId);
      assertThat(specId).isEqualTo(table.spec().specId());

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, rows, branch);
      assertSnapshotSize(checkpointId);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);
    }
  }

  private int getStagingManifestSpecId(OperatorStateStore operatorStateStore, long checkPointId)
      throws Exception {
    ListState<SortedMap<Long, byte[]>> checkpointsState =
        operatorStateStore.getListState(IcebergFilesCommitter.buildStateDescriptor());
    NavigableMap<Long, byte[]> statedDataFiles =
        Maps.newTreeMap(checkpointsState.get().iterator().next());
    DeltaManifests deltaManifests =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            DeltaManifestsSerializer.INSTANCE, statedDataFiles.get(checkPointId));
    return deltaManifests.dataManifest().partitionSpecId();
  }

  private DeleteFile writeEqDeleteFile(
      FileAppenderFactory<RowData> appenderFactory, String filename, List<RowData> deletes)
      throws IOException {
    return SimpleDataUtil.writeEqDeleteFile(table, format, filename, appenderFactory, deletes);
  }

  private DeleteFile writePosDeleteFile(
      FileAppenderFactory<RowData> appenderFactory,
      String filename,
      List<Pair<CharSequence, Long>> positions)
      throws IOException {
    return SimpleDataUtil.writePosDeleteFile(table, format, filename, appenderFactory, positions);
  }

  private FileAppenderFactory<RowData> createDeletableAppenderFactory() {
    int[] equalityFieldIds =
        new int[] {
          table.schema().findField("id").fieldId(), table.schema().findField("data").fieldId()
        };
    return new FlinkAppenderFactory(
        table,
        table.schema(),
        FlinkSchemaUtil.convert(table.schema()),
        table.properties(),
        table.spec(),
        equalityFieldIds,
        table.schema(),
        null);
  }

  private ManifestFile createTestingManifestFile(Path manifestPath) {
    return new GenericManifestFile(
        manifestPath.toAbsolutePath().toString(),
        manifestPath.toFile().length(),
        0,
        ManifestContent.DATA,
        0,
        0,
        0L,
        0,
        0,
        0,
        0,
        0,
        0,
        null,
        null);
  }

  private List<Path> assertFlinkManifests(int expectedCount) throws IOException {
    List<Path> manifests =
        Files.list(flinkManifestFolder.toPath())
            .filter(p -> !p.toString().endsWith(".crc"))
            .collect(Collectors.toList());
    assertThat(manifests).hasSize(expectedCount);
    return manifests;
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(
        table,
        table.schema(),
        table.spec(),
        CONF,
        table.location(),
        format.addExtension(filename),
        rows);
  }

  private DataFile writeDataFile(
      String filename, List<RowData> rows, PartitionSpec spec, StructLike partition)
      throws IOException {
    return SimpleDataUtil.writeFile(
        table,
        table.schema(),
        spec,
        CONF,
        table.location(),
        format.addExtension(filename),
        rows,
        partition);
  }

  private void assertMaxCommittedCheckpointId(JobID jobID, OperatorID operatorID, long expectedId) {
    table.refresh();
    long actualId =
        SinkUtil.getMaxCommittedCheckpointId(
            table, jobID.toString(), operatorID.toString(), branch);
    assertThat(actualId).isEqualTo(expectedId);
  }

  private void assertSnapshotSize(int expectedSnapshotSize) {
    table.refresh();
    assertThat(table.snapshots()).hasSize(expectedSnapshotSize);
  }

  private OneInputStreamOperatorTestHarness<FlinkWriteResult, Void> createStreamSink(JobID jobID)
      throws Exception {
    TestOperatorFactory factory = TestOperatorFactory.of(table.location(), branch, table.spec());
    return new OneInputStreamOperatorTestHarness<>(factory, createEnvironment(jobID));
  }

  private static MockEnvironment createEnvironment(JobID jobID) {
    return new MockEnvironmentBuilder()
        .setTaskName("test task")
        .setManagedMemorySize(32 * 1024)
        .setInputSplitProvider(new MockInputSplitProvider())
        .setBufferSize(256)
        .setTaskConfiguration(new org.apache.flink.configuration.Configuration())
        .setExecutionConfig(new ExecutionConfig())
        .setMaxParallelism(16)
        .setJobID(jobID)
        .build();
  }

  private static class TestOperatorFactory extends AbstractStreamOperatorFactory<Void>
      implements OneInputStreamOperatorFactory<FlinkWriteResult, Void> {
    private final String tablePath;
    private final String branch;
    private final PartitionSpec spec;

    private TestOperatorFactory(String tablePath, String branch, PartitionSpec spec) {
      this.tablePath = tablePath;
      this.branch = branch;
      this.spec = spec;
    }

    private static TestOperatorFactory of(String tablePath, String branch, PartitionSpec spec) {
      return new TestOperatorFactory(tablePath, branch, spec);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Void>> T createStreamOperator(
        StreamOperatorParameters<Void> param) {
      IcebergFilesCommitter committer =
          new IcebergFilesCommitter(
              new TestTableLoader(tablePath),
              false,
              Collections.singletonMap("flink.test", TestIcebergFilesCommitter.class.getName()),
              ThreadPools.WORKER_THREAD_POOL_SIZE,
              branch,
              spec);
      committer.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) committer;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergFilesCommitter.class;
    }
  }
}
