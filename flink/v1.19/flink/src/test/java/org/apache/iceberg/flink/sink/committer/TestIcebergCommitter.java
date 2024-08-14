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
package org.apache.iceberg.flink.sink.committer;

import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;
import static org.apache.iceberg.flink.sink.SinkTestUtil.extractAndAssertCommittableSummary;
import static org.apache.iceberg.flink.sink.SinkTestUtil.extractAndAssertCommittableWithLineage;
import static org.apache.iceberg.flink.sink.SinkTestUtil.transformsToStreamElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.sink.FlinkAppenderFactory;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.flink.sink.IcebergFilesCommitterMetrics;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ParameterizedTestExtension.class)
class TestIcebergCommitter extends TestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergCommitter.class);
  public static final String OPERATOR_ID = "flink-sink";
  @TempDir File temporaryFolder;

  @TempDir File flinkManifestFolder;

  private Table table;

  private TableLoader tableLoader;

  @Parameter(index = 1)
  private Boolean isStreamingMode;

  @Parameter(index = 2)
  private String branch;

  private final String jobId = "jobId";
  private final long dataFIleRowCount = 5L;

  private final TestCommittableMessageTypeSerializer committableMessageTypeSerializer =
      new TestCommittableMessageTypeSerializer();

  private final DataFile dataFileTest1 =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(0)
          .withMetrics(
              new Metrics(
                  dataFIleRowCount,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L), // value count
                  ImmutableMap.of(1, 0L), // null count
                  null,
                  ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(4L)) // upper bounds
                  ))
          .build();

  private final DataFile dataFileTest2 =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-2.parquet")
          .withFileSizeInBytes(0)
          .withMetrics(
              new Metrics(
                  dataFIleRowCount,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L), // value count
                  ImmutableMap.of(1, 0L), // null count
                  null,
                  ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(4L)) // upper bounds
                  ))
          .build();

  @SuppressWarnings("checkstyle:NestedForDepth")
  @Parameters(name = "formatVersion={0} isStreaming={1}, branch={2}")
  protected static List<Object> parameters() {
    List<Object> parameters = Lists.newArrayList();
    for (Boolean isStreamingMode : new Boolean[] {true, false}) {
      for (int formatVersion : new int[] {1, 2}) {
        parameters.add(new Object[] {formatVersion, isStreamingMode, SnapshotRef.MAIN_BRANCH});
        parameters.add(new Object[] {formatVersion, isStreamingMode, "test-branch"});
      }
    }
    return parameters;
  }

  @BeforeEach
  public void before() throws Exception {
    String warehouse = temporaryFolder.getAbsolutePath();

    String tablePath = warehouse.concat("/test");
    assertThat(new File(tablePath).mkdir()).as("Should create the table path correctly.").isTrue();

    Map<String, String> props =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            String.valueOf(formatVersion),
            FLINK_MANIFEST_LOCATION,
            flinkManifestFolder.getAbsolutePath(),
            IcebergCommitter.MAX_CONTINUOUS_EMPTY_COMMITS,
            "1");
    table = SimpleDataUtil.createTable(tablePath, props, false);
    tableLoader = TableLoader.fromHadoopTable(tablePath);
  }

  @TestTemplate
  public void testCommitTxnWithoutDataFiles() throws Exception {
    IcebergCommitter committer = getCommitter();
    SimpleDataUtil.assertTableRows(table, Lists.newArrayList(), branch);
    assertSnapshotSize(0);
    assertMaxCommittedCheckpointId(jobId, -1);

    for (long i = 1; i <= 3; i++) {
      Committer.CommitRequest<IcebergCommittable> commitRequest =
          buildCommitRequestFor(jobId, i, Lists.newArrayList());
      committer.commit(Lists.newArrayList(commitRequest));
      assertMaxCommittedCheckpointId(jobId, i);
      assertSnapshotSize((int) i);
    }
  }

  @TestTemplate
  public void testMxContinuousEmptyCommits() throws Exception {
    table.updateProperties().set(IcebergCommitter.MAX_CONTINUOUS_EMPTY_COMMITS, "3").commit();
    IcebergCommitter committer = getCommitter();
    for (int i = 1; i <= 9; i++) {
      Committer.CommitRequest<IcebergCommittable> commitRequest =
          buildCommitRequestFor(jobId, i, Lists.newArrayList());
      committer.commit(Lists.newArrayList(commitRequest));
      assertFlinkManifests(0);
      assertSnapshotSize(i / 3);
    }
  }

  @TestTemplate
  public void testCommitTxn() throws Exception {
    IcebergCommitter committer = getCommitter();
    assertSnapshotSize(0);
    List<RowData> rows = Lists.newArrayListWithExpectedSize(3);
    for (int i = 1; i <= 3; i++) {
      RowData rowData = SimpleDataUtil.createRowData(i, "hello" + i);
      DataFile dataFile = writeDataFile("data-" + i, ImmutableList.of(rowData));
      rows.add(rowData);
      WriteResult writeResult = of(dataFile);
      Committer.CommitRequest<IcebergCommittable> commitRequest =
          buildCommitRequestFor(jobId, i, Lists.newArrayList(writeResult));
      committer.commit(Lists.newArrayList(commitRequest));
      assertFlinkManifests(0);
      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(rows), branch);
      assertSnapshotSize(i);
      assertMaxCommittedCheckpointId(jobId, i);
      Map<String, String> summary = SimpleDataUtil.latestSnapshot(table, branch).summary();
      assertThat(summary)
          .containsEntry(
              "flink.test", "org.apache.iceberg.flink.sink.committer.TestIcebergCommitter")
          .containsEntry("added-data-files", "1")
          .containsEntry("flink.operator-id", OPERATOR_ID)
          .containsEntry("flink.job-id", "jobId");
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
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        harness = getTestHarness()) {

      harness.open();

      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      processElement(jobId, 1, harness, 1, OPERATOR_ID, dataFile1);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 1. snapshotState for checkpoint#1
      long firstCheckpointId = 1;
      harness.snapshot(firstCheckpointId, ++timestamp);
      assertFlinkManifests(1);

      RowData row2 = SimpleDataUtil.createRowData(2, "world");
      DataFile dataFile2 = writeDataFile("data-2", ImmutableList.of(row2));
      processElement(jobId, 2, harness, 1, OPERATOR_ID, dataFile2);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 2. snapshotState for checkpoint#2
      long secondCheckpointId = 2;
      OperatorSubtaskState snapshot = harness.snapshot(secondCheckpointId, ++timestamp);
      assertFlinkManifests(2);

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);
      assertMaxCommittedCheckpointId(jobId, firstCheckpointId);
      assertFlinkManifests(1);

      // 4. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2), branch);
      assertMaxCommittedCheckpointId(jobId, secondCheckpointId);
      assertFlinkManifests(0);
    }
  }

  @TestTemplate
  public void testDisorderedEventsBetweenCheckpoints() throws Exception {
    // It's possible that two checkpoints happen in the following orders:
    //   1. snapshotState for checkpoint#1;
    //   2. snapshotState for checkpoint#2;
    //   3. notifyCheckpointComplete for checkpoint#2;
    //   4. notifyCheckpointComplete for checkpoint#1;

    long timestamp = 0;
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        harness = getTestHarness()) {

      harness.open();
      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      processElement(jobId, 1, harness, 1, OPERATOR_ID, dataFile1);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 1. snapshotState for checkpoint#1
      long firstCheckpointId = 1;
      harness.snapshot(firstCheckpointId, ++timestamp);
      assertFlinkManifests(1);

      RowData row2 = SimpleDataUtil.createRowData(2, "world");
      DataFile dataFile2 = writeDataFile("data-2", ImmutableList.of(row2));
      processElement(jobId, 2, harness, 1, OPERATOR_ID, dataFile2);
      assertMaxCommittedCheckpointId(jobId, -1L);

      // 2. snapshotState for checkpoint#2
      long secondCheckpointId = 2;
      harness.snapshot(secondCheckpointId, ++timestamp);
      assertFlinkManifests(2);

      // 3. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2), branch);
      assertMaxCommittedCheckpointId(jobId, secondCheckpointId);
      assertFlinkManifests(0);

      // 4. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2), branch);
      assertMaxCommittedCheckpointId(jobId, secondCheckpointId);
      assertFlinkManifests(0);
    }
  }

  @TestTemplate
  public void testEmitCommittables() throws Exception {
    TestCommitter committer = new TestCommitter(tableLoader);
    IcebergSink sink =
        spy(IcebergSink.forRowData(null).table(table).tableLoader(tableLoader).build());
    doReturn(committer).when(sink).createCommitter(any());

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        testHarness =
            new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(sink, false, true))) {
      testHarness.open();

      WriteResult writeResult = WriteResult.builder().build();

      IcebergCommittable commit =
          new IcebergCommittable(
              buildIcebergWriteAggregator(jobId, OPERATOR_ID)
                  .writeToManifest(Lists.newArrayList(writeResult), 1L),
              jobId,
              OPERATOR_ID,
              1L);

      CommittableSummary<IcebergCommittable> committableSummary =
          new CommittableSummary<>(1, 1, 1L, 1, 1, 0);
      testHarness.processElement(new StreamRecord<>(committableSummary));
      CommittableWithLineage<IcebergCommittable> committableWithLineage =
          new CommittableWithLineage<>(commit, 1L, 1);
      testHarness.processElement(new StreamRecord<>(committableWithLineage));

      // Trigger commit
      testHarness.notifyOfCompletedCheckpoint(1);

      assertThat(committer.getSuccessfulCommits()).isEqualTo(1);
      List<StreamElement> output = transformsToStreamElement(testHarness.getOutput());
      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output.get(0)))
          .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
          .hasOverallCommittables(committableSummary.getNumberOfCommittables())
          .hasPendingCommittables(0);

      SinkV2Assertions.assertThat(extractAndAssertCommittableWithLineage(output.get(1)))
          .hasCheckpointId(1L)
          .hasSubtaskId(0)
          .hasCommittable(commit);
    }
  }

  @TestTemplate
  public void testSingleCommit() throws Exception {
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        testHarness = getTestHarness()) {
      testHarness.open();

      long checkpointId = 1;

      RowData row1 = SimpleDataUtil.createRowData(1, "hello1");
      DataFile dataFile1 = writeDataFile("data-1-1", ImmutableList.of(row1));
      CommittableSummary<IcebergCommittable> committableSummary =
          processElement(jobId, checkpointId, testHarness, 1, OPERATOR_ID, dataFile1);

      // Trigger commit
      testHarness.notifyOfCompletedCheckpoint(checkpointId);

      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId, 1L);

      List<StreamElement> output = transformsToStreamElement(testHarness.getOutput());

      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);
      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output.get(0)))
          .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
          .hasOverallCommittables(committableSummary.getNumberOfCommittables())
          .hasPendingCommittables(0);

      SinkV2Assertions.assertThat(extractAndAssertCommittableWithLineage(output.get(1)))
          .hasSubtaskId(0)
          .hasCheckpointId(checkpointId);
    }

    table.refresh();
    Snapshot currentSnapshot = table.snapshot(branch);

    assertThat(currentSnapshot.summary())
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, "1")
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1");
  }

  /** The data was not committed in the previous job. */
  @TestTemplate
  public void testStateRestoreFromPreJob() throws Exception {
    String jobId1 = "jobId1";
    OperatorSubtaskState snapshot;

    // We cannot test a different checkpoint thant 0 because when using the OperatorTestHarness
    // for recovery the lastCompleted checkpoint is always reset to 0.
    // see: https://github.com/apache/iceberg/issues/10942
    long checkpointId = 0;
    long timestamp = 0;
    CommittableSummary<IcebergCommittable> committableSummary;

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        preJobTestHarness = getTestHarness()) {

      preJobTestHarness.open();

      committableSummary =
          processElement(jobId1, checkpointId, preJobTestHarness, 1, OPERATOR_ID, dataFileTest1);

      snapshot = preJobTestHarness.snapshot(checkpointId, ++timestamp);

      assertThat(preJobTestHarness.getOutput()).isEmpty();
    }

    assertSnapshotSize(0);
    assertMaxCommittedCheckpointId(jobId1, -1L);

    String jobId2 = "jobId2";
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        restored = getTestHarness()) {
      restored.setup(committableMessageTypeSerializer);
      restored.initializeState(snapshot);
      restored.open();

      // Previous committables are immediately committed if possible
      List<StreamElement> output = transformsToStreamElement(restored.getOutput());
      assertThat(output).hasSize(2);

      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output.get(0)))
          .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
          .hasOverallCommittables(committableSummary.getNumberOfCommittables())
          .hasPendingCommittables(0);

      SinkV2Assertions.assertThat(extractAndAssertCommittableWithLineage(output.get(1)))
          .hasCheckpointId(0L)
          .hasSubtaskId(0);

      table.refresh();

      Snapshot currentSnapshot = table.snapshot(branch);

      assertThat(currentSnapshot.summary())
          .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, String.valueOf(dataFIleRowCount))
          .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
          .containsEntry("flink.job-id", jobId1);

      checkpointId++;
      CommittableSummary<IcebergCommittable> committableSummary2 =
          processElement(jobId2, checkpointId, restored, 1, OPERATOR_ID, dataFileTest2);

      // Trigger commit
      restored.notifyOfCompletedCheckpoint(checkpointId);

      List<StreamElement> output2 = transformsToStreamElement(restored.getOutput());
      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output2.get(0)))
          .hasFailedCommittables(committableSummary2.getNumberOfFailedCommittables())
          .hasOverallCommittables(committableSummary2.getNumberOfCommittables())
          .hasPendingCommittables(0);

      SinkV2Assertions.assertThat(extractAndAssertCommittableWithLineage(output2.get(1)))
          .hasCheckpointId(0L)
          .hasSubtaskId(0);
    }

    assertSnapshotSize(2);
    assertMaxCommittedCheckpointId(jobId2, 1);

    table.refresh();
    Snapshot currentSnapshot2 = table.snapshot(branch);

    assertThat(currentSnapshot2.summary())
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, String.valueOf(dataFIleRowCount * 2))
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "2")
        .containsEntry("flink.job-id", jobId2);
  }

  /** The data was committed in the previous job. */
  @TestTemplate
  public void testStateRestoreFromPreJob2() throws Exception {
    String jobId1 = "jobId1";
    OperatorSubtaskState snapshot;

    // We cannot test a different checkpoint thant 0 because when using the OperatorTestHarness
    // for recovery the lastCompleted checkpoint is always reset to 0.
    // see: https://github.com/apache/iceberg/issues/10942
    long checkpointId = 0;

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        preJobTestHarness = getTestHarness()) {

      preJobTestHarness.open();

      CommittableSummary<IcebergCommittable> committableSummary =
          processElement(jobId1, checkpointId, preJobTestHarness, 1, OPERATOR_ID, dataFileTest1);

      assertFlinkManifests(1);
      snapshot = preJobTestHarness.snapshot(checkpointId, 2L);
      // commit snapshot
      preJobTestHarness.notifyOfCompletedCheckpoint(checkpointId);

      List<StreamElement> output = transformsToStreamElement(preJobTestHarness.getOutput());
      assertThat(output).hasSize(2);

      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output.get(0)))
          .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
          .hasOverallCommittables(committableSummary.getNumberOfCommittables())
          .hasPendingCommittables(0);

      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId1, checkpointId);
    }

    table.refresh();
    long preJobSnapshotId = table.snapshot(branch).snapshotId();

    String jobId2 = "jobId2";
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        restored = getTestHarness()) {
      restored.setup();
      restored.initializeState(snapshot);
      restored.open();

      // Makes sure that data committed in the previous job is available in this job
      List<StreamElement> output2 = transformsToStreamElement(restored.getOutput());
      assertThat(output2).hasSize(2);

      table.refresh();
      long restoredSnapshotId = table.snapshot(branch).snapshotId();

      assertThat(restoredSnapshotId)
          .as("The table does not generate a new snapshot without data being committed.")
          .isEqualTo(preJobSnapshotId);

      assertThat(table.snapshot(branch).summary())
          .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, String.valueOf(dataFIleRowCount))
          .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
          .containsEntry("flink.job-id", jobId1);

      // Commit new data file
      checkpointId = 1;
      CommittableSummary<IcebergCommittable> committableSummary2 =
          processElement(jobId2, checkpointId, restored, 1, OPERATOR_ID, dataFileTest2);

      // Trigger commit
      restored.notifyOfCompletedCheckpoint(checkpointId);

      List<StreamElement> output3 = transformsToStreamElement(restored.getOutput());
      assertThat(output3).hasSize(4);
      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output3.get(0)))
          .hasFailedCommittables(committableSummary2.getNumberOfFailedCommittables())
          .hasOverallCommittables(committableSummary2.getNumberOfCommittables())
          .hasPendingCommittables(0);
    }

    assertSnapshotSize(2);
    assertMaxCommittedCheckpointId(jobId2, 1L);

    table.refresh();
    Snapshot currentSnapshot2 = table.snapshot(branch);
    assertThat(Long.parseLong(currentSnapshot2.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP)))
        .isEqualTo(dataFIleRowCount * 2);

    assertThat(currentSnapshot2.summary())
        .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, String.valueOf(dataFIleRowCount * 2))
        .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "2")
        .containsEntry("flink.job-id", jobId2);
  }

  @TestTemplate
  public void testStateRestoreFromCurrJob() throws Exception {
    String jobId1 = "jobId1";
    CommittableSummary<IcebergCommittable> committableSummary;
    OperatorSubtaskState snapshot;

    // We cannot test a different checkpoint thant 0 because when using the OperatorTestHarness
    // for recovery the lastCompleted checkpoint is always reset to 0.
    // see: https://github.com/apache/iceberg/issues/10942
    long checkpointId = 0;

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        testHarness = getTestHarness()) {

      testHarness.open();

      committableSummary =
          processElement(jobId1, checkpointId, testHarness, 1, OPERATOR_ID, dataFileTest1);
      snapshot = testHarness.snapshot(checkpointId, 2L);

      assertThat(testHarness.getOutput()).isEmpty();
    }

    assertSnapshotSize(0);
    assertMaxCommittedCheckpointId(jobId1, -1L);

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        restored = getTestHarness()) {

      restored.setup(committableMessageTypeSerializer);

      restored.initializeState(snapshot);
      restored.open();

      // Previous committables are immediately committed if possible
      List<StreamElement> output = transformsToStreamElement(restored.getOutput());
      assertThat(output).hasSize(2);

      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output.get(0)))
          .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
          .hasOverallCommittables(committableSummary.getNumberOfCommittables())
          .hasPendingCommittables(0);

      table.refresh();
      Snapshot currentSnapshot = table.snapshot(branch);

      assertThat(currentSnapshot.summary())
          .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, String.valueOf(dataFIleRowCount))
          .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "1")
          .containsEntry("flink.job-id", jobId1);

      String jobId2 = "jobId2";
      checkpointId = 1;
      CommittableSummary<IcebergCommittable> committableSummary2 =
          processElement(jobId2, checkpointId, restored, 1, OPERATOR_ID, dataFileTest2);

      // Trigger commit
      restored.notifyOfCompletedCheckpoint(checkpointId);

      List<StreamElement> output2 = transformsToStreamElement(restored.getOutput());
      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output2.get(0)))
          .hasFailedCommittables(committableSummary2.getNumberOfFailedCommittables())
          .hasOverallCommittables(committableSummary2.getNumberOfCommittables())
          .hasPendingCommittables(0);
      restored.close();

      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(jobId2, 1L);

      table.refresh();
      Snapshot currentSnapshot2 = table.snapshot(branch);
      assertThat(currentSnapshot2.summary())
          .containsEntry(SnapshotSummary.TOTAL_RECORDS_PROP, String.valueOf(dataFIleRowCount * 2))
          .containsEntry(SnapshotSummary.TOTAL_DATA_FILES_PROP, "2")
          .containsEntry("flink.job-id", jobId2);
    }
  }

  @TestTemplate
  public void testRecoveryFromSnapshotWithoutCompletedNotification() throws Exception {
    // We've two steps in checkpoint: 1. snapshotState(ckp); 2. notifyCheckpointComplete(ckp).
    // The Flink job should be able to restore from a checkpoint with only step#1 finished.

    // We cannot test a different checkpoint thant 0 because when using the OperatorTestHarness
    // for recovery the lastCompleted checkpoint is always reset to 0.
    // see: https://github.com/apache/iceberg/issues/10942
    long checkpointId = 0;
    long timestamp = 0;
    OperatorSubtaskState snapshot;
    List<RowData> expectedRows = Lists.newArrayList();

    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        harness = getTestHarness()) {

      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(jobId, -1L);

      RowData row = SimpleDataUtil.createRowData(1, "hello");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-1", ImmutableList.of(row));
      processElement(jobId, checkpointId, harness, 1, operatorId.toString(), dataFile);

      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(), branch);
      assertMaxCommittedCheckpointId(jobId, -1L);
      assertFlinkManifests(1);
    }

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        harness = getTestHarness()) {

      harness.getStreamConfig().setOperatorID(operatorId);
      harness.initializeState(snapshot);
      harness.open();

      // All flink manifests should be cleaned because it has committed the unfinished iceberg
      // transaction.
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertMaxCommittedCheckpointId(jobId, operatorId.toString(), 0L);

      harness.snapshot(++checkpointId, ++timestamp);
      // Did not write any new record, so it won't generate new manifest.
      assertFlinkManifests(0);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(1);

      assertMaxCommittedCheckpointId(jobId, operatorId.toString(), 0);

      RowData row = SimpleDataUtil.createRowData(2, "world");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-2", ImmutableList.of(row));
      processElement(jobId, checkpointId, harness, 1, operatorId.toString(), dataFile);

      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(0);
    }

    // Redeploying flink job from external checkpoint.
    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        harness = getTestHarness()) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.initializeState(snapshot);
      harness.open();

      // All flink manifests should be cleaned because it has committed the unfinished iceberg
      // transaction.
      assertFlinkManifests(0);

      assertMaxCommittedCheckpointId(newJobId.toString(), operatorId.toString(), -1);
      assertMaxCommittedCheckpointId(jobId, operatorId.toString(), 2);
      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(2);

      RowData row = SimpleDataUtil.createRowData(3, "foo");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-3", ImmutableList.of(row));
      processElement(
          newJobId.toString(), checkpointId, harness, 1, operatorId.toString(), dataFile);

      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(newJobId.toString(), operatorId.toString(), 3);
    }
  }

  @TestTemplate
  public void testStartAnotherJobToWriteSameTable() throws Exception {
    long checkpointId = 1;
    long timestamp = 0;

    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();

    JobID oldJobId = new JobID();
    OperatorID oldOperatorId;
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        harness = getTestHarness()) {

      harness.open();
      oldOperatorId = harness.getOperator().getOperatorID();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(oldJobId.toString(), oldOperatorId.toString(), -1L);

      for (int i = 1; i <= 3; i++) {
        rows.add(SimpleDataUtil.createRowData(i, "hello" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);
        processElement(
            oldJobId.toString(), ++checkpointId, harness, 1, oldOperatorId.toString(), dataFile);
        harness.snapshot(checkpointId, ++timestamp);
        assertFlinkManifests(1);

        harness.notifyOfCompletedCheckpoint(checkpointId);

        assertFlinkManifests(0);

        SimpleDataUtil.assertTableRows(table, tableRows, branch);
        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(oldJobId.toString(), oldOperatorId.toString(), checkpointId);
      }
    }

    // The new started job will start with checkpoint = 1 again.
    checkpointId = 1;
    JobID newJobId = new JobID();
    OperatorID newOperatorId;
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        harness = getTestHarness()) {
      harness.open();
      newOperatorId = harness.getOperator().getOperatorID();

      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(oldJobId.toString(), oldOperatorId.toString(), 4);
      assertMaxCommittedCheckpointId(newJobId.toString(), newOperatorId.toString(), -1);

      rows.add(SimpleDataUtil.createRowData(2, "world"));
      tableRows.addAll(rows);

      DataFile dataFile = writeDataFile("data-new-1", rows);
      processElement(
          newJobId.toString(), checkpointId, harness, 1, newOperatorId.toString(), dataFile);
      harness.snapshot(checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);
      SimpleDataUtil.assertTableRows(table, tableRows, branch);
      assertSnapshotSize(4);
      assertMaxCommittedCheckpointId(newJobId.toString(), newOperatorId.toString(), checkpointId);
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
      JobID jobID = jobs[jobIndex];
      OperatorID operatorId = operatorIds[jobIndex];
      try (OneInputStreamOperatorTestHarness<
              CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
          harness = getTestHarness()) {
        harness.getStreamConfig().setOperatorID(operatorId);

        harness.open();

        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(
            jobID.toString(), operatorId.toString(), checkpointId == 0 ? -1 : checkpointId - 1);

        List<RowData> rows = Lists.newArrayList(SimpleDataUtil.createRowData(i, "word-" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);

        processElement(jobID.toString(), checkpointId, harness, 1, operatorId.toString(), dataFile);

        harness.snapshot(checkpointId, ++timestamp);
        assertFlinkManifests(1);

        harness.notifyOfCompletedCheckpoint(checkpointId);
        assertFlinkManifests(0);
        SimpleDataUtil.assertTableRows(table, tableRows, branch);
        assertSnapshotSize(i + 1);
        assertMaxCommittedCheckpointId(jobID.toString(), operatorId.toString(), checkpointId);
      }
    }
  }

  @TestTemplate
  public void testMultipleSinksRecoveryFromValidSnapshot() throws Exception {

    // We cannot test a different checkpoint thant 0 because when using the OperatorTestHarness
    // for recovery the lastCompleted checkpoint is always reset to 0.
    // see: https://github.com/apache/iceberg/issues/10942
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> expectedRows = Lists.newArrayList();
    OperatorSubtaskState snapshot1;
    OperatorSubtaskState snapshot2;

    JobID jobID = new JobID();
    OperatorID operatorId1 = new OperatorID();
    OperatorID operatorId2 = new OperatorID();

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        harness1 = getTestHarness()) {
      try (OneInputStreamOperatorTestHarness<
              CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
          harness2 = getTestHarness()) {
        harness1.getStreamConfig().setOperatorID(operatorId1);
        harness1.setup();
        harness1.open();
        harness2.getStreamConfig().setOperatorID(operatorId2);
        harness2.setup();
        harness2.open();

        assertSnapshotSize(0);
        assertMaxCommittedCheckpointId(jobID.toString(), operatorId1.toString(), -1L);
        assertMaxCommittedCheckpointId(jobID.toString(), operatorId2.toString(), -1L);

        RowData row1 = SimpleDataUtil.createRowData(1, "hello1");
        expectedRows.add(row1);
        DataFile dataFile1 = writeDataFile("data-1-1", ImmutableList.of(row1));
        processElement(
            jobID.toString(), checkpointId, harness1, 1, operatorId1.toString(), dataFile1);

        snapshot1 = harness1.snapshot(checkpointId, ++timestamp);

        RowData row2 = SimpleDataUtil.createRowData(1, "hello2");
        expectedRows.add(row2);
        DataFile dataFile2 = writeDataFile("data-1-2", ImmutableList.of(row2));
        processElement(
            jobID.toString(), checkpointId, harness2, 1, operatorId2.toString(), dataFile2);

        snapshot2 = harness2.snapshot(checkpointId, ++timestamp);
        assertFlinkManifests(2);

        // Only notify one of the committers
        harness1.notifyOfCompletedCheckpoint(checkpointId);
        assertFlinkManifests(1);

        // Only the first row is committed at this point
        SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);
        assertSnapshotSize(1);
        assertMaxCommittedCheckpointId(jobID.toString(), operatorId1.toString(), checkpointId);
        assertMaxCommittedCheckpointId(jobID.toString(), operatorId2.toString(), -1);
      }
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<
                CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
            harness1 = getTestHarness();
        OneInputStreamOperatorTestHarness<
                CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
            harness2 = getTestHarness()) {
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
      assertMaxCommittedCheckpointId(jobID.toString(), operatorId1.toString(), checkpointId);
      assertMaxCommittedCheckpointId(jobID.toString(), operatorId2.toString(), checkpointId);

      RowData row1 = SimpleDataUtil.createRowData(2, "world1");
      expectedRows.add(row1);
      DataFile dataFile1 = writeDataFile("data-2-1", ImmutableList.of(row1));

      checkpointId++;
      processElement(
          jobID.toString(), checkpointId, harness1, 1, operatorId1.toString(), dataFile1);

      harness1.snapshot(checkpointId, ++timestamp);

      RowData row2 = SimpleDataUtil.createRowData(2, "world2");
      expectedRows.add(row2);
      DataFile dataFile2 = writeDataFile("data-2-2", ImmutableList.of(row2));
      processElement(
          jobID.toString(), checkpointId, harness2, 1, operatorId2.toString(), dataFile2);

      harness2.snapshot(checkpointId, ++timestamp);

      assertFlinkManifests(2);

      harness1.notifyOfCompletedCheckpoint(checkpointId);
      harness2.notifyOfCompletedCheckpoint(checkpointId);
      assertFlinkManifests(0);

      SimpleDataUtil.assertTableRows(table, expectedRows, branch);
      assertSnapshotSize(4);
      assertMaxCommittedCheckpointId(jobID.toString(), operatorId1.toString(), checkpointId);
      assertMaxCommittedCheckpointId(jobID.toString(), operatorId2.toString(), checkpointId);
    }
  }

  @TestTemplate
  public void testFlinkManifests() throws Exception {
    long timestamp = 0;
    long checkpoint = 1;

    JobID jobID = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        harness = getTestHarness()) {

      harness.open();
      operatorId = harness.getOperator().getOperatorID();

      assertMaxCommittedCheckpointId(jobID.toString(), operatorId.toString(), -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      //      harness.processElement(of(dataFile1), ++timestamp);
      processElement(jobID.toString(), checkpoint, harness, 1, operatorId.toString(), dataFile1);

      assertMaxCommittedCheckpointId(jobID.toString(), operatorId.toString(), -1L);

      // 1. snapshotState for checkpoint#1
      harness.snapshot(checkpoint, ++timestamp);
      List<Path> manifestPaths = assertFlinkManifests(1);
      Path manifestPath = manifestPaths.get(0);
      assertThat(manifestPath.getFileName())
          .asString()
          .isEqualTo(
              String.format("%s-%s-%05d-%d-%d-%05d.avro", jobID, operatorId, 0, 0, checkpoint, 1));
      //
      // 2. Read the data files from manifests and assert.
      List<DataFile> dataFiles =
          FlinkManifestUtil.readDataFiles(
              createTestingManifestFile(manifestPath), table.io(), table.specs());
      assertThat(dataFiles).hasSize(1);
      TestHelpers.assertEquals(dataFile1, dataFiles.get(0));

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(checkpoint);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);
      assertMaxCommittedCheckpointId(jobID.toString(), operatorId.toString(), checkpoint);
      assertFlinkManifests(0);
    }
  }

  @TestTemplate
  public void testHandleEndInput() throws Exception {
    assumeThat(isStreamingMode).as("Only support batch mode").isFalse();

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        testHarness = getTestHarness()) {

      testHarness.open();

      long checkpointId = Long.MAX_VALUE;
      processElement(jobId, checkpointId, testHarness, 1, OPERATOR_ID, dataFileTest1);

      testHarness.endInput();

      assertMaxCommittedCheckpointId(jobId, OPERATOR_ID, Long.MAX_VALUE);

      List<StreamElement> output = transformsToStreamElement(testHarness.getOutput());
      assertThat(output).hasSize(2);

      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output.get(0)))
          .hasCheckpointId(checkpointId)
          .hasPendingCommittables(0)
          .hasOverallCommittables(1)
          .hasFailedCommittables(0);

      // endInput is idempotent
      testHarness.endInput();
      assertThat(testHarness.getOutput()).hasSize(2);
    }
  }

  @TestTemplate
  public void testDeleteFiles() throws Exception {

    assumeThat(formatVersion).as("Only support delete in format v2").isGreaterThanOrEqualTo(2);

    FileAppenderFactory<RowData> appenderFactory = createDeletableAppenderFactory();

    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        testHarness = getTestHarness()) {

      testHarness.open();

      long checkpointId = 1;
      RowData row1 = SimpleDataUtil.createInsert(1, "aaa");
      DataFile dataFile1 = writeDataFile("data-file-1", ImmutableList.of(row1));
      processElement(jobId, checkpointId, testHarness, 1, OPERATOR_ID, dataFile1);

      //  testHarness.snapshot(checkpointId, 0);
      testHarness.notifyOfCompletedCheckpoint(checkpointId);

      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId, checkpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);

      List<StreamElement> output = transformsToStreamElement(testHarness.getOutput());
      assertThat(output).hasSize(2);
      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output.get(0)))
          .hasCheckpointId(checkpointId)
          .hasPendingCommittables(0)
          .hasOverallCommittables(1)
          .hasFailedCommittables(0);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1), branch);

      // The 2. commit
      checkpointId = 2;
      RowData row2 = SimpleDataUtil.createInsert(2, "bbb");
      DataFile dataFile2 = writeDataFile("data-file-2", ImmutableList.of(row2));

      RowData row3 = SimpleDataUtil.createInsert(3, "ccc");
      DataFile dataFile3 = writeDataFile("data-file-3", ImmutableList.of(row3));
      processElement(jobId, checkpointId, testHarness, 2, OPERATOR_ID, dataFile2, dataFile3);

      // testHarness.snapshot(checkpointId, 1);
      testHarness.notifyOfCompletedCheckpoint(checkpointId);

      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(jobId, checkpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1, row2, row3), branch);

      List<StreamElement> output2 = transformsToStreamElement(testHarness.getOutput());
      assertThat(output2).hasSize(2 + 2);
      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output2.get(2)))
          .hasCheckpointId(checkpointId)
          .hasPendingCommittables(0)
          .hasOverallCommittables(1)
          .hasFailedCommittables(0);

      // The 3. commit
      checkpointId = 3;
      RowData delete1 = SimpleDataUtil.createDelete(1, "aaa");
      DeleteFile deleteFile1 =
          writeEqDeleteFile(appenderFactory, "delete-file-1", ImmutableList.of(delete1));
      RowData row4 = SimpleDataUtil.createInsert(4, "ddd");
      DataFile dataFile4 = writeDataFile("data-file-4", ImmutableList.of(row4));

      RowData row5 = SimpleDataUtil.createInsert(5, "eee");
      DataFile dataFile5 = writeDataFile("data-file-5", ImmutableList.of(row5));
      WriteResult withRecord4 =
          WriteResult.builder()
              .addDataFiles(dataFile4, dataFile5)
              .addDeleteFiles(deleteFile1)
              .build();
      processElement(withRecord4, jobId, checkpointId, testHarness, 2, OPERATOR_ID);

      // testHarness.snapshot(checkpointId, 3);
      testHarness.notifyOfCompletedCheckpoint(checkpointId);

      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(jobId, checkpointId);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(row2, row3, row4, row5), branch);

      List<StreamElement> output3 = transformsToStreamElement(testHarness.getOutput());
      assertThat(output3).hasSize(2 + 2 + 2);
      SinkV2Assertions.assertThat(extractAndAssertCommittableSummary(output3.get(4)))
          .hasCheckpointId(checkpointId)
          .hasPendingCommittables(0)
          .hasOverallCommittables(1)
          .hasFailedCommittables(0);
    }
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

  private IcebergWriteAggregator buildIcebergWriteAggregator(String myJobId, String operatorId) {
    IcebergWriteAggregator icebergWriteAggregator = spy(new IcebergWriteAggregator(tableLoader));
    StreamTask ctx = mock(StreamTask.class);
    Environment env = mock(Environment.class);
    StreamingRuntimeContext streamingRuntimeContext = mock(StreamingRuntimeContext.class);
    TaskInfo taskInfo = mock(TaskInfo.class);
    JobID myJobID = mock(JobID.class);
    OperatorID operatorID = mock(OperatorID.class);
    doReturn(myJobId).when(myJobID).toString();
    doReturn(myJobID).when(env).getJobID();
    doReturn(env).when(ctx).getEnvironment();
    doReturn(ctx).when(icebergWriteAggregator).getContainingTask();
    doReturn(operatorId).when(operatorID).toString();
    doReturn(operatorID).when(icebergWriteAggregator).getOperatorID();
    doReturn(0).when(taskInfo).getAttemptNumber();
    doReturn(taskInfo).when(streamingRuntimeContext).getTaskInfo();
    doReturn(streamingRuntimeContext).when(icebergWriteAggregator).getRuntimeContext();

    try {
      icebergWriteAggregator.open();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return icebergWriteAggregator;
  }

  private CommittableSummary<IcebergCommittable> processElement(
      WriteResult withRecord,
      String myJobId,
      long checkpointId,
      OneInputStreamOperatorTestHarness testHarness,
      int subTaskId,
      String operatorId)
      throws Exception {

    IcebergCommittable commit =
        new IcebergCommittable(
            buildIcebergWriteAggregator(myJobId, operatorId)
                .writeToManifest(Lists.newArrayList(withRecord), checkpointId),
            myJobId,
            operatorId,
            checkpointId);

    CommittableSummary<IcebergCommittable> committableSummary =
        new CommittableSummary<>(subTaskId, 1, checkpointId, 1, 1, 0);
    testHarness.processElement(new StreamRecord<>(committableSummary));

    CommittableWithLineage<IcebergCommittable> committable =
        new CommittableWithLineage<>(commit, checkpointId, subTaskId);
    testHarness.processElement(new StreamRecord<>(committable));

    return committableSummary;
  }

  private CommittableSummary<IcebergCommittable> processElement(
      String myJobID,
      long checkpointId,
      OneInputStreamOperatorTestHarness<
              CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
          testHarness,
      int subTaskId,
      String operatorId,
      DataFile... dataFile)
      throws Exception {
    WriteResult withRecord = WriteResult.builder().addDataFiles(dataFile).build();
    return processElement(withRecord, myJobID, checkpointId, testHarness, subTaskId, operatorId);
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
        new Configuration(),
        table.location(),
        FileFormat.PARQUET.addExtension(filename),
        rows);
  }

  private DeleteFile writeEqDeleteFile(
      FileAppenderFactory<RowData> appenderFactory, String filename, List<RowData> deletes)
      throws IOException {
    return SimpleDataUtil.writeEqDeleteFile(
        table, FileFormat.PARQUET, filename, appenderFactory, deletes);
  }

  private OneInputStreamOperatorTestHarness<
          CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
      getTestHarness() throws Exception {
    IcebergSink sink =
        IcebergSink.forRowData(null).table(table).toBranch(branch).tableLoader(tableLoader).build();

    OneInputStreamOperatorTestHarness<
            CommittableMessage<IcebergCommittable>, CommittableMessage<IcebergCommittable>>
        testHarness =
            new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(sink, !isStreamingMode, true));
    testHarness.setup(committableMessageTypeSerializer);
    return testHarness;
  }

  // ------------------------------- Mock Classes --------------------------------

  private static class TestCommitter extends IcebergCommitter {
    private int successfulCommits = 0;

    TestCommitter(TableLoader tableLoader) {
      super(tableLoader, null, Maps.newHashMap(), false, 2, "test-sink-prefix", null);
    }

    @Override
    public void commit(Collection<CommitRequest<IcebergCommittable>> commitRequests)
        throws IOException, InterruptedException {
      commitRequests.forEach(
          one -> LOG.info("Committed with numberOfRetries: {}", one.getNumberOfRetries()));
      successfulCommits += commitRequests.size();
    }

    @Override
    public void close() {}

    public int getSuccessfulCommits() {
      return successfulCommits;
    }
  }

  // ------------------------------- Utility Methods --------------------------------

  private IcebergCommitter getCommitter() {
    IcebergFilesCommitterMetrics metric = mock(IcebergFilesCommitterMetrics.class);
    return new IcebergCommitter(
        tableLoader,
        branch,
        Collections.singletonMap("flink.test", TestIcebergCommitter.class.getName()),
        false,
        10,
        "sinkId",
        metric);
  }

  private Committer.CommitRequest<IcebergCommittable> buildCommitRequestFor(
      String myJobID, long checkpoint, Collection<WriteResult> writeResults) throws IOException {
    IcebergCommittable commit =
        new IcebergCommittable(
            buildIcebergWriteAggregator(myJobID, OPERATOR_ID)
                .writeToManifest(writeResults, checkpoint),
            myJobID,
            OPERATOR_ID,
            checkpoint);

    CommittableWithLineage committableWithLineage =
        new CommittableWithLineage(commit, checkpoint, 1);
    Committer.CommitRequest<IcebergCommittable> commitRequest = mock(Committer.CommitRequest.class);

    doReturn(committableWithLineage.getCommittable()).when(commitRequest).getCommittable();

    return commitRequest;
  }

  private WriteResult of(DataFile dataFile) {
    return WriteResult.builder().addDataFiles(dataFile).build();
  }

  private void assertMaxCommittedCheckpointId(String myJobID, String operatorId, long expectedId) {
    table.refresh();
    long actualId =
        IcebergCommitter.getMaxCommittedCheckpointId(table, myJobID, operatorId, branch);
    assertThat(actualId).isEqualTo(expectedId);
  }

  private void assertMaxCommittedCheckpointId(String myJobID, long expectedId) {
    assertMaxCommittedCheckpointId(myJobID, OPERATOR_ID, expectedId);
  }

  private void assertSnapshotSize(int expectedSnapshotSize) {
    table.refresh();
    assertThat(table.snapshots()).hasSize(expectedSnapshotSize);
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }

  private static class TestCommittableMessageTypeSerializer
      extends TypeSerializer<CommittableMessage<IcebergCommittable>> {

    CommittableMessageSerializer<IcebergCommittable> serializer =
        new CommittableMessageSerializer<>(new IcebergCommittableSerializer());

    @Override
    public boolean isImmutableType() {
      return false;
    }

    @Override
    public TypeSerializer<CommittableMessage<IcebergCommittable>> duplicate() {
      return null;
    }

    @Override
    public CommittableMessage<IcebergCommittable> createInstance() {
      return null;
    }

    @Override
    public CommittableMessage<IcebergCommittable> copy(
        CommittableMessage<IcebergCommittable> from) {
      return from;
    }

    @Override
    public CommittableMessage<IcebergCommittable> copy(
        CommittableMessage<IcebergCommittable> from, CommittableMessage<IcebergCommittable> reuse) {
      return from;
    }

    @Override
    public int getLength() {
      return 0;
    }

    @Override
    public void serialize(CommittableMessage<IcebergCommittable> record, DataOutputView target)
        throws IOException {
      byte[] serialize = serializer.serialize(record);
      target.writeInt(serialize.length);
      target.write(serialize);
    }

    @Override
    public CommittableMessage<IcebergCommittable> deserialize(DataInputView source)
        throws IOException {
      int length = source.readInt();
      byte[] bytes = new byte[length];
      source.read(bytes);
      return serializer.deserialize(1, bytes);
    }

    @Override
    public CommittableMessage<IcebergCommittable> deserialize(
        CommittableMessage<IcebergCommittable> reuse, DataInputView source) throws IOException {
      return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
      CommittableMessage<IcebergCommittable> deserialize = deserialize(source);
      serialize(deserialize, target);
    }

    @Override
    public boolean equals(Object obj) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public TypeSerializerSnapshot<CommittableMessage<IcebergCommittable>> snapshotConfiguration() {
      return null;
    }
  };
}
