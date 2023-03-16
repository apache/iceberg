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

import static org.apache.iceberg.flink.sink.SinkTestUtil.fromOutput;
import static org.apache.iceberg.flink.sink.SinkTestUtil.toCommittableSummary;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.committer.FilesCommittable;
import org.apache.iceberg.flink.sink.committer.FilesCommittableSerializer;
import org.apache.iceberg.flink.sink.committer.FilesCommitter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFilesCommitterOperator extends TableTestBase {
  private static final Configuration CONF = new Configuration();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private Table table;
  private TableLoader tableLoader;
  private final FileFormat format;
  private final Boolean isBatchMode;
  private final Boolean isCheckpointingEnabled;

  private final long dataFIleRowCount = 5L;

  private ManifestOutputFileFactory manifestOutputFileFactory;

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

  @Parameterized.Parameters(name = "format={0}, isStreaming={1}, isCheckpointingEnabled={2}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC}) {
      for (Boolean isBatchMode : new Boolean[] {true, false}) {
        for (Boolean isCheckpointingEnabled : new Boolean[] {true, false}) {
          parameters.add(new Object[] {2, format, isBatchMode, isCheckpointingEnabled});
        }
      }
    }
    return parameters;
  }

  @Before
  public void before() throws IOException {
    File folder = TEMPORARY_FOLDER.newFolder();
    String warehouse = folder.getAbsolutePath();

    String tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdir());

    Map<String, String> props =
        ImmutableMap.of(
            TableProperties.DEFAULT_FILE_FORMAT, format.name(),
            TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    table = SimpleDataUtil.createTable(tablePath, props, false);
    tableLoader = TableLoader.fromHadoopTable(tablePath);

    manifestOutputFileFactory =
        FlinkManifestUtil.createOutputFileFactory(table, "flink_job", "0", 0, 0);
  }

  public TestFilesCommitterOperator(
      int formatVersion, FileFormat format, Boolean isBatchMode, Boolean isCheckpointingEnabled) {
    super(formatVersion);
    this.format = format;
    this.isBatchMode = isBatchMode;
    this.isCheckpointingEnabled = isCheckpointingEnabled;
  }

  @Test
  public void testEmitCommittablesMock() throws Exception {
    final ForwardingCommitter committer = new ForwardingCommitter(tableLoader);
    IcebergSink sink = IcebergSink.builder().table(table).tableLoader(tableLoader).build();
    sink.setCommitter(committer);

    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        testHarness =
            new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(
                    (TwoPhaseCommittingSink<RowData, FilesCommittable>) sink, false, true));
    testHarness.open();

    WriteResult writeResult = WriteResult.builder().build();

    FilesCommittable commit =
        new FilesCommittable(
            FilesCommittable.writeToManifest(writeResult, manifestOutputFileFactory, table.spec()));
    final CommittableSummary<FilesCommittable> committableSummary =
        new CommittableSummary<>(1, 1, 1L, 1, 1, 0);
    testHarness.processElement(new StreamRecord<>(committableSummary));
    final CommittableWithLineage<FilesCommittable> committableWithLineage =
        new CommittableWithLineage<>(commit, 1L, 1);
    testHarness.processElement(new StreamRecord<>(committableWithLineage));

    // Trigger commit
    testHarness.notifyOfCompletedCheckpoint(1);

    assertThat(committer.getSuccessfulCommits()).isEqualTo(1);
    final List<StreamElement> output = fromOutput(testHarness.getOutput());
    SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
        .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
        .hasOverallCommittables(committableSummary.getNumberOfCommittables())
        .hasPendingCommittables(0);
    testHarness.close();
  }

  @Test
  public void testEmitCommittables() throws Exception {
    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        testHarness = getTestHarness();
    testHarness.open();

    String jobId1 = "jobId1";
    long checkpointId = 0;
    CommittableSummary<FilesCommittable> committableSummary =
        processElement(dataFileTest1, jobId1, checkpointId, testHarness, 1);

    // Trigger commit
    testHarness.notifyOfCompletedCheckpoint(1);

    assertSnapshotSize(1);
    assertMaxCommittedCheckpointId(jobId1, 0L);

    final List<StreamElement> output = fromOutput(testHarness.getOutput());
    SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
        .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
        .hasOverallCommittables(committableSummary.getNumberOfCommittables())
        .hasPendingCommittables(0);
    testHarness.close();

    table.refresh();
    Snapshot currentSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        dataFIleRowCount,
        Long.parseLong(currentSnapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP)));
    Assert.assertEquals("1", currentSnapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
  }

  /** The data was not committed in the previous job. */
  @Test
  public void testStateRestoreFromPreJob() throws Exception {
    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        preJobTestHarness = getTestHarness();
    preJobTestHarness.open();

    String jobId1 = "jobId1";
    long checkpointId = 0;
    CommittableSummary<FilesCommittable> committableSummary =
        processElement(dataFileTest1, jobId1, checkpointId, preJobTestHarness, 1);

    final OperatorSubtaskState snapshot = preJobTestHarness.snapshot(checkpointId, 2L);

    assertThat(preJobTestHarness.getOutput()).isEmpty();
    preJobTestHarness.close();

    assertSnapshotSize(0);
    assertMaxCommittedCheckpointId(jobId1, -1L);

    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        restored = getTestHarness();
    restored.setup(committableMessageTypeSerializer);
    restored.initializeState(snapshot);
    restored.open();

    // Previous committables are immediately committed if possible
    final List<StreamElement> output = fromOutput(restored.getOutput());
    assertThat(output).hasSize(2);

    SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
        .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
        .hasOverallCommittables(committableSummary.getNumberOfCommittables())
        .hasPendingCommittables(0);

    table.refresh();
    Snapshot currentSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        dataFIleRowCount,
        Long.parseLong(currentSnapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP)));
    Assert.assertEquals("1", currentSnapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    Assert.assertEquals(jobId1, currentSnapshot.summary().get("flink.job-id"));

    String jobId2 = "jobId2";
    CommittableSummary<FilesCommittable> committableSummary2 =
        processElement(dataFileTest2, jobId2, checkpointId, restored, 1);

    // Trigger commit
    restored.notifyOfCompletedCheckpoint(0);

    final List<StreamElement> output2 = fromOutput(restored.getOutput());
    SinkV2Assertions.assertThat(toCommittableSummary(output2.get(0)))
        .hasFailedCommittables(committableSummary2.getNumberOfFailedCommittables())
        .hasOverallCommittables(committableSummary2.getNumberOfCommittables())
        .hasPendingCommittables(0);
    restored.close();

    assertSnapshotSize(2);
    assertMaxCommittedCheckpointId(jobId2, 0);

    table.refresh();
    Snapshot currentSnapshot2 = table.currentSnapshot();
    Assert.assertEquals(
        dataFIleRowCount * 2,
        Long.parseLong(currentSnapshot2.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP)));
    Assert.assertEquals("2", currentSnapshot2.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    Assert.assertEquals(jobId2, currentSnapshot2.summary().get("flink.job-id"));
  }

  /** The data was committed in the previous job. */
  @Test
  public void testStateRestoreFromPreJob2() throws Exception {
    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        preJobTestHarness = getTestHarness();

    preJobTestHarness.open();

    String jobId1 = "jobId1";
    long checkpointId = 0;
    CommittableSummary<FilesCommittable> committableSummary =
        processElement(dataFileTest1, jobId1, checkpointId, preJobTestHarness, 1);

    final OperatorSubtaskState snapshot = preJobTestHarness.snapshot(checkpointId, 2L);
    // commit snapshot
    preJobTestHarness.notifyOfCompletedCheckpoint(checkpointId);

    final List<StreamElement> output = fromOutput(preJobTestHarness.getOutput());
    assertThat(output).hasSize(2);

    SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
        .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
        .hasOverallCommittables(committableSummary.getNumberOfCommittables())
        .hasPendingCommittables(0);

    preJobTestHarness.close();

    assertSnapshotSize(1);
    assertMaxCommittedCheckpointId(jobId1, 0L);
    table.refresh();
    long proJobSnapshotId = table.currentSnapshot().snapshotId();

    String jobId2 = "jobId2";
    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        restored = getTestHarness();

    restored.initializeState(snapshot);
    restored.open();

    // The committed data will not be recommitted
    final List<StreamElement> output2 = fromOutput(restored.getOutput());
    assertThat(output2).hasSize(0);

    table.refresh();
    long restoredSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals(
        "The table does not generate a new snapshot without data being committed.",
        proJobSnapshotId,
        restoredSnapshotId);
    Assert.assertEquals(
        dataFIleRowCount,
        Long.parseLong(table.currentSnapshot().summary().get(SnapshotSummary.TOTAL_RECORDS_PROP)));
    Assert.assertEquals(
        "1", table.currentSnapshot().summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    Assert.assertEquals(jobId1, table.currentSnapshot().summary().get("flink.job-id"));

    // Commit new data file
    checkpointId = 1;
    CommittableSummary<FilesCommittable> committableSummary2 =
        processElement(dataFileTest2, jobId2, checkpointId, restored, 1);

    // Trigger commit
    restored.notifyOfCompletedCheckpoint(checkpointId);

    final List<StreamElement> output3 = fromOutput(restored.getOutput());
    assertThat(output3).hasSize(2);
    SinkV2Assertions.assertThat(toCommittableSummary(output3.get(0)))
        .hasFailedCommittables(committableSummary2.getNumberOfFailedCommittables())
        .hasOverallCommittables(committableSummary2.getNumberOfCommittables())
        .hasPendingCommittables(0);
    restored.close();

    assertSnapshotSize(2);
    assertMaxCommittedCheckpointId(jobId2, 1L);

    table.refresh();
    Snapshot currentSnapshot2 = table.currentSnapshot();
    Assert.assertEquals(
        dataFIleRowCount * 2,
        Long.parseLong(currentSnapshot2.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP)));
    Assert.assertEquals("2", currentSnapshot2.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    Assert.assertEquals(jobId2, currentSnapshot2.summary().get("flink.job-id"));
  }

  @Test
  public void testStateRestoreFromCurrJob() throws Exception {
    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        testHarness = getTestHarness();

    testHarness.open();

    String jobId1 = "jobId1";
    long checkpointId = 0;
    CommittableSummary<FilesCommittable> committableSummary =
        processElement(dataFileTest1, jobId1, checkpointId, testHarness, 1);

    final OperatorSubtaskState snapshot = testHarness.snapshot(checkpointId, 2L);

    assertThat(testHarness.getOutput()).isEmpty();
    testHarness.close();

    assertSnapshotSize(0);
    assertMaxCommittedCheckpointId(jobId1, -1L);

    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        restored = getTestHarness();

    restored.setup(committableMessageTypeSerializer);

    restored.initializeState(snapshot);
    restored.open();

    // Previous committables are immediately committed if possible
    final List<StreamElement> output = fromOutput(restored.getOutput());
    assertThat(output).hasSize(2);

    SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
        .hasFailedCommittables(committableSummary.getNumberOfFailedCommittables())
        .hasOverallCommittables(committableSummary.getNumberOfCommittables())
        .hasPendingCommittables(0);

    table.refresh();
    Snapshot currentSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        dataFIleRowCount,
        Long.parseLong(currentSnapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP)));
    Assert.assertEquals("1", currentSnapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    Assert.assertEquals(jobId1, currentSnapshot.summary().get("flink.job-id"));

    String jobId2 = "jobId2";
    checkpointId = 1;
    CommittableSummary<FilesCommittable> committableSummary2 =
        processElement(dataFileTest2, jobId2, checkpointId, restored, 1);

    // Trigger commit
    restored.notifyOfCompletedCheckpoint(checkpointId);

    final List<StreamElement> output2 = fromOutput(restored.getOutput());
    SinkV2Assertions.assertThat(toCommittableSummary(output2.get(0)))
        .hasFailedCommittables(committableSummary2.getNumberOfFailedCommittables())
        .hasOverallCommittables(committableSummary2.getNumberOfCommittables())
        .hasPendingCommittables(0);
    restored.close();

    assertSnapshotSize(2);
    assertMaxCommittedCheckpointId(jobId2, 1L);

    table.refresh();
    Snapshot currentSnapshot2 = table.currentSnapshot();
    Assert.assertEquals(
        dataFIleRowCount * 2,
        Long.parseLong(currentSnapshot2.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP)));
    Assert.assertEquals("2", currentSnapshot2.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    Assert.assertEquals(jobId2, currentSnapshot2.summary().get("flink.job-id"));
  }

  @Test
  public void testHandleEndInput() throws Exception {
    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        testHarness = getTestHarness();

    testHarness.open();

    String jobId1 = "jobId1";
    long checkpointId = 0;
    processElement(dataFileTest1, jobId1, checkpointId, testHarness, 1);

    testHarness.endInput();

    // If checkpointing enabled endInput does not emit anything because a final checkpoint follows
    if (isCheckpointingEnabled) {
      testHarness.notifyOfCompletedCheckpoint(checkpointId);
    }

    final List<StreamElement> output = fromOutput(testHarness.getOutput());
    assertThat(output).hasSize(2);
    SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
        .hasCheckpointId(checkpointId)
        .hasPendingCommittables(0)
        .hasOverallCommittables(1)
        .hasFailedCommittables(0);

    // Future emission calls should change the output
    testHarness.notifyOfCompletedCheckpoint(2);
    testHarness.endInput();

    assertThat(testHarness.getOutput()).hasSize(2);
  }

  @Test
  public void testDeleteFiles() throws Exception {
    Assume.assumeFalse("Only support delete in format v2.", formatVersion < 2);
    FileAppenderFactory<RowData> appenderFactory = createDeletableAppenderFactory();

    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        testHarness = getTestHarness();

    testHarness.open();
    String jobId = "jobId1";
    long checkpointId = 0;
    RowData row1 = SimpleDataUtil.createInsert(1, "aaa");
    DataFile dataFile1 = writeDataFile("data-file-1", ImmutableList.of(row1));
    processElement(dataFile1, jobId, checkpointId, testHarness, 1);

    testHarness.snapshot(checkpointId, 0);
    testHarness.notifyOfCompletedCheckpoint(checkpointId);

    assertSnapshotSize(1);
    assertMaxCommittedCheckpointId(jobId, checkpointId);

    final List<StreamElement> output = fromOutput(testHarness.getOutput());
    assertThat(output).hasSize(2);
    SinkV2Assertions.assertThat(toCommittableSummary(output.get(0)))
        .hasCheckpointId(checkpointId)
        .hasPendingCommittables(0)
        .hasOverallCommittables(1)
        .hasFailedCommittables(0);
    SimpleDataUtil.assertTableRows(table, ImmutableList.of(row1));

    // The 2. commit
    checkpointId = 1;
    RowData row2 = SimpleDataUtil.createInsert(2, "bbb");
    DataFile dataFile2 = writeDataFile("data-file-2", ImmutableList.of(row2));
    processElement(dataFile2, jobId, checkpointId, testHarness, 1);

    RowData row3 = SimpleDataUtil.createInsert(3, "ccc");
    DataFile dataFile3 = writeDataFile("data-file-3", ImmutableList.of(row3));
    processElement(dataFile3, jobId, checkpointId, testHarness, 2);

    testHarness.snapshot(checkpointId, 1);
    testHarness.notifyOfCompletedCheckpoint(checkpointId);

    assertSnapshotSize(2);
    assertMaxCommittedCheckpointId(jobId, checkpointId);

    final List<StreamElement> output2 = fromOutput(testHarness.getOutput());
    assertThat(output2).hasSize(2 + 3);
    SinkV2Assertions.assertThat(toCommittableSummary(output2.get(2)))
        .hasCheckpointId(checkpointId)
        .hasPendingCommittables(0)
        .hasOverallCommittables(2)
        .hasFailedCommittables(0);

    // The 3. commit
    checkpointId = 2;
    RowData delete1 = SimpleDataUtil.createDelete(1, "aaa");
    DeleteFile deleteFile1 =
        writeEqDeleteFile(appenderFactory, "delete-file-1", ImmutableList.of(delete1));
    RowData row4 = SimpleDataUtil.createInsert(4, "ddd");
    DataFile dataFile4 = writeDataFile("data-file-4", ImmutableList.of(row4));
    WriteResult withRecord4 =
        WriteResult.builder().addDataFiles(dataFile4).addDeleteFiles(deleteFile1).build();
    processElement(withRecord4, jobId, checkpointId, testHarness, 1);

    RowData row5 = SimpleDataUtil.createInsert(5, "eee");
    DataFile dataFile5 = writeDataFile("data-file-5", ImmutableList.of(row5));
    WriteResult withRecord5 = WriteResult.builder().addDataFiles(dataFile5).build();
    processElement(withRecord5, jobId, checkpointId, testHarness, 2);

    testHarness.snapshot(checkpointId, 3);
    testHarness.notifyOfCompletedCheckpoint(checkpointId);

    assertSnapshotSize(4);
    assertMaxCommittedCheckpointId(jobId, checkpointId);

    final List<StreamElement> output3 = fromOutput(testHarness.getOutput());
    assertThat(output3).hasSize(2 + 3 + 3);
    SinkV2Assertions.assertThat(toCommittableSummary(output3.get(5)))
        .hasCheckpointId(checkpointId)
        .hasPendingCommittables(0)
        .hasOverallCommittables(2)
        .hasFailedCommittables(0);

    SimpleDataUtil.assertTableRows(table, ImmutableList.of(row2, row3, row4, row5));
  }

  private CommittableSummary<FilesCommittable> processElement(
      WriteResult withRecord,
      String jobId,
      long checkpointId,
      OneInputStreamOperatorTestHarness testHarness,
      int subTaskId)
      throws Exception {

    FilesCommittable commit =
        new FilesCommittable(
            FilesCommittable.writeToManifest(withRecord, manifestOutputFileFactory, table.spec()),
            jobId,
            checkpointId,
            subTaskId);

    final CommittableSummary<FilesCommittable> committableSummary =
        new CommittableSummary<>(subTaskId, 1, checkpointId, 1, 1, 0);
    testHarness.processElement(new StreamRecord<>(committableSummary));
    final CommittableWithLineage<FilesCommittable> committable =
        new CommittableWithLineage<>(commit, checkpointId, subTaskId);
    testHarness.processElement(new StreamRecord<>(committable));
    return committableSummary;
  }

  private CommittableSummary<FilesCommittable> processElement(
      DataFile dataFile,
      String jobId,
      long checkpointId,
      OneInputStreamOperatorTestHarness testHarness,
      int subTaskId)
      throws Exception {
    WriteResult withRecord = WriteResult.builder().addDataFiles(dataFile).build();
    return processElement(withRecord, jobId, checkpointId, testHarness, subTaskId);
  }

  private FileAppenderFactory<RowData> createDeletableAppenderFactory() {
    int[] equalityFieldIds =
        new int[] {
          table.schema().findField("id").fieldId(), table.schema().findField("data").fieldId()
        };
    return new FlinkAppenderFactory(
        table.schema(),
        FlinkSchemaUtil.convert(table.schema()),
        table.properties(),
        table.spec(),
        equalityFieldIds,
        table.schema(),
        null);
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(
        table.schema(), table.spec(), CONF, table.location(), format.addExtension(filename), rows);
  }

  private DeleteFile writeEqDeleteFile(
      FileAppenderFactory<RowData> appenderFactory, String filename, List<RowData> deletes)
      throws IOException {
    return SimpleDataUtil.writeEqDeleteFile(table, format, filename, appenderFactory, deletes);
  }

  private OneInputStreamOperatorTestHarness<
          CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
      getTestHarness() throws Exception {
    IcebergSink sink = IcebergSink.builder().table(table).tableLoader(tableLoader).build();

    final OneInputStreamOperatorTestHarness<
            CommittableMessage<FilesCommittable>, CommittableMessage<FilesCommittable>>
        testHarness =
            new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(
                    (TwoPhaseCommittingSink<RowData, FilesCommittable>) sink,
                    isBatchMode,
                    isCheckpointingEnabled));
    testHarness.setup(committableMessageTypeSerializer);

    return testHarness;
  }

  // ------------------------------- Mock Classes --------------------------------

  private static class ForwardingCommitter extends FilesCommitter {
    private int successfulCommits = 0;

    ForwardingCommitter(TableLoader tableLoader) {
      super(tableLoader, false, Maps.newHashMap(), 2);
    }

    @Override
    public void commit(Collection<CommitRequest<FilesCommittable>> commitRequests)
        throws IOException, InterruptedException {
      // super.commit(requests);
      commitRequests.forEach(
          one -> {
            System.out.println(one.getNumberOfRetries());
          });
      successfulCommits += commitRequests.size();
    }

    @Override
    public void close() throws Exception {}

    public int getSuccessfulCommits() {
      return successfulCommits;
    }
  }

  // ------------------------------- Utility Methods --------------------------------

  private void assertMaxCommittedCheckpointId(String jobID, long expectedId) {
    table.refresh();
    long actualId = IcebergFilesCommitter.getMaxCommittedCheckpointId(table, jobID);
    Assert.assertEquals(expectedId, actualId);
  }

  private void assertSnapshotSize(int expectedSnapshotSize) {
    table.refresh();
    Assert.assertEquals(expectedSnapshotSize, Lists.newArrayList(table.snapshots()).size());
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }

  final TypeSerializer<CommittableMessage<FilesCommittable>> committableMessageTypeSerializer =
      new TypeSerializer<CommittableMessage<FilesCommittable>>() {

        final CommittableMessageSerializer<FilesCommittable> serializer =
            new CommittableMessageSerializer<>(new FilesCommittableSerializer());

        @Override
        public boolean isImmutableType() {
          return false;
        }

        @Override
        public TypeSerializer<CommittableMessage<FilesCommittable>> duplicate() {
          return null;
        }

        @Override
        public CommittableMessage<FilesCommittable> createInstance() {
          return null;
        }

        @Override
        public CommittableMessage<FilesCommittable> copy(
            CommittableMessage<FilesCommittable> from) {
          return from;
        }

        @Override
        public CommittableMessage<FilesCommittable> copy(
            CommittableMessage<FilesCommittable> from, CommittableMessage<FilesCommittable> reuse) {
          return from;
        }

        @Override
        public int getLength() {
          return 0;
        }

        @Override
        public void serialize(CommittableMessage<FilesCommittable> record, DataOutputView target)
            throws IOException {
          byte[] serialize = serializer.serialize(record);
          target.writeInt(serialize.length);
          target.write(serialize);
        }

        @Override
        public CommittableMessage<FilesCommittable> deserialize(DataInputView source)
            throws IOException {
          int length = source.readInt();
          byte[] bytes = new byte[length];
          source.read(bytes);
          return serializer.deserialize(1, bytes);
        }

        @Override
        public CommittableMessage<FilesCommittable> deserialize(
            CommittableMessage<FilesCommittable> reuse, DataInputView source) throws IOException {
          return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
          CommittableMessage<FilesCommittable> deserialize = deserialize(source);
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
        public TypeSerializerSnapshot<CommittableMessage<FilesCommittable>>
            snapshotConfiguration() {
          return null;
        }
      };
}
