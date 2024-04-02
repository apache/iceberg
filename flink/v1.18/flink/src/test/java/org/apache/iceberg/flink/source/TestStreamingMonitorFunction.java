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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestStreamingMonitorFunction extends TestBase {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));
  private static final FileFormat DEFAULT_FORMAT = FileFormat.PARQUET;
  private static final long WAIT_TIME_MILLIS = 10 * 1000L;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @BeforeEach
  @Override
  public void setupTable() throws IOException {
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
    this.metadataDir = new File(tableDir, "metadata");
    Assert.assertTrue(tableDir.delete());

    // Construct the iceberg table.
    table = create(SCHEMA, PartitionSpec.unpartitioned());
  }

  private void runSourceFunctionInTask(
      TestSourceContext sourceContext, StreamingMonitorFunction function) {
    Thread task =
        new Thread(
            () -> {
              try {
                function.run(sourceContext);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    task.start();
  }

  @TestTemplate
  public void testConsumeWithoutStartSnapshotId() throws Exception {
    List<List<Record>> recordsList = generateRecordsAndCommitTxn(10);
    ScanContext scanContext = ScanContext.builder().monitorInterval(Duration.ofMillis(100)).build();

    StreamingMonitorFunction function = createFunction(scanContext);
    try (AbstractStreamOperatorTestHarness<FlinkInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      TestSourceContext sourceContext = new TestSourceContext(new CountDownLatch(1));
      runSourceFunctionInTask(sourceContext, function);

      awaitExpectedSplits(sourceContext);

      // Stop the stream task.
      function.close();

      TestHelpers.assertRecords(
          sourceContext.toRows(), Lists.newArrayList(Iterables.concat(recordsList)), SCHEMA);
    }
  }

  @TestTemplate
  public void testConsumeFromStartSnapshotId() throws Exception {
    // Commit the first five transactions.
    generateRecordsAndCommitTxn(5);
    long startSnapshotId = table.currentSnapshot().snapshotId();

    // Commit the next five transactions.
    List<List<Record>> recordsList = generateRecordsAndCommitTxn(5);

    ScanContext scanContext =
        ScanContext.builder()
            .monitorInterval(Duration.ofMillis(100))
            .startSnapshotId(startSnapshotId)
            .build();

    StreamingMonitorFunction function = createFunction(scanContext);
    try (AbstractStreamOperatorTestHarness<FlinkInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      TestSourceContext sourceContext = new TestSourceContext(new CountDownLatch(1));
      runSourceFunctionInTask(sourceContext, function);

      awaitExpectedSplits(sourceContext);

      // Stop the stream task.
      function.close();

      TestHelpers.assertRecords(
          sourceContext.toRows(), Lists.newArrayList(Iterables.concat(recordsList)), SCHEMA);
    }
  }

  @TestTemplate
  public void testConsumeFromStartTag() throws Exception {
    // Commit the first five transactions.
    generateRecordsAndCommitTxn(5);
    long startSnapshotId = table.currentSnapshot().snapshotId();
    String tagName = "t1";
    table.manageSnapshots().createTag(tagName, startSnapshotId).commit();

    // Commit the next five transactions.
    List<List<Record>> recordsList = generateRecordsAndCommitTxn(5);

    ScanContext scanContext =
        ScanContext.builder().monitorInterval(Duration.ofMillis(100)).startTag(tagName).build();

    StreamingMonitorFunction function = createFunction(scanContext);
    try (AbstractStreamOperatorTestHarness<FlinkInputSplit> harness = createHarness(function)) {
      harness.setup();
      harness.open();

      TestSourceContext sourceContext = new TestSourceContext(new CountDownLatch(1));
      runSourceFunctionInTask(sourceContext, function);

      awaitExpectedSplits(sourceContext);

      // Stop the stream task.
      function.close();

      TestHelpers.assertRecords(
          sourceContext.toRows(), Lists.newArrayList(Iterables.concat(recordsList)), SCHEMA);
    }
  }

  @TestTemplate
  public void testCheckpointRestore() throws Exception {
    List<List<Record>> recordsList = generateRecordsAndCommitTxn(10);
    ScanContext scanContext = ScanContext.builder().monitorInterval(Duration.ofMillis(100)).build();

    StreamingMonitorFunction func = createFunction(scanContext);
    OperatorSubtaskState state;
    try (AbstractStreamOperatorTestHarness<FlinkInputSplit> harness = createHarness(func)) {
      harness.setup();
      harness.open();

      TestSourceContext sourceContext = new TestSourceContext(new CountDownLatch(1));
      runSourceFunctionInTask(sourceContext, func);

      awaitExpectedSplits(sourceContext);

      state = harness.snapshot(1, 1);

      // Stop the stream task.
      func.close();

      TestHelpers.assertRecords(
          sourceContext.toRows(), Lists.newArrayList(Iterables.concat(recordsList)), SCHEMA);
    }

    List<List<Record>> newRecordsList = generateRecordsAndCommitTxn(10);
    StreamingMonitorFunction newFunc = createFunction(scanContext);
    try (AbstractStreamOperatorTestHarness<FlinkInputSplit> harness = createHarness(newFunc)) {
      harness.setup();
      // Recover to process the remaining snapshots.
      harness.initializeState(state);
      harness.open();

      TestSourceContext sourceContext = new TestSourceContext(new CountDownLatch(1));
      runSourceFunctionInTask(sourceContext, newFunc);

      awaitExpectedSplits(sourceContext);

      // Stop the stream task.
      newFunc.close();

      TestHelpers.assertRecords(
          sourceContext.toRows(), Lists.newArrayList(Iterables.concat(newRecordsList)), SCHEMA);
    }
  }

  private void awaitExpectedSplits(TestSourceContext sourceContext) {
    Awaitility.await("expected splits should be produced")
        .atMost(Duration.ofMillis(WAIT_TIME_MILLIS))
        .untilAsserted(
            () -> {
              assertThat(sourceContext.latch.getCount()).isEqualTo(0);
              assertThat(sourceContext.splits).as("Should produce the expected splits").hasSize(1);
            });
  }

  @TestTemplate
  public void testInvalidMaxPlanningSnapshotCount() {
    ScanContext scanContext1 =
        ScanContext.builder()
            .monitorInterval(Duration.ofMillis(100))
            .maxPlanningSnapshotCount(0)
            .build();

    Assertions.assertThatThrownBy(() -> createFunction(scanContext1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The max-planning-snapshot-count must be greater than zero");

    ScanContext scanContext2 =
        ScanContext.builder()
            .monitorInterval(Duration.ofMillis(100))
            .maxPlanningSnapshotCount(-10)
            .build();

    Assertions.assertThatThrownBy(() -> createFunction(scanContext2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The max-planning-snapshot-count must be greater than zero");
  }

  @TestTemplate
  public void testConsumeWithMaxPlanningSnapshotCount() throws Exception {
    generateRecordsAndCommitTxn(10);

    // Use the oldest snapshot as starting to avoid the initial case.
    long oldestSnapshotId = SnapshotUtil.oldestAncestor(table).snapshotId();

    ScanContext scanContext =
        ScanContext.builder()
            .monitorInterval(Duration.ofMillis(100))
            .splitSize(1000L)
            .startSnapshotId(oldestSnapshotId)
            .maxPlanningSnapshotCount(Integer.MAX_VALUE)
            .build();

    FlinkInputSplit[] expectedSplits =
        FlinkSplitPlanner.planInputSplits(table, scanContext, ThreadPools.getWorkerPool());

    Assert.assertEquals("should produce 9 splits", 9, expectedSplits.length);

    // This covers three cases that maxPlanningSnapshotCount is less than, equal or greater than the
    // total splits number
    for (int maxPlanningSnapshotCount : ImmutableList.of(1, 9, 15)) {
      scanContext =
          ScanContext.builder()
              .monitorInterval(Duration.ofMillis(500))
              .startSnapshotId(oldestSnapshotId)
              .splitSize(1000L)
              .maxPlanningSnapshotCount(maxPlanningSnapshotCount)
              .build();

      StreamingMonitorFunction function = createFunction(scanContext);
      try (AbstractStreamOperatorTestHarness<FlinkInputSplit> harness = createHarness(function)) {
        harness.setup();
        harness.open();

        CountDownLatch latch = new CountDownLatch(1);
        TestSourceContext sourceContext = new TestSourceContext(latch);
        function.sourceContext(sourceContext);
        function.monitorAndForwardSplits();

        if (maxPlanningSnapshotCount < 10) {
          Assert.assertEquals(
              "Should produce same splits as max-planning-snapshot-count",
              maxPlanningSnapshotCount,
              sourceContext.splits.size());
        }
      }
    }
  }

  private List<List<Record>> generateRecordsAndCommitTxn(int commitTimes) throws IOException {
    List<List<Record>> expectedRecords = Lists.newArrayList();
    for (int i = 0; i < commitTimes; i++) {
      List<Record> records = RandomGenericData.generate(SCHEMA, 100, 0L);
      expectedRecords.add(records);

      // Commit those records to iceberg table.
      writeRecords(records);
    }
    return expectedRecords;
  }

  private void writeRecords(List<Record> records) throws IOException {
    GenericAppenderHelper appender = new GenericAppenderHelper(table, DEFAULT_FORMAT, temp);
    appender.appendToTable(records);
  }

  private StreamingMonitorFunction createFunction(ScanContext scanContext) {
    return new StreamingMonitorFunction(
        TestTableLoader.of(tableDir.getAbsolutePath()), scanContext);
  }

  private AbstractStreamOperatorTestHarness<FlinkInputSplit> createHarness(
      StreamingMonitorFunction function) throws Exception {
    StreamSource<FlinkInputSplit, StreamingMonitorFunction> streamSource =
        new StreamSource<>(function);
    return new AbstractStreamOperatorTestHarness<>(streamSource, 1, 1, 0);
  }

  private class TestSourceContext implements SourceFunction.SourceContext<FlinkInputSplit> {
    private final List<FlinkInputSplit> splits = Lists.newArrayList();
    private final Object checkpointLock = new Object();
    private final CountDownLatch latch;

    TestSourceContext(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void collect(FlinkInputSplit element) {
      splits.add(element);
      latch.countDown();
    }

    @Override
    public void collectWithTimestamp(FlinkInputSplit element, long timestamp) {
      collect(element);
    }

    @Override
    public void emitWatermark(Watermark mark) {}

    @Override
    public void markAsTemporarilyIdle() {}

    @Override
    public Object getCheckpointLock() {
      return checkpointLock;
    }

    @Override
    public void close() {}

    private List<Row> toRows() throws IOException {
      FlinkInputFormat format =
          FlinkSource.forRowData()
              .tableLoader(TestTableLoader.of(tableDir.getAbsolutePath()))
              .buildFormat();

      List<Row> rows = Lists.newArrayList();
      for (FlinkInputSplit split : splits) {
        format.open(split);

        RowData element = null;
        try {
          while (!format.reachedEnd()) {
            element = format.nextRecord(element);
            rows.add(Row.of(element.getInt(0), element.getString(1).toString()));
          }
        } finally {
          format.close();
        }
      }

      return rows;
    }
  }
}
