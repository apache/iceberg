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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.SteppingMailboxProcessor;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestStreamingReaderOperator extends TableTestBase {

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "data", Types.StringType.get())
  );
  private static final FileFormat DEFAULT_FORMAT = FileFormat.PARQUET;

  @Parameterized.Parameters(name = "FormatVersion={0}")
  public static Iterable<Object[]> parameters() {
    return ImmutableList.of(
        new Object[] {1},
        new Object[] {2}
    );
  }

  public TestStreamingReaderOperator(int formatVersion) {
    super(formatVersion);
  }

  @Before
  @Override
  public void setupTable() throws IOException {
    this.tableDir = temp.newFolder();
    this.metadataDir = new File(tableDir, "metadata");
    Assert.assertTrue(tableDir.delete());

    // Construct the iceberg table.
    table = create(SCHEMA, PartitionSpec.unpartitioned());
  }

  @Test
  public void testProcessAllRecords() throws Exception {
    List<List<Record>> expectedRecords = generateRecordsAndCommitTxn(10);

    List<FlinkInputSplit> splits = generateSplits();
    Assert.assertEquals("Should have 10 splits", 10, splits.size());

    try (OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = createReader()) {
      harness.setup();
      harness.open();

      SteppingMailboxProcessor processor = createLocalMailbox(harness);

      List<Record> expected = Lists.newArrayList();
      for (int i = 0; i < splits.size(); i++) {
        // Process this element to enqueue to mail-box.
        harness.processElement(splits.get(i), -1);

        // Run the mail-box once to read all records from the given split.
        Assert.assertTrue("Should processed 1 split", processor.runMailboxStep());

        // Assert the output has expected elements.
        expected.addAll(expectedRecords.get(i));
        TestFlinkScan.assertRecords(readOutputValues(harness), expected, SCHEMA);
      }
    }
  }

  @Test
  public void testTriggerCheckpoint() throws Exception {
    // Received emitted splits: split1, split2, split3, checkpoint request is triggered when reading records from
    // split1.
    List<List<Record>> expectedRecords = generateRecordsAndCommitTxn(3);

    List<FlinkInputSplit> splits = generateSplits();
    Assert.assertEquals("Should have 3 splits", 3, splits.size());

    long timestamp = 0;
    try (OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = createReader()) {
      harness.setup();
      harness.open();

      SteppingMailboxProcessor processor = createLocalMailbox(harness);

      harness.processElement(splits.get(0), ++timestamp);
      harness.processElement(splits.get(1), ++timestamp);
      harness.processElement(splits.get(2), ++timestamp);

      // Trigger snapshot state, it will start to work once all records from split0 are read.
      processor.getMainMailboxExecutor()
          .execute(() -> harness.snapshot(1, 3), "Trigger snapshot");

      Assert.assertTrue("Should have processed the split0", processor.runMailboxStep());
      Assert.assertTrue("Should have processed the snapshot state action", processor.runMailboxStep());

      TestFlinkScan.assertRecords(readOutputValues(harness), expectedRecords.get(0), SCHEMA);

      // Read records from split1.
      Assert.assertTrue("Should have processed the split1", processor.runMailboxStep());

      // Read records from split2.
      Assert.assertTrue("Should have processed the split2", processor.runMailboxStep());

      TestFlinkScan.assertRecords(readOutputValues(harness),
          Lists.newArrayList(Iterables.concat(expectedRecords)), SCHEMA);
    }
  }

  @Test
  public void testCheckpointRestore() throws Exception {
    List<List<Record>> expectedRecords = generateRecordsAndCommitTxn(15);

    List<FlinkInputSplit> splits = generateSplits();
    Assert.assertEquals("Should have 10 splits", 15, splits.size());

    OperatorSubtaskState state;
    List<Record> expected = Lists.newArrayList();
    try (OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = createReader()) {
      harness.setup();
      harness.open();

      // Enqueue all the splits.
      for (FlinkInputSplit split : splits) {
        harness.processElement(split, -1);
      }

      // Read all records from the first five splits.
      SteppingMailboxProcessor localMailbox = createLocalMailbox(harness);
      for (int i = 0; i < 5; i++) {
        expected.addAll(expectedRecords.get(i));
        Assert.assertTrue("Should have processed the split#" + i, localMailbox.runMailboxStep());

        TestFlinkScan.assertRecords(readOutputValues(harness), expected, SCHEMA);
      }

      // Snapshot state now,  there're 10 splits left in the state.
      state = harness.snapshot(1, 1);
    }

    expected.clear();
    try (OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = createReader()) {
      harness.setup();
      // Recover to process the remaining splits.
      harness.initializeState(state);
      harness.open();

      SteppingMailboxProcessor localMailbox = createLocalMailbox(harness);

      for (int i = 5; i < 10; i++) {
        expected.addAll(expectedRecords.get(i));
        Assert.assertTrue("Should have processed one split#" + i, localMailbox.runMailboxStep());

        TestFlinkScan.assertRecords(readOutputValues(harness), expected, SCHEMA);
      }

      // Let's process the final 5 splits now.
      for (int i = 10; i < 15; i++) {
        expected.addAll(expectedRecords.get(i));
        harness.processElement(splits.get(i), 1);

        Assert.assertTrue("Should have processed the split#" + i, localMailbox.runMailboxStep());
        TestFlinkScan.assertRecords(readOutputValues(harness), expected, SCHEMA);
      }
    }
  }

  private List<Row> readOutputValues(OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness) {
    List<Row> results = Lists.newArrayList();
    for (RowData rowData : harness.extractOutputValues()) {
      results.add(Row.of(rowData.getInt(0), rowData.getString(1).toString()));
    }
    return results;
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

  private List<FlinkInputSplit> generateSplits() {
    List<FlinkInputSplit> inputSplits = Lists.newArrayList();

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    for (int i = snapshotIds.size() - 1; i >= 0; i--) {
      ScanContext scanContext;
      if (i == snapshotIds.size() - 1) {
        // Generate the splits from the first snapshot.
        scanContext = ScanContext.builder()
            .useSnapshotId(snapshotIds.get(i))
            .build();
      } else {
        // Generate the splits between the previous snapshot and current snapshot.
        scanContext = ScanContext.builder()
            .startSnapshotId(snapshotIds.get(i + 1))
            .endSnapshotId(snapshotIds.get(i))
            .build();
      }

      Collections.addAll(inputSplits, FlinkSplitGenerator.createInputSplits(table, scanContext));
    }

    return inputSplits;
  }

  private OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> createReader() throws Exception {
    // This input format is used to opening the emitted split.
    FlinkInputFormat inputFormat = FlinkSource.forRowData()
        .tableLoader(TestTableLoader.of(tableDir.getAbsolutePath()))
        .buildFormat();

    OneInputStreamOperatorFactory<FlinkInputSplit, RowData> factory = StreamingReaderOperator.factory(inputFormat);
    OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = new OneInputStreamOperatorTestHarness<>(
        factory, 1, 1, 0);
    harness.getStreamConfig().setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    return harness;
  }

  private SteppingMailboxProcessor createLocalMailbox(
      OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness) {
    return new SteppingMailboxProcessor(
        MailboxDefaultAction.Controller::suspendDefaultAction,
        harness.getTaskMailbox(),
        StreamTaskActionExecutor.IMMEDIATE);
  }
}
