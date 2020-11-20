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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.SteppingMailboxProcessor;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestStreamingReaderOperator {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final Schema SCHEMA = new Schema(required(1, "data", Types.StringType.get()));

  private Table table;
  private String location;
  private List<Record> expected;

  @Before
  public void before() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    location = "file:" + warehouseFile;
    table = new HadoopTables(new Configuration()).create(SCHEMA, location);

    GenericAppenderHelper appender = new GenericAppenderHelper(table, FileFormat.AVRO, TEMPORARY_FOLDER);
    expected = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      List<Record> records = RandomGenericData.generate(SCHEMA, 100, 0L);
      appender.appendToTable(records);
      expected.addAll(records);
    }
  }

  @Test
  public void testCheckpointRestore() throws Exception {
    TableScan scan = table.newScan();
    List<FlinkInputSplit> splits = new ArrayList<>();
    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      List<CombinedScanTask> tasks = Lists.newArrayList(tasksIterable);
      for (int i = 0; i < tasks.size(); i++) {
        splits.add(new FlinkInputSplit(i, tasks.get(i)));
      }
    }
    OperatorSubtaskState state;
    try (OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = createReader()) {
      harness.setup();
      harness.open();
      for (FlinkInputSplit split : splits) {
        harness.processElement(split, -1);
      }

      state = harness.snapshot(1, 1);
    }

    List<Row> results;
    try (OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = createReader()) {
      harness.setup();
      harness.initializeState(state);
      harness.open();

      SteppingMailboxProcessor localMailbox = createLocalMailbox(harness);
      while (true) {
        if (!localMailbox.runMailboxStep()) {
          break;
        }
      }
      results = harness.extractOutputValues().stream().map(r -> Row.of(r.getString(0).toString()))
          .collect(Collectors.toList());
    }

    TestFlinkScan.assertRecords(results, expected, SCHEMA);
  }

  private OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> createReader() throws Exception {
    OneInputStreamOperatorFactory<FlinkInputSplit, RowData> factory = StreamingReaderOperator.factory(
        FlinkSource.forRowData().streaming(false).table(table).tableLoader(TableLoader.fromHadoopTable(location))
            .buildFormat());
    OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness = new OneInputStreamOperatorTestHarness<>(
        factory, 1, 1, 0);
    harness.getStreamConfig().setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    return harness;
  }

  private SteppingMailboxProcessor createLocalMailbox(
      OneInputStreamOperatorTestHarness<FlinkInputSplit, RowData> harness) {
    return new SteppingMailboxProcessor(
        MailboxDefaultAction.Controller::suspendDefaultAction, harness.getTaskMailbox(),
        StreamTaskActionExecutor.IMMEDIATE);
  }
}
