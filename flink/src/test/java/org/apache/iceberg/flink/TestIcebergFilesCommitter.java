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

package org.apache.iceberg.flink;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergFilesCommitter {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String warehouse;
  private String tablePath;
  private Table table;

  private final FileFormat format;

  // TODO Add ORC/Parquet unit test once those readers and writers are ready.
  @Parameterized.Parameters(name = "format = {0}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"avro"},
    };
  }

  public TestIcebergFilesCommitter(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    warehouse = folder.getAbsolutePath();

    tablePath = warehouse + "/test";
    Assert.assertTrue("Should create the table directory correctly.", new File(tablePath).mkdir());

    // Construct the iceberg table.
    table = SimpleDataUtil.createTable(tablePath, ImmutableMap.of(), false);
  }

  @Test
  public void testCommitTxnWithoutDataFiles() throws Exception {
    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();

      SimpleDataUtil.assertTableRows(tablePath, Lists.newArrayList());
      assertSnapshotSize(0);
    }
  }

  @Test
  public void testCommitTxn() throws Exception {
    // Test with 3 continues checkpoints:
    //   1. snapshotState for checkpoint#1
    //   2. notifyCheckpointComplete for checkpoint#1
    //   3. snapshotState for checkpoint#2
    //   4. notifyCheckpointComplete for checkpoint#2
    //   5. snapshotState for checkpoint#3
    //   6. notifyCheckpointComplete for checkpoint#3
    long timestamp = 0;

    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();
      assertSnapshotSize(0);

      List<RowData> rows = Lists.newArrayListWithExpectedSize(3);
      for (int i = 1; i <= 3; i++) {
        RowData rowData = SimpleDataUtil.createRowData(i, "hello" + i);
        DataFile dataFile = writeDataFile("data-" + i, ImmutableList.of(rowData));
        harness.processElement(dataFile, ++timestamp);
        rows.add(rowData);

        harness.snapshot(i, ++timestamp);

        harness.notifyOfCompletedCheckpoint(i);

        SimpleDataUtil.assertTableRows(tablePath, ImmutableList.copyOf(rows));
        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(filesCommitterUid, i);
      }
    }
  }

  @Test
  public void testOrderedEventsBetweenCheckpoints() throws Exception {
    // It's possible that two checkpoints happen in the following orders:
    //   1. snapshotState for checkpoint#1;
    //   2. snapshotState for checkpoint#2;
    //   3. notifyCheckpointComplete for checkpoint#1;
    //   4. notifyCheckpointComplete for checkpoint#2;
    long timestamp = 0;

    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();

      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      harness.processElement(dataFile1, ++timestamp);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      // 1. snapshotState for checkpoint#1
      long firstCheckpointId = 1;
      harness.snapshot(firstCheckpointId, ++timestamp);

      RowData row2 = SimpleDataUtil.createRowData(2, "world");
      DataFile dataFile2 = writeDataFile("data-2", ImmutableList.of(row2));
      harness.processElement(dataFile2, ++timestamp);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      // 2. snapshotState for checkpoint#2
      long secondCheckpointId = 2;
      harness.snapshot(secondCheckpointId, ++timestamp);

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      SimpleDataUtil.assertTableRows(tablePath, ImmutableList.of(row1));
      assertMaxCommittedCheckpointId(filesCommitterUid, firstCheckpointId);

      // 4. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      SimpleDataUtil.assertTableRows(tablePath, ImmutableList.of(row1, row2));
      assertMaxCommittedCheckpointId(filesCommitterUid, secondCheckpointId);
    }
  }

  @Test
  public void testDisorderedEventsBetweenCheckpoints() throws Exception {
    // It's possible that the two checkpoints happen in the following orders:
    //   1. snapshotState for checkpoint#1;
    //   2. snapshotState for checkpoint#2;
    //   3. notifyCheckpointComplete for checkpoint#2;
    //   4. notifyCheckpointComplete for checkpoint#1;
    long timestamp = 0;

    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();

      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      RowData row1 = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row1));

      harness.processElement(dataFile1, ++timestamp);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      // 1. snapshotState for checkpoint#1
      long firstCheckpointId = 1;
      harness.snapshot(firstCheckpointId, ++timestamp);

      RowData row2 = SimpleDataUtil.createRowData(2, "world");
      DataFile dataFile2 = writeDataFile("data-2", ImmutableList.of(row2));
      harness.processElement(dataFile2, ++timestamp);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      // 2. snapshotState for checkpoint#2
      long secondCheckpointId = 2;
      harness.snapshot(secondCheckpointId, ++timestamp);

      // 3. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      SimpleDataUtil.assertTableRows(tablePath, ImmutableList.of(row1, row2));
      assertMaxCommittedCheckpointId(filesCommitterUid, secondCheckpointId);

      // 4. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      SimpleDataUtil.assertTableRows(tablePath, ImmutableList.of(row1, row2));
      assertMaxCommittedCheckpointId(filesCommitterUid, secondCheckpointId);
    }
  }

  @Test
  public void testRecoveryFromInvalidSnapshot() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    OperatorSubtaskState snapshot;

    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      RowData row = SimpleDataUtil.createRowData(1, "hello");
      DataFile dataFile = writeDataFile("data-1", ImmutableList.of(row));

      harness.processElement(dataFile, ++timestamp);
      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);
      SimpleDataUtil.assertTableRows(tablePath, ImmutableList.of());
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      AssertHelpers.assertThrows("Could not restore because there's no valid snapshot.",
          IllegalArgumentException.class,
          "There should be an existing iceberg snapshot for current flink job",
          () -> {
            harness.initializeState(snapshot);
            return null;
          });
    }
  }

  @Test
  public void testRecoveryFromValidSnapshot() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> expectedRows = Lists.newArrayList();
    OperatorSubtaskState snapshot;

    String filesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(filesCommitterUid, -1L);

      RowData row = SimpleDataUtil.createRowData(1, "hello");
      expectedRows.add(row);
      DataFile dataFile1 = writeDataFile("data-1", ImmutableList.of(row));

      harness.processElement(dataFile1, ++timestamp);
      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, ImmutableList.of(row));
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(filesCommitterUid)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      SimpleDataUtil.assertTableRows(tablePath, expectedRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);

      RowData row = SimpleDataUtil.createRowData(2, "world");
      expectedRows.add(row);
      DataFile dataFile = writeDataFile("data-2", ImmutableList.of(row));
      harness.processElement(dataFile, ++timestamp);

      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, expectedRows);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(filesCommitterUid, checkpointId);
    }
  }

  @Test
  public void testStartAnotherJobToWriteSameTable() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();

    String oldFilesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(oldFilesCommitterUid)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(oldFilesCommitterUid, -1L);

      for (int i = 1; i <= 3; i++) {
        rows.add(SimpleDataUtil.createRowData(i, "hello" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);
        harness.processElement(dataFile, ++timestamp);
        harness.snapshot(++checkpointId, ++timestamp);

        harness.notifyOfCompletedCheckpoint(checkpointId);
        SimpleDataUtil.assertTableRows(tablePath, tableRows);
        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(oldFilesCommitterUid, checkpointId);
      }
    }

    // The new started job will start with checkpoint = 1 again.
    checkpointId = 0;
    timestamp = 0;
    String newFilesCommitterUid = UUID.randomUUID().toString();
    try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(newFilesCommitterUid)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(oldFilesCommitterUid, 3);
      assertMaxCommittedCheckpointId(newFilesCommitterUid, -1);

      rows.add(SimpleDataUtil.createRowData(2, "world"));
      tableRows.addAll(rows);

      DataFile dataFile = writeDataFile("data-new-1", rows);
      harness.processElement(dataFile, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(tablePath, tableRows);
      assertSnapshotSize(4);
      assertMaxCommittedCheckpointId(newFilesCommitterUid, checkpointId);
    }
  }

  @Test
  public void testMultipleJobsWriteSameTable() throws Exception {
    long timestamp = 0;
    List<RowData> tableRows = Lists.newArrayList();

    for (int i = 0; i < 20; i++) {
      int jobId = i % 3;
      int checkpointId = i / 3;
      String jobUid = String.format("job-%d", jobId);
      try (OneInputStreamOperatorTestHarness<DataFile, Void> harness = createStreamSink(jobUid)) {
        harness.setup();
        harness.open();

        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(jobUid, checkpointId == 0 ? -1 : checkpointId);

        List<RowData> rows = Lists.newArrayList(SimpleDataUtil.createRowData(i, "word-" + i));
        tableRows.addAll(rows);

        DataFile dataFile = writeDataFile(String.format("data-%d", i), rows);
        harness.processElement(dataFile, ++timestamp);
        harness.snapshot(checkpointId + 1, ++timestamp);

        harness.notifyOfCompletedCheckpoint(checkpointId + 1);
        SimpleDataUtil.assertTableRows(tablePath, tableRows);
        assertSnapshotSize(i + 1);
        assertMaxCommittedCheckpointId(jobUid, checkpointId + 1);
      }
    }
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(table.schema(), table.spec(), CONF, tablePath, format.addExtension(filename), rows);
  }

  private void assertMaxCommittedCheckpointId(String filesCommitterUid, long expectedId) {
    table.refresh();
    long actualId = IcebergFilesCommitter.getMaxCommittedCheckpointId(table, filesCommitterUid);
    Assert.assertEquals(expectedId, actualId);
  }

  private void assertSnapshotSize(int expectedSnapshotSize) {
    table.refresh();
    Assert.assertEquals(expectedSnapshotSize, Lists.newArrayList(table.snapshots()).size());
  }

  private OneInputStreamOperatorTestHarness<DataFile, Void> createStreamSink(String filesCommitterUid)
      throws Exception {
    Map<String, String> options = ImmutableMap.of(
        "type", "iceberg",
        FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop",
        FlinkCatalogFactory.HADOOP_WAREHOUSE_LOCATION, warehouse
    );

    IcebergFilesCommitter committer = new IcebergFilesCommitter(filesCommitterUid, "test", options, CONF);
    return new OneInputStreamOperatorTestHarness<>(committer, 1, 1, 0);
  }
}
