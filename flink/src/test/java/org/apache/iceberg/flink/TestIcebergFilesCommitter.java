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
import java.util.Map;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergFilesCommitter {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String path;
  private Table table;

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    path = folder.getAbsolutePath();

    // Construct the iceberg table.
    table = SimpleDataUtil.createTable(path, ImmutableMap.of(), false);
  }

  @Test
  public void testCommitTxnWithoutDataFiles() throws Exception {
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink()) {
      harness.setup();
      harness.open();

      SimpleDataUtil.assertTableRows(path, Lists.newArrayList());
      assertSnapshotSize(0);
    }
  }

  @Test
  public void testCommitTxn() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();

    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink()) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);

      rows.add(SimpleDataUtil.createRowData(1, "hello"));
      tableRows.addAll(rows);
      DataFile dataFile1 = SimpleDataUtil.writeFile(CONF, path, "data-1.parquet", rows);

      harness.processElement(dataFile1, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(path, tableRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(checkpointId);

      rows.add(SimpleDataUtil.createRowData(2, "world"));
      tableRows.addAll(rows);
      DataFile dataFile2 = SimpleDataUtil.writeFile(CONF, path, "data-2.parquet", rows);

      harness.processElement(dataFile2, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(path, tableRows);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(checkpointId);

      rows.add(SimpleDataUtil.createRowData(3, "foo"));
      tableRows.addAll(rows);
      DataFile dataFile3 = SimpleDataUtil.writeFile(CONF, path, "data-3.parquet", rows);

      harness.processElement(dataFile3, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(path, tableRows);
      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(checkpointId);

      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(path, tableRows);
      assertSnapshotSize(3);
      assertMaxCommittedCheckpointId(checkpointId - 1);
    }
  }

  @Test
  public void testRecoveryFromSnapshotWithoutNotifying() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();
    OperatorSubtaskState snapshot;

    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink()) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(-1L);

      rows.add(SimpleDataUtil.createRowData(1, "hello"));
      tableRows.addAll(rows);
      DataFile dataFile1 = SimpleDataUtil.writeFile(CONF, path, "data-1.parquet", rows);

      harness.processElement(dataFile1, ++timestamp);
      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      assertMaxCommittedCheckpointId(-1L);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink()) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      SimpleDataUtil.assertTableRows(path, Lists.newArrayList());
      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(-1L);

      rows.add(SimpleDataUtil.createRowData(2, "foo"));
      tableRows.addAll(rows);
      DataFile dataFile2 = SimpleDataUtil.writeFile(CONF, path, "data-2.parquet", rows);
      harness.processElement(dataFile2, ++timestamp);

      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(path, tableRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(checkpointId);

      rows.add(SimpleDataUtil.createRowData(3, "bar"));
      tableRows.addAll(rows);
      DataFile dataFile3 = SimpleDataUtil.writeFile(CONF, path, "data-3.parquet", rows);

      harness.processElement(dataFile3, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(path, tableRows);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(checkpointId);
    }
  }

  @Test
  public void testRecoveryFromSnapshotWithNotifying() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<RowData> rows = Lists.newArrayList();
    List<RowData> tableRows = Lists.newArrayList();
    OperatorSubtaskState snapshot;

    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink()) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      assertMaxCommittedCheckpointId(-1L);

      rows.add(SimpleDataUtil.createRowData(1, "hello"));
      tableRows.addAll(rows);
      DataFile dataFile1 = SimpleDataUtil.writeFile(CONF, path, "data-1.parquet", rows);

      harness.processElement(dataFile1, ++timestamp);
      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(path, tableRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(checkpointId);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<DataFile, Object> harness = createStreamSink()) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      SimpleDataUtil.assertTableRows(path, tableRows);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(checkpointId);

      rows.add(SimpleDataUtil.createRowData(2, "world"));
      tableRows.addAll(rows);
      DataFile dataFile2 = SimpleDataUtil.writeFile(CONF, path, "data-2.parquet", rows);
      harness.processElement(dataFile2, ++timestamp);

      harness.snapshot(++checkpointId, ++timestamp);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      SimpleDataUtil.assertTableRows(path, tableRows);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(checkpointId);
    }
  }

  private void assertMaxCommittedCheckpointId(long expectedId) {
    table.refresh();
    long actualId = IcebergFilesCommitter.parseMaxCommittedCheckpointId(table.currentSnapshot());
    Assert.assertEquals(expectedId, actualId);
  }

  private void assertSnapshotSize(int expectedSnapshotSize) {
    table.refresh();
    Assert.assertEquals(expectedSnapshotSize, Lists.newArrayList(table.snapshots()).size());
  }

  private OneInputStreamOperatorTestHarness<DataFile, Object> createStreamSink() throws Exception {
    Map<String, String> options = ImmutableMap.of(
        "type", "iceberg",
        FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop",
        FlinkCatalogFactory.HADOOP_WAREHOUSE_LOCATION, "file:" + path
    );

    IcebergFilesCommitter committer = new IcebergFilesCommitter(path, options, CONF);
    return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(committer), 1, 1, 0);
  }
}
