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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.flink.sink.FlinkSinkOptions.STREAMING_REWRITE_MAX_FILES_COUNT;
import static org.apache.iceberg.flink.sink.FlinkSinkOptions.STREAMING_REWRITE_TARGET_FILE_SIZE;

@RunWith(Parameterized.class)
public class TestIcebergStreamRewriter {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;
  private String tablePath;
  private File flinkManifestFolder;
  private FileAppenderFactory<RowData> appenderFactory;

  private final FileFormat format;
  private final int formatVersion;
  private final boolean partitioned;

  @Parameterized.Parameters(name = "FileFormat = {0}, FormatVersion={1}, Partitioned={2}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"avro", 1, false},
        new Object[] {"avro", 1, true},
        new Object[] {"avro", 2, false},
        new Object[] {"avro", 2, true},
        new Object[] {"parquet", 1, false},
        new Object[] {"parquet", 1, true},
        new Object[] {"parquet", 2, false},
        new Object[] {"parquet", 2, true},
        new Object[] {"orc", 1, false},
        new Object[] {"orc", 1, true},
        new Object[] {"orc", 2, false},
        new Object[] {"orc", 2, true},
    };
  }

  public TestIcebergStreamRewriter(String format, int formatVersion, boolean partitioned) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.formatVersion = formatVersion;
    this.partitioned = partitioned;
  }

  @Before
  public void setupTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    tablePath = tableDir.getAbsolutePath();
    flinkManifestFolder = temp.newFolder();

    // Construct the iceberg table.
    Map<String, String> props = ImmutableMap.of(
        TableProperties.DEFAULT_FILE_FORMAT, format.name(),
        TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
        ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath()
    );
    table = SimpleDataUtil.createTable(tablePath, props, partitioned);

    appenderFactory = createDeletableAppenderFactory();
  }

  @Test
  public void testRewriteWithTargetFileSize() throws Exception {
    long targetFileSize = 5000;
    table.updateProperties()
        .set(STREAMING_REWRITE_TARGET_FILE_SIZE, String.valueOf(targetFileSize))
        .set(TableProperties.SPLIT_OPEN_FILE_COST, "0")
        .commit();

    long timestamp = 0;
    long checkpointId = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<PartitionFileGroup, RewriteResult> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      StructLike partition = SimpleDataUtil.createPartition(partitioned ? "xxx" : null);

      List<RowData> expected = Lists.newArrayList();
      expected.add(SimpleDataUtil.createRowData(0, "xxx"));
      DataFile triggerFile = writeDataFile("data-0", partition, ImmutableList.copyOf(expected));

      int cnt = 0;
      long commitFilesSize = 0;
      long weight = triggerFile.fileSizeInBytes();
      while (targetFileSize - commitFilesSize > weight) {
        RowData rowData = SimpleDataUtil.createRowData(cnt, "xxx");
        expected.add(rowData);
        DataFile dataFile = writeDataFile("data-" + cnt, partition, ImmutableList.of(rowData));

        commitFilesSize += dataFile.fileSizeInBytes();
        cnt++;

        PartitionFileGroup fileGroup = commit(partition, ImmutableList.of(dataFile), ImmutableList.of());
        harness.processElement(fileGroup, ++timestamp);
        harness.snapshot(++checkpointId, ++timestamp);
        assertFlinkManifests(cnt);
        Assert.assertTrue(harness.extractOutputValues().isEmpty());
      }

      // trigger rewrite
      PartitionFileGroup fileGroup = commit(partition, ImmutableList.of(triggerFile), ImmutableList.of());
      harness.processElement(fileGroup, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(0);
      Assert.assertEquals(1, harness.extractOutputValues().size());
      RewriteResult rewriteResult = harness.extractOutputValues().get(0);

      Assert.assertEquals(1, rewriteResult.partitions().size());
      Assert.assertEquals(wrap(partition), wrap(rewriteResult.partitions().iterator().next()));
      Assert.assertEquals(formatVersion > 1 ? fileGroup.snapshotId() : 0, rewriteResult.startingSnapshotId());
      Assert.assertEquals(++cnt, Iterables.size(rewriteResult.rewrittenDataFiles()));
      Assert.assertEquals(1, Iterables.size(rewriteResult.addedDataFiles()));

      commitRewrite(rewriteResult);
      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(expected));
    }
  }

  @Test
  public void testRecoveryFromValidSnapshot() throws Exception {
    table.updateProperties()
        .set(STREAMING_REWRITE_MAX_FILES_COUNT, "4")
        .commit();

    StructLike partition = SimpleDataUtil.createPartition(partitioned ? "xxx" : null);

    OperatorSubtaskState snapshot;
    long timestamp = 0;
    long checkpointId = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<PartitionFileGroup, RewriteResult> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      DataFile dataFile11 = writeDataFile("data-txn1-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "xxx"),
          SimpleDataUtil.createRowData(2, "xxx"),
          SimpleDataUtil.createRowData(3, "xxx")
      ));
      PartitionFileGroup fileGroup1 = commit(partition, ImmutableList.of(dataFile11), ImmutableList.of());

      harness.processElement(fileGroup1, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      DataFile dataFile21 = writeDataFile("data-txn2-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(4, "xxx"),
          SimpleDataUtil.createRowData(5, "xxx")
      ));
      DataFile dataFile22 = writeDataFile("data-txn2-2", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(6, "xxx")
      ));
      PartitionFileGroup fileGroup2 = commit(partition, ImmutableList.of(dataFile21, dataFile22), ImmutableList.of());

      harness.processElement(fileGroup2, ++timestamp);
      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(2);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());
    }

    try (OneInputStreamOperatorTestHarness<PartitionFileGroup, RewriteResult> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      DataFile dataFile31 = writeDataFile("data-txn3-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(7, "xxx")
      ));
      PartitionFileGroup fileGroup3 = commit(partition, ImmutableList.of(dataFile31), ImmutableList.of());

      harness.processElement(fileGroup3, ++timestamp);
      assertFlinkManifests(3);  // manifests should not be deleted before snapshot
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(0);
      Assert.assertEquals(1, harness.extractOutputValues().size());
      RewriteResult rewriteResult = harness.extractOutputValues().get(0);

      Assert.assertEquals(1, rewriteResult.partitions().size());
      Assert.assertEquals(wrap(partition), wrap(rewriteResult.partitions().iterator().next()));
      Assert.assertEquals(formatVersion > 1 ? fileGroup3.snapshotId() : 0, rewriteResult.startingSnapshotId());
      Assert.assertEquals(4, Iterables.size(rewriteResult.rewrittenDataFiles()));
      Assert.assertEquals(1, Iterables.size(rewriteResult.addedDataFiles()));

      commitRewrite(rewriteResult);
      List<RowData> expected = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "xxx"),
          SimpleDataUtil.createRowData(2, "xxx"),
          SimpleDataUtil.createRowData(3, "xxx"),
          SimpleDataUtil.createRowData(4, "xxx"),
          SimpleDataUtil.createRowData(5, "xxx"),
          SimpleDataUtil.createRowData(6, "xxx"),
          SimpleDataUtil.createRowData(7, "xxx")
      );
      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(expected));
    }
  }

  @Test
  public void testRewriteUnpartitionedAppendOnlyTable() throws Exception {
    Assume.assumeTrue("Only test for unpartitioned table.", !partitioned);

    table.updateProperties()
        .set(STREAMING_REWRITE_MAX_FILES_COUNT, "4")
        .commit();

    StructLike partition = SimpleDataUtil.createPartition(null);

    long timestamp = 0;
    long checkpointId = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<PartitionFileGroup, RewriteResult> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      // Txn#1 Table[<1, "aaa">, <2, "bbb">, <3, "ccc">]
      DataFile dataFile11 = writeDataFile("data-txn1-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "ccc")
      ));
      PartitionFileGroup fileGroup1 = commit(partition, ImmutableList.of(dataFile11), ImmutableList.of());

      harness.processElement(fileGroup1, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // Txn#2 Table[<1, "aaa">, <2, "bbb">, <3, "ccc">, <4, "ddd">, <3, "eee">, <2, "fff">]
      DataFile dataFile21 = writeDataFile("data-txn2-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(4, "ddd"),
          SimpleDataUtil.createRowData(3, "eee")
      ));
      DataFile dataFile22 = writeDataFile("data-txn2-2", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(2, "fff")
      ));
      PartitionFileGroup fileGroup2 = commit(partition, ImmutableList.of(dataFile21, dataFile22), ImmutableList.of());

      harness.processElement(fileGroup2, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(2);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // Txn#3 Table[<1, "aaa">, <2, "bbb">, <3, "ccc">, <4, "ddd">, <3, "eee">, <2, "fff">, <1, "ggg">]
      DataFile dataFile31 = writeDataFile("data-txn3-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "ggg")
      ));
      PartitionFileGroup fileGroup3 = commit(partition, ImmutableList.of(dataFile31), ImmutableList.of());

      harness.processElement(fileGroup3, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(0);
      Assert.assertEquals(1, harness.extractOutputValues().size());
      RewriteResult rewriteResult = harness.extractOutputValues().get(0);

      Assert.assertEquals(1, rewriteResult.partitions().size());
      Assert.assertEquals(wrap(partition), wrap(rewriteResult.partitions().iterator().next()));
      Assert.assertEquals(formatVersion > 1 ? fileGroup3.snapshotId() : 0, rewriteResult.startingSnapshotId());
      Assert.assertEquals(4, Iterables.size(rewriteResult.rewrittenDataFiles()));
      Assert.assertEquals(1, Iterables.size(rewriteResult.addedDataFiles()));

      commitRewrite(rewriteResult);
      List<RowData> expected = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "ccc"),
          SimpleDataUtil.createRowData(4, "ddd"),
          SimpleDataUtil.createRowData(3, "eee"),
          SimpleDataUtil.createRowData(2, "fff"),
          SimpleDataUtil.createRowData(1, "ggg")
      );
      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(expected));
    }
  }

  @Test
  public void testRewritePartitionedAppendOnlyTable() throws Exception {
    Assume.assumeTrue("Only test for partitioned table.", partitioned);

    table.updateProperties()
        .set(STREAMING_REWRITE_MAX_FILES_COUNT, "3")
        .commit();

    long timestamp = 0;
    long checkpointId = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<PartitionFileGroup, RewriteResult> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      StructLike partitionA = SimpleDataUtil.createPartition("aaa");
      StructLike partitionB = SimpleDataUtil.createPartition("bbb");

      // Txn#1: PartitionA[<1, "aaa">, <2, "aaa">] PartitionB[<1, "bbb">]
      DataFile dataFile11 = writeDataFile("data-txn1-1", partitionA, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "aaa")
      ));
      PartitionFileGroup fileGroup11 = commit(dataFile11.partition(), ImmutableList.of(dataFile11),
          ImmutableList.of());

      harness.processElement(fileGroup11, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      DataFile dataFile12 = writeDataFile("data-txn1-2", partitionB, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "bbb")
      ));
      PartitionFileGroup fileGroup12 = commit(dataFile12.partition(), ImmutableList.of(dataFile12),
          ImmutableList.of());

      harness.processElement(fileGroup12, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(2);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // Txn#2: PartitionA[<1, "aaa">, <2, "aaa">] PartitionB[<1, "bbb">, <2, "bbb">]
      DataFile dataFile21 = writeDataFile("data-txn2-1", partitionB, ImmutableList.of(
          SimpleDataUtil.createRowData(2, "bbb")
      ));
      PartitionFileGroup fileGroup21 = commit(dataFile21.partition(), ImmutableList.of(dataFile21),
          ImmutableList.of());

      harness.processElement(fileGroup21, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(3);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // Txn#3: PartitionA[<1, "aaa">, <2, "aaa">, <3, "aaa">] PartitionB[<1, "bbb">, <2, "bbb">, <3, "bbb">]
      DataFile dataFile31 = writeDataFile("data-txn3-1", partitionA, ImmutableList.of(
          SimpleDataUtil.createRowData(3, "aaa")
      ));
      PartitionFileGroup fileGroup31 = commit(dataFile31.partition(), ImmutableList.of(dataFile31),
          ImmutableList.of());

      harness.processElement(fileGroup31, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(4);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      DataFile dataFile32 = writeDataFile("data-txn3-2", partitionB, ImmutableList.of(
          SimpleDataUtil.createRowData(3, "bbb")
      ));
      PartitionFileGroup fileGroup32 = commit(dataFile32.partition(), ImmutableList.of(dataFile32),
          ImmutableList.of());

      harness.processElement(fileGroup32, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(2);
      Assert.assertEquals(1, harness.extractOutputValues().size());
      RewriteResult rewriteResult = harness.extractOutputValues().get(0);

      Assert.assertEquals(1, rewriteResult.partitions().size());
      Assert.assertEquals(wrap(fileGroup32.partition()), wrap(rewriteResult.partitions().iterator().next()));
      Assert.assertEquals(formatVersion > 1 ? fileGroup32.snapshotId() : 0, rewriteResult.startingSnapshotId());
      Assert.assertEquals(3, Iterables.size(rewriteResult.rewrittenDataFiles()));
      Assert.assertEquals(1, Iterables.size(rewriteResult.addedDataFiles()));

      commitRewrite(rewriteResult);
      List<RowData> expected = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "aaa"),
          SimpleDataUtil.createRowData(3, "aaa"),
          SimpleDataUtil.createRowData(1, "bbb"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "bbb")
      );
      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(expected));
    }
  }

  @Test
  public void testRewriteUnpartitionedUpdatableTable() throws Exception {
    Assume.assumeTrue("Rewriting delete files is only supported in iceberg format v2.", formatVersion > 1);
    Assume.assumeTrue("Only test for unpartitioned table.", !partitioned);

    table.updateProperties()
        .set(STREAMING_REWRITE_MAX_FILES_COUNT, "4")
        .commit();

    StructLike partition = SimpleDataUtil.createPartition(null);

    long timestamp = 0;
    long checkpointId = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<PartitionFileGroup, RewriteResult> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      // Txn#1 Table[<1, "aaa">, <3, "ccc">]
      DataFile dataFile11 = writeDataFile("data-txn1-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "ccc")
      ));
      DeleteFile deleteFile11 = writePosDeleteFile("pos-delete-txn1-1", partition, ImmutableList.of(
          Pair.of(dataFile11.path(), 1L))
      );
      PartitionFileGroup fileGroup1 = commit(partition, ImmutableList.of(dataFile11),
          ImmutableList.of(deleteFile11));

      harness.processElement(fileGroup1, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(2);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // Txn#2 Table[<1, "aaa">, <3, "xxx">, <4, "ddd">, <5, "eee">]
      DataFile dataFile21 = writeDataFile("data-txn2-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(3, "xxx"),
          SimpleDataUtil.createRowData(4, "ddd")
      ));
      DeleteFile deleteFile21 = writeEqDeleteFile("eq-delete-txn2-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(3, "ccc")
      ));
      DataFile dataFile22 = writeDataFile("data-txn2-2", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(5, "eee")
      ));
      PartitionFileGroup fileGroup2 = commit(partition, ImmutableList.of(dataFile21, dataFile22),
          ImmutableList.of(deleteFile21));

      harness.processElement(fileGroup2, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(4);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // Txn#3 Table[<1, "aaa">, <3, "xxx">, <5, "eee">, <7, "ggg">]
      DataFile dataFile31 = writeDataFile("data-txn3-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(7, "ggg")
      ));
      DeleteFile deleteFile31 = writeEqDeleteFile("eq-delete-txn3-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(4, "ddd")
      ));
      PartitionFileGroup fileGroup3 = commit(partition, ImmutableList.of(dataFile31),
          ImmutableList.of(deleteFile31));

      harness.processElement(fileGroup3, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(0);
      Assert.assertEquals(1, harness.extractOutputValues().size());
      RewriteResult rewriteResult = harness.extractOutputValues().get(0);

      Assert.assertEquals(1, rewriteResult.partitions().size());
      Assert.assertEquals(wrap(partition), wrap(rewriteResult.partitions().iterator().next()));
      Assert.assertEquals(fileGroup3.snapshotId(), rewriteResult.startingSnapshotId());
      Assert.assertEquals(4, Iterables.size(rewriteResult.rewrittenDataFiles()));
      Assert.assertEquals(1, Iterables.size(rewriteResult.addedDataFiles()));

      commitRewrite(rewriteResult);
      List<RowData> expected = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(3, "xxx"),
          SimpleDataUtil.createRowData(5, "eee"),
          SimpleDataUtil.createRowData(7, "ggg")
      );
      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(expected));
    }
  }

  @Test
  public void testRewritePartitionedUpdatableTable() throws Exception {
    Assume.assumeTrue("Rewriting delete files is only supported in iceberg format v2.", formatVersion > 1);
    Assume.assumeTrue("Only test for partitioned table.", partitioned);

    table.updateProperties()
        .set(STREAMING_REWRITE_MAX_FILES_COUNT, "3")
        .commit();

    long timestamp = 0;
    long checkpointId = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<PartitionFileGroup, RewriteResult> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      StructLike partitionA = SimpleDataUtil.createPartition("aaa");
      StructLike partitionB = SimpleDataUtil.createPartition("bbb");

      // Txn#1: PartitionA[<1, "aaa">, <3, "aaa">] PartitionB[<4, "bbb">]
      DataFile dataFile11 = writeDataFile("data-txn1-1", partitionA, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "aaa"),
          SimpleDataUtil.createRowData(3, "aaa")
      ));
      DeleteFile deleteFile11 = writePosDeleteFile("pos-delete-txn1-1", partitionA, ImmutableList.of(
          Pair.of(dataFile11.path(), 1L))
      );
      PartitionFileGroup fileGroup11 = commit(dataFile11.partition(), ImmutableList.of(dataFile11),
          ImmutableList.of(deleteFile11));

      harness.processElement(fileGroup11, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(2);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      DataFile dataFile12 = writeDataFile("data-txn1-2", partitionB, ImmutableList.of(
          SimpleDataUtil.createRowData(4, "bbb")
      ));
      PartitionFileGroup fileGroup12 = commit(dataFile12.partition(), ImmutableList.of(dataFile12),
          ImmutableList.of());

      harness.processElement(fileGroup12, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(3);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // Txn#2: PartitionA[<1, "aaa">] PartitionB[<3, "bbb">, <6, "bbb">]
      DeleteFile deleteFile21 = writeEqDeleteFile("eq-delete-txn2-1", partitionA, ImmutableList.of(
          SimpleDataUtil.createRowData(3, "aaa")
      ));
      PartitionFileGroup fileGroup21 = commit(deleteFile21.partition(), ImmutableList.of(),
          ImmutableList.of(deleteFile21));

      harness.processElement(fileGroup21, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(4);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      DataFile dataFile22 = writeDataFile("data-txn2-2", partitionB, ImmutableList.of(
          SimpleDataUtil.createRowData(3, "bbb"),
          SimpleDataUtil.createRowData(5, "bbb"),
          SimpleDataUtil.createRowData(6, "bbb")
      ));
      DeleteFile deleteFile22 = writePosDeleteFile("pos-delete-txn2-2", partitionB, ImmutableList.of(
          Pair.of(dataFile22.path(), 1L))
      );
      DeleteFile deleteFile23 = writeEqDeleteFile("eq-delete-txn2-3", partitionB, ImmutableList.of(
          SimpleDataUtil.createRowData(4, "bbb")
      ));
      PartitionFileGroup fileGroup22 = commit(dataFile22.partition(), ImmutableList.of(dataFile22),
          ImmutableList.of(deleteFile22, deleteFile23));

      harness.processElement(fileGroup22, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(6);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // Txn#3: PartitionA[<1, "aaa">, <5, "aaa">] PartitionB[<3, "bbb">, <7, "bbb">]
      DataFile dataFile31 = writeDataFile("data-txn3-1", partitionA, ImmutableList.of(
          SimpleDataUtil.createRowData(5, "aaa")
      ));
      PartitionFileGroup fileGroup31 = commit(dataFile31.partition(), ImmutableList.of(dataFile31),
          ImmutableList.of());

      harness.processElement(fileGroup31, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(7);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      DataFile dataFile32 = writeDataFile("data-txn3-2", partitionB, ImmutableList.of(
          SimpleDataUtil.createRowData(7, "bbb")
      ));
      DeleteFile deleteFile32 = writeEqDeleteFile("eq-delete-txn3-2", partitionB, ImmutableList.of(
          SimpleDataUtil.createRowData(6, "bbb")
      ));
      PartitionFileGroup fileGroup32 = commit(dataFile32.partition(), ImmutableList.of(dataFile32),
          ImmutableList.of(deleteFile32));

      harness.processElement(fileGroup32, ++timestamp);
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(4);
      Assert.assertEquals(1, harness.extractOutputValues().size());
      RewriteResult rewriteResult = harness.extractOutputValues().get(0);

      Assert.assertEquals(1, rewriteResult.partitions().size());
      Assert.assertEquals(wrap(fileGroup32.partition()), wrap(rewriteResult.partitions().iterator().next()));
      Assert.assertEquals(fileGroup32.snapshotId(), rewriteResult.startingSnapshotId());
      Assert.assertEquals(3, Iterables.size(rewriteResult.rewrittenDataFiles()));
      Assert.assertEquals(1, Iterables.size(rewriteResult.addedDataFiles()));

      commitRewrite(rewriteResult);
      List<RowData> expected = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(3, "bbb"),
          SimpleDataUtil.createRowData(5, "aaa"),
          SimpleDataUtil.createRowData(7, "bbb")
      );
      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(expected));
    }
  }

  private StructLikeWrapper wrap(StructLike partition) {
    return StructLikeWrapper.forType(table.spec().partitionType()).set(partition);
  }

  private PartitionFileGroup commit(StructLike partition, List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
    RowDelta rowDelta = table.newRowDelta();
    dataFiles.forEach(rowDelta::addRows);
    deleteFiles.forEach(rowDelta::addDeletes);
    rowDelta.commit();
    CreateSnapshotEvent updateEvent = (CreateSnapshotEvent) rowDelta.updateEvent();
    return PartitionFileGroup
        .builder(updateEvent.sequenceNumber(), updateEvent.snapshotId(), partition)
        .addDataFile(dataFiles)
        .addDeleteFile(deleteFiles)
        .build();
  }

  private void commitRewrite(RewriteResult result) {
    RewriteFiles rewriteFiles = table.newRewrite()
        .validateFromSnapshot(result.startingSnapshotId())
        .rewriteFiles(Sets.newHashSet(result.rewrittenDataFiles()), Sets.newHashSet(result.addedDataFiles()));
    rewriteFiles.commit();
  }

  private DataFile writeDataFile(String filename, StructLike partition, List<RowData> rows)
      throws IOException {
    return SimpleDataUtil.writeDataFile(table, format, filename, appenderFactory, partition, rows);
  }

  private DeleteFile writeEqDeleteFile(String filename, StructLike partition, List<RowData> deletes)
      throws IOException {
    return SimpleDataUtil.writeEqDeleteFile(table, format, filename, appenderFactory, partition, deletes);
  }

  private DeleteFile writePosDeleteFile(String filename, StructLike partition, List<Pair<CharSequence, Long>> positions)
      throws IOException {
    return SimpleDataUtil.writePosDeleteFile(table, format, filename, appenderFactory, partition, positions);
  }

  private FileAppenderFactory<RowData> createDeletableAppenderFactory() {
    int[] equalityFieldIds = new int[] {
        table.schema().findField("id").fieldId(),
        table.schema().findField("data").fieldId()
    };
    return new FlinkAppenderFactory(table.schema(),
        FlinkSchemaUtil.convert(table.schema()), table.properties(), table.spec(), equalityFieldIds,
        table.schema(), null);
  }

  private void assertFlinkManifests(int expectedCount) throws IOException {
    List<Path> manifests = Files.list(flinkManifestFolder.toPath())
        .filter(p -> !p.toString().endsWith(".crc"))
        .collect(Collectors.toList());
    Assert.assertEquals(String.format("Expected %s flink manifests, but the list is: %s", expectedCount, manifests),
        expectedCount, manifests.size());
  }

  private OneInputStreamOperatorTestHarness<PartitionFileGroup, RewriteResult> createStreamOpr(JobID jobID)
      throws Exception {
    TestOperatorFactory factory = TestOperatorFactory.of(tablePath);
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

  private static class TestOperatorFactory extends AbstractStreamOperatorFactory<RewriteResult>
      implements OneInputStreamOperatorFactory<PartitionFileGroup, RewriteResult> {
    private final String tablePath;

    private TestOperatorFactory(String tablePath) {
      this.tablePath = tablePath;
    }

    private static TestOperatorFactory of(String tablePath) {
      return new TestOperatorFactory(tablePath);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<RewriteResult>> T createStreamOperator(
        StreamOperatorParameters<RewriteResult> param) {
      IcebergStreamRewriter operator = new IcebergStreamRewriter(TableLoader.fromHadoopTable(tablePath));
      operator.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergStreamRewriter.class;
    }
  }
}
