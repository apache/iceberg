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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergRewriteFilesCommitter {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;
  private String tablePath;

  private final FileFormat format;
  private final int formatVersion;

  @Parameterized.Parameters(name = "FileFormat = {0}, FormatVersion={1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"avro", 1},
        new Object[] {"avro", 2},
        new Object[] {"parquet", 1},
        new Object[] {"parquet", 2},
        new Object[] {"orc", 1},
        new Object[] {"orc", 2}
    };
  }

  public TestIcebergRewriteFilesCommitter(String format, int formatVersion) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.formatVersion = formatVersion;
  }

  @Before
  public void setupTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    tablePath = tableDir.getAbsolutePath();
    File flinkManifestFolder = temp.newFolder();

    // Construct the iceberg table.
    Map<String, String> props = ImmutableMap.of(
        TableProperties.DEFAULT_FILE_FORMAT, format.name(),
        TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
        ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath()
    );
    table = SimpleDataUtil.createTable(tablePath, props, true);
  }

  @Test
  public void testCommitPreCheckpoint() throws Exception {
    long timestamp = 0;
    long checkpointId = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<RewriteResult, Void> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      StructLike partitionA = SimpleDataUtil.createPartition("aaa");
      StructLike partitionB = SimpleDataUtil.createPartition("bbb");

      DataFile rewrittenFileA1 = createDataFile("data-old-1", partitionA);
      DataFile rewrittenFileA2 = createDataFile("data-old-2", partitionA);
      DataFile rewrittenFileB1 = createDataFile("data-old-1", partitionB);
      DataFile rewrittenFileB2 = createDataFile("data-old-2", partitionB);
      long snapshot1 = commit(rewrittenFileA1, rewrittenFileA2, rewrittenFileB1, rewrittenFileB2);

      DataFile addedFileA1 = createDataFile("data-new-1", partitionA);
      RewriteResult rewriteResultA1 = RewriteResult.builder(snapshot1, table.spec().partitionType())
          .partition(partitionA)
          .addRewrittenDataFiles(ImmutableList.of(rewrittenFileA1, rewrittenFileA2))
          .addAddedDataFiles(ImmutableList.of(addedFileA1))
          .build();
      harness.processElement(rewriteResultA1, ++timestamp);
      assertSnapshots(1);

      DataFile addedFileB1 = createDataFile("data-new-1", partitionB);
      RewriteResult rewriteResultB1 = RewriteResult.builder(snapshot1, table.spec().partitionType())
          .partition(partitionB)
          .addRewrittenDataFiles(ImmutableList.of(rewrittenFileB1, rewrittenFileB2))
          .addAddedDataFiles(ImmutableList.of(addedFileB1))
          .build();
      harness.processElement(rewriteResultB1, ++timestamp);
      assertSnapshots(1);

      DataFile rewrittenFileA3 = createDataFile("data-old-3", partitionA);
      long snapshot2 = commit(rewrittenFileA3);

      DataFile addedFileA2 = createDataFile("data-new-2", partitionA);
      DataFile addedFileA3 = createDataFile("data-new-3", partitionA);
      RewriteResult rewriteResultA2 = RewriteResult.builder(snapshot2, table.spec().partitionType())
          .partition(partitionA)
          .addRewrittenDataFiles(ImmutableList.of(rewrittenFileA3))
          .addAddedDataFiles(ImmutableList.of(addedFileA2, addedFileA3))
          .build();
      harness.processElement(rewriteResultA2, ++timestamp);
      assertSnapshots(2);

      // rewrite results should be committed in same time if they have same snapshot id.
      harness.snapshot(++checkpointId, ++timestamp);
      assertSnapshots(4);

      List<Snapshot> snapshots = Lists.newArrayList(table.snapshots().iterator());
      Assert.assertEquals("Snapshot id should equal", snapshot1, snapshots.get(0).snapshotId());
      Assert.assertEquals("Snapshot id should equal", snapshot2, snapshots.get(1).snapshotId());
      validateSnapshotFiles(snapshots.get(2).snapshotId(), addedFileA1, addedFileB1, rewrittenFileA3);
      validateSnapshotFiles(snapshots.get(3).snapshotId(), addedFileA1, addedFileB1, addedFileA2, addedFileA3);
    }
  }

  @Test
  public void testCommitRewriteValidation() throws Exception {
    Assume.assumeTrue("Sequence number is only supported in iceberg format v2.", formatVersion > 1);

    long timestamp = 0;
    long checkpointId = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<RewriteResult, Void> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      StructLike partition = SimpleDataUtil.createPartition(null);

      DataFile rewrittenFile = createDataFile("data-old", partition);
      long snapshot1 = commit(rewrittenFile);

      // add deletes files
      DeleteFile posDeleteFile = createPosDeleteFile("pos-delete", partition);
      DeleteFile eqDeleteFile = createEqDeleteFile("eq-delete", partition);
      long snapshot2 = commit(ImmutableList.of(), ImmutableList.of(posDeleteFile, eqDeleteFile));

      DataFile addedFile = createDataFile("data-new-1", partition);
      RewriteResult rewriteResult = RewriteResult.builder(snapshot1, table.spec().partitionType())
          .partition(partition)
          .addRewrittenDataFiles(ImmutableList.of(rewrittenFile))
          .addAddedDataFiles(ImmutableList.of(addedFile))
          .build();
      harness.processElement(rewriteResult, ++timestamp);
      assertSnapshots(2);

      // should commit rewrite fail and abort for found new position delete for rewritten data file.
      harness.snapshot(++checkpointId, ++timestamp);
      assertSnapshots(2);

      List<Snapshot> snapshots = Lists.newArrayList(table.snapshots().iterator());
      Assert.assertEquals("Snapshot id should equal", snapshot1, snapshots.get(0).snapshotId());
      Assert.assertEquals("Snapshot id should equal", snapshot2, snapshots.get(1).snapshotId());
    }
  }

  private long commit(DataFile... dataFiles) {
    return commit(Arrays.asList(dataFiles), ImmutableList.of());
  }

  private long commit(List<DataFile> dataFiles, List<DeleteFile> deleteFiles) {
    RowDelta rowDelta = table.newRowDelta();
    dataFiles.forEach(rowDelta::addRows);
    deleteFiles.forEach(rowDelta::addDeletes);
    rowDelta.commit();
    CreateSnapshotEvent event = (CreateSnapshotEvent) rowDelta.updateEvent();
    return event.snapshotId();
  }

  private String getFilePath(String filename, StructLike partition) {
    return partition == null ? table.locationProvider().newDataLocation(filename) :
        table.locationProvider().newDataLocation(table.spec(), partition, filename);
  }

  private DataFile createDataFile(String filename, StructLike partition) {
    return DataFiles.builder(table.spec())
        .withFormat(format)
        .withPath(getFilePath(filename, partition))
        .withFileSizeInBytes(10)
        .withPartition(partition)
        .withRecordCount(1)
        .build();
  }

  private DeleteFile createPosDeleteFile(String filename, StructLike partition) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withFormat(format)
        .withPath(getFilePath(filename, partition))
        .withFileSizeInBytes(10)
        .withPartition(partition)
        .withRecordCount(1)
        .build();
  }

  private DeleteFile createEqDeleteFile(String filename, StructLike partition) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofEqualityDeletes()
        .withFormat(format)
        .withPath(getFilePath(filename, partition))
        .withFileSizeInBytes(10)
        .withPartition(partition)
        .withRecordCount(1)
        .build();
  }

  private void assertSnapshots(int expectedCount) {
    table.refresh();
    int actualCount = Iterables.size(table.snapshots());
    Assert.assertEquals(expectedCount, actualCount);
  }

  private void validateSnapshotFiles(long snapshotId, DataFile... expectedFiles) {
    Set<CharSequence> expectedFilePaths = Sets.newHashSet();
    for (DataFile file : expectedFiles) {
      expectedFilePaths.add(file.path());
    }
    Set<CharSequence> actualFilePaths = Sets.newHashSet();
    for (FileScanTask task : table.newScan().useSnapshot(snapshotId).planFiles()) {
      actualFilePaths.add(task.file().path());
    }
    Assert.assertEquals("Files should match", expectedFilePaths, actualFilePaths);
  }

  private OneInputStreamOperatorTestHarness<RewriteResult, Void> createStreamOpr(JobID jobID)
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

  private static class TestOperatorFactory extends AbstractStreamOperatorFactory<Void>
      implements OneInputStreamOperatorFactory<RewriteResult, Void> {
    private final String tablePath;

    private TestOperatorFactory(String tablePath) {
      this.tablePath = tablePath;
    }

    private static TestOperatorFactory of(String tablePath) {
      return new TestOperatorFactory(tablePath);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Void>> T createStreamOperator(
        StreamOperatorParameters<Void> param) {
      IcebergRewriteFilesCommitter operator = new IcebergRewriteFilesCommitter(TableLoader.fromHadoopTable(tablePath));
      operator.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergRewriteFilesCommitter.class;
    }
  }
}
