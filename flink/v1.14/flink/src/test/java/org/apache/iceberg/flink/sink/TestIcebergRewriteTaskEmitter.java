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
import java.util.Arrays;
import java.util.Comparator;
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
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergRewriteTaskEmitter {

  private static final String FLINK_JOB_ID = "flink.job-id";

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;
  private String tablePath;
  private File flinkManifestFolder;

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
        new Object[] {"orc", 2},
    };
  }

  public TestIcebergRewriteTaskEmitter(String format, int formatVersion) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.formatVersion = formatVersion;
  }

  @Before
  public void setupTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    tablePath = tableDir.getAbsolutePath();

    // Construct the iceberg table.
    flinkManifestFolder = temp.newFolder();
    Map<String, String> props = ImmutableMap.of(
        TableProperties.DEFAULT_FILE_FORMAT, format.name(),
        TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
        ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath()
    );
    table = SimpleDataUtil.createTable(tablePath, props, false);
  }

  @Test
  public void testSplitLargeFile() throws Exception {
    long targetFileSize = 5000;

    table.updateProperties()
        .set(FlinkSinkOptions.STREAMING_REWRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSize))
        .set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(Integer.MAX_VALUE))
        .commit();

    long timestamp = 0;
    long checkpoint = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      StructLike partition = SimpleDataUtil.createPartition(null);

      // qualified file should not trigger emit.
      DataFile qualifiedFile = createDataFile("data-qualified", partition, (long) (targetFileSize * 1.2));
      CommitResult commit1 = commit(
          jobID.toString(),
          ImmutableList.of(qualifiedFile),
          ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit1, ++timestamp);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());
      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(1);

      // large file will trigger emit.
      DataFile largeFile = createDataFile("data-large", partition, (targetFileSize * 2));
      DeleteFile posDeleteFile = createPosDeleteFile("pos-delete", partition);
      DeleteFile eqDeleteFile = createEqDeleteFile("eq-delete", partition);
      CommitResult commit2 = commit(
          jobID.toString(),
          ImmutableList.of(largeFile),
          formatVersion > 1 ? ImmutableList.of(posDeleteFile, eqDeleteFile) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(largeFile.path()) : ImmutableList.of()
      );

      harness.processElement(commit2, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 3 : 2);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(2, harness.extractOutputValues().size());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(0);  // delete rewritten file groups manifests after checkpoint.

      // qualified file no need to rewrite and should be ignored.
      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(largeFile, posDeleteFile)
          ),
          createRewriteTask(commit2.snapshotId(), partition,
              combined(largeFile, posDeleteFile)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(largeFile)
          ),
          createRewriteTask(commit2.snapshotId(), partition,
              combined(largeFile)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);

      actual.forEach(task -> Assert.assertEquals(targetFileSize, task.task().files().iterator().next().length()));
    }
  }

  @Test
  public void testCombineMixedFiles() throws Exception {
    long targetFileSize = 5000;

    table.updateProperties()
        .set(FlinkSinkOptions.STREAMING_REWRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSize))
        .set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(Integer.MAX_VALUE))
        .commit();

    long timestamp = 0;
    long checkpoint = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      StructLike partition = SimpleDataUtil.createPartition(null);

      // qualified file should not trigger emit.
      DataFile qualifiedFile = createDataFile("data-qualified", partition, (long) (targetFileSize * 1.2));
      CommitResult commit1 = commit(
          jobID.toString(),
          ImmutableList.of(qualifiedFile),
          ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit1, ++timestamp);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());
      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(1);

      // small files will trigger emit
      DataFile largeFile = createDataFile("data-large", partition, (long) (targetFileSize * 0.7));
      DataFile mediumFile = createDataFile("data-medium ", partition, (long) (targetFileSize * 0.2));
      DataFile smallFile = createDataFile("data-small", partition, (long) (targetFileSize * 0.1));
      DeleteFile posDeleteFile = createPosDeleteFile("pos-delete", partition);
      DeleteFile eqDeleteFile = createEqDeleteFile("eq-delete", partition);
      CommitResult commit2 = commit(
          jobID.toString(),
          ImmutableList.of(largeFile, mediumFile, smallFile),
          formatVersion > 1 ? ImmutableList.of(posDeleteFile, eqDeleteFile) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(largeFile.path()) : ImmutableList.of()
      );

      harness.processElement(commit2, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 3 : 2);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(1, harness.extractOutputValues().size());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(0);  // delete rewritten file groups manifests after checkpoint.

      // qualified file no need to rewrite and should be ignored.
      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(largeFile, posDeleteFile),
              combined(mediumFile, posDeleteFile),
              combined(smallFile, posDeleteFile)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(largeFile),
              combined(mediumFile),
              combined(smallFile)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);
    }
  }

  @Test
  public void testCombineFilesByFilesCount() throws Exception {
    table.updateProperties().set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(3)).commit();

    long timestamp = 0;
    long checkpoint = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      StructLike partition = SimpleDataUtil.createPartition(null);

      DataFile dataFile1 = createDataFile("data-1", partition);
      DataFile dataFile2 = createDataFile("data-2", partition);
      DeleteFile posDeleteFile = createPosDeleteFile("pos-delete", partition);
      CommitResult commit1 = commit(
          jobID.toString(),
          ImmutableList.of(dataFile1, dataFile2),
          formatVersion > 1 ? ImmutableList.of(posDeleteFile) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(dataFile1.path()) : ImmutableList.of()
      );
      harness.processElement(commit1, ++timestamp);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());
      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(formatVersion > 1 ? 2 : 1);

      // emit rewrite task when reach files count.
      DataFile dataFile3 = createDataFile("data-3", partition);
      DeleteFile eqDeleteFile = createEqDeleteFile("eq-delete", partition);
      CommitResult commit2 = commit(
          jobID.toString(),
          ImmutableList.of(dataFile3),
          formatVersion > 1 ? ImmutableList.of(eqDeleteFile) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 4 : 2);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(1, harness.extractOutputValues().size());
      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(0);  // delete rewritten file groups manifests after checkpoint.

      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(dataFile1, posDeleteFile, eqDeleteFile),
              combined(dataFile2, posDeleteFile, eqDeleteFile),
              combined(dataFile3)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(dataFile1),
              combined(dataFile2),
              combined(dataFile3)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);
    }
  }

  @Test
  public void testFlushPendingRewriteFiles() throws Exception {
    table.updateSpec().addField("data").commit();

    table.updateProperties()
        .set(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, String.valueOf(Integer.MAX_VALUE))
        .set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(Integer.MAX_VALUE))
        .set(FlinkSinkOptions.STREAMING_REWRITE_MIN_GROUP_FILES, String.valueOf(2))
        .set(FlinkSinkOptions.STREAMING_REWRITE_MAX_WAITING_COMMITS, String.valueOf(1))
        .commit();

    long timestamp = 0;
    long checkpoint = 10;
    JobID jobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(jobId)) {
      harness.setup();
      harness.open();

      StructLike partitionPass = SimpleDataUtil.createPartition("hour=00");
      StructLike partitionCurr = SimpleDataUtil.createPartition("hour=01");

      // add files to pass partition
      DataFile remainDataFile1 = createDataFile("data-remain-1", partitionPass);
      DeleteFile remainPosDeleteFile = createPosDeleteFile("pos-delete-remain", partitionPass);
      CommitResult commit1 = commit(
          jobId.toString(),
          ImmutableList.of(remainDataFile1),
          formatVersion > 1 ? ImmutableList.of(remainPosDeleteFile) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(remainDataFile1.path()) : ImmutableList.of()
      );
      harness.processElement(commit1, ++timestamp);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());
      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(formatVersion > 1 ? 2 : 1);

      // start write files to curr partition and never have new file will add to pass partition.
      DataFile remainDataFile2 = createDataFile("data-remain-2", partitionPass);
      DeleteFile remainEqDeleteFile = createEqDeleteFile("eq-delete-remain", partitionPass);
      DataFile newDataFile1 = createDataFile("data-new-1", partitionCurr);
      CommitResult commit2 = commit(
          jobId.toString(),
          ImmutableList.of(remainDataFile2, newDataFile1),
          formatVersion > 1 ? ImmutableList.of(remainEqDeleteFile) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());
      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(formatVersion > 1 ? 5 : 3);

      // pass partition wait one commit.
      DataFile newDataFile2 = createDataFile("data-new-2", partitionCurr);
      CommitResult commit3 = commit(
          jobId.toString(),
          ImmutableList.of(newDataFile2),
          ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit3, ++timestamp);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());
      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(formatVersion > 1 ? 6 : 4);

      // flush pass partition files as rewrite task.
      DataFile newDataFile3 = createDataFile("data-new-3", partitionCurr);
      CommitResult commit4 = commit(
          jobId.toString(),
          ImmutableList.of(newDataFile3),
          ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit4, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 7 : 5);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(1, harness.extractOutputValues().size());
      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(3);  // delete rewritten file groups manifests after checkpoint.

      // rewrite task snapshot id should be the latest commit of the pass partition.
      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partitionPass,
              combined(remainDataFile1, remainPosDeleteFile, remainEqDeleteFile),
              combined(remainDataFile2)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partitionPass,
              combined(remainDataFile1),
              combined(remainDataFile2)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);

    }
  }

  @Test
  public void testRecoveryFromValidSnapshotForUnpartitionedTable() throws Exception {
    table.updateSpec().addField("data").commit();
    table.updateProperties().set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(4)).commit();

    long timestamp = 0;
    long checkpoint = 10;
    OperatorSubtaskState snapshot;

    StructLike partition = SimpleDataUtil.createPartition(null);

    // appended files in old flink job, should be included in rewrite tasks
    JobID oldJobId = new JobID();
    DataFile dataFile = createDataFile("data-txn1-1", partition);
    CommitResult commit1 = commit(
        oldJobId.toString(),
        ImmutableList.of(dataFile),
        ImmutableList.of(),
        ImmutableList.of()
    );

    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(oldJobId)) {
      harness.setup();
      harness.open();

      harness.processElement(commit1, ++timestamp);
      assertFlinkManifests(1);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      snapshot = harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(1);  // no rewritten file groups manifests should be deleted.
    }

    // add not appended files
    DataFile missingDataFile11 = createDataFile("data-txn2-1", partition);
    DataFile missingDataFile12 = createDataFile("data-txn2-2", partition);
    DeleteFile missingDeleteFile11 = createPosDeleteFile("pos-delete-txn2-1", partition);
    DeleteFile missingDeleteFile12 = createEqDeleteFile("eq-delete-txn2-1", partition);
    CommitResult commit2 = commit(
        oldJobId.toString(),
        ImmutableList.of(missingDataFile11, missingDataFile12),
        formatVersion > 1 ? ImmutableList.of(missingDeleteFile11, missingDeleteFile12) : ImmutableList.of(),
        formatVersion > 1 ? ImmutableList.of(missingDataFile11.path()) : ImmutableList.of()
    );

    // add other files to table between two txn, only the added eq-delete files should be appended
    DataFile otherDataFile = createDataFile("data-other-1", partition);
    DeleteFile otherPosDeleteFile = createPosDeleteFile("pos-delete-other-1", partition);
    DeleteFile otherEqDeleteFile = createEqDeleteFile("eq-delete-other-1", partition);
    commit(
        null,
        ImmutableList.of(otherDataFile),
        formatVersion > 1 ? ImmutableList.of(otherPosDeleteFile, otherEqDeleteFile) : ImmutableList.of(),
        formatVersion > 1 ? ImmutableList.of(otherDataFile.path()) : ImmutableList.of()
    );

    // appended latest committed files
    DataFile missingDataFile21 = createDataFile("data-txn3-1", partition);
    DeleteFile missingDeleteFile22 = createEqDeleteFile("eq-delete-txn3-2", partition);
    CommitResult commit3 = commit(
        oldJobId.toString(),
        ImmutableList.of(missingDataFile21),
        formatVersion > 1 ? ImmutableList.of(missingDeleteFile22) : ImmutableList.of(),
        ImmutableList.of()
    );

    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(newJobId)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      assertFlinkManifests(formatVersion > 1 ? 6 : 3);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(1, harness.extractOutputValues().size());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(0);  // delete rewritten file groups manifests after checkpoint.

      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit3.snapshotId(), partition,
              combined(dataFile, missingDeleteFile11, missingDeleteFile12, otherEqDeleteFile, missingDeleteFile22),
              combined(missingDataFile11, missingDeleteFile11, otherEqDeleteFile, missingDeleteFile22),
              combined(missingDataFile12, missingDeleteFile11, otherEqDeleteFile, missingDeleteFile22),
              combined(missingDataFile21)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit3.snapshotId(), partition,
              combined(dataFile),
              combined(missingDataFile11),
              combined(missingDataFile12),
              combined(missingDataFile21)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);
    }
  }

  @Test
  public void testRecoveryFromValidSnapshotForPartitionedTable() throws Exception {
    table.updateSpec().addField("data").commit();
    table.updateProperties().set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(4)).commit();

    long timestamp = 0;
    long checkpoint = 10;
    OperatorSubtaskState snapshot;

    StructLike partitionA = SimpleDataUtil.createPartition("aaa");
    StructLike partitionB = SimpleDataUtil.createPartition("bbb");

    // appended files in old flink job, should be included in rewrite tasks
    JobID oldJobId = new JobID();
    DataFile dataFileA = createDataFile("data-txn1-1", partitionA);
    DataFile dataFileB = createDataFile("data-txn1-2", partitionB);
    CommitResult commit1 = commit(
        oldJobId.toString(),
        ImmutableList.of(dataFileA, dataFileB),
        ImmutableList.of(),
        ImmutableList.of()
    );

    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(oldJobId)) {
      harness.setup();
      harness.open();

      harness.processElement(commit1, ++timestamp);
      assertFlinkManifests(2);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      snapshot = harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(2);  // no rewritten file groups manifests should be deleted.
    }

    // add not appended files
    DataFile missingDataFileA11 = createDataFile("data-txn2-1", partitionA);
    DataFile missingDataFileA12 = createDataFile("data-txn2-2", partitionA);
    DeleteFile missingDeleteFileA11 = createPosDeleteFile("pos-delete-txn2-1", partitionA);
    DeleteFile missingDeleteFileA12 = createEqDeleteFile("eq-delete-txn2-1", partitionA);
    DataFile missingDataFileB11 = createDataFile("data-txn2-1", partitionB);
    DataFile missingDataFileB12 = createDataFile("data-txn2-2", partitionB);
    CommitResult commit2 = commit(
        oldJobId.toString(),
        ImmutableList.of(missingDataFileA11, missingDataFileA12, missingDataFileB11, missingDataFileB12),
        formatVersion > 1 ? ImmutableList.of(missingDeleteFileA11, missingDeleteFileA12) : ImmutableList.of(),
        formatVersion > 1 ? ImmutableList.of(missingDataFileA11.path()) : ImmutableList.of()
    );

    // add other files to table between two txn, only the added eq-delete files should be appended
    DataFile otherDataFileA = createDataFile("data-other-1", partitionA);
    DeleteFile otherPosDeleteFileA = createPosDeleteFile("pos-delete-other-1", partitionA);
    DeleteFile otherEqDeleteFileA = createEqDeleteFile("eq-delete-other-1", partitionA);
    DataFile otherDataFileB = createDataFile("data-other-1", partitionB);
    commit(
        null,
        ImmutableList.of(otherDataFileA, otherDataFileB),
        formatVersion > 1 ? ImmutableList.of(otherPosDeleteFileA, otherEqDeleteFileA) : ImmutableList.of(),
        formatVersion > 1 ? ImmutableList.of(otherDataFileA.path()) : ImmutableList.of()
    );

    // appended latest committed files
    DataFile missingDataFileA21 = createDataFile("data-txn3-1", partitionA);
    DeleteFile missingDeleteFileA21 = createEqDeleteFile("eq-delete-txn3-1", partitionA);
    DataFile missingDataFileB21 = createDataFile("data-txn3-1", partitionB);
    DeleteFile missingDeleteFileB22 = createEqDeleteFile("eq-delete-txn3-2", partitionB);
    CommitResult commit3 = commit(
        oldJobId.toString(),
        ImmutableList.of(missingDataFileA21, missingDataFileB21),
        formatVersion > 1 ? ImmutableList.of(missingDeleteFileA21, missingDeleteFileB22) : ImmutableList.of(),
        ImmutableList.of()
    );

    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(newJobId)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      assertFlinkManifests(formatVersion > 1 ? 10 : 6);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(2, harness.extractOutputValues().size());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(0);  // delete rewritten file groups manifests after checkpoint.

      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit3.snapshotId(), partitionA,
              combined(dataFileA, missingDeleteFileA11, missingDeleteFileA12, otherEqDeleteFileA, missingDeleteFileA21),
              combined(missingDataFileA11, missingDeleteFileA11, otherEqDeleteFileA, missingDeleteFileA21),
              combined(missingDataFileA12, missingDeleteFileA11, otherEqDeleteFileA, missingDeleteFileA21),
              combined(missingDataFileA21)
          ),
          createRewriteTask(commit3.snapshotId(), partitionB,
              combined(dataFileB, missingDeleteFileB22),
              combined(missingDataFileB11, missingDeleteFileB22),
              combined(missingDataFileB12, missingDeleteFileB22),
              combined(missingDataFileB21)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit3.snapshotId(), partitionA,
              combined(dataFileA),
              combined(missingDataFileA11),
              combined(missingDataFileA12),
              combined(missingDataFileA21)
          ),
          createRewriteTask(commit3.snapshotId(), partitionB,
              combined(dataFileB),
              combined(missingDataFileB11),
              combined(missingDataFileB12),
              combined(missingDataFileB21)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);
    }
  }

  @Test
  public void testContinuousCommitsForUnpartitionedTable() throws Exception {
    table.updateProperties().set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(3)).commit();

    long timestamp = 0;
    long checkpoint = 10;
    JobID currentJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(currentJobId)) {
      harness.setup();
      harness.open();

      StructLike partition = SimpleDataUtil.createPartition(null);

      // add exists files to table which should be ignored by emitter
      DataFile existDataFile1 = createDataFile("data-exist-1", partition);
      DataFile existDataFile2 = createDataFile("data-exist-2", partition);
      commit(null, ImmutableList.of(existDataFile1, existDataFile2), ImmutableList.of(), ImmutableList.of());

      // Txn#1: init last emitted snapshot id and emit last committed files
      DataFile newDataFile11 = createDataFile("data-txn1-1", partition);
      DataFile newDataFile12 = createDataFile("data-txn1-2", partition);
      DeleteFile newDeleteFile11 = createPosDeleteFile("pos-delete-txn1-1", partition);
      CommitResult commit1 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFile11, newDataFile12),
          formatVersion > 1 ? ImmutableList.of(newDeleteFile11) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(newDataFile11.path()) : ImmutableList.of()
      );

      harness.processElement(commit1, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 2 : 1);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(formatVersion > 1 ? 2 : 1);  // no rewritten file groups manifests should be deleted.

      // Txn#2: only emit last committed files
      DataFile newDataFile21 = createDataFile("data-txn2-1", partition);
      DeleteFile newDeleteFile21 = createEqDeleteFile("eq-delete-txn2-1", partition);
      CommitResult commit2 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFile21),
          formatVersion > 1 ? ImmutableList.of(newDeleteFile21) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 4 : 2);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(1, harness.extractOutputValues().size());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(0);  // delete rewritten file groups manifests after checkpoint.

      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(newDataFile11, newDeleteFile11, newDeleteFile21),
              combined(newDataFile12, newDeleteFile11, newDeleteFile21),
              combined(newDataFile21)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(newDataFile11),
              combined(newDataFile12),
              combined(newDataFile21)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);
    }
  }

  @Test
  public void testContinuousCommitsForPartitionedTable() throws Exception {
    table.updateSpec().addField("data").commit();
    table.updateProperties().set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(3)).commit();

    long timestamp = 0;
    long checkpoint = 10;
    JobID currentJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(currentJobId)) {
      harness.setup();
      harness.open();

      StructLike partitionA = SimpleDataUtil.createPartition("aaa");
      StructLike partitionB = SimpleDataUtil.createPartition("bbb");

      // add exists files to table which should be ignored by emitter
      DataFile existDataFileA = createDataFile("data-exist-1", partitionA);
      DataFile existDataFileB = createDataFile("data-exist-2", partitionB);
      commit(null, ImmutableList.of(existDataFileA, existDataFileB), ImmutableList.of(), ImmutableList.of());

      // Txn#1: init last received snapshot id and emit last committed files
      DataFile newDataFileA11 = createDataFile("data-txn1-1", partitionA);
      DataFile newDataFileA12 = createDataFile("data-txn1-2", partitionA);
      DeleteFile newDeleteFileA11 = createPosDeleteFile("pos-delete-txn1-1", partitionA);
      DataFile newDataFileB11 = createDataFile("data-txn1-1", partitionB);
      DataFile newDataFileB12 = createDataFile("data-txn1-1", partitionB);
      CommitResult commit1 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFileA11, newDataFileA12, newDataFileB11, newDataFileB12),
          formatVersion > 1 ? ImmutableList.of(newDeleteFileA11) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(newDataFileA11.path()) : ImmutableList.of()
      );

      harness.processElement(commit1, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 3 : 2);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(formatVersion > 1 ? 3 : 2);  // no rewritten file groups manifests should be deleted.

      // Txn#2: only append last committed files
      DataFile newDataFileA21 = createDataFile("data-txn2-1", partitionA);
      DeleteFile newDeleteFileA21 = createEqDeleteFile("eq-delete-txn2-1", partitionA);
      DataFile newDataFileB21 = createDataFile("data-txn2-1", partitionB);
      DeleteFile newDeleteFileB21 = createEqDeleteFile("eq-delete-txn2-1", partitionB);
      CommitResult commit2 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFileA21, newDataFileB21),
          formatVersion > 1 ? ImmutableList.of(newDeleteFileA21, newDeleteFileB21) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 7 : 4);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(2, harness.extractOutputValues().size());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(0);  // delete rewritten file groups manifests after checkpoint.

      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partitionA,
              combined(newDataFileA11, newDeleteFileA11, newDeleteFileA21),
              combined(newDataFileA12, newDeleteFileA11, newDeleteFileA21),
              combined(newDataFileA21)
          ),
          createRewriteTask(commit2.snapshotId(), partitionB,
              combined(newDataFileB11, newDeleteFileB21),
              combined(newDataFileB12, newDeleteFileB21),
              combined(newDataFileB21)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partitionA,
              combined(newDataFileA11),
              combined(newDataFileA12),
              combined(newDataFileA21)
          ),
          createRewriteTask(commit2.snapshotId(), partitionB,
              combined(newDataFileB11),
              combined(newDataFileB12),
              combined(newDataFileB21)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);
    }
  }

  @Test
  public void testDiscontinuousCommitsForUnpartitionedTable() throws Exception {
    table.updateProperties().set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(3)).commit();

    long timestamp = 0;
    long checkpoint = 10;
    JobID currentJobId = new JobID();
    JobID otherJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(currentJobId)) {
      harness.setup();
      harness.open();

      StructLike partition = SimpleDataUtil.createPartition(null);

      // add exists files to table which should be ignored by emitter
      DataFile existDataFile1 = createDataFile("data-exist-1", partition);
      DataFile existDataFile2 = createDataFile("data-exist-2", partition);
      commit(null, ImmutableList.of(existDataFile1, existDataFile2), ImmutableList.of(), ImmutableList.of());

      // Txn#1: init last emitted snapshot id and emit last committed files
      DataFile newDataFile11 = createDataFile("data-txn1-1", partition);
      DataFile newDataFile12 = createDataFile("data-txn1-2", partition);
      DeleteFile newDeleteFile11 = createPosDeleteFile("pos-delete-txn1-1", partition);
      CommitResult commit1 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFile11, newDataFile12),
          formatVersion > 1 ? ImmutableList.of(newDeleteFile11) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(newDataFile11.path()) : ImmutableList.of()
      );

      harness.processElement(commit1, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 2 : 1);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(formatVersion > 1 ? 2 : 1);  // no rewritten file groups manifests should be deleted.

      // add other files to table between two txn, the added eq-delete files should be emitted
      DataFile otherDataFile = createDataFile("data-other-1", partition);
      DeleteFile otherPosDeleteFile = createPosDeleteFile("pos-delete-other-1", partition);
      DeleteFile otherEqDeleteFile = createEqDeleteFile("eq-delete-other-1", partition);
      commit(
          otherJobId.toString(),
          ImmutableList.of(otherDataFile),
          formatVersion > 1 ? ImmutableList.of(otherPosDeleteFile, otherEqDeleteFile) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(otherDataFile.path()) : ImmutableList.of()
      );

      // Txn#2: only emit last committed files
      DataFile newDataFile21 = createDataFile("data-txn2-1", partition);
      DeleteFile newDeleteFile21 = createEqDeleteFile("eq-delete-txn2-1", partition);
      CommitResult commit2 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFile21),
          formatVersion > 1 ? ImmutableList.of(newDeleteFile21) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 5 : 2);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(1, harness.extractOutputValues().size());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(0);  // delete rewritten file groups manifests after checkpoint.

      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(newDataFile11, newDeleteFile11, otherEqDeleteFile, newDeleteFile21),
              combined(newDataFile12, newDeleteFile11, otherEqDeleteFile, newDeleteFile21),
              combined(newDataFile21)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partition,
              combined(newDataFile11),
              combined(newDataFile12),
              combined(newDataFile21)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);
    }
  }

  @Test
  public void testDiscontinuousCommitsForPartitionedTable() throws Exception {
    table.updateSpec().addField("data").commit();
    table.updateProperties().set(FlinkSinkOptions.STREAMING_REWRITE_MAX_GROUP_FILES, String.valueOf(3)).commit();

    long timestamp = 0;
    long checkpoint = 10;
    JobID currentJobId = new JobID();
    JobID otherJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> harness = createStreamOpr(currentJobId)) {
      harness.setup();
      harness.open();

      StructLike partitionA = SimpleDataUtil.createPartition("aaa");
      StructLike partitionB = SimpleDataUtil.createPartition("bbb");

      // add exists files to table which should be ignored by emitter
      DataFile existDataFileA = createDataFile("data-exist-1", partitionA);
      DataFile existDataFileB = createDataFile("data-exist-2", partitionB);
      commit(null, ImmutableList.of(existDataFileA, existDataFileB), ImmutableList.of(), ImmutableList.of());

      // Txn#1: init last received snapshot id and emit last committed files
      DataFile newDataFileA11 = createDataFile("data-txn1-1", partitionA);
      DataFile newDataFileA12 = createDataFile("data-txn1-2", partitionA);
      DeleteFile newDeleteFileA11 = createPosDeleteFile("pos-delete-txn1-1", partitionA);
      DataFile newDataFileB11 = createDataFile("data-txn1-1", partitionB);
      DataFile newDataFileB12 = createDataFile("data-txn1-1", partitionB);
      CommitResult commit1 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFileA11, newDataFileA12, newDataFileB11, newDataFileB12),
          formatVersion > 1 ? ImmutableList.of(newDeleteFileA11) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(newDataFileA11.path()) : ImmutableList.of()
      );

      harness.processElement(commit1, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 3 : 2);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(formatVersion > 1 ? 3 : 2);  // no rewritten file groups manifests should be deleted.

      // add other files to table between two txn, only the added eq-delete files should be appended
      DataFile otherDataFileA = createDataFile("data-other-1", partitionA);
      DeleteFile otherPosDeleteFileA = createPosDeleteFile("pos-delete-other-1", partitionA);
      DeleteFile otherEqDeleteFileA = createEqDeleteFile("eq-delete-other-1", partitionA);
      DataFile otherDataFileB = createDataFile("data-other-1", partitionB);
      commit(
          otherJobId.toString(),
          ImmutableList.of(otherDataFileA, otherDataFileB),
          formatVersion > 1 ? ImmutableList.of(otherPosDeleteFileA, otherEqDeleteFileA) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(otherDataFileA.path()) : ImmutableList.of()
      );

      // Txn#2: only append last committed files
      DataFile newDataFileA21 = createDataFile("data-txn2-1", partitionA);
      DeleteFile newDeleteFileA21 = createEqDeleteFile("eq-delete-txn2-1", partitionA);
      DataFile newDataFileB21 = createDataFile("data-txn2-1", partitionB);
      DeleteFile newDeleteFileB21 = createEqDeleteFile("eq-delete-txn2-1", partitionB);
      CommitResult commit2 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFileA21, newDataFileB21),
          formatVersion > 1 ? ImmutableList.of(newDeleteFileA21, newDeleteFileB21) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);
      assertFlinkManifests(formatVersion > 1 ? 8 : 4);  // write delta manifests for pre snapshot and pre partition.
      Assert.assertEquals(2, harness.extractOutputValues().size());

      harness.snapshot(++checkpoint, timestamp);
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertFlinkManifests(0);  // delete rewritten file groups manifests after checkpoint.

      List<RewriteTask> expected = formatVersion > 1 ? Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partitionA,
              combined(newDataFileA11, newDeleteFileA11, otherEqDeleteFileA, newDeleteFileA21),
              combined(newDataFileA12, newDeleteFileA11, otherEqDeleteFileA, newDeleteFileA21),
              combined(newDataFileA21)
          ),
          createRewriteTask(commit2.snapshotId(), partitionB,
              combined(newDataFileB11, newDeleteFileB21),
              combined(newDataFileB12, newDeleteFileB21),
              combined(newDataFileB21)
          )
      ) : Lists.newArrayList(
          createRewriteTask(commit2.snapshotId(), partitionA,
              combined(newDataFileA11),
              combined(newDataFileA12),
              combined(newDataFileA21)
          ),
          createRewriteTask(commit2.snapshotId(), partitionB,
              combined(newDataFileB11),
              combined(newDataFileB12),
              combined(newDataFileB21)
          )
      );
      List<RewriteTask> actual = harness.extractOutputValues();
      assertRewriteTasks(expected, actual);
    }
  }

  private StructLikeWrapper wrap(StructLike partition) {
    return StructLikeWrapper.forType(table.spec().partitionType()).set(partition);
  }

  private String getFilePath(String filename, StructLike partition) {
    return partition == null ? table.locationProvider().newDataLocation(filename) :
        table.locationProvider().newDataLocation(table.spec(), partition, filename);
  }

  private DataFile createDataFile(String filename, StructLike partition) {
    return createDataFile(filename, partition, 10);
  }

  private DataFile createDataFile(String filename, StructLike partition, long fileSize) {
    return DataFiles.builder(table.spec())
        .withFormat(format)
        .withPath(getFilePath(filename, partition))
        .withFileSizeInBytes(fileSize)
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

  private CommitResult commit(String flinkJobId, List<DataFile> dataFiles, List<DeleteFile> deleteFiles,
                              List<CharSequence> referencedFiles) {
    RowDelta rowDelta = table.newRowDelta();
    dataFiles.forEach(rowDelta::addRows);
    deleteFiles.forEach(rowDelta::addDeletes);
    if (flinkJobId != null) {
      rowDelta.set(FLINK_JOB_ID, flinkJobId);
    }
    rowDelta.commit();

    CreateSnapshotEvent event = (CreateSnapshotEvent) rowDelta.updateEvent();
    WriteResult writeResult = WriteResult.builder()
        .addDataFiles(dataFiles)
        .addDeleteFiles(deleteFiles)
        .addReferencedDataFiles(referencedFiles)
        .build();
    return CommitResult.builder(event.sequenceNumber(), event.snapshotId()).add(writeResult).build();
  }

  private Pair<DataFile, List<DeleteFile>> combined(DataFile dataFile, DeleteFile... deleteFiles) {
    return Pair.of(dataFile, Lists.newArrayList(deleteFiles));
  }

  @SafeVarargs
  private final RewriteTask createRewriteTask(long snapshotId, StructLike partition,
                                              Pair<DataFile, List<DeleteFile>>... scanFiles) {
    List<FileScanTask> fileScanTasks = Arrays.stream(scanFiles)
        .map(pair -> new MockFileScanTask(pair.first(), pair.second().toArray(new DeleteFile[0])))
        .collect(Collectors.toList());

    CombinedScanTask combinedScanTask = new BaseCombinedScanTask(fileScanTasks);
    return new RewriteTask(snapshotId, partition, combinedScanTask);
  }

  private void assertRewriteTasks(List<RewriteTask> expectedFileGroups, List<RewriteTask> actualFileGroups) {
    Assert.assertEquals("expected RewriteTasks and actual RewriteTasks should have same size",
        expectedFileGroups.size(), actualFileGroups.size());

    if (!table.spec().isUnpartitioned()) {
      expectedFileGroups.sort(Comparator.comparing(o -> o.partition().toString()));
      actualFileGroups.sort(Comparator.comparing(o -> o.partition().toString()));
    }

    for (int i = 0; i < actualFileGroups.size(); i++) {
      RewriteTask expected = expectedFileGroups.get(i);
      RewriteTask actual = actualFileGroups.get(i);
      Assert.assertEquals("Snapshot id should match", expected.snapshotId(), actual.snapshotId());
      Assert.assertEquals("Partition should match", wrap(expected.partition()), wrap(actual.partition()));
      Assert.assertEquals(
          "Data files should match",
          expected.task().files().stream().map(t -> t.file().path()).collect(Collectors.toSet()),
          actual.task().files().stream().map(t -> t.file().path()).collect(Collectors.toSet())
      );
      Assert.assertEquals(
          "Delete files should match",
          Sets.newHashSet(Iterables.transform(Iterables.concat(Iterables.transform(
              expected.task().files(), FileScanTask::deletes)), ContentFile::path)),
          Sets.newHashSet(Iterables.transform(Iterables.concat(Iterables.transform(
              actual.task().files(), FileScanTask::deletes)), ContentFile::path))
      );
    }
  }

  private void assertFlinkManifests(int expectedCount) throws IOException {
    List<Path> manifests = Files.list(flinkManifestFolder.toPath())
        .filter(p -> !p.toString().endsWith(".crc"))
        .collect(Collectors.toList());
    Assert.assertEquals(String.format("Expected %s flink manifests, but the list is: %s", expectedCount, manifests),
        expectedCount, manifests.size());
  }

  private OneInputStreamOperatorTestHarness<CommitResult, RewriteTask> createStreamOpr(JobID jobID)
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

  private static class TestOperatorFactory extends AbstractStreamOperatorFactory<RewriteTask>
      implements OneInputStreamOperatorFactory<CommitResult, RewriteTask> {
    private final String tablePath;

    private TestOperatorFactory(String tablePath) {
      this.tablePath = tablePath;
    }

    private static TestOperatorFactory of(String tablePath) {
      return new TestOperatorFactory(tablePath);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<RewriteTask>> T createStreamOperator(
        StreamOperatorParameters<RewriteTask> param) {
      IcebergRewriteTaskEmitter operator = new IcebergRewriteTaskEmitter(TableLoader.fromHadoopTable(tablePath));
      operator.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergRewriteTaskEmitter.class;
    }
  }
}
