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
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergCommittedFilesEmitter {

  private static final String FLINK_JOB_ID = "flink.job-id";

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;
  private String tablePath;

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

  public TestIcebergCommittedFilesEmitter(String format, int formatVersion, boolean partitioned) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.formatVersion = formatVersion;
    this.partitioned = partitioned;
  }

  @Before
  public void setupTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    tablePath = tableDir.getAbsolutePath();

    // Construct the iceberg table.
    File flinkManifestFolder = temp.newFolder();
    Map<String, String> props = ImmutableMap.of(
        TableProperties.DEFAULT_FILE_FORMAT, format.name(),
        TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
        ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath()
    );
    table = SimpleDataUtil.createTable(tablePath, props, partitioned);
  }

  @Test
  public void testRecoveryFromValidSnapshotForUnpartitionedTable() throws Exception {
    Assume.assumeTrue("Only test for unpartitioned table.", !partitioned);

    int specId = table.spec().specId();
    StructLike partition = SimpleDataUtil.createPartition(null);

    long timestamp = 0;
    long checkpoint = 10;
    OperatorSubtaskState snapshot;
    JobID oldJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, PartitionFileGroup> harness = createStreamOpr(oldJobId)) {
      harness.setup();
      harness.open();

      // emitted files in old flink job should be ignored
      DataFile emittedDataFile1 = createDataFile("data-exist-1", partition);
      DataFile emittedDataFile2 = createDataFile("data-exist-2", partition);
      CommitResult commit = commit(
          oldJobId.toString(),
          ImmutableList.of(emittedDataFile1, emittedDataFile2),
          ImmutableList.of(),
          ImmutableList.of()
      );

      harness.processElement(commit, ++timestamp);
      snapshot = harness.snapshot(++checkpoint, timestamp);
    }

    // add not emitted files
    DataFile missingDataFile11 = createDataFile("data-txn1-1", partition);
    DataFile missingDataFile12 = createDataFile("data-txn1-2", partition);
    DeleteFile missingDeleteFile11 = createPosDeleteFile("pos-delete-txn1-1", partition);
    DeleteFile missingDeleteFile12 = createEqDeleteFile("eq-delete-txn1-1", partition);
    DataFile missingDataFile13 = createDataFile("data-txn1-3", partition);
    DataFile missingDataFile14 = createDataFile("data-txn1-4", partition);
    CommitResult commit1 = commit(
        oldJobId.toString(),
        ImmutableList.of(missingDataFile11, missingDataFile12, missingDataFile13, missingDataFile14),
        formatVersion > 1 ? ImmutableList.of(missingDeleteFile11, missingDeleteFile12) : ImmutableList.of(),
        formatVersion > 1 ? ImmutableList.of(missingDataFile11.path()) : ImmutableList.of()
    );

    // add other files to table between two txn, the added eq-delete files should be emitted
    DataFile otherDataFile = createDataFile("data-other-1", partition);
    DeleteFile otherPosDeleteFile = createPosDeleteFile("pos-delete-other-1", partition);
    DeleteFile otherEqDeleteFile = createEqDeleteFile("eq-delete-other-1", partition);
    CommitResult commit2 = commit(
        null,
        ImmutableList.of(otherDataFile),
        formatVersion > 1 ? ImmutableList.of(otherPosDeleteFile, otherEqDeleteFile) : ImmutableList.of(),
        formatVersion > 1 ? ImmutableList.of(otherDataFile.path()) : ImmutableList.of()
    );

    // add latest committed files
    DeleteFile missingDeleteFile21 = createEqDeleteFile("eq-delete-txn2-1", partition);
    DataFile missingDataFile21 = createDataFile("data-txn2-1", partition);
    DeleteFile missingDeleteFile22 = createEqDeleteFile("eq-delete-txn2-2", partition);
    CommitResult commit3 = commit(
        oldJobId.toString(),
        ImmutableList.of(missingDataFile21),
        formatVersion > 1 ? ImmutableList.of(missingDeleteFile21, missingDeleteFile22) : ImmutableList.of(),
        ImmutableList.of()
    );

    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, PartitionFileGroup> harness = createStreamOpr(newJobId)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      List<PartitionFileGroup> expected = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partition)
              .addDataFile(missingDataFile11, missingDataFile12, missingDataFile13, missingDataFile14)
              .addDeleteFile(missingDeleteFile11, missingDeleteFile12)
              .build(),
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partition)
              .addDeleteFile(otherEqDeleteFile)
              .build(),
          PartitionFileGroup.builder(commit3.sequenceNumber(), commit3.snapshotId(), partition)
              .addDataFile(missingDataFile21)
              .addDeleteFile(missingDeleteFile21, missingDeleteFile22)
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partition)
              .addDataFile(missingDataFile11, missingDataFile12, missingDataFile13, missingDataFile14)
              .build(),
          PartitionFileGroup.builder(commit3.sequenceNumber(), commit3.snapshotId(), partition)
              .addDataFile(missingDataFile21)
              .build()
      );
      List<PartitionFileGroup> actual = harness.extractOutputValues();
      assertPartitionFileGroups(expected, actual);
    }
  }

  @Test
  public void testRecoveryFromValidSnapshotForPartitionedTable() throws Exception {
    Assume.assumeTrue("Only test for partitioned table.", partitioned);

    int specId = table.spec().specId();
    StructLike partitionA = SimpleDataUtil.createPartition("aaa");
    StructLike partitionB = SimpleDataUtil.createPartition("bbb");

    long timestamp = 0;
    long checkpoint = 10;
    OperatorSubtaskState snapshot;
    JobID oldJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, PartitionFileGroup> harness = createStreamOpr(oldJobId)) {
      harness.setup();
      harness.open();

      // emitted files in old flink job should be ignored
      DataFile emittedDataFileA = createDataFile("data-exist-1", partitionA);
      DataFile emittedDataFileB = createDataFile("data-exist-2", partitionB);
      CommitResult commit = commit(
          oldJobId.toString(),
          ImmutableList.of(emittedDataFileA, emittedDataFileB),
          ImmutableList.of(),
          ImmutableList.of()
      );

      harness.processElement(commit, ++timestamp);
      snapshot = harness.snapshot(++checkpoint, timestamp);
    }

    // add not emitted files
    DataFile missingDataFileA11 = createDataFile("data-txn1-1", partitionA);
    DataFile missingDataFileA12 = createDataFile("data-txn1-2", partitionA);
    DeleteFile missingDeleteFileA11 = createPosDeleteFile("pos-delete-txn1-1", partitionA);
    DeleteFile missingDeleteFileA12 = createEqDeleteFile("eq-delete-txn1-1", partitionA);
    DataFile missingDataFileB11 = createDataFile("data-txn1-1", partitionB);
    DataFile missingDataFileB12 = createDataFile("data-txn1-2", partitionB);
    CommitResult commit1 = commit(
        oldJobId.toString(),
        ImmutableList.of(missingDataFileA11, missingDataFileA12, missingDataFileB11, missingDataFileB12),
        formatVersion > 1 ? ImmutableList.of(missingDeleteFileA11, missingDeleteFileA12) : ImmutableList.of(),
        formatVersion > 1 ? ImmutableList.of(missingDataFileA11.path()) : ImmutableList.of()
    );

    // add other files to table between two txn, the added eq-delete files should be emitted
    DataFile otherDataFileA = createDataFile("data-other-1", partitionA);
    DeleteFile otherPosDeleteFileA = createPosDeleteFile("pos-delete-other-1", partitionA);
    DeleteFile otherEqDeleteFileA = createEqDeleteFile("eq-delete-other-1", partitionA);
    DataFile otherDataFileB = createDataFile("data-other-1", partitionB);
    CommitResult commit2 = commit(
        null,
        ImmutableList.of(otherDataFileA, otherDataFileB),
        formatVersion > 1 ? ImmutableList.of(otherPosDeleteFileA, otherEqDeleteFileA) : ImmutableList.of(),
        formatVersion > 1 ? ImmutableList.of(otherDataFileA.path()) : ImmutableList.of()
    );

    // add latest committed files
    DeleteFile missingDeleteFileA21 = createEqDeleteFile("eq-delete-txn2-1", partitionA);
    DataFile missingDataFileB21 = createDataFile("data-txn2-1", partitionB);
    DeleteFile missingDeleteFileB22 = createEqDeleteFile("eq-delete-txn2-2", partitionB);
    CommitResult commit3 = commit(
        oldJobId.toString(),
        ImmutableList.of(missingDataFileB21),
        formatVersion > 1 ? ImmutableList.of(missingDeleteFileA21, missingDeleteFileB22) : ImmutableList.of(),
        ImmutableList.of()
    );

    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, PartitionFileGroup> harness = createStreamOpr(newJobId)) {
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      List<PartitionFileGroup> expected = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionA)
              .addDataFile(missingDataFileA11, missingDataFileA12)
              .addDeleteFile(missingDeleteFileA11, missingDeleteFileA12)
              .build(),
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionB)
              .addDataFile(missingDataFileB11, missingDataFileB12)
              .build(),
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partitionA)
              .addDeleteFile(otherEqDeleteFileA)
              .build(),
          PartitionFileGroup.builder(commit3.sequenceNumber(), commit3.snapshotId(), partitionA)
              .addDeleteFile(missingDeleteFileA21)
              .build(),
          PartitionFileGroup.builder(commit3.sequenceNumber(), commit3.snapshotId(), partitionB)
              .addDataFile(missingDataFileB21)
              .addDeleteFile(ImmutableList.of(missingDeleteFileB22))
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionA)
              .addDataFile(missingDataFileA11, missingDataFileA12)
              .build(),
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionB)
              .addDataFile(missingDataFileB11, missingDataFileB12)
              .build(),
          PartitionFileGroup.builder(commit3.sequenceNumber(), commit3.snapshotId(), partitionB)
              .addDataFile(missingDataFileB21)
              .build()
      );
      List<PartitionFileGroup> actual = harness.extractOutputValues();
      assertPartitionFileGroups(expected, actual);
    }
  }

  @Test
  public void testEmitContinuousCommitForUnpartitionedTable() throws Exception {
    Assume.assumeTrue("Only test for unpartitioned table.", !partitioned);

    long timestamp = 0;
    long checkpoint = 10;
    JobID currentJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, PartitionFileGroup> harness = createStreamOpr(currentJobId)) {
      harness.setup();
      harness.open();

      int specId = table.spec().specId();
      StructLike partition = SimpleDataUtil.createPartition(null);

      // Txn#1: init last emitted snapshot id and emit last committed files
      DataFile newDataFile11 = createDataFile("data-txn1-1", partition);
      DataFile newDataFile12 = createDataFile("data-txn1-2", partition);
      DeleteFile newDeleteFile11 = createPosDeleteFile("pos-delete-txn1-1", partition);
      DataFile newDataFile13 = createDataFile("data-txn1-3", partition);
      DataFile newDataFile14 = createDataFile("data-txn1-4", partition);
      CommitResult commit1 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFile11, newDataFile12, newDataFile13, newDataFile14),
          formatVersion > 1 ? ImmutableList.of(newDeleteFile11) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(newDataFile11.path()) : ImmutableList.of()
      );

      harness.processElement(commit1, ++timestamp);
      harness.snapshot(++checkpoint, timestamp);

      List<PartitionFileGroup> expected1 = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partition)
              .addDataFile(newDataFile11, newDataFile12, newDataFile13, newDataFile14)
              .addDeleteFile(newDeleteFile11)
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partition)
              .addDataFile(newDataFile11, newDataFile12, newDataFile13, newDataFile14)
              .build()
      );
      List<PartitionFileGroup> actual1 = harness.extractOutputValues();
      assertPartitionFileGroups(expected1, actual1);

      // Txn#2: only emit last committed files
      DeleteFile newDeleteFile21 = createEqDeleteFile("eq-delete-txn2-1", partition);
      DataFile newDataFile21 = createDataFile("data-txn2-1", partition);
      DeleteFile newDeleteFile22 = createEqDeleteFile("eq-delete-txn2-2", partition);
      DeleteFile newDeleteFile23 = createEqDeleteFile("eq-delete-txn2-3", partition);
      CommitResult commit2 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFile21),
          formatVersion > 1 ? ImmutableList.of(newDeleteFile21, newDeleteFile22, newDeleteFile23) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);

      List<PartitionFileGroup> expected2 = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partition)
              .addDataFile(newDataFile21)
              .addDeleteFile(newDeleteFile21, newDeleteFile22, newDeleteFile23)
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partition)
              .addDataFile(newDataFile21)
              .build()
      );
      List<PartitionFileGroup> actual2 = harness.extractOutputValues();
      actual2.removeAll(actual1);  // remove first commit output
      assertPartitionFileGroups(expected2, actual2);
    }
  }

  @Test
  public void testEmitContinuousCommitForPartitionedTable() throws Exception {
    Assume.assumeTrue("Only test for partitioned table.", partitioned);

    long timestamp = 0;
    long checkpoint = 10;
    JobID currentJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, PartitionFileGroup> harness = createStreamOpr(currentJobId)) {
      harness.setup();
      harness.open();

      int specId = table.spec().specId();
      StructLike partitionA = SimpleDataUtil.createPartition("aaa");
      StructLike partitionB = SimpleDataUtil.createPartition("bbb");

      // Txn#1: init last emitted snapshot id and emit last committed files
      DataFile newDataFileA11 = createDataFile("data-txn1-1", partitionA);
      DataFile newDataFileA12 = createDataFile("data-txn1-2", partitionA);
      DeleteFile newDeleteFileA11 = createPosDeleteFile("pos-delete-txn1-1", partitionA);
      DataFile newDataFileB11 = createDataFile("data-txn1-1", partitionB);
      DataFile newDataFileB12 = createDataFile("data-txn1-2", partitionB);
      CommitResult commit1 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFileA11, newDataFileA12, newDataFileB11, newDataFileB12),
          formatVersion > 1 ? ImmutableList.of(newDeleteFileA11) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(newDataFileA11.path()) : ImmutableList.of()
      );

      harness.processElement(commit1, ++timestamp);
      harness.snapshot(++checkpoint, timestamp);

      List<PartitionFileGroup> expected1 = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionA)
              .addDataFile(newDataFileA11, newDataFileA12)
              .addDeleteFile(newDeleteFileA11)
              .build(),
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionB)
              .addDataFile(newDataFileB11, newDataFileB12)
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionA)
              .addDataFile(newDataFileA11, newDataFileA12)
              .build(),
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionB)
              .addDataFile(newDataFileB11, newDataFileB12)
              .build()
      );
      List<PartitionFileGroup> actual1 = harness.extractOutputValues();
      assertPartitionFileGroups(expected1, actual1);

      // Txn#2: only emit last committed files
      DeleteFile newDeleteFileA21 = createEqDeleteFile("eq-delete-txn2-1", partitionA);
      DataFile newDataFileB21 = createDataFile("data-txn2-1", partitionB);
      DeleteFile newDeleteFileB21 = createEqDeleteFile("eq-delete-txn2-1", partitionB);
      CommitResult commit2 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFileB21),
          formatVersion > 1 ? ImmutableList.of(newDeleteFileA21, newDeleteFileB21) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);

      List<PartitionFileGroup> expected2 = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partitionA)
              .addDeleteFile(newDeleteFileA21)
              .build(),
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partitionB)
              .addDataFile(newDataFileB21)
              .addDeleteFile(ImmutableList.of(newDeleteFileB21))
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partitionB)
              .addDataFile(newDataFileB21)
              .build()
      );
      List<PartitionFileGroup> actual2 = harness.extractOutputValues();
      actual2.removeAll(actual1);  // remove first commit output
      assertPartitionFileGroups(expected2, actual2);
    }
  }

  @Test
  public void testEmitDiscontinuousCommitForUnpartitionedTable() throws Exception {
    Assume.assumeTrue("Only test for unpartitioned table.", !partitioned);

    long timestamp = 0;
    long checkpoint = 10;
    JobID currentJobId = new JobID();
    JobID otherJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, PartitionFileGroup> harness = createStreamOpr(currentJobId)) {
      harness.setup();
      harness.open();

      int specId = table.spec().specId();
      StructLike partition = SimpleDataUtil.createPartition(null);

      // add exists files to table which should be ignored by emitter
      DataFile existDataFile1 = createDataFile("data-exist-1", partition);
      DataFile existDataFile2 = createDataFile("data-exist-2", partition);
      commit(null, ImmutableList.of(existDataFile1, existDataFile2), ImmutableList.of(), ImmutableList.of());

      // Txn#1: init last emitted snapshot id and emit last committed files
      DataFile newDataFile11 = createDataFile("data-txn1-1", partition);
      DataFile newDataFile12 = createDataFile("data-txn1-2", partition);
      DeleteFile newDeleteFile11 = createPosDeleteFile("pos-delete-txn1-1", partition);
      DataFile newDataFile13 = createDataFile("data-txn1-3", partition);
      DataFile newDataFile14 = createDataFile("data-txn1-4", partition);
      CommitResult commit1 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFile11, newDataFile12, newDataFile13, newDataFile14),
          formatVersion > 1 ? ImmutableList.of(newDeleteFile11) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(newDataFile11.path()) : ImmutableList.of()
      );

      harness.processElement(commit1, ++timestamp);
      harness.snapshot(++checkpoint, timestamp);

      List<PartitionFileGroup> expected1 = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partition)
              .addDataFile(newDataFile11, newDataFile12, newDataFile13, newDataFile14)
              .addDeleteFile(newDeleteFile11)
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partition)
              .addDataFile(newDataFile11, newDataFile12, newDataFile13, newDataFile14)
              .build()
      );
      List<PartitionFileGroup> actual1 = harness.extractOutputValues();
      assertPartitionFileGroups(expected1, actual1);

      // add other files to table between two txn, the added eq-delete files should be emitted
      DataFile otherDataFile = createDataFile("data-other-1", partition);
      DeleteFile otherPosDeleteFile = createPosDeleteFile("pos-delete-other-1", partition);
      DeleteFile otherEqDeleteFile = createEqDeleteFile("eq-delete-other-1", partition);
      CommitResult otherCommit = commit(
          otherJobId.toString(),
          ImmutableList.of(otherDataFile),
          formatVersion > 1 ? ImmutableList.of(otherPosDeleteFile, otherEqDeleteFile) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(otherDataFile.path()) : ImmutableList.of()
      );

      // Txn#2: only emit last committed files
      DeleteFile newDeleteFile21 = createEqDeleteFile("eq-delete-txn2-1", partition);
      DataFile newDataFile21 = createDataFile("data-txn2-1", partition);
      DeleteFile newDeleteFile22 = createEqDeleteFile("eq-delete-txn2-2", partition);
      DeleteFile newDeleteFileB23 = createEqDeleteFile("eq-delete-txn2-3", partition);
      CommitResult commit2 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFile21),
          formatVersion > 1 ? ImmutableList.of(newDeleteFile21, newDeleteFile22, newDeleteFileB23) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);

      List<PartitionFileGroup> expected2 = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(otherCommit.sequenceNumber(), otherCommit.snapshotId(), partition)
              .addDeleteFile(otherEqDeleteFile)
              .build(),
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partition)
              .addDataFile(newDataFile21)
              .addDeleteFile(newDeleteFile21, newDeleteFile22, newDeleteFileB23)
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partition)
              .addDataFile(newDataFile21)
              .build()
      );
      List<PartitionFileGroup> actual2 = harness.extractOutputValues();
      actual2.removeAll(actual1);  // remove first commit output
      assertPartitionFileGroups(expected2, actual2);
    }
  }

  @Test
  public void testEmitDiscontinuousCommitForPartitionedTable() throws Exception {
    Assume.assumeTrue("Only test for partitioned table.", partitioned);

    long timestamp = 0;
    long checkpoint = 10;
    JobID currentJobId = new JobID();
    JobID otherJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, PartitionFileGroup> harness = createStreamOpr(currentJobId)) {
      harness.setup();
      harness.open();

      int specId = table.spec().specId();
      StructLike partitionA = SimpleDataUtil.createPartition("aaa");
      StructLike partitionB = SimpleDataUtil.createPartition("bbb");

      // add exists files to table which should be ignored by emitter
      DataFile existDataFileA = createDataFile("data-exist-1", partitionA);
      DataFile existDataFileB = createDataFile("data-exist-2", partitionB);
      commit(null, ImmutableList.of(existDataFileA, existDataFileB), ImmutableList.of(), ImmutableList.of());

      // Txn#1: init last emitted snapshot id and emit last committed files
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
      harness.snapshot(++checkpoint, timestamp);

      List<PartitionFileGroup> expected1 = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionA)
              .addDataFile(newDataFileA11, newDataFileA12)
              .addDeleteFile(newDeleteFileA11)
              .build(),
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionB)
              .addDataFile(newDataFileB11, newDataFileB12)
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionA)
              .addDataFile(newDataFileA11, newDataFileA12)
              .build(),
          PartitionFileGroup.builder(commit1.sequenceNumber(), commit1.snapshotId(), partitionB)
              .addDataFile(newDataFileB11, newDataFileB12)
              .build()
      );
      List<PartitionFileGroup> actual1 = harness.extractOutputValues();
      assertPartitionFileGroups(expected1, actual1);

      // add other files to table between two txn, the added eq-delete files should be emitted
      DataFile otherDataFileA = createDataFile("data-other-1", partitionA);
      DeleteFile otherPosDeleteFileA = createPosDeleteFile("pos-delete-other-1", partitionA);
      DeleteFile otherEqDeleteFileA = createEqDeleteFile("eq-delete-other-1", partitionA);
      DataFile otherDataFileB = createDataFile("data-other-1", partitionB);
      CommitResult otherCommit = commit(
          otherJobId.toString(),
          ImmutableList.of(otherDataFileA, otherDataFileB),
          formatVersion > 1 ? ImmutableList.of(otherPosDeleteFileA, otherEqDeleteFileA) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(otherDataFileA.path()) : ImmutableList.of()
      );

      // Txn#2: only emit last committed files
      DeleteFile newDeleteFileA21 = createEqDeleteFile("eq-delete-txn2-1", partitionA);
      DataFile newDataFileB21 = createDataFile("data-txn2-1", partitionB);
      DeleteFile newDeleteFileB21 = createEqDeleteFile("eq-delete-txn2-1", partitionB);
      CommitResult commit2 = commit(
          currentJobId.toString(),
          ImmutableList.of(newDataFileB21),
          formatVersion > 1 ? ImmutableList.of(newDeleteFileA21, newDeleteFileB21) : ImmutableList.of(),
          ImmutableList.of()
      );
      harness.processElement(commit2, ++timestamp);

      List<PartitionFileGroup> expected2 = formatVersion > 1 ? Lists.newArrayList(
          PartitionFileGroup.builder(otherCommit.sequenceNumber(), otherCommit.snapshotId(), partitionA)
              .addDeleteFile(otherEqDeleteFileA)
              .build(),
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partitionA)
              .addDeleteFile(newDeleteFileA21)
              .build(),
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partitionB)
              .addDataFile(newDataFileB21)
              .addDeleteFile(ImmutableList.of(newDeleteFileB21))
              .build()
      ) : Lists.newArrayList(
          PartitionFileGroup.builder(commit2.sequenceNumber(), commit2.snapshotId(), partitionB)
              .addDataFile(newDataFileB21)
              .build()
      );
      List<PartitionFileGroup> actual2 = harness.extractOutputValues();
      actual2.removeAll(actual1);  // remove first commit output
      assertPartitionFileGroups(expected2, actual2);
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

  private CommitResult commit(String flinkJobId, List<DataFile> dataFiles,
                              List<DeleteFile> deleteFiles, List<CharSequence> referencedFiles) {
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

  private void assertPartitionFileGroups(List<PartitionFileGroup> expectedFileGroups,
                                         List<PartitionFileGroup> actualFileGroups) {
    Assert.assertEquals("expected PartitionFileGroups and actual PartitionFileGroups should have same size",
        expectedFileGroups.size(), actualFileGroups.size());

    if (partitioned) {
      expectedFileGroups.sort(Comparator.comparing(o -> o.partition().toString()));
      actualFileGroups.sort(Comparator.comparing(o -> o.partition().toString()));
    }

    for (int i = 0; i < actualFileGroups.size(); i++) {
      PartitionFileGroup expected = expectedFileGroups.get(i);
      PartitionFileGroup actual = actualFileGroups.get(i);
      Assert.assertEquals("Sequence number should match", expected.sequenceNumber(), actual.sequenceNumber());
      Assert.assertEquals("Snapshot id should match", expected.snapshotId(), actual.snapshotId());
      Assert.assertEquals("Partition should match", wrap(expected.partition()), wrap(actual.partition()));
      Assert.assertEquals(
          "Data files should match",
          Arrays.stream(expected.dataFiles()).map(ContentFile::path).collect(Collectors.toSet()),
          Arrays.stream(actual.dataFiles()).map(ContentFile::path).collect(Collectors.toSet())
      );
      Assert.assertEquals(
          "Delete files should match",
          Arrays.stream(expected.deleteFiles()).map(ContentFile::path).collect(Collectors.toSet()),
          Arrays.stream(actual.deleteFiles()).map(ContentFile::path).collect(Collectors.toSet())
      );
    }
  }

  private OneInputStreamOperatorTestHarness<CommitResult, PartitionFileGroup> createStreamOpr(JobID jobID)
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

  private static class TestOperatorFactory extends AbstractStreamOperatorFactory<PartitionFileGroup>
      implements OneInputStreamOperatorFactory<CommitResult, PartitionFileGroup> {
    private final String tablePath;

    private TestOperatorFactory(String tablePath) {
      this.tablePath = tablePath;
    }

    private static TestOperatorFactory of(String tablePath) {
      return new TestOperatorFactory(tablePath);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<PartitionFileGroup>> T createStreamOperator(
        StreamOperatorParameters<PartitionFileGroup> param) {
      IcebergCommittedFilesEmitter operator = new IcebergCommittedFilesEmitter(TableLoader.fromHadoopTable(tablePath));
      operator.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergCommittedFilesEmitter.class;
    }
  }
}
