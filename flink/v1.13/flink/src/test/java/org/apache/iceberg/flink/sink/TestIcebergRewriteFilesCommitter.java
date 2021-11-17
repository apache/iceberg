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
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
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
import org.apache.iceberg.util.Pair;
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
        new Object[] {"orc", 1}
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

    flinkManifestFolder = temp.newFolder();

    // Construct the iceberg table.
    Map<String, String> props = ImmutableMap.of(
        TableProperties.DEFAULT_FILE_FORMAT, format.name(),
        TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
        ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath()
    );
    table = SimpleDataUtil.createTable(tablePath, props, false);
  }

  @Test
  public void testCommitRewriteResult() throws Exception {
    table.updateProperties()
        .set(IcebergRewriteFilesCommitter.COMMIT_GROUP_SIZE, "2")
        .commit();

    long timestamp = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<RewriteResult, Void> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      List<RowData> rows = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "xxx"),
          SimpleDataUtil.createRowData(2, "xxx"),
          SimpleDataUtil.createRowData(3, "xxx"),
          SimpleDataUtil.createRowData(4, "xxx")
      );

      List<RowData> expected = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "ccc"),
          SimpleDataUtil.createRowData(4, "ddd")
      );

      DataFile deletedFile1 = writeDataFile("data-1", null, rows.subList(0, 1));
      CommitResult commit1 = commitData(table.spec().specId(), null,
          ImmutableList.of(deletedFile1), ImmutableList.of(), ImmutableList.of());

      DataFile deletedFile2 = writeDataFile("data-2", null, rows.subList(1, 3));
      CommitResult commit2 = commitData(table.spec().specId(), null,
          ImmutableList.of(deletedFile2), ImmutableList.of(), ImmutableList.of());

      DataFile addedFile1 = writeDataFile("rewrite-1", null, expected.subList(0, 3));
      RewriteResult rewriteResult1 = RewriteResult.builder()
          .partition(null)
          .startingSnapshotSeqNum(commit2.sequenceNumber())
          .startingSnapshotId(commit2.snapshotId())
          .addDeletedDataFiles(ImmutableList.of(deletedFile1, deletedFile2))
          .addAddedDataFiles(ImmutableList.of(addedFile1))
          .build();

      // not commit rewrite result
      harness.processElement(rewriteResult1, ++timestamp);
      SimpleDataUtil.assertTableRows(table, rows.subList(0, 3));

      DataFile deletedFile3 = writeDataFile("data-3", null, rows.subList(3, 4));
      CommitResult commit3 = commitData(table.spec().specId(), null,
          ImmutableList.of(deletedFile3), ImmutableList.of(), ImmutableList.of());

      DataFile addedFile2 = writeDataFile("rewrite-2", null, expected.subList(3, 4));
      RewriteResult rewriteResult2 = RewriteResult.builder()
          .partition(null)
          .startingSnapshotSeqNum(commit3.sequenceNumber())
          .startingSnapshotId(commit3.snapshotId())
          .addDeletedDataFiles(ImmutableList.of(deletedFile3))
          .addAddedDataFiles(ImmutableList.of(addedFile2))
          .build();

      // commit rewrite result
      harness.processElement(rewriteResult2, ++timestamp);
      SimpleDataUtil.assertTableRows(table, expected);
    }
  }

  @Test
  public void testCommitPreCheckpoint() throws Exception {
    table.updateProperties()
        .set(IcebergRewriteFilesCommitter.COMMIT_GROUP_SIZE, "2")
        .commit();

    long checkpointId = 0;
    long timestamp = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<RewriteResult, Void> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      List<RowData> rows = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "xxx"),
          SimpleDataUtil.createRowData(2, "xxx"),
          SimpleDataUtil.createRowData(3, "xxx"),
          SimpleDataUtil.createRowData(4, "xxx")
      );

      List<RowData> expected = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "ccc"),
          SimpleDataUtil.createRowData(4, "ddd")
      );

      DataFile deletedFile1 = writeDataFile("data-1", null, rows.subList(0, 1));
      CommitResult commit1 = commitData(table.spec().specId(), null,
          ImmutableList.of(deletedFile1), ImmutableList.of(), ImmutableList.of());

      DataFile deletedFile2 = writeDataFile("data-2", null, rows.subList(1, 3));
      CommitResult commit2 = commitData(table.spec().specId(), null,
          ImmutableList.of(deletedFile2), ImmutableList.of(), ImmutableList.of());

      DataFile addedFile1 = writeDataFile("rewrite-1", null, expected.subList(0, 3));
      RewriteResult rewriteResult1 = RewriteResult.builder()
          .partition(null)
          .startingSnapshotSeqNum(commit2.sequenceNumber())
          .startingSnapshotId(commit2.snapshotId())
          .addDeletedDataFiles(ImmutableList.of(deletedFile1, deletedFile2))
          .addAddedDataFiles(ImmutableList.of(addedFile1))
          .build();

      // not commit rewrite result
      harness.processElement(rewriteResult1, ++timestamp);
      SimpleDataUtil.assertTableRows(table, rows.subList(0, 3));

      // commit all remain rewrite result
      harness.prepareSnapshotPreBarrier(++checkpointId);
      SimpleDataUtil.assertTableRows(table, expected.subList(0, 3));

      DataFile deletedFile3 = writeDataFile("data-3", null, rows.subList(3, 4));
      CommitResult commit3 = commitData(table.spec().specId(), null,
          ImmutableList.of(deletedFile3), ImmutableList.of(), ImmutableList.of());

      DataFile addedFile2 = writeDataFile("rewrite-2", null, expected.subList(3, 4));
      RewriteResult rewriteResult2 = RewriteResult.builder()
          .partition(null)
          .startingSnapshotSeqNum(commit3.sequenceNumber())
          .startingSnapshotId(commit3.snapshotId())
          .addDeletedDataFiles(ImmutableList.of(deletedFile3))
          .addAddedDataFiles(ImmutableList.of(addedFile2))
          .build();

      // not commit rewrite result
      harness.processElement(rewriteResult2, ++timestamp);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(
          expected.get(0), expected.get(1), expected.get(2), rows.get(3)
      ));

      // commit all remain rewrite result
      harness.prepareSnapshotPreBarrier(++checkpointId);
      SimpleDataUtil.assertTableRows(table, expected);
    }
  }

  @Test
  public void testValidateFromSnapshot() throws Exception {
    Assume.assumeTrue("Validate from snapshot only supported in iceberg format v2.", formatVersion > 1);
    table.updateProperties()
        .set(IcebergRewriteFilesCommitter.COMMIT_GROUP_SIZE, "1")
        .commit();

    long checkpointId = 0;
    long timestamp = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<RewriteResult, Void> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      DataFile deletedFile1 = writeDataFile("data-1", null, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "xxx"),
          SimpleDataUtil.createRowData(2, "xxx")
      ));
      CommitResult commit1 = commitData(table.spec().specId(), null,
          ImmutableList.of(deletedFile1), ImmutableList.of(), ImmutableList.of());

      DataFile deletedFile2 = writeDataFile("data-2", null, ImmutableList.of(
          SimpleDataUtil.createRowData(3, "xxx")
      ));
      CommitResult commit2 = commitData(table.spec().specId(), null,
          ImmutableList.of(deletedFile2), ImmutableList.of(), ImmutableList.of());

      // construct rewrite result
      DataFile addedFile1 = writeDataFile("rewrite-1", null, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "ccc")
      ));
      RewriteResult rewriteResult1 = RewriteResult.builder()
          .partition(null)
          .startingSnapshotSeqNum(commit2.sequenceNumber())
          .startingSnapshotId(commit2.snapshotId())
          .addDeletedDataFiles(ImmutableList.of(deletedFile1, deletedFile2))
          .addAddedDataFiles(ImmutableList.of(addedFile1))
          .build();

      // update rewritten records
      DataFile updateFile1 = writeDataFile("data-3", null, ImmutableList.of(
          SimpleDataUtil.createRowData(2, "yyy"),
          SimpleDataUtil.createRowData(4, "xxx")
      ));
      DeleteFile deleteFile1 = writePosDeleteFile("pos-delete-1", null, ImmutableList.of(
          Pair.of(updateFile1.path(), 1L))
      );
      DeleteFile deleteFile2 = writeEqDeleteFile("eq-delete-1", null, ImmutableList.of(
          SimpleDataUtil.createRowData(2, "xxx")
      ));
      CommitResult commit3 = commitData(table.spec().specId(), null,
          ImmutableList.of(updateFile1), ImmutableList.of(deleteFile1, deleteFile2),
          ImmutableList.of(updateFile1.path()));

      // commit fail and ignore rewrite result
      harness.processElement(rewriteResult1, ++timestamp);
      SimpleDataUtil.assertTableRows(table, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "xxx"),
          SimpleDataUtil.createRowData(2, "yyy"),
          SimpleDataUtil.createRowData(3, "xxx")
      ));
    }
  }

  private CommitResult commitData(int specId, StructLike partition, List<DataFile> dataFiles,
                                  List<DeleteFile> deleteFiles, List<CharSequence> referencedFiles) {
    RowDelta rowDelta = table.newRowDelta();
    dataFiles.forEach(rowDelta::addRows);
    deleteFiles.forEach(rowDelta::addDeletes);
    rowDelta.commit();
    CreateSnapshotEvent updateEvent = (CreateSnapshotEvent) rowDelta.updateEvent();
    return CommitResult.builder(updateEvent.snapshotId(), updateEvent.sequenceNumber())
        .partition(specId, partition)
        .addDataFile(dataFiles)
        .addDeleteFile(deleteFiles)
        .addReferencedDataFile(referencedFiles)
        .build();
  }

  private DataFile writeDataFile(String filename, StructLike partition, List<RowData> rows)
      throws IOException {
    return SimpleDataUtil.writeDataFile(table, FileFormat.PARQUET, tablePath, filename,
        createDeletableAppenderFactory(), partition, rows);
  }

  private DeleteFile writeEqDeleteFile(String filename, StructLike partition, List<RowData> deletes)
      throws IOException {
    return SimpleDataUtil.writeEqDeleteFile(table, FileFormat.PARQUET, tablePath, filename,
        createDeletableAppenderFactory(), partition, deletes);
  }

  private DeleteFile writePosDeleteFile(String filename, StructLike partition, List<Pair<CharSequence, Long>> positions)
      throws IOException {
    return SimpleDataUtil.writePosDeleteFile(table, FileFormat.PARQUET, tablePath, filename,
        createDeletableAppenderFactory(), partition, positions);
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

  private ManifestFile createTestingManifestFile(Path manifestPath) {
    return new GenericManifestFile(manifestPath.toAbsolutePath().toString(), manifestPath.toFile().length(), 0,
        ManifestContent.DATA, 0, 0, 0L, 0, 0, 0, 0, 0, 0, null, null);
  }

  private List<Path> assertFlinkManifests(int expectedCount) throws IOException {
    List<Path> manifests = Files.list(flinkManifestFolder.toPath())
        .filter(p -> !p.toString().endsWith(".crc"))
        .collect(Collectors.toList());
    Assert.assertEquals(String.format("Expected %s flink manifests, but the list is: %s", expectedCount, manifests),
        expectedCount, manifests.size());
    return manifests;
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
      IcebergRewriteFilesCommitter committer = new IcebergRewriteFilesCommitter(TableLoader.fromHadoopTable(tablePath));
      committer.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) committer;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergRewriteFilesCommitter.class;
    }
  }
}
