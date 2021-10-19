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

import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergStreamRewriter {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tableDir = null;
  private File metadataDir = null;
  private Table table;
  private String tablePath;
  private File flinkManifestFolder;

  private final FileFormat format;
  private final int formatVersion;
  private final boolean partitioned;
  private final int parallelism;

  @Parameterized.Parameters(name = "FileFormat = {0}, FormatVersion={1}, Partitioned={2}, Parallelism = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"parquet", 2, false, 2},
//        new Object[] {"parquet", 2, true, 2},
    };
  }

  public TestIcebergStreamRewriter(String format, int formatVersion, boolean partitioned, int parallelism) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.formatVersion = formatVersion;
    this.partitioned = partitioned;
    this.parallelism = parallelism;
  }

  @Before
  public void setupTable() throws IOException {
    tableDir = temp.newFolder();
    metadataDir = new File(tableDir, "metadata");
    Assert.assertTrue(tableDir.delete());

    tablePath = tableDir.getAbsolutePath();

    flinkManifestFolder = temp.newFolder();

    // Construct the iceberg table.
    Map<String, String> props = ImmutableMap.of(
        TableProperties.DEFAULT_FILE_FORMAT, format.name(),
        TableProperties.FORMAT_VERSION, String.valueOf(formatVersion),
        FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath()
    );
    table = SimpleDataUtil.createTable(tablePath, props, partitioned);
  }

  @Test
  public void testRewriteV2Table() throws Exception {
    table.updateProperties()
        .set(IcebergStreamRewriter.MAX_FILES_COUNT, "4")
        .commit();

    long timestamp = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<CommitResult, RewriteResult> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      // receive commitResult2
      DataFile dataFile11 = writeDataFile("data-ckpt1-1", ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "ccc")
      ));
      DeleteFile deleteFile11 = writePosDeleteFile("pos-delete-ckpt1-1" , ImmutableList.of(
          Pair.of(dataFile11.path(), 1L))
      );
      CommitResult commitResult1 = commitData(table.spec().specId(), null,
          ImmutableList.of(dataFile11), ImmutableList.of(deleteFile11), ImmutableList.of(deleteFile11.path()));

      harness.processElement(commitResult1, ++timestamp);
      assertFlinkManifests(2);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // receive commitResult2
      DataFile dataFile21 = writeDataFile("data-ckpt2-1", ImmutableList.of(
          SimpleDataUtil.createRowData(3, "xxx"),
          SimpleDataUtil.createRowData(4, "ddd")
      ));
      DeleteFile deleteFile21 = writeEqDeleteFile("eq-delete-ckpt2-1" , ImmutableList.of(
          SimpleDataUtil.createRowData(3, "ccc")
      ));
      DataFile dataFile22 = writeDataFile("data-ckpt2-2", ImmutableList.of(
          SimpleDataUtil.createRowData(5, "xxx"),
          SimpleDataUtil.createRowData(6, "yyy")
      ));
      CommitResult commitResult2 = commitData(table.spec().specId(), null,
          ImmutableList.of(dataFile21, dataFile22), ImmutableList.of(deleteFile21), ImmutableList.of());

      harness.processElement(commitResult2, ++timestamp);
      assertFlinkManifests(4);
      Assert.assertTrue(harness.extractOutputValues().isEmpty());

      // receive commitResult3
      DataFile dataFile31 = writeDataFile("data-ckpt3-1", ImmutableList.of(
          SimpleDataUtil.createRowData(7, "xxx")
      ));
      CommitResult commitResult3 = commitData(table.spec().specId(), null,
          ImmutableList.of(dataFile31), ImmutableList.of(), ImmutableList.of());

      harness.processElement(commitResult3, ++timestamp);
      assertFlinkManifests(0);
      Assert.assertEquals(1, harness.extractOutputValues().size());
      RewriteResult rewriteResult = harness.extractOutputValues().get(0);

      Assert.assertEquals(null, rewriteResult.partition());
      Assert.assertEquals(commitResult3.snapshotId(), rewriteResult.startingSnapshotId());
      Assert.assertEquals(4, Iterables.size(rewriteResult.deletedDataFiles()));
      Assert.assertEquals(1, Iterables.size(rewriteResult.addedDataFiles()));

      commitRewrite(rewriteResult);
      List<RowData> expected = ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(3, "xxx"),
          SimpleDataUtil.createRowData(4, "ddd"),
          SimpleDataUtil.createRowData(5, "xxx"),
          SimpleDataUtil.createRowData(6, "yyy"),
          SimpleDataUtil.createRowData(7, "xxx")
      );
      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(expected));

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

  private void commitRewrite(RewriteResult result) {
    RewriteFiles rewriteFiles = table.newRewrite()
        .validateFromSnapshot(result.startingSnapshotId())
        .rewriteFiles(Sets.newHashSet(result.deletedDataFiles()), Sets.newHashSet(result.addedDataFiles()));
    rewriteFiles.commit();
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(table.schema(), table.spec(), CONF, tablePath, format.addExtension(filename), rows);
  }

  private DeleteFile writeEqDeleteFile(String filename, List<RowData> deletes) throws IOException {
    return SimpleDataUtil.writeEqDeleteFile(table, FileFormat.PARQUET, tablePath, filename,
        createDeletableAppenderFactory(), deletes);
  }

  private DeleteFile writePosDeleteFile(String filename, List<Pair<CharSequence, Long>> positions) throws IOException {
    return SimpleDataUtil.writePosDeleteFile(table, FileFormat.PARQUET, tablePath, filename,
        createDeletableAppenderFactory(), positions);
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

  private OneInputStreamOperatorTestHarness<CommitResult, RewriteResult> createStreamOpr(JobID jobID)
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
      implements OneInputStreamOperatorFactory<CommitResult, RewriteResult> {
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
      IcebergStreamRewriter streamRewriter = new IcebergStreamRewriter(TableLoader.fromHadoopTable(tablePath));
      streamRewriter.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) streamRewriter;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergStreamRewriter.class;
    }
  }
}
