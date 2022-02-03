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
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.FileAppenderFactory;
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
public class TestIcebergStreamRewriter {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;
  private String tablePath;
  private FileAppenderFactory<RowData> appenderFactory;

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

  public TestIcebergStreamRewriter(String format, int formatVersion) {
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
    // partitioned must be false for `createRewriteTask`
    table = SimpleDataUtil.createTable(tablePath, props, false);

    appenderFactory = createDeletableAppenderFactory();
  }

  @Test
  public void testRewriteFiles() throws Exception {
    long timestamp = 0;
    JobID jobID = new JobID();
    try (OneInputStreamOperatorTestHarness<RewriteTask, RewriteResult> harness = createStreamOpr(jobID)) {
      harness.setup();
      harness.open();

      StructLike partition = SimpleDataUtil.createPartition(null);

      DataFile dataFile1 = writeDataFile("data-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "ccc")
      ));
      DataFile dataFile2 = writeDataFile("data-2", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(4, "ddd"),
          SimpleDataUtil.createRowData(5, "eee")
      ));
      DeleteFile posDeleteFile = writePosDeleteFile("pos-delete-txn1-1", partition, ImmutableList.of(
          Pair.of(dataFile1.path(), 1L))
      );
      commit(
          ImmutableList.of(dataFile1, dataFile2),
          formatVersion > 1 ? ImmutableList.of(posDeleteFile) : ImmutableList.of(),
          formatVersion > 1 ? ImmutableList.of(dataFile1.path()) : ImmutableList.of()
      );

      DataFile dataFile3 = writeDataFile("data-3", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(6, "fff"),
          SimpleDataUtil.createRowData(7, "ggg")
      ));
      DeleteFile eqDeleteFile = writeEqDeleteFile("eq-delete-txn2-1", partition, ImmutableList.of(
          SimpleDataUtil.createRowData(3, "ccc"),
          SimpleDataUtil.createRowData(4, "ddd")
      ));
      CommitResult commit = commit(
          ImmutableList.of(dataFile3),
          formatVersion > 1 ? ImmutableList.of(eqDeleteFile) : ImmutableList.of(),
          ImmutableList.of()
      );

      RewriteTask rewriteTask = formatVersion > 1 ? createRewriteTask(commit.snapshotId(), partition,
          combined(dataFile1, posDeleteFile, eqDeleteFile),
          combined(dataFile2, eqDeleteFile),
          combined(dataFile3)
      ) : createRewriteTask(commit.snapshotId(), partition,
          combined(dataFile1),
          combined(dataFile2),
          combined(dataFile3)
      );

      harness.processElement(rewriteTask, ++timestamp);
      Assert.assertEquals(1, harness.extractOutputValues().size());

      RewriteResult rewriteResult = harness.extractOutputValues().get(0);
      Assert.assertEquals(1, rewriteResult.partitions().size());
      Assert.assertEquals(wrap(partition), wrap(rewriteResult.partitions().iterator().next()));
      Assert.assertEquals(rewriteTask.snapshotId(), rewriteResult.startingSnapshotId());
      Assert.assertEquals(3, Iterables.size(rewriteResult.rewrittenDataFiles()));
      Assert.assertEquals(1, Iterables.size(rewriteResult.addedDataFiles()));

      commitRewrite(rewriteResult);
      List<RowData> expected = formatVersion > 1 ? ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(5, "eee"),
          SimpleDataUtil.createRowData(6, "fff"),
          SimpleDataUtil.createRowData(7, "ggg")
      ) : ImmutableList.of(
          SimpleDataUtil.createRowData(1, "aaa"),
          SimpleDataUtil.createRowData(2, "bbb"),
          SimpleDataUtil.createRowData(3, "ccc"),
          SimpleDataUtil.createRowData(4, "ddd"),
          SimpleDataUtil.createRowData(5, "eee"),
          SimpleDataUtil.createRowData(6, "fff"),
          SimpleDataUtil.createRowData(7, "ggg")
      );
      SimpleDataUtil.assertTableRows(table, ImmutableList.copyOf(expected));
    }
  }
  private StructLikeWrapper wrap(StructLike partition) {
    return StructLikeWrapper.forType(table.spec().partitionType()).set(partition);
  }

  private Pair<DataFile, List<DeleteFile>> combined(DataFile dataFile, DeleteFile... deleteFiles) {
    return Pair.of(dataFile, Lists.newArrayList(deleteFiles));
  }

  @SafeVarargs
  private final RewriteTask createRewriteTask(long snapshotId, StructLike partition,
                                              Pair<DataFile, List<DeleteFile>>... scanFiles) {
    String schemaStr = SchemaParser.toJson(table.schema());
    String specStr = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
    ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(Expressions.alwaysTrue());
    List<FileScanTask> fileScanTasks = Arrays.stream(scanFiles)
        .map(p -> new MockFileScanTask(p.first(), p.second().toArray(new DeleteFile[0]), schemaStr, specStr, residuals))
        .collect(Collectors.toList());

    CombinedScanTask combinedScanTask = new BaseCombinedScanTask(fileScanTasks);
    return new RewriteTask(snapshotId, partition, combinedScanTask);
  }

  private CommitResult commit(List<DataFile> dataFiles, List<DeleteFile> deleteFiles,
                              List<CharSequence> referencedFiles) {
    RowDelta rowDelta = table.newRowDelta();
    dataFiles.forEach(rowDelta::addRows);
    deleteFiles.forEach(rowDelta::addDeletes);
    rowDelta.commit();

    CreateSnapshotEvent event = (CreateSnapshotEvent) rowDelta.updateEvent();
    WriteResult writeResult = WriteResult.builder()
        .addDataFiles(dataFiles)
        .addDeleteFiles(deleteFiles)
        .addReferencedDataFiles(referencedFiles)
        .build();
    return CommitResult.builder(event.sequenceNumber(), event.snapshotId()).add(writeResult).build();
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

  private OneInputStreamOperatorTestHarness<RewriteTask, RewriteResult> createStreamOpr(JobID jobID)
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
      implements OneInputStreamOperatorFactory<RewriteTask, RewriteResult> {
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
