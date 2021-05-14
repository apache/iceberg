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

package org.apache.iceberg.spark.actions;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles.Result;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction.FileGroupInfo;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public abstract class TestNewRewriteDataFilesAction extends SparkTestBase {

  protected abstract ActionsProvider actions();

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testRewriteDataFilesEmptyTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    Assert.assertNull("Table must be empty", table.currentSnapshot());

    actions().rewriteDataFiles(table).execute();

    Assert.assertNull("Table must stay empty", table.currentSnapshot());
  }

  @Test
  public void testRewriteDataFilesUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "AAAA"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"),
        new ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 4 data files before rewrite", 4, dataFiles.size());

    Result result = actions().rewriteDataFiles(table).execute();
    Assert.assertEquals("Action should rewrite 4 data files", 4, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFilesCount());

    table.refresh();

    CloseableIterable<FileScanTask> tasks1 = table.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 1 data files before rewrite", 1, dataFiles1.size());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
        new ThreeColumnRecord(1, "AAAAAAAAAA", "CCCC")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "EEEE"),
        new ThreeColumnRecord(2, "AAAAAAAAAA", "GGGG")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(2, "BBBBBBBBBB", "FFFF"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "HHHH")
    );
    writeRecords(records4);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 8 data files before rewrite", 8, dataFiles.size());

    Result result = actions().rewriteDataFiles(table).execute();
    Assert.assertEquals("Action should rewrite 8 data files", 8, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 4 data file", 4, result.addedDataFilesCount());

    table.refresh();

    CloseableIterable<FileScanTask> tasks1 = table.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 4 data files before rewrite", 4, dataFiles1.size());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteDataFilesWithFilter() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
        .identity("c1")
        .truncate("c2", 2)
        .build();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
        new ThreeColumnRecord(1, "AAAAAAAAAA", "CCCC")
    );
    writeRecords(records1);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "BBBBBBBBBB", "DDDD")
    );
    writeRecords(records2);

    List<ThreeColumnRecord> records3 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "EEEE"),
        new ThreeColumnRecord(2, "AAAAAAAAAA", "GGGG")
    );
    writeRecords(records3);

    List<ThreeColumnRecord> records4 = Lists.newArrayList(
        new ThreeColumnRecord(2, "BBBBBBBBBB", "FFFF"),
        new ThreeColumnRecord(2, "BBBBBBBBBB", "HHHH")
    );
    writeRecords(records4);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 8 data files before rewrite", 8, dataFiles.size());

    Result result = actions().rewriteDataFiles(table)
        .filter(Expressions.equal("c1", 1))
        .filter(Expressions.startsWith("c2", "AA"))
        .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFilesCount());

    table.refresh();

    CloseableIterable<FileScanTask> tasks1 = table.newScan().planFiles();
    List<DataFile> dataFiles1 = Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    Assert.assertEquals("Should have 7 data files before rewrite", 7, dataFiles1.size());

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records1);
    expectedRecords.addAll(records2);
    expectedRecords.addAll(records3);
    expectedRecords.addAll(records4);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testRewriteLargeTableHasResiduals() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "100");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // all records belong to the same partition
    List<ThreeColumnRecord> records = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      records.add(new ThreeColumnRecord(i, String.valueOf(i), String.valueOf(i % 4)));
    }
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan()
        .ignoreResiduals()
        .filter(Expressions.equal("c3", "0"))
        .planFiles();
    for (FileScanTask task : tasks) {
      Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
    }
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Assert.assertEquals("Should have 2 data files before rewrite", 2, dataFiles.size());

    Result result = actions().rewriteDataFiles(table)
        .filter(Expressions.equal("c3", "0"))
        .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFilesCount());

    table.refresh();

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF.sort("c1")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();

    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testRewriteDataFilesForLargeFile() throws AnalysisException {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    Assert.assertNull("Table must be empty", table.currentSnapshot());

    List<ThreeColumnRecord> records1 = Lists.newArrayList();

    IntStream.range(0, 2000).forEach(i -> records1.add(new ThreeColumnRecord(i, "foo" + i, "bar" + i)));
    Dataset<Row> df = spark.createDataFrame(records1, ThreeColumnRecord.class).repartition(1);
    writeDF(df);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"),
        new ThreeColumnRecord(1, "DDDDDDDDDD", "DDDD")
    );
    writeRecords(records2);

    table.refresh();

    CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
    List<DataFile> dataFiles = Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    DataFile maxSizeFile = Collections.max(dataFiles, Comparator.comparingLong(DataFile::fileSizeInBytes));
    Assert.assertEquals("Should have 3 files before rewrite", 3, dataFiles.size());

    spark.read().format("iceberg").load(tableLocation).createTempView("origin");
    long originalNumRecords = spark.read().format("iceberg").load(tableLocation).count();
    List<Object[]> originalRecords = sql("SELECT * from origin sort by c2");

    long targetSizeInBytes = maxSizeFile.fileSizeInBytes() - 10;
    Result result = actions().rewriteDataFiles(table)
        .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(targetSizeInBytes))
        .option(BinPackStrategy.MIN_FILE_SIZE_BYTES, Long.toString(targetSizeInBytes - 1))
        .option(BinPackStrategy.MAX_FILE_SIZE_BYTES, Long.toString(targetSizeInBytes + 1))
        .execute();

    Assert.assertEquals("Action should delete 3 data files", 3, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 2 data files", 2, result.addedDataFilesCount());

    spark.read().format("iceberg").load(tableLocation).createTempView("postRewrite");
    long postRewriteNumRecords = spark.read().format("iceberg").load(tableLocation).count();
    List<Object[]> rewrittenRecords = sql("SELECT * from postRewrite sort by c2");

    Assert.assertEquals(originalNumRecords, postRewriteNumRecords);
    assertEquals("Rows should be unchanged", originalRecords, rewrittenRecords);
  }

  @Test
  public void testPartialProgressEnabled() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        actions().rewriteDataFiles(table)
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "10")
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.resultMap().keySet().size(), 10);

    table.refresh();

    shouldHaveSnapshots(table, 11);

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);
  }

  @Test
  public void testMultipleGroups() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        actions().rewriteDataFiles(table)
            .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.resultMap().keySet().size(), 10);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
  }

  @Test
  public void testPartialProgressMaxCommits() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        actions().rewriteDataFiles(table)
            .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3")
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.resultMap().keySet().size(), 10);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 4);
  }

  @Test
  public void testSingleCommitWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            actions().rewriteDataFiles(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100));

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(argThat(failGroup), anyInt(), any(), any(), any(), any());

    AssertHelpers.assertThrows("Should fail entire rewrite if part fails", RuntimeException.class,
        () -> spyRewrite.execute());

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
  }

  @Test
  public void testSingleCommitWithCommitFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            actions().rewriteDataFiles(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100));

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail to commit
    doThrow(new RuntimeException("Commit Failure"))
        .when(spyRewrite)
        .commitFileGroups(any());

    AssertHelpers.assertThrows("Should fail entire rewrite if commit fails", RuntimeException.class,
        () -> spyRewrite.execute());

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
  }

  @Test
  public void testParallelSingleCommitWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            actions().rewriteDataFiles(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
                .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_ACTIONS, "3");

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(argThat(failGroup), anyInt(), any(), any(), any(), any());

    AssertHelpers.assertThrows("Should fail entire rewrite if part fails", RuntimeException.class,
        () -> spyRewrite.execute());

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
  }

  @Test
  public void testPartialProgressWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            actions().rewriteDataFiles(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
                .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3");

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(argThat(failGroup), anyInt(), any(), any(), any(), any());

    RewriteDataFiles.Result result = spyRewrite.execute();

    Assert.assertEquals("Should have 7 fileGroups", result.resultMap().keySet().size(), 7);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // With 10 original groups and Max Commits of 3, we should have commits with 4, 4, and 2.
    // removing 3 groups leaves us with only 2 new commits, 4 and 3
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
  }

  @Test
  public void testParallelPartialProgressWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            actions().rewriteDataFiles(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
                .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_ACTIONS, "3")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3");

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(argThat(failGroup), anyInt(), any(), any(), any(), any());

    RewriteDataFiles.Result result = spyRewrite.execute();

    Assert.assertEquals("Should have 7 fileGroups", result.resultMap().keySet().size(), 7);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // With 10 original groups and Max Commits of 3, we should have commits with 4, 4, and 2.
    // removing 3 groups leaves us with only 2 new commits, 4 and 3
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
  }

  @Test
  public void testParallelPartialProgressWithCommitFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    BaseRewriteDataFilesSparkAction realRewrite =
        (org.apache.iceberg.spark.actions.BaseRewriteDataFilesSparkAction)
            actions().rewriteDataFiles(table)
                .option(RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 100))
                .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_ACTIONS, "3")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
                .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3");

    BaseRewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // First and Third commits work, second does not
    doCallRealMethod()
        .doThrow(new RuntimeException("Commit Failed"))
        .doCallRealMethod()
        .when(spyRewrite)
        .commitFileGroups(any());

    RewriteDataFiles.Result result = spyRewrite.execute();

    // Commit 1: 4, Commit 2 failed : 4, Commit 3: 2
    Assert.assertEquals("Should have 6 fileGroups", 6, result.resultMap().keySet().size());

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // Only 2 new commits because we broke one
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
  }

  private List<Object[]> currentData() {
    return rowsToJava(spark.read().format("iceberg").load(tableLocation).sort("c1").collectAsList());
  }

  private void shouldHaveSnapshots(Table table, int expectedSnapshots) {
    int actualSnapshots = Iterables.size(table.snapshots());
    Assert.assertEquals("Table did not have the expected number of snapshots",
        expectedSnapshots, actualSnapshots);
  }

  private void shouldHaveNoOrphans(Table table) {
    Assert.assertEquals("Should not have found any orphan files", Collections.EMPTY_LIST,
        actions().removeOrphanFiles(table).execute().orphanFileLocations());
  }

  /**
   * Create a table with a certain number of files, returns the size of a file
   * @param files number of files to create
   * @return size of a file
   */
  private Table createTable(int files) {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    Assert.assertNull("Table must be empty", table.currentSnapshot());

    List<ThreeColumnRecord> records1 = Lists.newArrayList();

    IntStream.range(0, 2000).forEach(i -> records1.add(new ThreeColumnRecord(i, "foo" + i, "bar" + i)));
    Dataset<Row> df = spark.createDataFrame(records1, ThreeColumnRecord.class).repartition(files);
    writeDF(df);
    table.refresh();

    return table;
  }

  private int averageFileSize(Table table) {
    return (int) Streams.stream(table.currentSnapshot().addedFiles().iterator())
        .mapToLong(DataFile::fileSizeInBytes)
        .average()
        .getAsDouble();
  }

  private void writeRecords(List<ThreeColumnRecord> records) {
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);
  }

  private void writeDF(Dataset<Row> df) {
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
  }

  class GroupInfoMatcher implements ArgumentMatcher<Pair<FileGroupInfo, List<FileScanTask>>> {
    private final Set<Integer> groupIDs;

    GroupInfoMatcher(Integer... globalIndex) {
      this.groupIDs = ImmutableSet.copyOf(globalIndex);
    }

    @Override
    public boolean matches(Pair<FileGroupInfo, List<FileScanTask>> argument) {
      return groupIDs.contains(argument.first().globalIndex());
    }
  }
}
