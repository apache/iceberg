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

import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles.Result;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.actions.SizeBasedDataRewriter;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.actions.RewriteDataFilesSparkAction.RewriteExecutionContext;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

public class TestRewriteDataFilesAction extends SparkTestBase {

  private static final int SCALE = 400000;

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private final FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();
  private final ScanTaskSetManager manager = ScanTaskSetManager.get();
  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  private RewriteDataFilesSparkAction basicRewrite(Table table) {
    // Always compact regardless of input files
    table.refresh();
    return actions().rewriteDataFiles(table).option(SizeBasedFileRewriter.MIN_INPUT_FILES, "1");
  }

  @Test
  public void testEmptyTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    Assert.assertNull("Table must be empty", table.currentSnapshot());

    basicRewrite(table).execute();

    Assert.assertNull("Table must stay empty", table.currentSnapshot());
  }

  @Test
  public void testBinPackUnpartitionedTable() {
    Table table = createTable(4);
    shouldHaveFiles(table, 4);
    List<Object[]> expectedRecords = currentData();
    long dataSizeBefore = testDataSize(table);

    Result result = basicRewrite(table).execute();
    Assert.assertEquals("Action should rewrite 4 data files", 4, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 1);
    List<Object[]> actual = currentData();

    assertEquals("Rows must match", expectedRecords, actual);
  }

  @Test
  public void testBinPackPartitionedTable() {
    Table table = createTablePartitioned(4, 2);
    shouldHaveFiles(table, 8);
    List<Object[]> expectedRecords = currentData();
    long dataSizeBefore = testDataSize(table);

    Result result = basicRewrite(table).execute();
    Assert.assertEquals("Action should rewrite 8 data files", 8, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 4 data file", 4, result.addedDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 4);
    List<Object[]> actualRecords = currentData();

    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testBinPackWithFilter() {
    Table table = createTablePartitioned(4, 2);
    shouldHaveFiles(table, 8);
    List<Object[]> expectedRecords = currentData();
    long dataSizeBefore = testDataSize(table);

    Result result =
        basicRewrite(table)
            .filter(Expressions.equal("c1", 1))
            .filter(Expressions.startsWith("c2", "foo"))
            .execute();

    Assert.assertEquals("Action should rewrite 2 data files", 2, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isGreaterThan(0L).isLessThan(dataSizeBefore);

    shouldHaveFiles(table, 7);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testBinPackAfterPartitionChange() {
    Table table = createTable();

    writeRecords(20, SCALE, 20);
    shouldHaveFiles(table, 20);
    table.updateSpec().addField(Expressions.ref("c1")).commit();

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(SizeBasedFileRewriter.MIN_INPUT_FILES, "1")
            .option(
                SizeBasedFileRewriter.MIN_FILE_SIZE_BYTES,
                Integer.toString(averageFileSize(table) + 1000))
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
                Integer.toString(averageFileSize(table) + 1001))
            .execute();

    Assert.assertEquals(
        "Should have 1 fileGroup because all files were not correctly partitioned",
        1,
        result.rewriteResults().size());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
    shouldHaveFiles(table, 20);
  }

  @Test
  public void testBinPackWithDeletes() throws Exception {
    Table table = createTablePartitioned(4, 2);
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    shouldHaveFiles(table, 8);
    table.refresh();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    int total = (int) dataFiles.stream().mapToLong(ContentFile::recordCount).sum();

    RowDelta rowDelta = table.newRowDelta();
    // add 1 delete file for data files 0, 1, 2
    for (int i = 0; i < 3; i++) {
      writePosDeletesToFile(table, dataFiles.get(i), 1).forEach(rowDelta::addDeletes);
    }

    // add 2 delete files for data files 3, 4
    for (int i = 3; i < 5; i++) {
      writePosDeletesToFile(table, dataFiles.get(i), 2).forEach(rowDelta::addDeletes);
    }

    rowDelta.commit();
    table.refresh();
    List<Object[]> expectedRecords = currentData();
    long dataSizeBefore = testDataSize(table);

    Result result =
        actions()
            .rewriteDataFiles(table)
            // do not include any file based on bin pack file size configs
            .option(SizeBasedFileRewriter.MIN_FILE_SIZE_BYTES, "0")
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .option(SizeBasedFileRewriter.MAX_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE))
            .option(SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, "2")
            .execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.rewrittenDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isGreaterThan(0L).isLessThan(dataSizeBefore);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
    Assert.assertEquals("7 rows are removed", total - 7, actualRecords.size());
  }

  @Test
  public void testBinPackWithDeleteAllData() {
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, "2");
    Table table = createTablePartitioned(1, 1, 1, options);
    shouldHaveFiles(table, 1);
    table.refresh();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    int total = (int) dataFiles.stream().mapToLong(ContentFile::recordCount).sum();

    RowDelta rowDelta = table.newRowDelta();
    // remove all data
    writePosDeletesToFile(table, dataFiles.get(0), total).forEach(rowDelta::addDeletes);

    rowDelta.commit();
    table.refresh();
    List<Object[]> expectedRecords = currentData();
    long dataSizeBefore = testDataSize(table);

    Result result =
        actions()
            .rewriteDataFiles(table)
            .option(SizeBasedDataRewriter.DELETE_FILE_THRESHOLD, "1")
            .execute();
    Assert.assertEquals("Action should rewrite 1 data files", 1, result.rewrittenDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
    Assert.assertEquals(
        "Data manifest should not have existing data file",
        0,
        (long) table.currentSnapshot().dataManifests(table.io()).get(0).existingFilesCount());
    Assert.assertEquals(
        "Data manifest should have 1 delete data file",
        1L,
        (long) table.currentSnapshot().dataManifests(table.io()).get(0).deletedFilesCount());
    Assert.assertEquals(
        "Delete manifest added row count should equal total count",
        total,
        (long) table.currentSnapshot().deleteManifests(table.io()).get(0).addedRowsCount());
  }

  @Test
  public void testBinPackWithStartingSequenceNumber() {
    Table table = createTablePartitioned(4, 2);
    shouldHaveFiles(table, 8);
    List<Object[]> expectedRecords = currentData();
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    table.refresh();
    long oldSequenceNumber = table.currentSnapshot().sequenceNumber();
    long dataSizeBefore = testDataSize(table);

    Result result =
        basicRewrite(table).option(RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER, "true").execute();
    Assert.assertEquals("Action should rewrite 8 data files", 8, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 4 data file", 4, result.addedDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 4);
    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);

    table.refresh();
    Assert.assertTrue(
        "Table sequence number should be incremented",
        oldSequenceNumber < table.currentSnapshot().sequenceNumber());

    Dataset<Row> rows = SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.ENTRIES);
    for (Row row : rows.collectAsList()) {
      if (row.getInt(0) == 1) {
        Assert.assertEquals(
            "Expect old sequence number for added entries", oldSequenceNumber, row.getLong(2));
      }
    }
  }

  @Test
  public void testBinPackWithStartingSequenceNumberV1Compatibility() {
    Map<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "1");
    Table table = createTablePartitioned(4, 2, SCALE, properties);
    shouldHaveFiles(table, 8);
    List<Object[]> expectedRecords = currentData();
    table.refresh();
    long oldSequenceNumber = table.currentSnapshot().sequenceNumber();
    Assert.assertEquals("Table sequence number should be 0", 0, oldSequenceNumber);
    long dataSizeBefore = testDataSize(table);

    Result result =
        basicRewrite(table).option(RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER, "true").execute();
    Assert.assertEquals("Action should rewrite 8 data files", 8, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 4 data file", 4, result.addedDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 4);
    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);

    table.refresh();
    Assert.assertEquals(
        "Table sequence number should still be 0",
        oldSequenceNumber,
        table.currentSnapshot().sequenceNumber());

    Dataset<Row> rows = SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.ENTRIES);
    for (Row row : rows.collectAsList()) {
      Assert.assertEquals(
          "Expect sequence number 0 for all entries", oldSequenceNumber, row.getLong(2));
    }
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

    List<Object[]> expectedRecords = currentData();

    table.refresh();

    CloseableIterable<FileScanTask> tasks =
        table.newScan().ignoreResiduals().filter(Expressions.equal("c3", "0")).planFiles();
    for (FileScanTask task : tasks) {
      Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
    }

    shouldHaveFiles(table, 2);

    long dataSizeBefore = testDataSize(table);
    Result result = basicRewrite(table).filter(Expressions.equal("c3", "0")).execute();
    Assert.assertEquals("Action should rewrite 2 data files", 2, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 1 data file", 1, result.addedDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    List<Object[]> actualRecords = currentData();

    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testBinPackSplitLargeFile() {
    Table table = createTable(1);
    shouldHaveFiles(table, 1);

    List<Object[]> expectedRecords = currentData();
    long targetSize = testDataSize(table) / 2;

    long dataSizeBefore = testDataSize(table);
    Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(targetSize))
            .option(SizeBasedFileRewriter.MAX_FILE_SIZE_BYTES, Long.toString(targetSize * 2 - 2000))
            .execute();

    Assert.assertEquals("Action should delete 1 data files", 1, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 2 data files", 2, result.addedDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 2);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testBinPackCombineMixedFiles() {
    Table table = createTable(1); // 400000
    shouldHaveFiles(table, 1);

    // Add one more small file, and one large file
    writeRecords(1, SCALE);
    writeRecords(1, SCALE * 3);
    shouldHaveFiles(table, 3);

    List<Object[]> expectedRecords = currentData();

    int targetSize = averageFileSize(table);

    long dataSizeBefore = testDataSize(table);
    Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Integer.toString(targetSize + 1000))
            .option(SizeBasedFileRewriter.MAX_FILE_SIZE_BYTES, Integer.toString(targetSize + 80000))
            .option(SizeBasedFileRewriter.MIN_FILE_SIZE_BYTES, Integer.toString(targetSize - 1000))
            .execute();

    Assert.assertEquals("Action should delete 3 data files", 3, result.rewrittenDataFilesCount());
    // Should Split the big files into 3 pieces, one of which should be combined with the two
    // smaller files
    Assert.assertEquals("Action should add 3 data files", 3, result.addedDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 3);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testBinPackCombineMediumFiles() {
    Table table = createTable(4);
    shouldHaveFiles(table, 4);

    List<Object[]> expectedRecords = currentData();
    int targetSize = ((int) testDataSize(table) / 3);
    // The test is to see if we can combine parts of files to make files of the correct size

    long dataSizeBefore = testDataSize(table);
    Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Integer.toString(targetSize))
            .option(
                SizeBasedFileRewriter.MAX_FILE_SIZE_BYTES,
                Integer.toString((int) (targetSize * 1.8)))
            .option(
                SizeBasedFileRewriter.MIN_FILE_SIZE_BYTES,
                Integer.toString(targetSize - 100)) // All files too small
            .execute();

    Assert.assertEquals("Action should delete 4 data files", 4, result.rewrittenDataFilesCount());
    Assert.assertEquals("Action should add 3 data files", 3, result.addedDataFilesCount());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 3);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testPartialProgressEnabled() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    table.updateProperties().set(COMMIT_NUM_RETRIES, "10").commit();

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "10")
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.rewriteResults().size(), 10);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    table.refresh();

    shouldHaveSnapshots(table, 11);
    shouldHaveACleanCache(table);

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);
  }

  @Test
  public void testMultipleGroups() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .option(SizeBasedFileRewriter.MIN_INPUT_FILES, "1")
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.rewriteResults().size(), 10);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testPartialProgressMaxCommits() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3")
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.rewriteResults().size(), 10);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 4);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testSingleCommitWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    RewriteDataFilesSparkAction realRewrite =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000));

    RewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    assertThatThrownBy(() -> spyRewrite.execute())
        .as("Should fail entire rewrite if part fails")
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Rewrite Failed");

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testSingleCommitWithCommitFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    RewriteDataFilesSparkAction realRewrite =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000));

    RewriteDataFilesSparkAction spyRewrite = spy(realRewrite);
    RewriteDataFilesCommitManager util = spy(new RewriteDataFilesCommitManager(table));

    // Fail to commit
    doThrow(new CommitFailedException("Commit Failure")).when(util).commitFileGroups(any());

    doReturn(util).when(spyRewrite).commitManager(table.currentSnapshot().snapshotId());

    assertThatThrownBy(() -> spyRewrite.execute())
        .as("Should fail entire rewrite if commit fails")
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Cannot commit rewrite");

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testCommitFailsWithUncleanableFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    RewriteDataFilesSparkAction realRewrite =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000));

    RewriteDataFilesSparkAction spyRewrite = spy(realRewrite);
    RewriteDataFilesCommitManager util = spy(new RewriteDataFilesCommitManager(table));

    // Fail to commit with an arbitrary failure and validate that orphans are not cleaned up
    doThrow(new RuntimeException("Arbitrary Failure")).when(util).commitFileGroups(any());

    doReturn(util).when(spyRewrite).commitManager(table.currentSnapshot().snapshotId());

    assertThatThrownBy(spyRewrite::execute)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Arbitrary Failure");

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testParallelSingleCommitWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    RewriteDataFilesSparkAction realRewrite =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "3");

    RewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new CommitFailedException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    assertThatThrownBy(() -> spyRewrite.execute())
        .as("Should fail entire rewrite if part fails")
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Rewrite Failed");

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testPartialProgressWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFilesSparkAction realRewrite =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3");

    RewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    RewriteDataFiles.Result result = spyRewrite.execute();

    Assert.assertEquals("Should have 7 fileGroups", result.rewriteResults().size(), 7);
    assertThat(result.rewrittenBytesCount()).isGreaterThan(0L).isLessThan(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // With 10 original groups and Max Commits of 3, we should have commits with 4, 4, and 2.
    // removing 3 groups leaves us with only 2 new commits, 4 and 3
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testParallelPartialProgressWithRewriteFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFilesSparkAction realRewrite =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "3")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3");

    RewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    RewriteDataFiles.Result result = spyRewrite.execute();

    Assert.assertEquals("Should have 7 fileGroups", result.rewriteResults().size(), 7);
    assertThat(result.rewrittenBytesCount()).isGreaterThan(0L).isLessThan(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // With 10 original groups and Max Commits of 3, we should have commits with 4, 4, and 2.
    // removing 3 groups leaves us with only 2 new commits, 4 and 3
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testParallelPartialProgressWithCommitFailure() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFilesSparkAction realRewrite =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "3")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3");

    RewriteDataFilesSparkAction spyRewrite = spy(realRewrite);
    RewriteDataFilesCommitManager util = spy(new RewriteDataFilesCommitManager(table));

    // First and Third commits work, second does not
    doCallRealMethod()
        .doThrow(new CommitFailedException("Commit Failed"))
        .doCallRealMethod()
        .when(util)
        .commitFileGroups(any());

    doReturn(util).when(spyRewrite).commitManager(table.currentSnapshot().snapshotId());

    RewriteDataFiles.Result result = spyRewrite.execute();

    // Commit 1: 4/4 + Commit 2 failed 0/4 + Commit 3: 2/2 == 6 out of 10 total groups comitted
    Assert.assertEquals("Should have 6 fileGroups", 6, result.rewriteResults().size());
    assertThat(result.rewrittenBytesCount()).isGreaterThan(0L).isLessThan(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // Only 2 new commits because we broke one
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testInvalidOptions() {
    Table table = createTable(20);

    assertThatThrownBy(
            () ->
                basicRewrite(table)
                    .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
                    .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "-5")
                    .execute())
        .as("No negative values for partial progress max commits")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot set partial-progress.max-commits to -5, "
                + "the value must be positive when partial-progress.enabled is true");

    assertThatThrownBy(
            () ->
                basicRewrite(table)
                    .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "-5")
                    .execute())
        .as("No negative values for max concurrent groups")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot set max-concurrent-file-group-rewrites to -5, the value must be positive.");

    assertThatThrownBy(() -> basicRewrite(table).option("foobarity", "-5").execute())
        .as("No unknown options allowed")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot use options [foobarity], they are not supported by the action or the rewriter BIN-PACK");

    assertThatThrownBy(
            () -> basicRewrite(table).option(RewriteDataFiles.REWRITE_JOB_ORDER, "foo").execute())
        .as("Cannot set rewrite-job-order to foo")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid rewrite job order name: foo");
  }

  @Test
  public void testSortMultipleGroups() {
    Table table = createTable(20);
    shouldHaveFiles(table, 20);
    table.replaceSortOrder().asc("c2").commit();
    shouldHaveLastCommitUnsorted(table, "c2");
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    // Perform a rewrite but only allow 2 files to be compacted at a time
    RewriteDataFiles.Result result =
        basicRewrite(table)
            .sort()
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .execute();

    Assert.assertEquals("Should have 10 fileGroups", result.rewriteResults().size(), 10);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testSimpleSort() {
    Table table = createTable(20);
    shouldHaveFiles(table, 20);
    table.replaceSortOrder().asc("c2").commit();
    shouldHaveLastCommitUnsorted(table, "c2");

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .sort()
            .option(SizeBasedFileRewriter.MIN_INPUT_FILES, "1")
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Integer.toString(averageFileSize(table)))
            .execute();

    Assert.assertEquals("Should have 1 fileGroups", result.rewriteResults().size(), 1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
    shouldHaveMultipleFiles(table);
    shouldHaveLastCommitSorted(table, "c2");
  }

  @Test
  public void testSortAfterPartitionChange() {
    Table table = createTable(20);
    shouldHaveFiles(table, 20);
    table.updateSpec().addField(Expressions.bucket("c1", 4)).commit();
    table.replaceSortOrder().asc("c2").commit();
    shouldHaveLastCommitUnsorted(table, "c2");

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .sort()
            .option(SizeBasedFileRewriter.MIN_INPUT_FILES, "1")
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Integer.toString(averageFileSize(table)))
            .execute();

    Assert.assertEquals(
        "Should have 1 fileGroup because all files were not correctly partitioned",
        result.rewriteResults().size(),
        1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
    shouldHaveMultipleFiles(table);
    shouldHaveLastCommitSorted(table, "c2");
  }

  @Test
  public void testSortCustomSortOrder() {
    Table table = createTable(20);
    shouldHaveLastCommitUnsorted(table, "c2");
    shouldHaveFiles(table, 20);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .sort(SortOrder.builderFor(table.schema()).asc("c2").build())
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Integer.toString(averageFileSize(table)))
            .execute();

    Assert.assertEquals("Should have 1 fileGroups", result.rewriteResults().size(), 1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
    shouldHaveMultipleFiles(table);
    shouldHaveLastCommitSorted(table, "c2");
  }

  @Test
  public void testSortCustomSortOrderRequiresRepartition() {
    int partitions = 4;
    Table table = createTable();
    writeRecords(20, SCALE, partitions);
    shouldHaveLastCommitUnsorted(table, "c3");

    // Add a partition column so this requires repartitioning
    table.updateSpec().addField("c1").commit();
    // Add a sort order which our repartitioning needs to ignore
    table.replaceSortOrder().asc("c2").apply();
    shouldHaveFiles(table, 20);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .sort(SortOrder.builderFor(table.schema()).asc("c3").build())
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
                Integer.toString(averageFileSize(table) / partitions))
            .execute();

    Assert.assertEquals("Should have 1 fileGroups", result.rewriteResults().size(), 1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
    shouldHaveMultipleFiles(table);
    shouldHaveLastCommitUnsorted(table, "c2");
    shouldHaveLastCommitSorted(table, "c3");
  }

  @Test
  public void testAutoSortShuffleOutput() {
    Table table = createTable(20);
    shouldHaveLastCommitUnsorted(table, "c2");
    shouldHaveFiles(table, 20);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .sort(SortOrder.builderFor(table.schema()).asc("c2").build())
            .option(
                SizeBasedFileRewriter.MAX_FILE_SIZE_BYTES,
                Integer.toString((averageFileSize(table) / 2) + 2))
            // Divide files in 2
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
                Integer.toString(averageFileSize(table) / 2))
            .option(SizeBasedFileRewriter.MIN_INPUT_FILES, "1")
            .execute();

    Assert.assertEquals("Should have 1 fileGroups", result.rewriteResults().size(), 1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    Assert.assertTrue(
        "Should have written 40+ files",
        Iterables.size(table.currentSnapshot().addedDataFiles(table.io())) >= 40);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
    shouldHaveMultipleFiles(table);
    shouldHaveLastCommitSorted(table, "c2");
  }

  @Test
  public void testCommitStateUnknownException() {
    Table table = createTable(20);
    shouldHaveFiles(table, 20);

    List<Object[]> originalData = currentData();

    RewriteDataFilesSparkAction action = basicRewrite(table);
    RewriteDataFilesSparkAction spyAction = spy(action);
    RewriteDataFilesCommitManager util = spy(new RewriteDataFilesCommitManager(table));

    doAnswer(
            invocationOnMock -> {
              invocationOnMock.callRealMethod();
              throw new CommitStateUnknownException(new RuntimeException("Unknown State"));
            })
        .when(util)
        .commitFileGroups(any());

    doReturn(util).when(spyAction).commitManager(table.currentSnapshot().snapshotId());

    assertThatThrownBy(() -> spyAction.execute())
        .as("Should propagate CommitStateUnknown Exception")
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith(
            "Unknown State\n" + "Cannot determine whether the commit was successful or not");

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2); // Commit actually Succeeded
  }

  @Test
  public void testZOrderSort() {
    int originalFiles = 20;
    Table table = createTable(originalFiles);
    shouldHaveLastCommitUnsorted(table, "c2");
    shouldHaveFiles(table, originalFiles);

    List<Object[]> originalData = currentData();
    double originalFilesC2 = percentFilesRequired(table, "c2", "foo23");
    double originalFilesC3 = percentFilesRequired(table, "c3", "bar21");
    double originalFilesC2C3 =
        percentFilesRequired(table, new String[] {"c2", "c3"}, new String[] {"foo23", "bar23"});

    Assert.assertTrue("Should require all files to scan c2", originalFilesC2 > 0.99);
    Assert.assertTrue("Should require all files to scan c3", originalFilesC3 > 0.99);

    long dataSizeBefore = testDataSize(table);
    RewriteDataFiles.Result result =
        basicRewrite(table)
            .zOrder("c2", "c3")
            .option(
                SizeBasedFileRewriter.MAX_FILE_SIZE_BYTES,
                Integer.toString((averageFileSize(table) / 2) + 2))
            // Divide files in 2
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
                Integer.toString(averageFileSize(table) / 2))
            .option(SizeBasedFileRewriter.MIN_INPUT_FILES, "1")
            .execute();

    Assert.assertEquals("Should have 1 fileGroups", 1, result.rewriteResults().size());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    int zOrderedFilesTotal = Iterables.size(table.currentSnapshot().addedDataFiles(table.io()));
    Assert.assertTrue("Should have written 40+ files", zOrderedFilesTotal >= 40);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);

    double filesScannedC2 = percentFilesRequired(table, "c2", "foo23");
    double filesScannedC3 = percentFilesRequired(table, "c3", "bar21");
    double filesScannedC2C3 =
        percentFilesRequired(table, new String[] {"c2", "c3"}, new String[] {"foo23", "bar23"});

    Assert.assertTrue(
        "Should have reduced the number of files required for c2",
        filesScannedC2 < originalFilesC2);
    Assert.assertTrue(
        "Should have reduced the number of files required for c3",
        filesScannedC3 < originalFilesC3);
    Assert.assertTrue(
        "Should have reduced the number of files required for a c2,c3 predicate",
        filesScannedC2C3 < originalFilesC2C3);
  }

  @Test
  public void testZOrderAllTypesSort() {
    Table table = createTypeTestTable();
    shouldHaveFiles(table, 10);

    List<Row> originalRaw =
        spark.read().format("iceberg").load(tableLocation).sort("longCol").collectAsList();
    List<Object[]> originalData = rowsToJava(originalRaw);
    long dataSizeBefore = testDataSize(table);

    // TODO add in UUID when it is supported in Spark
    RewriteDataFiles.Result result =
        basicRewrite(table)
            .zOrder(
                "longCol",
                "intCol",
                "floatCol",
                "doubleCol",
                "dateCol",
                "timestampCol",
                "stringCol",
                "binaryCol",
                "booleanCol")
            .option(SizeBasedFileRewriter.MIN_INPUT_FILES, "1")
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();

    Assert.assertEquals("Should have 1 fileGroups", 1, result.rewriteResults().size());
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    int zOrderedFilesTotal = Iterables.size(table.currentSnapshot().addedDataFiles(table.io()));
    Assert.assertEquals("Should have written 1 file", 1, zOrderedFilesTotal);

    table.refresh();

    List<Row> postRaw =
        spark.read().format("iceberg").load(tableLocation).sort("longCol").collectAsList();
    List<Object[]> postRewriteData = rowsToJava(postRaw);
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
  }

  @Test
  public void testInvalidAPIUsage() {
    Table table = createTable(1);

    SortOrder sortOrder = SortOrder.builderFor(table.schema()).asc("c2").build();

    assertThatThrownBy(() -> actions().rewriteDataFiles(table).binPack().sort())
        .as("Should be unable to set Strategy more than once")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Must use only one rewriter type");

    assertThatThrownBy(() -> actions().rewriteDataFiles(table).sort(sortOrder).binPack())
        .as("Should be unable to set Strategy more than once")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Must use only one rewriter type");

    assertThatThrownBy(() -> actions().rewriteDataFiles(table).sort(sortOrder).binPack())
        .as("Should be unable to set Strategy more than once")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Must use only one rewriter type");
  }

  @Test
  public void testRewriteJobOrderBytesAsc() {
    Table table = createTablePartitioned(4, 2);
    writeRecords(1, SCALE, 1);
    writeRecords(2, SCALE, 2);
    writeRecords(3, SCALE, 3);
    writeRecords(4, SCALE, 4);
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    RewriteDataFilesSparkAction basicRewrite = basicRewrite(table).binPack();
    List<Long> expected =
        toGroupStream(table, basicRewrite)
            .mapToLong(RewriteFileGroup::sizeInBytes)
            .boxed()
            .collect(Collectors.toList());

    RewriteDataFilesSparkAction jobOrderRewrite =
        basicRewrite(table)
            .option(RewriteDataFiles.REWRITE_JOB_ORDER, RewriteJobOrder.BYTES_ASC.orderName())
            .binPack();
    List<Long> actual =
        toGroupStream(table, jobOrderRewrite)
            .mapToLong(RewriteFileGroup::sizeInBytes)
            .boxed()
            .collect(Collectors.toList());

    expected.sort(Comparator.naturalOrder());
    Assert.assertEquals("Size in bytes order should be ascending", actual, expected);
    Collections.reverse(expected);
    Assert.assertNotEquals("Size in bytes order should not be descending", actual, expected);
  }

  @Test
  public void testRewriteJobOrderBytesDesc() {
    Table table = createTablePartitioned(4, 2);
    writeRecords(1, SCALE, 1);
    writeRecords(2, SCALE, 2);
    writeRecords(3, SCALE, 3);
    writeRecords(4, SCALE, 4);
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    RewriteDataFilesSparkAction basicRewrite = basicRewrite(table).binPack();
    List<Long> expected =
        toGroupStream(table, basicRewrite)
            .mapToLong(RewriteFileGroup::sizeInBytes)
            .boxed()
            .collect(Collectors.toList());

    RewriteDataFilesSparkAction jobOrderRewrite =
        basicRewrite(table)
            .option(RewriteDataFiles.REWRITE_JOB_ORDER, RewriteJobOrder.BYTES_DESC.orderName())
            .binPack();
    List<Long> actual =
        toGroupStream(table, jobOrderRewrite)
            .mapToLong(RewriteFileGroup::sizeInBytes)
            .boxed()
            .collect(Collectors.toList());

    expected.sort(Comparator.reverseOrder());
    Assert.assertEquals("Size in bytes order should be descending", actual, expected);
    Collections.reverse(expected);
    Assert.assertNotEquals("Size in bytes order should not be ascending", actual, expected);
  }

  @Test
  public void testRewriteJobOrderFilesAsc() {
    Table table = createTablePartitioned(4, 2);
    writeRecords(1, SCALE, 1);
    writeRecords(2, SCALE, 2);
    writeRecords(3, SCALE, 3);
    writeRecords(4, SCALE, 4);
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    RewriteDataFilesSparkAction basicRewrite = basicRewrite(table).binPack();
    List<Long> expected =
        toGroupStream(table, basicRewrite)
            .mapToLong(RewriteFileGroup::numFiles)
            .boxed()
            .collect(Collectors.toList());

    RewriteDataFilesSparkAction jobOrderRewrite =
        basicRewrite(table)
            .option(RewriteDataFiles.REWRITE_JOB_ORDER, RewriteJobOrder.FILES_ASC.orderName())
            .binPack();
    List<Long> actual =
        toGroupStream(table, jobOrderRewrite)
            .mapToLong(RewriteFileGroup::numFiles)
            .boxed()
            .collect(Collectors.toList());

    expected.sort(Comparator.naturalOrder());
    Assert.assertEquals("Number of files order should be ascending", actual, expected);
    Collections.reverse(expected);
    Assert.assertNotEquals("Number of files order should not be descending", actual, expected);
  }

  @Test
  public void testRewriteJobOrderFilesDesc() {
    Table table = createTablePartitioned(4, 2);
    writeRecords(1, SCALE, 1);
    writeRecords(2, SCALE, 2);
    writeRecords(3, SCALE, 3);
    writeRecords(4, SCALE, 4);
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    RewriteDataFilesSparkAction basicRewrite = basicRewrite(table).binPack();
    List<Long> expected =
        toGroupStream(table, basicRewrite)
            .mapToLong(RewriteFileGroup::numFiles)
            .boxed()
            .collect(Collectors.toList());

    RewriteDataFilesSparkAction jobOrderRewrite =
        basicRewrite(table)
            .option(RewriteDataFiles.REWRITE_JOB_ORDER, RewriteJobOrder.FILES_DESC.orderName())
            .binPack();
    List<Long> actual =
        toGroupStream(table, jobOrderRewrite)
            .mapToLong(RewriteFileGroup::numFiles)
            .boxed()
            .collect(Collectors.toList());

    expected.sort(Comparator.reverseOrder());
    Assert.assertEquals("Number of files order should be descending", actual, expected);
    Collections.reverse(expected);
    Assert.assertNotEquals("Number of files order should not be ascending", actual, expected);
  }

  @Test
  public void testBinPackRewriterWithSpecificUnparitionedOutputSpec() {
    Table table = createTable(10);
    shouldHaveFiles(table, 10);
    int outputSpecId = table.spec().specId();
    table.updateSpec().addField(Expressions.truncate("c2", 2)).commit();

    long dataSizeBefore = testDataSize(table);
    long count = currentData().size();

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(outputSpecId))
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .binPack()
            .execute();

    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(currentData().size()).isEqualTo(count);
    shouldRewriteDataFilesWithPartitionSpec(table, outputSpecId);
  }

  @Test
  public void testBinPackRewriterWithSpecificOutputSpec() {
    Table table = createTable(10);
    shouldHaveFiles(table, 10);
    table.updateSpec().addField(Expressions.truncate("c2", 2)).commit();
    int outputSpecId = table.spec().specId();
    table.updateSpec().addField(Expressions.bucket("c3", 2)).commit();

    long dataSizeBefore = testDataSize(table);
    long count = currentData().size();

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(outputSpecId))
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .binPack()
            .execute();

    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(currentData().size()).isEqualTo(count);
    shouldRewriteDataFilesWithPartitionSpec(table, outputSpecId);
  }

  @Test
  public void testBinpackRewriteWithInvalidOutputSpecId() {
    Table table = createTable(10);
    shouldHaveFiles(table, 10);
    assertThatThrownBy(
            () ->
                actions()
                    .rewriteDataFiles(table)
                    .option(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(1234))
                    .binPack()
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot use output spec id 1234 because the table does not contain a reference to this spec-id.");
  }

  @Test
  public void testSortRewriterWithSpecificOutputSpecId() {
    Table table = createTable(10);
    shouldHaveFiles(table, 10);
    table.updateSpec().addField(Expressions.truncate("c2", 2)).commit();
    int outputSpecId = table.spec().specId();
    table.updateSpec().addField(Expressions.bucket("c3", 2)).commit();

    long dataSizeBefore = testDataSize(table);
    long count = currentData().size();

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(outputSpecId))
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .sort(SortOrder.builderFor(table.schema()).asc("c2").asc("c3").build())
            .execute();

    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(currentData().size()).isEqualTo(count);
    shouldRewriteDataFilesWithPartitionSpec(table, outputSpecId);
  }

  @Test
  public void testZOrderRewriteWithSpecificOutputSpecId() {
    Table table = createTable(10);
    shouldHaveFiles(table, 10);
    table.updateSpec().addField(Expressions.truncate("c2", 2)).commit();
    int outputSpecId = table.spec().specId();
    table.updateSpec().addField(Expressions.bucket("c3", 2)).commit();

    long dataSizeBefore = testDataSize(table);
    long count = currentData().size();

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(outputSpecId))
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .zOrder("c2", "c3")
            .execute();

    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(currentData().size()).isEqualTo(count);
    shouldRewriteDataFilesWithPartitionSpec(table, outputSpecId);
  }

  protected void shouldRewriteDataFilesWithPartitionSpec(Table table, int outputSpecId) {
    List<DataFile> rewrittenFiles = currentDataFiles(table);
    assertThat(rewrittenFiles).allMatch(file -> file.specId() == outputSpecId);
    assertThat(rewrittenFiles)
        .allMatch(
            file ->
                ((PartitionData) file.partition())
                    .getPartitionType()
                    .equals(table.specs().get(outputSpecId).partitionType()));
  }

  protected List<DataFile> currentDataFiles(Table table) {
    return Streams.stream(table.newScan().planFiles())
        .map(FileScanTask::file)
        .collect(Collectors.toList());
  }

  private Stream<RewriteFileGroup> toGroupStream(Table table, RewriteDataFilesSparkAction rewrite) {
    rewrite.validateAndInitOptions();
    StructLikeMap<List<List<FileScanTask>>> fileGroupsByPartition =
        rewrite.planFileGroups(table.currentSnapshot().snapshotId());

    return rewrite.toGroupStream(
        new RewriteExecutionContext(fileGroupsByPartition), fileGroupsByPartition);
  }

  protected List<Object[]> currentData() {
    return rowsToJava(
        spark.read().format("iceberg").load(tableLocation).sort("c1", "c2", "c3").collectAsList());
  }

  protected long testDataSize(Table table) {
    return Streams.stream(table.newScan().planFiles()).mapToLong(FileScanTask::length).sum();
  }

  protected void shouldHaveMultipleFiles(Table table) {
    table.refresh();
    int numFiles = Iterables.size(table.newScan().planFiles());
    Assert.assertTrue(String.format("Should have multiple files, had %d", numFiles), numFiles > 1);
  }

  protected void shouldHaveFiles(Table table, int numExpected) {
    table.refresh();
    int numFiles = Iterables.size(table.newScan().planFiles());
    Assert.assertEquals("Did not have the expected number of files", numExpected, numFiles);
  }

  protected void shouldHaveSnapshots(Table table, int expectedSnapshots) {
    table.refresh();
    int actualSnapshots = Iterables.size(table.snapshots());
    Assert.assertEquals(
        "Table did not have the expected number of snapshots", expectedSnapshots, actualSnapshots);
  }

  protected void shouldHaveNoOrphans(Table table) {
    Assert.assertEquals(
        "Should not have found any orphan files",
        ImmutableList.of(),
        actions()
            .deleteOrphanFiles(table)
            .olderThan(System.currentTimeMillis())
            .execute()
            .orphanFileLocations());
  }

  protected void shouldHaveOrphans(Table table) {
    assertThat(
            actions()
                .deleteOrphanFiles(table)
                .olderThan(System.currentTimeMillis())
                .execute()
                .orphanFileLocations())
        .as("Should have found orphan files")
        .isNotEmpty();
  }

  protected void shouldHaveACleanCache(Table table) {
    Assert.assertEquals(
        "Should not have any entries in cache", ImmutableSet.of(), cacheContents(table));
  }

  protected <T> void shouldHaveLastCommitSorted(Table table, String column) {
    List<Pair<Pair<T, T>, Pair<T, T>>> overlappingFiles = checkForOverlappingFiles(table, column);

    Assert.assertEquals("Found overlapping files", Collections.emptyList(), overlappingFiles);
  }

  protected <T> void shouldHaveLastCommitUnsorted(Table table, String column) {
    List<Pair<Pair<T, T>, Pair<T, T>>> overlappingFiles = checkForOverlappingFiles(table, column);

    Assert.assertNotEquals("Found no overlapping files", Collections.emptyList(), overlappingFiles);
  }

  private <T> Pair<T, T> boundsOf(DataFile file, NestedField field, Class<T> javaClass) {
    int columnId = field.fieldId();
    return Pair.of(
        javaClass.cast(Conversions.fromByteBuffer(field.type(), file.lowerBounds().get(columnId))),
        javaClass.cast(Conversions.fromByteBuffer(field.type(), file.upperBounds().get(columnId))));
  }

  private <T> List<Pair<Pair<T, T>, Pair<T, T>>> checkForOverlappingFiles(
      Table table, String column) {
    table.refresh();
    NestedField field = table.schema().caseInsensitiveFindField(column);
    Class<T> javaClass = (Class<T>) field.type().typeId().javaClass();

    Snapshot snapshot = table.currentSnapshot();
    Map<StructLike, List<DataFile>> filesByPartition =
        Streams.stream(snapshot.addedDataFiles(table.io()))
            .collect(Collectors.groupingBy(DataFile::partition));

    Stream<Pair<Pair<T, T>, Pair<T, T>>> overlaps =
        filesByPartition.entrySet().stream()
            .flatMap(
                entry -> {
                  List<DataFile> datafiles = entry.getValue();
                  Preconditions.checkArgument(
                      datafiles.size() > 1,
                      "This test is checking for overlaps in a situation where no overlaps can actually occur because the "
                          + "partition %s does not contain multiple datafiles",
                      entry.getKey());

                  List<Pair<Pair<T, T>, Pair<T, T>>> boundComparisons =
                      Lists.cartesianProduct(datafiles, datafiles).stream()
                          .filter(tuple -> tuple.get(0) != tuple.get(1))
                          .map(
                              tuple ->
                                  Pair.of(
                                      boundsOf(tuple.get(0), field, javaClass),
                                      boundsOf(tuple.get(1), field, javaClass)))
                          .collect(Collectors.toList());

                  Comparator<T> comparator = Comparators.forType(field.type().asPrimitiveType());

                  List<Pair<Pair<T, T>, Pair<T, T>>> overlappingFiles =
                      boundComparisons.stream()
                          .filter(
                              filePair -> {
                                Pair<T, T> left = filePair.first();
                                T lMin = left.first();
                                T lMax = left.second();
                                Pair<T, T> right = filePair.second();
                                T rMin = right.first();
                                T rMax = right.second();
                                boolean boundsDoNotOverlap =
                                    // Min and Max of a range are greater than or equal to the max
                                    // value of the other range
                                    (comparator.compare(rMax, lMax) >= 0
                                            && comparator.compare(rMin, lMax) >= 0)
                                        || (comparator.compare(lMax, rMax) >= 0
                                            && comparator.compare(lMin, rMax) >= 0);

                                return !boundsDoNotOverlap;
                              })
                          .collect(Collectors.toList());
                  return overlappingFiles.stream();
                });

    return overlaps.collect(Collectors.toList());
  }

  protected Table createTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    table
        .updateProperties()
        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(20 * 1024))
        .commit();
    Assert.assertNull("Table must be empty", table.currentSnapshot());
    return table;
  }

  /**
   * Create a table with a certain number of files, returns the size of a file
   *
   * @param files number of files to create
   * @return the created table
   */
  protected Table createTable(int files) {
    Table table = createTable();
    writeRecords(files, SCALE);
    return table;
  }

  protected Table createTablePartitioned(
      int partitions, int files, int numRecords, Map<String, String> options) {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").truncate("c2", 2).build();
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);
    Assert.assertNull("Table must be empty", table.currentSnapshot());

    writeRecords(files, numRecords, partitions);
    return table;
  }

  protected Table createTablePartitioned(int partitions, int files) {
    return createTablePartitioned(partitions, files, SCALE, Maps.newHashMap());
  }

  private Table createTypeTestTable() {
    Schema schema =
        new Schema(
            required(1, "longCol", Types.LongType.get()),
            required(2, "intCol", Types.IntegerType.get()),
            required(3, "floatCol", Types.FloatType.get()),
            optional(4, "doubleCol", Types.DoubleType.get()),
            optional(5, "dateCol", Types.DateType.get()),
            optional(6, "timestampCol", Types.TimestampType.withZone()),
            optional(7, "stringCol", Types.StringType.get()),
            optional(8, "booleanCol", Types.BooleanType.get()),
            optional(9, "binaryCol", Types.BinaryType.get()));

    Map<String, String> options = Maps.newHashMap();
    Table table = TABLES.create(schema, PartitionSpec.unpartitioned(), options, tableLocation);

    spark
        .range(0, 10, 1, 10)
        .withColumnRenamed("id", "longCol")
        .withColumn("intCol", expr("CAST(longCol AS INT)"))
        .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
        .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
        .withColumn("dateCol", date_add(current_date(), 1))
        .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
        .withColumn("stringCol", expr("CAST(dateCol AS STRING)"))
        .withColumn("booleanCol", expr("longCol > 5"))
        .withColumn("binaryCol", expr("CAST(longCol AS BINARY)"))
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    return table;
  }

  protected int averageFileSize(Table table) {
    table.refresh();
    return (int)
        Streams.stream(table.newScan().planFiles())
            .mapToLong(FileScanTask::length)
            .average()
            .getAsDouble();
  }

  private void writeRecords(int files, int numRecords) {
    writeRecords(files, numRecords, 0);
  }

  private void writeRecords(int files, int numRecords, int partitions) {
    List<ThreeColumnRecord> records = Lists.newArrayList();
    int rowDimension = (int) Math.ceil(Math.sqrt(numRecords));
    List<Pair<Integer, Integer>> data =
        IntStream.range(0, rowDimension)
            .boxed()
            .flatMap(x -> IntStream.range(0, rowDimension).boxed().map(y -> Pair.of(x, y)))
            .collect(Collectors.toList());
    Collections.shuffle(data, new Random(42));
    if (partitions > 0) {
      data.forEach(
          i ->
              records.add(
                  new ThreeColumnRecord(
                      i.first() % partitions, "foo" + i.first(), "bar" + i.second())));
    } else {
      data.forEach(
          i ->
              records.add(new ThreeColumnRecord(i.first(), "foo" + i.first(), "bar" + i.second())));
    }
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).repartition(files);
    writeDF(df);
  }

  private void writeDF(Dataset<Row> df) {
    df.select("c1", "c2", "c3")
        .sortWithinPartitions("c1", "c2")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);
  }

  private List<DeleteFile> writePosDeletesToFile(
      Table table, DataFile dataFile, int outputDeleteFiles) {
    return writePosDeletes(
        table, dataFile.partition(), dataFile.path().toString(), outputDeleteFiles);
  }

  private List<DeleteFile> writePosDeletes(
      Table table, StructLike partition, String path, int outputDeleteFiles) {
    List<DeleteFile> results = Lists.newArrayList();
    int rowPosition = 0;
    for (int file = 0; file < outputDeleteFiles; file++) {
      OutputFile outputFile =
          table
              .io()
              .newOutputFile(
                  table.locationProvider().newDataLocation(UUID.randomUUID().toString()));
      EncryptedOutputFile encryptedOutputFile =
          EncryptedFiles.encryptedOutput(outputFile, EncryptionKeyMetadata.EMPTY);

      GenericAppenderFactory appenderFactory =
          new GenericAppenderFactory(table.schema(), table.spec(), null, null, null);
      PositionDeleteWriter<Record> posDeleteWriter =
          appenderFactory
              .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "full")
              .newPosDeleteWriter(encryptedOutputFile, FileFormat.PARQUET, partition);

      PositionDelete<Record> posDelete = PositionDelete.create();
      posDeleteWriter.write(posDelete.set(path, rowPosition, null));
      try {
        posDeleteWriter.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      results.add(posDeleteWriter.toDeleteFile());
      rowPosition++;
    }

    return results;
  }

  private SparkActions actions() {
    return SparkActions.get();
  }

  private Set<String> cacheContents(Table table) {
    return ImmutableSet.<String>builder()
        .addAll(manager.fetchSetIds(table))
        .addAll(coordinator.fetchSetIds(table))
        .build();
  }

  private double percentFilesRequired(Table table, String col, String value) {
    return percentFilesRequired(table, new String[] {col}, new String[] {value});
  }

  private double percentFilesRequired(Table table, String[] cols, String[] values) {
    Preconditions.checkArgument(cols.length == values.length);
    Expression restriction = Expressions.alwaysTrue();
    for (int i = 0; i < cols.length; i++) {
      restriction = Expressions.and(restriction, Expressions.equal(cols[i], values[i]));
    }
    int totalFiles = Iterables.size(table.newScan().planFiles());
    int filteredFiles = Iterables.size(table.newScan().filter(restriction).planFiles());
    return (double) filteredFiles / (double) totalFiles;
  }

  class GroupInfoMatcher implements ArgumentMatcher<RewriteFileGroup> {
    private final Set<Integer> groupIDs;

    GroupInfoMatcher(Integer... globalIndex) {
      this.groupIDs = ImmutableSet.copyOf(globalIndex);
    }

    @Override
    public boolean matches(RewriteFileGroup argument) {
      return groupIDs.contains(argument.info().globalIndex());
    }
  }
}
