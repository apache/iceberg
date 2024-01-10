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
import java.nio.file.Path;
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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
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
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
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
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

public class TestRewriteDataFilesAction extends TestBase {

  private static final int SCALE = 400000;

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @TempDir private Path temp;

  private final FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();
  private final ScanTaskSetManager manager = ScanTaskSetManager.get();
  private String tableLocation = null;

  @BeforeAll
  public static void setupSpark() {
    // disable AQE as tests assume that writes generate a particular number of files
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  public void setupTableLocation() throws Exception {
    File tableDir = temp.resolve("junit").toFile();
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

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();

    basicRewrite(table).execute();

    assertThat(table.currentSnapshot()).as("Table must stay empty").isNull();
  }

  @Test
  public void testBinPackUnpartitionedTable() {
    Table table = createTable(4);
    shouldHaveFiles(table, 4);
    List<Object[]> expectedRecords = currentData();
    long dataSizeBefore = testDataSize(table);

    Result result = basicRewrite(table).execute();
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 4 data files")
        .isEqualTo(4);
    assertThat(result.addedDataFilesCount()).as("Action should add 1 data file").isOne();
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
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 8 data files")
        .isEqualTo(8);
    assertThat(result.addedDataFilesCount()).as("Action should add 4 data file").isEqualTo(4);
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

    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 2 data files")
        .isEqualTo(2);
    assertThat(result.addedDataFilesCount()).as("Action should add 1 data file").isOne();
    assertThat(result.rewrittenBytesCount()).isGreaterThan(0L).isLessThan(dataSizeBefore);

    shouldHaveFiles(table, 7);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testBinPackWithFilterOnBucketExpression() {
    Table table = createTablePartitioned(4, 2);

    shouldHaveFiles(table, 8);
    List<Object[]> expectedRecords = currentData();
    long dataSizeBefore = testDataSize(table);

    Result result =
        basicRewrite(table)
            .filter(Expressions.equal("c1", 1))
            .filter(Expressions.equal(Expressions.bucket("c2", 2), 0))
            .execute();

    assertThat(result)
        .extracting(Result::rewrittenDataFilesCount, Result::addedDataFilesCount)
        .as("Action should rewrite 2 data files into 1 data file")
        .contains(2, 1);
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

    assertThat(result.rewriteResults())
        .as("Should have 1 fileGroup because all files were not correctly partitioned")
        .hasSize(1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
    shouldHaveFiles(table, 20);
  }

  @Test
  public void testBinPackWithDeletes() {
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
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 2 data files")
        .isEqualTo(2);
    assertThat(result.rewrittenBytesCount()).isGreaterThan(0L).isLessThan(dataSizeBefore);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertThat(actualRecords).as("7 rows are removed").hasSize(total - 7);
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
    assertThat(result.rewrittenDataFilesCount()).as("Action should rewrite 1 data files").isOne();
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertThat(table.currentSnapshot().dataManifests(table.io()).get(0).existingFilesCount())
        .as("Data manifest should not have existing data file")
        .isZero();

    assertThat((long) table.currentSnapshot().dataManifests(table.io()).get(0).deletedFilesCount())
        .as("Data manifest should have 1 delete data file")
        .isEqualTo(1L);

    assertThat(table.currentSnapshot().deleteManifests(table.io()).get(0).addedRowsCount())
        .as("Delete manifest added row count should equal total count")
        .isEqualTo(total);
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
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 8 data files")
        .isEqualTo(8);
    assertThat(result.addedDataFilesCount()).as("Action should add 4 data files").isEqualTo(4);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 4);
    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);

    table.refresh();
    assertThat(table.currentSnapshot().sequenceNumber())
        .as("Table sequence number should be incremented")
        .isGreaterThan(oldSequenceNumber);

    Dataset<Row> rows = SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.ENTRIES);
    for (Row row : rows.collectAsList()) {
      if (row.getInt(0) == 1) {
        assertThat(row.getLong(2))
            .as("Expect old sequence number for added entries")
            .isEqualTo(oldSequenceNumber);
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
    assertThat(oldSequenceNumber).as("Table sequence number should be 0").isZero();
    long dataSizeBefore = testDataSize(table);

    Result result =
        basicRewrite(table).option(RewriteDataFiles.USE_STARTING_SEQUENCE_NUMBER, "true").execute();
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 8 data files")
        .isEqualTo(8);
    assertThat(result.addedDataFilesCount()).as("Action should add 4 data files").isEqualTo(4);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 4);
    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);

    table.refresh();
    assertThat(table.currentSnapshot().sequenceNumber())
        .as("Table sequence number should still be 0")
        .isEqualTo(oldSequenceNumber);

    Dataset<Row> rows = SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.ENTRIES);
    for (Row row : rows.collectAsList()) {
      assertThat(row.getLong(2))
          .as("Expect sequence number 0 for all entries")
          .isEqualTo(oldSequenceNumber);
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
      assertThat(task.residual())
          .as("Residuals must be ignored")
          .isEqualTo(Expressions.alwaysTrue());
    }

    shouldHaveFiles(table, 2);

    long dataSizeBefore = testDataSize(table);
    Result result = basicRewrite(table).filter(Expressions.equal("c3", "0")).execute();
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 2 data files")
        .isEqualTo(2);
    assertThat(result.addedDataFilesCount()).as("Action should add 1 data file").isOne();
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

    assertThat(result.rewrittenDataFilesCount()).as("Action should delete 1 data files").isOne();
    assertThat(result.addedDataFilesCount()).as("Action should add 2 data files").isEqualTo(2);
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

    assertThat(result.rewrittenDataFilesCount())
        .as("Action should delete 3 data files")
        .isEqualTo(3);
    // Should Split the big files into 3 pieces, one of which should be combined with the two
    // smaller files
    assertThat(result.addedDataFilesCount()).as("Action should add 3 data files").isEqualTo(3);
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

    assertThat(result.rewrittenDataFilesCount())
        .as("Action should delete 4 data files")
        .isEqualTo(4);
    assertThat(result.addedDataFilesCount()).as("Action should add 3 data files").isEqualTo(3);
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

    assertThat(result.rewriteResults()).as("Should have 10 fileGroups").hasSize(10);
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

    assertThat(result.rewriteResults()).as("Should have 10 fileGroups").hasSize(10);
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

    assertThat(result.rewriteResults()).as("Should have 10 fileGroups").hasSize(10);
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

    assertThatThrownBy(spyRewrite::execute)
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
    doThrow(new RuntimeException("Commit Failure")).when(util).commitFileGroups(any());

    doReturn(util).when(spyRewrite).commitManager(table.currentSnapshot().snapshotId());

    assertThatThrownBy(spyRewrite::execute)
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Commit Failure");

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
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
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    assertThatThrownBy(spyRewrite::execute)
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

    assertThat(result.rewriteResults()).hasSize(7);
    assertThat(result.rewriteFailures()).hasSize(3);
    assertThat(result.failedDataFilesCount()).isEqualTo(6);
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

    assertThat(result.rewriteResults()).hasSize(7);
    assertThat(result.rewriteFailures()).hasSize(3);
    assertThat(result.failedDataFilesCount()).isEqualTo(6);
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
        .doThrow(new RuntimeException("Commit Failed"))
        .doCallRealMethod()
        .when(util)
        .commitFileGroups(any());

    doReturn(util).when(spyRewrite).commitManager(table.currentSnapshot().snapshotId());

    RewriteDataFiles.Result result = spyRewrite.execute();

    // Commit 1: 4/4 + Commit 2 failed 0/4 + Commit 3: 2/2 == 6 out of 10 total groups comitted
    assertThat(result.rewriteResults()).as("Should have 6 fileGroups").hasSize(6);
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
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot set partial-progress.max-commits to -5, "
                + "the value must be positive when partial-progress.enabled is true");

    assertThatThrownBy(
            () ->
                basicRewrite(table)
                    .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "-5")
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot set max-concurrent-file-group-rewrites to -5, the value must be positive.");

    assertThatThrownBy(() -> basicRewrite(table).option("foobarity", "-5").execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot use options [foobarity], they are not supported by the action or the rewriter BIN-PACK");

    assertThatThrownBy(
            () -> basicRewrite(table).option(RewriteDataFiles.REWRITE_JOB_ORDER, "foo").execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid rewrite job order name: foo");

    assertThatThrownBy(
            () ->
                basicRewrite(table)
                    .sort(SortOrder.builderFor(table.schema()).asc("c2").build())
                    .option(SparkShufflingDataRewriter.SHUFFLE_PARTITIONS_PER_FILE, "5")
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires enabling Iceberg Spark session extensions");
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

    assertThat(result.rewriteResults()).as("Should have 10 fileGroups").hasSize(10);
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

    assertThat(result.rewriteResults()).as("Should have 1 fileGroups").hasSize(1);
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

    assertThat(result.rewriteResults())
        .as("Should have 1 fileGroups because all files were not correctly partitioned")
        .hasSize(1);
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

    assertThat(result.rewriteResults()).as("Should have 1 fileGroups").hasSize(1);
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

    assertThat(result.rewriteResults()).as("Should have 1 fileGroups").hasSize(1);
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

    assertThat(result.rewriteResults()).as("Should have 1 fileGroups").hasSize(1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(result.rewriteResults()).as("Should have 1 fileGroups").hasSize(1);
    assertThat(table.currentSnapshot().addedDataFiles(table.io()))
        .as("Should have written 40+ files")
        .hasSizeGreaterThanOrEqualTo(40);

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

    assertThatThrownBy(spyAction::execute)
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

    assertThat(originalFilesC2).as("Should require all files to scan c2").isGreaterThan(0.99);
    assertThat(originalFilesC3).as("Should require all files to scan c3").isGreaterThan(0.99);

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

    assertThat(result.rewriteResults()).as("Should have 1 fileGroups").hasSize(1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(table.currentSnapshot().addedDataFiles(table.io()))
        .as("Should have written 40+ files")
        .hasSizeGreaterThanOrEqualTo(40);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);

    double filesScannedC2 = percentFilesRequired(table, "c2", "foo23");
    double filesScannedC3 = percentFilesRequired(table, "c3", "bar21");
    double filesScannedC2C3 =
        percentFilesRequired(table, new String[] {"c2", "c3"}, new String[] {"foo23", "bar23"});

    assertThat(originalFilesC2)
        .as("Should have reduced the number of files required for c2")
        .isGreaterThan(filesScannedC2);
    assertThat(originalFilesC3)
        .as("Should have reduced the number of files required for c3")
        .isGreaterThan(filesScannedC3);
    assertThat(originalFilesC2C3)
        .as("Should have reduced the number of files required for c2,c3 predicate")
        .isGreaterThan(filesScannedC2C3);
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

    assertThat(result.rewriteResults()).as("Should have 1 fileGroups").hasSize(1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(table.currentSnapshot().addedDataFiles(table.io()))
        .as("Should have written 1 file")
        .hasSize(1);

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
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must use only one rewriter type (bin-pack, sort, zorder)");

    assertThatThrownBy(() -> actions().rewriteDataFiles(table).sort(sortOrder).binPack())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must use only one rewriter type (bin-pack, sort, zorder)");

    assertThatThrownBy(() -> actions().rewriteDataFiles(table).sort(sortOrder).binPack())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must use only one rewriter type (bin-pack, sort, zorder)");
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
    assertThat(actual).as("Size in bytes order should be ascending").isEqualTo(expected);
    Collections.reverse(expected);
    assertThat(actual).as("Size in bytes order should not be descending").isNotEqualTo(expected);
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
    assertThat(actual).as("Size in bytes order should be descending").isEqualTo(expected);
    Collections.reverse(expected);
    assertThat(actual).as("Size in bytes order should not be ascending").isNotEqualTo(expected);
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
    assertThat(actual).as("Number of files order should be ascending").isEqualTo(expected);
    Collections.reverse(expected);
    assertThat(actual).as("Number of files order should not be descending").isNotEqualTo(expected);
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
    assertThat(actual).as("Number of files order should be descending").isEqualTo(expected);
    Collections.reverse(expected);
    assertThat(actual).as("Number of files order should not be ascending").isNotEqualTo(expected);
  }

  @Test
  public void testSnapshotProperty() {
    Table table = createTable(4);
    Result ignored = basicRewrite(table).snapshotProperty("key", "value").execute();
    assertThat(table.currentSnapshot().summary())
        .containsAllEntriesOf(ImmutableMap.of("key", "value"));
    // make sure internal produced properties is not lost
    String[] commitMetricsKeys =
        new String[] {
          SnapshotSummary.ADDED_FILES_PROP,
          SnapshotSummary.DELETED_FILES_PROP,
          SnapshotSummary.TOTAL_DATA_FILES_PROP,
          SnapshotSummary.CHANGED_PARTITION_COUNT_PROP
        };
    assertThat(table.currentSnapshot().summary()).containsKeys(commitMetricsKeys);
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
    assertThat(numFiles)
        .as(String.format("Should have multiple files, had %d", numFiles))
        .isGreaterThan(1);
  }

  protected void shouldHaveFiles(Table table, int numExpected) {
    table.refresh();
    int numFiles = Iterables.size(table.newScan().planFiles());
    assertThat(numFiles).as("Did not have the expected number of files").isEqualTo(numExpected);
  }

  protected void shouldHaveSnapshots(Table table, int expectedSnapshots) {
    table.refresh();
    int actualSnapshots = Iterables.size(table.snapshots());
    assertThat(actualSnapshots)
        .as("Table did not have the expected number of snapshots")
        .isEqualTo(expectedSnapshots);
  }

  protected void shouldHaveNoOrphans(Table table) {
    assertThat(
            actions()
                .deleteOrphanFiles(table)
                .olderThan(System.currentTimeMillis())
                .execute()
                .orphanFileLocations())
        .as("Should not have found any orphan files")
        .isEmpty();
  }

  protected void shouldHaveACleanCache(Table table) {
    assertThat(cacheContents(table)).as("Should not have any entries in cache").isEmpty();
  }

  protected <T> void shouldHaveLastCommitSorted(Table table, String column) {
    List<Pair<Pair<T, T>, Pair<T, T>>> overlappingFiles = checkForOverlappingFiles(table, column);

    assertThat(overlappingFiles).as("Found overlapping files").isEmpty();
  }

  protected <T> void shouldHaveLastCommitUnsorted(Table table, String column) {
    List<Pair<Pair<T, T>, Pair<T, T>>> overlappingFiles = checkForOverlappingFiles(table, column);

    assertThat(overlappingFiles).as("Found no overlapping files").isNotEmpty();
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
    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();
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
    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();

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
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
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
