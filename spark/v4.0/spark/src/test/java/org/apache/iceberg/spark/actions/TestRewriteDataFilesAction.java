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
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.min;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
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
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles.Result;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
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
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteDataFilesAction extends TestBase {

  @TempDir private File tableDir;
  private static final int SCALE = 400000;

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  @Parameter private int formatVersion;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> parameters() {
    return org.apache.iceberg.TestHelpers.V2_AND_ABOVE;
  }

  private final FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();
  private final ScanTaskSetManager manager = ScanTaskSetManager.get();
  private String tableLocation = null;

  @BeforeAll
  public static void setupSpark() {
    // disable AQE as tests assume that writes generate a particular number of files
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  public void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  private RewriteDataFilesSparkAction basicRewrite(Table table) {
    // Always compact regardless of input files
    table.refresh();
    return actions()
        .rewriteDataFiles(table)
        .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1");
  }

  @TestTemplate
  public void testEmptyTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options =
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();

    basicRewrite(table).execute();

    assertThat(table.currentSnapshot()).as("Table must stay empty").isNull();
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  public void testBinPackAfterPartitionChange() {
    Table table = createTable();

    writeRecords(20, SCALE, 20);
    shouldHaveFiles(table, 20);
    table.updateSpec().addField(Expressions.ref("c1")).commit();

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(
                SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES,
                Integer.toString(averageFileSize(table) + 1000))
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
                // Increase max file size for V3 to account for additional row lineage fields
                Integer.toString(averageFileSize(table) + (formatVersion >= 3 ? 12000 : 1100)))
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

  @TestTemplate
  public void testDataFilesRewrittenWithMaxDeleteRatio() throws Exception {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);
    Table table = createTable();
    int numDataFiles = 5;
    // 100 / 5 = 20 records per data file
    writeRecords(numDataFiles, 100);
    // delete > 100% of records for each data file
    int numPositionsToDelete = 1000;
    table.refresh();
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    assertThat(dataFiles).hasSize(numDataFiles);

    RowDelta rowDelta = table.newRowDelta();
    for (DataFile dataFile : dataFiles) {
      if (formatVersion >= 3) {
        writeDV(table, dataFile.partition(), dataFile.location(), numPositionsToDelete)
            .forEach(rowDelta::addDeletes);
      } else {
        writePosDeletes(table, dataFile.partition(), dataFile.location(), 4, numPositionsToDelete)
            .forEach(rowDelta::addDeletes);
      }
    }

    rowDelta.commit();

    Set<DeleteFile> deleteFiles = TestHelpers.deleteFiles(table);
    int expectedDataFiles = formatVersion >= 3 ? numDataFiles : numDataFiles * 4;
    assertThat(deleteFiles).hasSize(expectedDataFiles);

    // there are 5 data files with a delete ratio of > 100% each, so all data files should be
    // rewritten. Set MIN_INPUT_FILES > to the number of data files so that compaction is only
    // triggered when the delete ratio of >= 30% is hit
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "10")
            .option(SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES, "0")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(numDataFiles);

    table.refresh();
    List<DataFile> newDataFiles = TestHelpers.dataFiles(table);
    assertThat(newDataFiles).isEmpty();

    Set<DeleteFile> newDeleteFiles = TestHelpers.deleteFiles(table);
    assertThat(newDeleteFiles).isEmpty();
  }

  @TestTemplate
  public void testDataFilesRewrittenWithHighDeleteRatio() throws Exception {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);
    Table table = createTable();
    int numDataFiles = 5;
    // 100 / 5 = 20 records per data file
    writeRecords(numDataFiles, 100);
    // delete 40% of records for each data file
    int numPositionsToDelete = 8;
    table.refresh();
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    assertThat(dataFiles).hasSize(numDataFiles);

    RowDelta rowDelta = table.newRowDelta();
    for (DataFile dataFile : dataFiles) {
      if (formatVersion >= 3) {
        writeDV(table, dataFile.partition(), dataFile.location(), numPositionsToDelete)
            .forEach(rowDelta::addDeletes);
      } else {
        writePosDeletes(table, dataFile.partition(), dataFile.location(), 4, numPositionsToDelete)
            .forEach(rowDelta::addDeletes);
      }
    }

    rowDelta.commit();

    Set<DeleteFile> deleteFiles = TestHelpers.deleteFiles(table);
    int expectedDataFiles = formatVersion >= 3 ? numDataFiles : numDataFiles * 4;
    assertThat(deleteFiles).hasSize(expectedDataFiles);

    // there are 5 data files with a delete ratio of 40% each, so all data files should be
    // rewritten. Set MIN_INPUT_FILES > to the number of data files so that compaction is only
    // triggered when the delete ratio of >= 30% is hit
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "10")
            .option(SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES, "0")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(numDataFiles);

    table.refresh();
    List<DataFile> newDataFiles = TestHelpers.dataFiles(table);
    assertThat(newDataFiles).hasSize(1);

    Set<DeleteFile> newDeleteFiles = TestHelpers.deleteFiles(table);
    assertThat(newDeleteFiles).isEmpty();
  }

  @TestTemplate
  public void testDataFilesNotRewrittenWithLowDeleteRatio() throws Exception {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);
    Table table = createTable();
    int numDataFiles = 5;
    // 100 / 5 = 20 records per data file
    writeRecords(numDataFiles, 100);
    // delete 25% of records for each data file
    int numPositionsToDelete = 5;
    table.refresh();
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    assertThat(dataFiles).hasSize(numDataFiles);

    RowDelta rowDelta = table.newRowDelta();
    for (DataFile dataFile : dataFiles) {
      if (formatVersion >= 3) {
        writeDV(table, dataFile.partition(), dataFile.location(), numPositionsToDelete)
            .forEach(rowDelta::addDeletes);
      } else {
        writePosDeletes(table, dataFile.partition(), dataFile.location(), 5, numPositionsToDelete)
            .forEach(rowDelta::addDeletes);
      }
    }

    rowDelta.commit();

    Set<DeleteFile> deleteFiles = TestHelpers.deleteFiles(table);
    int expectedDataFiles = formatVersion >= 3 ? numDataFiles : numDataFiles * 5;
    assertThat(deleteFiles).hasSize(expectedDataFiles);

    // there are 5 data files with a delete ratio of 25% each, so data files should not be
    // rewritten. Set MIN_INPUT_FILES > to the number of data files so that compaction is only
    // triggered when the delete ratio of >= 30% is hit
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "10")
            .option(SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES, "0")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(0);

    table.refresh();
    List<DataFile> newDataFiles = TestHelpers.dataFiles(table);
    assertThat(newDataFiles).hasSameSizeAs(dataFiles);

    Set<DeleteFile> newDeleteFiles = TestHelpers.deleteFiles(table);
    assertThat(newDeleteFiles).hasSameSizeAs(deleteFiles);
  }

  @TestTemplate
  public void testBinPackWithV2PositionDeletes() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);
    Table table = createTablePartitioned(4, 2);
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
            .option(BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "0")
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .option(BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE))
            .option(BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD, "2")
            .execute();
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 2 data files")
        .isEqualTo(2);
    assertThat(result.rewrittenBytesCount()).isGreaterThan(0L).isLessThan(dataSizeBefore);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
    assertThat(actualRecords).as("7 rows are removed").hasSize(total - 7);
  }

  @TestTemplate
  public void testBinPackWithDVs() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    Table table = createTablePartitioned(4, 2);
    shouldHaveFiles(table, 8);
    table.refresh();
    List<Object[]> initialRecords = currentDataWithLineage();
    Set<Long> rowIds =
        initialRecords.stream().map(record -> (Long) record[0]).collect(Collectors.toSet());
    Set<Long> lastUpdatedSequenceNumbers =
        initialRecords.stream().map(record -> (Long) record[1]).collect(Collectors.toSet());
    assertThat(rowIds)
        .isEqualTo(LongStream.range(0, initialRecords.size()).boxed().collect(Collectors.toSet()));
    assertThat(lastUpdatedSequenceNumbers).allMatch(sequenceNumber -> sequenceNumber.equals(1L));
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    int total = (int) dataFiles.stream().mapToLong(ContentFile::recordCount).sum();

    RowDelta rowDelta = table.newRowDelta();
    Set<Long> rowIdsBeingRemoved = Sets.newHashSet();

    // add 1 DV for data files 0, 1, 2
    for (int i = 0; i < 3; i++) {
      writeDV(table, dataFiles.get(i).partition(), dataFiles.get(i).location(), 1)
          .forEach(rowDelta::addDeletes);
      rowIdsBeingRemoved.add(dataFiles.get(i).firstRowId());
    }

    // delete 2 positions for data files 3, 4
    for (int i = 3; i < 5; i++) {
      writeDV(table, dataFiles.get(i).partition(), dataFiles.get(i).location(), 2)
          .forEach(rowDelta::addDeletes);
      long dataFileFirstRowId = dataFiles.get(i).firstRowId();
      rowIdsBeingRemoved.add(dataFileFirstRowId);
      rowIdsBeingRemoved.add(dataFileFirstRowId + 1);
    }

    rowDelta.commit();
    table.refresh();
    List<Object[]> recordsWithLineageAfterDelete = currentDataWithLineage();
    rowIds.removeAll(rowIdsBeingRemoved);
    assertThat(rowIds)
        .isEqualTo(
            recordsWithLineageAfterDelete.stream()
                .map(record -> (Long) record[0])
                .collect(Collectors.toSet()));

    long dataSizeBefore = testDataSize(table);

    Result result =
        actions()
            .rewriteDataFiles(table)
            // do not include any file based on bin pack file size configs
            .option(BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "0")
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE - 1))
            .option(BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES, Long.toString(Long.MAX_VALUE))
            // set DELETE_FILE_THRESHOLD to 1 since DVs only produce one delete file per data file
            .option(BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD, "1")
            .execute();
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 5 data files")
        .isEqualTo(5);
    assertThat(result.rewrittenBytesCount()).isGreaterThan(0L).isLessThan(dataSizeBefore);
    List<Object[]> actualRecordsWithLineage = currentDataWithLineage();
    assertEquals("Rows must match", recordsWithLineageAfterDelete, actualRecordsWithLineage);
    assertThat(actualRecordsWithLineage).as("7 rows are removed").hasSize(total - 7);
  }

  @TestTemplate
  public void removeDanglingDVsFromDeleteManifest() throws Exception {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    Table table = createTable();
    int numDataFiles = 5;
    // 100 / 5 = 20 records per data file
    writeRecords(numDataFiles, 100);
    int numPositionsToDelete = 10;
    table.refresh();
    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    assertThat(dataFiles).hasSize(numDataFiles);

    RowDelta rowDelta = table.newRowDelta();
    for (DataFile dataFile : dataFiles) {
      writeDV(table, dataFile.partition(), dataFile.location(), numPositionsToDelete)
          .forEach(rowDelta::addDeletes);
    }

    rowDelta.commit();

    Set<DeleteFile> deleteFiles = TestHelpers.deleteFiles(table);
    assertThat(deleteFiles).hasSize(numDataFiles);

    Set<String> validDataFilePaths =
        TestHelpers.dataFiles(table).stream()
            .map(ContentFile::location)
            .collect(Collectors.toSet());
    for (ManifestFile manifestFile : table.currentSnapshot().deleteManifests(table.io())) {
      ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(
              manifestFile, table.io(), ((BaseTable) table).operations().current().specsById());
      for (DeleteFile deleteFile : reader) {
        // make sure there are no orphaned DVs
        assertThat(validDataFilePaths).contains(deleteFile.referencedDataFile());
      }
    }

    // all data files should be rewritten. Set MIN_INPUT_FILES > to the number of data files so that
    // compaction is only triggered when the delete ratio of >= 30% is hit
    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "10")
            .option(SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES, "0")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).isEqualTo(numDataFiles);
    assertThat(result.removedDeleteFilesCount()).isEqualTo(numDataFiles);

    table.refresh();
    assertThat(TestHelpers.dataFiles(table)).hasSize(1);
    assertThat(TestHelpers.deleteFiles(table)).isEmpty();

    validDataFilePaths =
        TestHelpers.dataFiles(table).stream()
            .map(ContentFile::location)
            .collect(Collectors.toSet());
    for (ManifestFile manifestFile : table.currentSnapshot().deleteManifests(table.io())) {
      ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(
              manifestFile, table.io(), ((BaseTable) table).operations().current().specsById());
      for (DeleteFile deleteFile : reader) {
        // make sure there are no orphaned DVs
        assertThat(validDataFilePaths).contains(deleteFile.referencedDataFile());
      }
    }
  }

  @TestTemplate
  public void testRemoveDangledEqualityDeletesPartitionEvolution() {
    Table table =
        TABLES.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tableLocation);

    // data seq = 1, write 4 files in 2 partitions
    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "AAAA"), new ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB"));
    writeRecords(records1);
    List<ThreeColumnRecord> records2 =
        Lists.newArrayList(
            new ThreeColumnRecord(0, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(0, "DDDDDDDDDD", "DDDD"));
    writeRecords(records2);
    table.refresh();
    shouldHaveFiles(table, 4);

    // data seq = 2 & 3, write 2 equality deletes in both partitions
    writeEqDeleteRecord(table, "c1", 1, "c3", "AAAA");
    writeEqDeleteRecord(table, "c1", 2, "c3", "CCCC");
    table.refresh();
    Set<DeleteFile> existingDeletes = TestHelpers.deleteFiles(table);
    assertThat(existingDeletes)
        .as("Only one equality delete c1=1 is used in query planning")
        .hasSize(1);

    // partition evolution
    table.refresh();
    table.updateSpec().addField(Expressions.ref("c3")).commit();

    // data seq = 4, write 2 new data files in both partitions for evolved spec
    List<ThreeColumnRecord> records3 =
        Lists.newArrayList(
            new ThreeColumnRecord(1, "A", "CCCC"), new ThreeColumnRecord(2, "D", "DDDD"));
    writeRecords(records3);

    List<Object[]> originalData = currentData();

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .filter(Expressions.equal("c1", 1))
            .option(RewriteDataFiles.REMOVE_DANGLING_DELETES, "true")
            .execute();

    existingDeletes = TestHelpers.deleteFiles(table);
    assertThat(existingDeletes).as("Shall pruned dangling deletes after rewrite").hasSize(0);

    assertThat(result)
        .extracting(
            Result::addedDataFilesCount,
            Result::rewrittenDataFilesCount,
            Result::removedDeleteFilesCount)
        .as("Should compact 3 data files into 2 and remove both dangled equality delete file")
        .containsExactly(2, 3, 2);
    shouldHaveMinSequenceNumberInPartition(table, "data_file.partition.c1 == 1", 5);

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 7);
    shouldHaveFiles(table, 5);
  }

  @TestTemplate
  public void testRemoveDangledPositionDeletesPartitionEvolution() throws IOException {
    Table table =
        TABLES.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tableLocation);

    // data seq = 1, write 4 files in 2 partitions
    writeRecords(2, 2, 2);
    List<DataFile> dataFilesBefore = TestHelpers.dataFiles(table, null);
    shouldHaveFiles(table, 4);

    DeleteFile deleteFile;
    // data seq = 2, write 1 position deletes in c1=1
    DataFile dataFile = dataFilesBefore.get(3);
    if (formatVersion >= 3) {
      deleteFile = writeDV(table, dataFile.partition(), dataFile.location(), 1).get(0);
    } else {
      deleteFile = writePosDeletesToFile(table, dataFile, 1).get(0);
    }
    table.newRowDelta().addDeletes(deleteFile).commit();

    // partition evolution
    table.updateSpec().addField(Expressions.ref("c3")).commit();

    // data seq = 3, write 1 new data files in c1=1 for evolved spec
    writeRecords(1, 1, 1);
    shouldHaveFiles(table, 5);
    List<Object[]> expectedRecords = currentData();

    Result result =
        actions()
            .rewriteDataFiles(table)
            .filter(Expressions.equal("c1", 1))
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .option(RewriteDataFiles.REMOVE_DANGLING_DELETES, "true")
            .execute();

    assertThat(result)
        .extracting(
            Result::addedDataFilesCount,
            Result::rewrittenDataFilesCount,
            Result::removedDeleteFilesCount)
        .as("Should rewrite 2 data files into 1 and remove 1 dangled position delete file")
        .containsExactly(1, 2, 1);
    shouldHaveMinSequenceNumberInPartition(table, "data_file.partition.c1 == 1", 3);

    // v3 removes orphaned DVs during rewrite already, so there should be one snapshot less
    int expectedSnapshots = formatVersion >= 3 ? 4 : 5;
    shouldHaveSnapshots(table, expectedSnapshots);
    assertThat(table.currentSnapshot().summary()).containsEntry("total-position-deletes", "0");
    assertEquals("Rows must match", expectedRecords, currentData());
  }

  @TestTemplate
  public void testBinPackWithDeleteAllData() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);
    Table table = createTablePartitioned(1, 1, 1);
    shouldHaveFiles(table, 1);
    table.refresh();

    List<DataFile> dataFiles = TestHelpers.dataFiles(table);
    int total = (int) dataFiles.stream().mapToLong(ContentFile::recordCount).sum();

    RowDelta rowDelta = table.newRowDelta();
    DataFile dataFile = dataFiles.get(0);
    // remove all data
    if (formatVersion >= 3) {
      writeDV(table, dataFile.partition(), dataFile.location(), total)
          .forEach(rowDelta::addDeletes);
    } else {
      writePosDeletesToFile(table, dataFile, total).forEach(rowDelta::addDeletes);
    }

    rowDelta.commit();
    table.refresh();
    List<Object[]> expectedRecords = currentData();
    long dataSizeBefore = testDataSize(table);

    Result result =
        actions()
            .rewriteDataFiles(table)
            .option(BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD, "1")
            .execute();
    assertThat(result.rewrittenDataFilesCount()).as("Action should rewrite 1 data files").isOne();
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    if (formatVersion >= 3) {
      assertThat(result.removedDeleteFilesCount()).isEqualTo(dataFiles.size());
    }

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
        // v3 removes orphaned DVs during rewrite
        .isEqualTo(formatVersion >= 3 ? 0 : 1);
  }

  @TestTemplate
  public void testBinPackWithStartingSequenceNumber() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);
    Table table = createTablePartitioned(4, 2);
    shouldHaveFiles(table, 8);
    List<Object[]> expectedRecords = currentData();
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

  @TestTemplate
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

  @TestTemplate
  public void testRewriteLargeTableHasResiduals() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
    Map<String, String> options =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            String.valueOf(formatVersion),
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
            "100");
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

  @TestTemplate
  public void testBinPackSplitLargeFile() {
    Table table = createTable(1);
    shouldHaveFiles(table, 1);

    List<Object[]> expectedRecords = currentData();
    long targetSize = testDataSize(table) / 2;

    long dataSizeBefore = testDataSize(table);
    Result result =
        basicRewrite(table)
            .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, Long.toString(targetSize))
            .option(
                SizeBasedFileRewritePlanner.MAX_FILE_SIZE_BYTES,
                Long.toString(targetSize * 2 - 2000))
            .execute();

    assertThat(result.rewrittenDataFilesCount()).as("Action should delete 1 data files").isOne();
    assertThat(result.addedDataFilesCount()).as("Action should add 2 data files").isEqualTo(2);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    shouldHaveFiles(table, 2);

    List<Object[]> actualRecords = currentData();
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @TestTemplate
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
            .option(
                SizeBasedFileRewritePlanner.MAX_FILE_SIZE_BYTES,
                // Increase max file size for V3 to account for additional row lineage fields
                Integer.toString(targetSize + (formatVersion >= 3 ? 1850000 : 80000)))
            .option(
                SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES,
                Integer.toString(targetSize - 1000))
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

  @TestTemplate
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
                SizeBasedFileRewritePlanner.MAX_FILE_SIZE_BYTES,
                // Increase max file size for V3 to account for additional row lineage fields
                Integer.toString((int) (targetSize * (formatVersion >= 3 ? 2 : 1.8))))
            .option(
                SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES,
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

  @TestTemplate
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

  @TestTemplate
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
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewriteResults()).as("Should have 10 fileGroups").hasSize(10);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

    assertThatThrownBy(spyRewrite::execute)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Cannot commit rewrite");

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @TestTemplate
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

  @TestTemplate
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

    assertThatThrownBy(spyRewrite::execute)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Rewrite Failed");

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 1);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @TestTemplate
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

  @TestTemplate
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

    // With 10 original groups and max commits of 3, we have 4 groups per commit.
    // Removing 3 groups, we are left with 4 groups and 3 groups in two commits.
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @TestTemplate
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

    // Commit 1: 4/4 + Commit 2 failed 0/4 + Commit 3: 2/2 == 6 out of 10 total groups committed
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

  @TestTemplate
  public void testParallelPartialProgressWithMaxFailedCommits() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    RewriteDataFilesSparkAction realRewrite =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "3")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "3")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_FAILED_COMMITS, "0");

    RewriteDataFilesSparkAction spyRewrite = Mockito.spy(realRewrite);

    // Fail groups 1, 3, and 7 during rewrite
    GroupInfoMatcher failGroup = new GroupInfoMatcher(1, 3, 7);
    doThrow(new RuntimeException("Rewrite Failed"))
        .when(spyRewrite)
        .rewriteFiles(any(), argThat(failGroup));

    assertThatThrownBy(() -> spyRewrite.execute())
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining(
            "1 rewrite commits failed. This is more than the maximum allowed failures of 0");

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    // With 10 original groups and max commits of 3, we have 4 groups per commit.
    // Removing 3 groups, we are left with 4 groups and 3 groups in two commits.
    // Adding max allowed failed commits doesn't change the number of successful commits.
    shouldHaveSnapshots(table, 3);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @TestTemplate
  public void testParallelPartialProgressWithMaxCommitsLargerThanTotalGroupCount() {
    Table table = createTable(20);
    int fileSize = averageFileSize(table);

    List<Object[]> originalData = currentData();

    RewriteDataFilesSparkAction rewrite =
        basicRewrite(table)
            .option(
                RewriteDataFiles.MAX_FILE_GROUP_SIZE_BYTES, Integer.toString(fileSize * 2 + 1000))
            .option(RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_REWRITES, "3")
            .option(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED, "true")
            // Since we can have at most one commit per file group and there are only 10 file
            // groups, actual number of commits is 10
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS, "20")
            // Setting max-failed-commits to 1 to tolerate random commit failure
            .option(RewriteDataFiles.PARTIAL_PROGRESS_MAX_FAILED_COMMITS, "1");
    rewrite.execute();

    table.refresh();

    List<Object[]> postRewriteData = currentData();
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);
    table.refresh();
    assertThat(table.snapshots())
        .as("Table did not have the expected number of snapshots")
        // To tolerate 1 random commit failure
        .hasSizeGreaterThanOrEqualTo(10);
    shouldHaveNoOrphans(table);
    shouldHaveACleanCache(table);
  }

  @TestTemplate
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
                    .option(SparkShufflingFileRewriteRunner.SHUFFLE_PARTITIONS_PER_FILE, "5")
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires enabling Iceberg Spark session extensions");
  }

  @TestTemplate
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
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
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

  @TestTemplate
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
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
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

  @TestTemplate
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
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
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

  @TestTemplate
  public void testSortCustomSortOrder() {
    Table table = createTable(20);
    shouldHaveLastCommitUnsorted(table, "c2");
    shouldHaveFiles(table, 20);

    List<Object[]> originalData = currentData();
    long dataSizeBefore = testDataSize(table);

    RewriteDataFiles.Result result =
        basicRewrite(table)
            .sort(SortOrder.builderFor(table.schema()).asc("c2").build())
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
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

  @TestTemplate
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
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
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

  @TestTemplate
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
                SizeBasedFileRewritePlanner.MAX_FILE_SIZE_BYTES,
                Integer.toString((averageFileSize(table) / 2) + 2))
            // Divide files in 2
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
                Integer.toString(averageFileSize(table) / 2))
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
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

  @TestTemplate
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

  @TestTemplate
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
                SizeBasedFileRewritePlanner.MAX_FILE_SIZE_BYTES,
                Integer.toString((averageFileSize(table) / 2) + 2))
            // Divide files in 2
            .option(
                RewriteDataFiles.TARGET_FILE_SIZE_BYTES,
                Integer.toString(averageFileSize(table) / 2))
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
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

  @TestTemplate
  public void testZOrderAllTypesSort() {
    spark.conf().set("spark.sql.ansi.enabled", "false");
    Table table = createTypeTestTable();
    shouldHaveFiles(table, 10);

    List<Row> originalRaw =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SPLIT_SIZE, 1024 * 1024 * 64)
            .option(SparkReadOptions.FILE_OPEN_COST, 0)
            .load(tableLocation)
            .coalesce(1)
            .sort("longCol")
            .collectAsList();
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
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .execute();

    assertThat(result.rewriteResults()).as("Should have 1 fileGroups").hasSize(1);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(table.currentSnapshot().addedDataFiles(table.io()))
        .as("Should have written 1 file")
        .hasSize(1);

    table.refresh();

    List<Row> postRaw =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SPLIT_SIZE, 1024 * 1024 * 64)
            .option(SparkReadOptions.FILE_OPEN_COST, 0)
            .load(tableLocation)
            .coalesce(1)
            .sort("longCol")
            .collectAsList();
    List<Object[]> postRewriteData = rowsToJava(postRaw);
    assertEquals("We shouldn't have changed the data", originalData, postRewriteData);

    shouldHaveSnapshots(table, 2);
    shouldHaveACleanCache(table);
  }

  @TestTemplate
  public void testInvalidAPIUsage() {
    Table table = createTable(1);

    SortOrder sortOrder = SortOrder.builderFor(table.schema()).asc("c2").build();

    assertThatThrownBy(() -> actions().rewriteDataFiles(table).binPack().sort())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot set rewrite mode, it has already been set to ");

    assertThatThrownBy(() -> actions().rewriteDataFiles(table).sort(sortOrder).binPack())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot set rewrite mode, it has already been set to ");

    assertThatThrownBy(() -> actions().rewriteDataFiles(table).sort(sortOrder).binPack())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot set rewrite mode, it has already been set to ");
  }

  @TestTemplate
  public void testSnapshotProperty() {
    Table table = createTable(4);
    Result ignored = basicRewrite(table).snapshotProperty("key", "value").execute();
    assertThat(table.currentSnapshot().summary())
        .containsAllEntriesOf(ImmutableMap.of("key", "value"));
    // make sure internal produced properties are not lost
    String[] commitMetricsKeys =
        new String[] {
          SnapshotSummary.ADDED_FILES_PROP,
          SnapshotSummary.DELETED_FILES_PROP,
          SnapshotSummary.TOTAL_DATA_FILES_PROP,
          SnapshotSummary.CHANGED_PARTITION_COUNT_PROP
        };
    assertThat(table.currentSnapshot().summary()).containsKeys(commitMetricsKeys);
  }

  @TestTemplate
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
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .binPack()
            .execute();

    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(currentData()).hasSize((int) count);
    shouldRewriteDataFilesWithPartitionSpec(table, outputSpecId);
  }

  @TestTemplate
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
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .binPack()
            .execute();

    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(currentData()).hasSize((int) count);
    shouldRewriteDataFilesWithPartitionSpec(table, outputSpecId);
  }

  @TestTemplate
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

  @TestTemplate
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
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .sort(SortOrder.builderFor(table.schema()).asc("c2").asc("c3").build())
            .execute();

    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(currentData()).hasSize((int) count);
    shouldRewriteDataFilesWithPartitionSpec(table, outputSpecId);
  }

  @TestTemplate
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
            .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
            .zOrder("c2", "c3")
            .execute();

    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    assertThat(currentData()).hasSize((int) count);
    shouldRewriteDataFilesWithPartitionSpec(table, outputSpecId);
  }

  @TestTemplate
  public void testUnpartitionedRewriteDataFilesPreservesLineage() throws NoSuchTableException {
    assumeThat(formatVersion).isGreaterThan(2);

    // Verify the initial row IDs and sequence numbers
    Table table = createTable(4);
    shouldHaveFiles(table, 4);
    List<Object[]> expectedRecordsWithLineage = currentDataWithLineage();
    List<Long> rowIds =
        expectedRecordsWithLineage.stream()
            .map(record -> (Long) record[0])
            .collect(Collectors.toList());
    List<Long> lastUpdatedSequenceNumbers =
        expectedRecordsWithLineage.stream()
            .map(record -> (Long) record[1])
            .collect(Collectors.toList());
    assertThat(rowIds)
        .isEqualTo(
            LongStream.range(0, expectedRecordsWithLineage.size())
                .boxed()
                .collect(Collectors.toList()));
    assertThat(lastUpdatedSequenceNumbers).allMatch(sequenceNumber -> sequenceNumber.equals(1L));

    // Perform and validate compaction
    long dataSizeBefore = testDataSize(table);
    Result result = basicRewrite(table).execute();
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 4 data files")
        .isEqualTo(4);
    assertThat(result.addedDataFilesCount()).as("Action should add 1 data file").isOne();
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    shouldHaveFiles(table, 1);
    List<Object[]> actualRecordsWithLineage = currentDataWithLineage();
    assertEquals("Rows must match", expectedRecordsWithLineage, actualRecordsWithLineage);
  }

  @TestTemplate
  public void testRewriteDataFilesPreservesLineage() throws NoSuchTableException {
    assumeThat(formatVersion).isGreaterThan(2);

    Table table = createTablePartitioned(4 /* partitions */, 2 /* files per partition */);
    shouldHaveFiles(table, 8);

    // Verify the initial row IDs and sequence numbers
    List<Object[]> expectedRecords = currentDataWithLineage();
    List<Long> rowIds =
        expectedRecords.stream().map(record -> (Long) record[0]).collect(Collectors.toList());
    List<Long> lastUpdatedSequenceNumbers =
        expectedRecords.stream().map(record -> (Long) record[1]).collect(Collectors.toList());
    assertThat(rowIds)
        .isEqualTo(
            LongStream.range(0, expectedRecords.size()).boxed().collect(Collectors.toList()));
    assertThat(lastUpdatedSequenceNumbers).allMatch(sequenceNumber -> sequenceNumber.equals(1L));

    // Perform and validate compaction
    long dataSizeBefore = testDataSize(table);
    Result result = basicRewrite(table).execute();
    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite 8 data files")
        .isEqualTo(8);
    assertThat(result.addedDataFilesCount()).as("Action should add 4 data file").isEqualTo(4);
    assertThat(result.rewrittenBytesCount()).isEqualTo(dataSizeBefore);
    shouldHaveFiles(table, 4);
    List<Object[]> actualRecordsWithLineage = currentDataWithLineage();
    assertEquals("Rows must match", expectedRecords, actualRecordsWithLineage);
  }

  @TestTemplate
  public void testExecutorCacheForDeleteFilesDisabled() {
    Table table = createTablePartitioned(1, 1);
    RewriteDataFilesSparkAction action = SparkActions.get(spark).rewriteDataFiles(table);

    // The constructor should have set the configuration to false
    SparkReadConf readConf = new SparkReadConf(action.spark(), table, Collections.emptyMap());
    assertThat(readConf.cacheDeleteFilesOnExecutors())
        .as("Executor cache for delete files should be disabled in RewriteDataFilesSparkAction")
        .isFalse();
  }

  @TestTemplate
  public void testZOrderUDFWithDateType() {
    SparkZOrderUDF zorderUDF = new SparkZOrderUDF(1, 16, 1024);
    Dataset<Row> result =
        spark
            .sql("SELECT DATE '2025-01-01' as test_col")
            .withColumn(
                "zorder_result",
                zorderUDF.sortedLexicographically(col("test_col"), DataTypes.DateType));

    assertThat(result.schema().apply("zorder_result").dataType()).isEqualTo(DataTypes.BinaryType);
    List<Row> rows = result.collectAsList();
    Row row = rows.get(0);
    byte[] zorderBytes = row.getAs("zorder_result");
    assertThat(zorderBytes).isNotNull().isNotEmpty();
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

  protected List<Object[]> currentData() {
    return rowsToJava(
        spark
            .read()
            .option(SparkReadOptions.SPLIT_SIZE, 1024 * 1024 * 64)
            .option(SparkReadOptions.FILE_OPEN_COST, 0)
            .format("iceberg")
            .load(tableLocation)
            .coalesce(1)
            .sort("c1", "c2", "c3")
            .collectAsList());
  }

  protected List<Object[]> currentDataWithLineage() {
    return rowsToJava(
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SPLIT_SIZE, 1024 * 1024 * 64)
            .option(SparkReadOptions.FILE_OPEN_COST, 0)
            .load(tableLocation)
            .coalesce(1)
            .sort("_row_id")
            .selectExpr("_row_id", "_last_updated_sequence_number", "*")
            .collectAsList());
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
    List<FileScanTask> files =
        StreamSupport.stream(table.newScan().planFiles().spliterator(), false)
            .collect(Collectors.toList());
    assertThat(files.size()).as("Did not have the expected number of files").isEqualTo(numExpected);
  }

  protected long shouldHaveMinSequenceNumberInPartition(
      Table table, String partitionFilter, long expected) {
    long actual =
        SparkTableUtil.loadMetadataTable(spark, table, MetadataTableType.ENTRIES)
            .filter("status != 2")
            .filter(partitionFilter)
            .select("sequence_number")
            .agg(min("sequence_number"))
            .as(Encoders.LONG())
            .collectAsList()
            .get(0);
    assertThat(actual).as("Did not have the expected min sequence number").isEqualTo(expected);
    return actual;
  }

  protected void shouldHaveSnapshots(Table table, int expectedSnapshots) {
    table.refresh();
    assertThat(table.snapshots())
        .as("Table did not have the expected number of snapshots")
        .hasSize(expectedSnapshots);
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
    Map<String, String> options =
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
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
    return createTablePartitioned(
        partitions,
        files,
        SCALE,
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));
  }

  protected Table createTablePartitioned(int partitions, int files, int numRecords) {
    return createTablePartitioned(
        partitions,
        files,
        numRecords,
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));
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

    Map<String, String> options =
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion));
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

  private void writeRecords(List<ThreeColumnRecord> records) {
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    writeDF(df);
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
    return writePosDeletes(table, dataFile.partition(), dataFile.location(), outputDeleteFiles);
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
                  table
                      .locationProvider()
                      .newDataLocation(
                          FileFormat.PARQUET.addExtension(UUID.randomUUID().toString())));
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

  private List<DeleteFile> writePosDeletes(
      Table table,
      StructLike partition,
      String path,
      int outputDeleteFiles,
      int totalPositionsToDelete) {
    List<DeleteFile> results = Lists.newArrayList();
    for (int file = 0; file < outputDeleteFiles; file++) {
      OutputFile outputFile =
          table
              .io()
              .newOutputFile(
                  table
                      .locationProvider()
                      .newDataLocation(
                          FileFormat.PARQUET.addExtension(UUID.randomUUID().toString())));
      EncryptedOutputFile encryptedOutputFile =
          EncryptedFiles.encryptedOutput(outputFile, EncryptionKeyMetadata.EMPTY);

      GenericAppenderFactory appenderFactory =
          new GenericAppenderFactory(table.schema(), table.spec(), null, null, null);
      PositionDeleteWriter<Record> posDeleteWriter =
          appenderFactory
              .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "full")
              .newPosDeleteWriter(encryptedOutputFile, FileFormat.PARQUET, partition);

      PositionDelete<Record> posDelete = PositionDelete.create();
      int positionsPerDeleteFile = totalPositionsToDelete / outputDeleteFiles;

      for (int position = file * positionsPerDeleteFile;
          position < (file + 1) * positionsPerDeleteFile;
          position++) {
        posDeleteWriter.write(posDelete.set(path, position, null));
      }

      try {
        posDeleteWriter.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      results.add(posDeleteWriter.toDeleteFile());
    }

    return results;
  }

  private List<DeleteFile> writeDV(
      Table table, StructLike partition, String path, int numPositionsToDelete) throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, p -> null);
    try (DVFileWriter closeableWriter = writer) {
      for (int row = 0; row < numPositionsToDelete; row++) {
        closeableWriter.delete(path, row, table.spec(), partition);
      }
    }

    return writer.result().deleteFiles();
  }

  private void writeEqDeleteRecord(
      Table table, String partCol, Object partVal, String delCol, Object delVal) {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField(delCol).fieldId());
    Schema eqDeleteRowSchema = table.schema().select(delCol);
    Record partitionRecord =
        GenericRecord.create(table.schema().select(partCol))
            .copy(ImmutableMap.of(partCol, partVal));
    Record record = GenericRecord.create(eqDeleteRowSchema).copy(ImmutableMap.of(delCol, delVal));
    writeEqDeleteRecord(table, equalityFieldIds, partitionRecord, eqDeleteRowSchema, record);
  }

  private void writeEqDeleteRecord(
      Table table,
      List<Integer> equalityFieldIds,
      Record partitionRecord,
      Schema eqDeleteRowSchema,
      Record deleteRecord) {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            ArrayUtil.toIntArray(equalityFieldIds),
            eqDeleteRowSchema,
            null);

    EncryptedOutputFile file =
        createEncryptedOutputFile(createPartitionKey(table, partitionRecord), fileFactory);

    EqualityDeleteWriter<Record> eqDeleteWriter =
        appenderFactory.newEqDeleteWriter(
            file, FileFormat.PARQUET, createPartitionKey(table, partitionRecord));

    try (EqualityDeleteWriter<Record> clsEqDeleteWriter = eqDeleteWriter) {
      clsEqDeleteWriter.write(deleteRecord);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    table.newRowDelta().addDeletes(eqDeleteWriter.toDeleteFile()).commit();
  }

  private PartitionKey createPartitionKey(Table table, Record record) {
    if (table.spec().isUnpartitioned()) {
      return null;
    }

    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    partitionKey.partition(record);

    return partitionKey;
  }

  private EncryptedOutputFile createEncryptedOutputFile(
      PartitionKey partition, OutputFileFactory fileFactory) {
    if (partition == null) {
      return fileFactory.newOutputFile();
    } else {
      return fileFactory.newOutputFile(partition);
    }
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
