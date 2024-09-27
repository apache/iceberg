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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupRewriteResult;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.Result;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestRewritePositionDeleteFiles extends ExtensionsTestBase {

  private static final Map<String, String> CATALOG_PROPS =
      ImmutableMap.of(
          "type", "hive",
          "default-namespace", "default",
          "cache-enabled", "false");

  private static final String PARTITION_COL = "partition_col";
  private static final int NUM_DATA_FILES = 5;
  private static final int ROWS_PER_DATA_FILE = 100;
  private static final int DELETE_FILES_PER_PARTITION = 2;
  private static final int DELETE_FILE_SIZE = 10;

  @Parameters(name = "formatVersion = {0}, catalogName = {1}, implementation = {2}, config = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS
      }
    };
  }

  @AfterEach
  public void cleanup() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testDatePartition() throws Exception {
    createTable("date");
    Date baseDate = Date.valueOf("2023-01-01");
    insertData(i -> Date.valueOf(baseDate.toLocalDate().plusDays(i)));
    testDanglingDelete();
  }

  @TestTemplate
  public void testBooleanPartition() throws Exception {
    createTable("boolean");
    insertData(i -> i % 2 == 0, 2);
    testDanglingDelete(2);
  }

  @TestTemplate
  public void testTimestampPartition() throws Exception {
    createTable("timestamp");
    Timestamp baseTimestamp = Timestamp.valueOf("2023-01-01 15:30:00");
    insertData(i -> Timestamp.valueOf(baseTimestamp.toLocalDateTime().plusDays(i)));
    testDanglingDelete();
  }

  @TestTemplate
  public void testTimestampNtz() throws Exception {
    createTable("timestamp_ntz");
    LocalDateTime baseTimestamp = Timestamp.valueOf("2023-01-01 15:30:00").toLocalDateTime();
    insertData(baseTimestamp::plusDays);
    testDanglingDelete();
  }

  @TestTemplate
  public void testBytePartition() throws Exception {
    createTable("byte");
    insertData(i -> i);
    testDanglingDelete();
  }

  @TestTemplate
  public void testDecimalPartition() throws Exception {
    createTable("decimal(18, 10)");
    BigDecimal baseDecimal = new BigDecimal("1.0");
    insertData(i -> baseDecimal.add(new BigDecimal(i)));
    testDanglingDelete();
  }

  @TestTemplate
  public void testBinaryPartition() throws Exception {
    createTable("binary");
    insertData(i -> java.nio.ByteBuffer.allocate(4).putInt(i).array());
    testDanglingDelete();
  }

  @TestTemplate
  public void testCharPartition() throws Exception {
    createTable("char(10)");
    insertData(Object::toString);
    testDanglingDelete();
  }

  @TestTemplate
  public void testVarcharPartition() throws Exception {
    createTable("varchar(10)");
    insertData(Object::toString);
    testDanglingDelete();
  }

  @TestTemplate
  public void testIntPartition() throws Exception {
    createTable("int");
    insertData(i -> i);
    testDanglingDelete();
  }

  @TestTemplate
  public void testDaysPartitionTransform() throws Exception {
    createTable("timestamp", PARTITION_COL, String.format("days(%s)", PARTITION_COL));
    Timestamp baseTimestamp = Timestamp.valueOf("2023-01-01 15:30:00");
    insertData(i -> Timestamp.valueOf(baseTimestamp.toLocalDateTime().plusDays(i)));
    testDanglingDelete();
  }

  @TestTemplate
  public void testNullTransform() throws Exception {
    createTable("int");
    insertData(i -> i == 0 ? null : 1, 2);
    testDanglingDelete(2);
  }

  @TestTemplate
  public void testPartitionColWithDot() throws Exception {
    String partitionColWithDot = "`partition.col`";
    createTable("int", partitionColWithDot, partitionColWithDot);
    insertData(partitionColWithDot, i -> i, NUM_DATA_FILES);
    testDanglingDelete(partitionColWithDot, NUM_DATA_FILES);
  }

  private void testDanglingDelete() throws Exception {
    testDanglingDelete(NUM_DATA_FILES);
  }

  private void testDanglingDelete(int numDataFiles) throws Exception {
    testDanglingDelete(PARTITION_COL, numDataFiles);
  }

  private void testDanglingDelete(String partitionCol, int numDataFiles) throws Exception {
    Table table = Spark3Util.loadIcebergTable(spark, tableName);

    List<DataFile> dataFiles = dataFiles(table);
    assertThat(dataFiles).hasSize(numDataFiles);

    SparkActions.get(spark)
        .rewriteDataFiles(table)
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .execute();

    // write dangling delete files for 'old data files'
    writePosDeletesForFiles(table, dataFiles);
    List<DeleteFile> deleteFiles = deleteFiles(table);
    assertThat(deleteFiles).hasSize(numDataFiles * DELETE_FILES_PER_PARTITION);

    List<Object[]> expectedRecords = records(tableName, partitionCol);

    Result result =
        SparkActions.get(spark)
            .rewritePositionDeletes(table)
            .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
            .execute();

    List<DeleteFile> newDeleteFiles = deleteFiles(table);
    assertThat(newDeleteFiles).as("Remaining dangling deletes").isEmpty();
    checkResult(result, deleteFiles, Lists.newArrayList(), numDataFiles);

    List<Object[]> actualRecords = records(tableName, partitionCol);
    assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  private void createTable(String partitionType) {
    createTable(partitionType, PARTITION_COL, PARTITION_COL);
  }

  private void createTable(String partitionType, String partitionCol, String partitionTransform) {
    sql(
        "CREATE TABLE %s (id long, %s %s, c1 string, c2 string) "
            + "USING iceberg "
            + "PARTITIONED BY (%s) "
            + "TBLPROPERTIES('format-version'='2')",
        tableName, partitionCol, partitionType, partitionTransform);
  }

  private void insertData(Function<Integer, ?> partitionValueFunction) throws Exception {
    insertData(partitionValueFunction, NUM_DATA_FILES);
  }

  private void insertData(Function<Integer, ?> partitionValueFunction, int numDataFiles)
      throws Exception {
    insertData(PARTITION_COL, partitionValueFunction, numDataFiles);
  }

  private void insertData(
      String partitionCol, Function<Integer, ?> partitionValue, int numDataFiles) throws Exception {
    for (int i = 0; i < numDataFiles; i++) {
      Dataset<Row> df =
          spark
              .range(0, ROWS_PER_DATA_FILE)
              .withColumn(partitionCol, lit(partitionValue.apply(i)))
              .withColumn("c1", expr("CAST(id AS STRING)"))
              .withColumn("c2", expr("CAST(id AS STRING)"));
      appendAsFile(df);
    }
  }

  private void appendAsFile(Dataset<Row> df) throws Exception {
    // ensure the schema is precise
    StructType sparkSchema = spark.table(tableName).schema();
    spark.createDataFrame(df.rdd(), sparkSchema).coalesce(1).writeTo(tableName).append();
  }

  private void writePosDeletesForFiles(Table table, List<DataFile> files) throws IOException {

    Map<StructLike, List<DataFile>> filesByPartition =
        files.stream().collect(Collectors.groupingBy(ContentFile::partition));
    List<DeleteFile> deleteFiles =
        Lists.newArrayListWithCapacity(DELETE_FILES_PER_PARTITION * filesByPartition.size());

    for (Map.Entry<StructLike, List<DataFile>> filesByPartitionEntry :
        filesByPartition.entrySet()) {

      StructLike partition = filesByPartitionEntry.getKey();
      List<DataFile> partitionFiles = filesByPartitionEntry.getValue();

      int deletesForPartition = partitionFiles.size() * DELETE_FILE_SIZE;
      assertThat(deletesForPartition % DELETE_FILE_SIZE)
          .as("Number of delete files per partition modulo number of data files in this partition")
          .isEqualTo(0);
      int deleteFileSize = deletesForPartition / DELETE_FILES_PER_PARTITION;

      int counter = 0;
      List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
      for (DataFile partitionFile : partitionFiles) {
        for (int deletePos = 0; deletePos < DELETE_FILE_SIZE; deletePos++) {
          deletes.add(Pair.of(partitionFile.path(), (long) deletePos));
          counter++;
          if (counter == deleteFileSize) {
            // Dump to file and reset variables
            OutputFile output =
                Files.localOutput(temp.resolve(UUID.randomUUID().toString()).toFile());
            deleteFiles.add(writeDeleteFile(table, output, partition, deletes));
            counter = 0;
            deletes.clear();
          }
        }
      }
    }

    RowDelta rowDelta = table.newRowDelta();
    deleteFiles.forEach(rowDelta::addDeletes);
    rowDelta.commit();
  }

  private DeleteFile writeDeleteFile(
      Table table, OutputFile out, StructLike partition, List<Pair<CharSequence, Long>> deletes)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    FileAppenderFactory<Record> factory = new GenericAppenderFactory(table.schema(), table.spec());

    PositionDeleteWriter<Record> writer =
        factory.newPosDeleteWriter(encrypt(out), format, partition);
    PositionDelete<Record> posDelete = PositionDelete.create();
    try (Closeable toClose = writer) {
      for (Pair<CharSequence, Long> delete : deletes) {
        writer.write(posDelete.set(delete.first(), delete.second(), null));
      }
    }

    return writer.toDeleteFile();
  }

  private static EncryptedOutputFile encrypt(OutputFile out) {
    return EncryptedFiles.encryptedOutput(out, EncryptionKeyMetadata.EMPTY);
  }

  private static FileFormat defaultFormat(Map<String, String> properties) {
    String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.fromString(formatString);
  }

  private List<Object[]> records(String table, String partitionCol) {
    return rowsToJava(
        spark.read().format("iceberg").load(table).sort(partitionCol, "id").collectAsList());
  }

  private long size(List<DeleteFile> deleteFiles) {
    return deleteFiles.stream().mapToLong(DeleteFile::fileSizeInBytes).sum();
  }

  private List<DataFile> dataFiles(Table table) {
    CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles();
    return Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
  }

  private List<DeleteFile> deleteFiles(Table table) {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);
    CloseableIterable<ScanTask> tasks = deletesTable.newBatchScan().planFiles();
    return Lists.newArrayList(
        CloseableIterable.transform(tasks, t -> ((PositionDeletesScanTask) t).file()));
  }

  private void checkResult(
      Result result,
      List<DeleteFile> rewrittenDeletes,
      List<DeleteFile> newDeletes,
      int expectedGroups) {
    assertThat(result.rewrittenDeleteFilesCount())
        .as("Rewritten delete files")
        .isEqualTo(rewrittenDeletes.size());
    assertThat(result.addedDeleteFilesCount())
        .as("Added delete files")
        .isEqualTo(newDeletes.size());
    assertThat(result.rewrittenBytesCount())
        .as("Rewritten delete bytes")
        .isEqualTo(size(rewrittenDeletes));
    assertThat(result.addedBytesCount()).as("New Delete byte count").isEqualTo(size(newDeletes));

    assertThat(result.rewriteResults()).as("Rewritten group count").hasSize(expectedGroups);
    assertThat(
            result.rewriteResults().stream()
                .mapToInt(FileGroupRewriteResult::rewrittenDeleteFilesCount)
                .sum())
        .as("Rewritten delete file count in all groups")
        .isEqualTo(rewrittenDeletes.size());
    assertThat(
            result.rewriteResults().stream()
                .mapToInt(FileGroupRewriteResult::addedDeleteFilesCount)
                .sum())
        .as("Added delete file count in all groups")
        .isEqualTo(newDeletes.size());
    assertThat(
            result.rewriteResults().stream()
                .mapToLong(FileGroupRewriteResult::rewrittenBytesCount)
                .sum())
        .as("Rewritten delete bytes in all groups")
        .isEqualTo(size(rewrittenDeletes));
    assertThat(
            result.rewriteResults().stream()
                .mapToLong(FileGroupRewriteResult::addedBytesCount)
                .sum())
        .as("Added delete bytes in all groups")
        .isEqualTo(size(newDeletes));
  }
}
