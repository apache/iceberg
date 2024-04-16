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
package org.apache.iceberg.flink.actions;

import static org.apache.iceberg.flink.SimpleDataUtil.RECORD;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogTestBase;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

public class TestRewriteDataFilesAction extends CatalogTestBase {

  private static final String TABLE_NAME_UNPARTITIONED = "test_table_unpartitioned";
  private static final String TABLE_NAME_PARTITIONED = "test_table_partitioned";
  private static final String TABLE_NAME_WITH_PK = "test_table_with_pk";

  @Parameter(index = 2)
  private FileFormat format;

  private Table icebergTableUnPartitioned;
  private Table icebergTablePartitioned;
  private Table icebergTableWithPk;

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv().getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
    return super.getTableEnv();
  }

  @Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}")
  public static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET}) {
      for (Object[] catalogParams : CatalogTestBase.parameters()) {
        String catalogName = (String) catalogParams[0];
        Namespace baseNamespace = (Namespace) catalogParams[1];
        parameters.add(new Object[] {catalogName, baseNamespace, format});
      }
    }
    return parameters;
  }

  private @TempDir Path temp;

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql(
        "CREATE TABLE %s (id int, data varchar) with ('write.format.default'='%s')",
        TABLE_NAME_UNPARTITIONED, format.name());
    icebergTableUnPartitioned =
        validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_UNPARTITIONED));

    sql(
        "CREATE TABLE %s (id int, data varchar,spec varchar) "
            + " PARTITIONED BY (data,spec) with ('write.format.default'='%s')",
        TABLE_NAME_PARTITIONED, format.name());
    icebergTablePartitioned =
        validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_PARTITIONED));

    sql(
        "CREATE TABLE %s (id int, data varchar, PRIMARY KEY(`id`) NOT ENFORCED) with ('write.format.default'='%s', 'format-version'='2')",
        TABLE_NAME_WITH_PK, format.name());
    icebergTableWithPk =
        validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_WITH_PK));
  }

  @Override
  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_UNPARTITIONED);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_PARTITIONED);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_WITH_PK);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @TestTemplate
  public void testRewriteDataFilesEmptyTable() throws Exception {
    assertThat(icebergTableUnPartitioned.currentSnapshot()).isNull();
    Actions.forTable(icebergTableUnPartitioned).rewriteDataFiles().execute();
    assertThat(icebergTableUnPartitioned.currentSnapshot()).isNull();
  }

  @TestTemplate
  public void testRewriteDataFilesUnpartitionedTable() throws Exception {
    sql("INSERT INTO %s SELECT 1, 'hello'", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'world'", TABLE_NAME_UNPARTITIONED);

    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTableUnPartitioned.newScan().planFiles();
    List<DataFile> dataFiles =
        Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    assertThat(dataFiles).hasSize(2);
    RewriteDataFilesActionResult result =
        Actions.forTable(icebergTableUnPartitioned).rewriteDataFiles().execute();

    assertThat(result.deletedDataFiles()).hasSize(2);
    assertThat(result.addedDataFiles()).hasSize(1);

    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks1 = icebergTableUnPartitioned.newScan().planFiles();
    List<DataFile> dataFiles1 =
        Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    assertThat(dataFiles1).hasSize(1);
    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(
        icebergTableUnPartitioned,
        Lists.newArrayList(
            SimpleDataUtil.createRecord(1, "hello"), SimpleDataUtil.createRecord(2, "world")));
  }

  @TestTemplate
  public void testRewriteDataFilesPartitionedTable() throws Exception {
    sql("INSERT INTO %s SELECT 1, 'hello' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'hello' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 3, 'world' ,'b'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 4, 'world' ,'b'", TABLE_NAME_PARTITIONED);

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles =
        Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    assertThat(dataFiles).hasSize(4);
    RewriteDataFilesActionResult result =
        Actions.forTable(icebergTablePartitioned).rewriteDataFiles().execute();

    assertThat(result.deletedDataFiles()).hasSize(4);
    assertThat(result.addedDataFiles()).hasSize(2);

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks1 = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles1 =
        Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    assertThat(dataFiles1).hasSize(2);
    // Assert the table records as expected.
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "spec", Types.StringType.get()));

    Record record = GenericRecord.create(schema);
    SimpleDataUtil.assertTableRecords(
        icebergTablePartitioned,
        Lists.newArrayList(
            record.copy("id", 1, "data", "hello", "spec", "a"),
            record.copy("id", 2, "data", "hello", "spec", "a"),
            record.copy("id", 3, "data", "world", "spec", "b"),
            record.copy("id", 4, "data", "world", "spec", "b")));
  }

  @TestTemplate
  public void testRewriteDataFilesWithFilter() throws Exception {
    sql("INSERT INTO %s SELECT 1, 'hello' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'hello' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 3, 'world' ,'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 4, 'world' ,'b'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 5, 'world' ,'b'", TABLE_NAME_PARTITIONED);

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles =
        Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    assertThat(dataFiles).hasSize(5);
    RewriteDataFilesActionResult result =
        Actions.forTable(icebergTablePartitioned)
            .rewriteDataFiles()
            .filter(Expressions.equal("spec", "a"))
            .filter(Expressions.startsWith("data", "he"))
            .execute();
    assertThat(result.deletedDataFiles()).hasSize(2);
    assertThat(result.addedDataFiles()).hasSize(1);

    icebergTablePartitioned.refresh();

    CloseableIterable<FileScanTask> tasks1 = icebergTablePartitioned.newScan().planFiles();
    List<DataFile> dataFiles1 =
        Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    assertThat(dataFiles1).hasSize(4);
    // Assert the table records as expected.
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "spec", Types.StringType.get()));

    Record record = GenericRecord.create(schema);
    SimpleDataUtil.assertTableRecords(
        icebergTablePartitioned,
        Lists.newArrayList(
            record.copy("id", 1, "data", "hello", "spec", "a"),
            record.copy("id", 2, "data", "hello", "spec", "a"),
            record.copy("id", 3, "data", "world", "spec", "a"),
            record.copy("id", 4, "data", "world", "spec", "b"),
            record.copy("id", 5, "data", "world", "spec", "b")));
  }

  @TestTemplate
  public void testRewriteLargeTableHasResiduals() throws IOException {
    // all records belong to the same partition
    List<String> records1 = Lists.newArrayList();
    List<String> records2 = Lists.newArrayList();
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      int id = i;
      String data = String.valueOf(i % 3);
      if (i % 2 == 0) {
        records1.add("(" + id + ",'" + data + "')");
      } else {
        records2.add("(" + id + ",'" + data + "')");
      }
      Record record = RECORD.copy();
      record.setField("id", id);
      record.setField("data", data);
      expected.add(record);
    }

    sql("INSERT INTO %s values " + StringUtils.join(records1, ","), TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s values " + StringUtils.join(records2, ","), TABLE_NAME_UNPARTITIONED);

    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks =
        icebergTableUnPartitioned
            .newScan()
            .ignoreResiduals()
            .filter(Expressions.equal("data", "0"))
            .planFiles();
    for (FileScanTask task : tasks) {
      assertThat(task.residual())
          .as("Residuals must be ignored")
          .isEqualTo(Expressions.alwaysTrue());
    }
    List<DataFile> dataFiles =
        Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    assertThat(dataFiles).hasSize(2);
    Actions actions = Actions.forTable(icebergTableUnPartitioned);

    RewriteDataFilesActionResult result =
        actions.rewriteDataFiles().filter(Expressions.equal("data", "0")).execute();
    assertThat(result.deletedDataFiles()).hasSize(2);
    assertThat(result.addedDataFiles()).hasSize(1);
    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(icebergTableUnPartitioned, expected);
  }

  /**
   * a test case to test avoid repeate compress
   *
   * <p>If datafile cannot be combined to CombinedScanTask with other DataFiles, the size of the
   * CombinedScanTask list size is 1, so we remove these CombinedScanTasks to avoid compressed
   * repeatedly.
   *
   * <p>In this test case,we generated 3 data files and set targetSizeInBytes greater than the
   * largest file size so that it cannot be combined a CombinedScanTask with other datafiles. The
   * datafile with the largest file size will not be compressed.
   *
   * @throws IOException IOException
   */
  @TestTemplate
  public void testRewriteAvoidRepeateCompress() throws IOException {
    List<Record> expected = Lists.newArrayList();
    Schema schema = icebergTableUnPartitioned.schema();
    GenericAppenderFactory genericAppenderFactory = new GenericAppenderFactory(schema);
    File file = File.createTempFile("junit", null, temp.toFile());
    int count = 0;
    try (FileAppender<Record> fileAppender =
        genericAppenderFactory.newAppender(Files.localOutput(file), format)) {
      long filesize = 20000;
      for (; fileAppender.length() < filesize; count++) {
        Record record = SimpleDataUtil.createRecord(count, UUID.randomUUID().toString());
        fileAppender.add(record);
        expected.add(record);
      }
    }

    DataFile dataFile =
        DataFiles.builder(icebergTableUnPartitioned.spec())
            .withPath(file.getAbsolutePath())
            .withFileSizeInBytes(file.length())
            .withFormat(format)
            .withRecordCount(count)
            .build();

    icebergTableUnPartitioned.newAppend().appendFile(dataFile).commit();

    sql("INSERT INTO %s SELECT 1,'a' ", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s SELECT 2,'b' ", TABLE_NAME_UNPARTITIONED);

    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks = icebergTableUnPartitioned.newScan().planFiles();
    List<DataFile> dataFiles =
        Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    assertThat(dataFiles).hasSize(3);
    Actions actions = Actions.forTable(icebergTableUnPartitioned);

    long targetSizeInBytes = file.length() + 10;
    RewriteDataFilesActionResult result =
        actions
            .rewriteDataFiles()
            .targetSizeInBytes(targetSizeInBytes)
            .splitOpenFileCost(1)
            .execute();
    assertThat(result.deletedDataFiles()).hasSize(2);
    assertThat(result.addedDataFiles()).hasSize(1);
    icebergTableUnPartitioned.refresh();

    CloseableIterable<FileScanTask> tasks1 = icebergTableUnPartitioned.newScan().planFiles();
    List<DataFile> dataFilesRewrote =
        Lists.newArrayList(CloseableIterable.transform(tasks1, FileScanTask::file));
    assertThat(dataFilesRewrote).hasSize(2);
    // the biggest file do not be rewrote
    List rewroteDataFileNames =
        dataFilesRewrote.stream().map(ContentFile::path).collect(Collectors.toList());
    assertThat(rewroteDataFileNames).contains(file.getAbsolutePath());

    // Assert the table records as expected.
    expected.add(SimpleDataUtil.createRecord(1, "a"));
    expected.add(SimpleDataUtil.createRecord(2, "b"));
    SimpleDataUtil.assertTableRecords(icebergTableUnPartitioned, expected);
  }

  @TestTemplate
  public void testRewriteNoConflictWithEqualityDeletes() throws IOException {
    // Add 2 data files
    sql("INSERT INTO %s SELECT 1, 'hello'", TABLE_NAME_WITH_PK);
    sql("INSERT INTO %s SELECT 2, 'world'", TABLE_NAME_WITH_PK);

    // Load 2 stale tables to pass to rewrite actions
    // Since the first rewrite will refresh stale1, we need another stale2 for the second rewrite
    Table stale1 =
        validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_WITH_PK));
    Table stale2 =
        validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_WITH_PK));

    // Add 1 data file and 1 equality-delete file
    sql("INSERT INTO %s /*+ OPTIONS('upsert-enabled'='true')*/ SELECT 1, 'hi'", TABLE_NAME_WITH_PK);

    icebergTableWithPk.refresh();
    assertThat(icebergTableWithPk.currentSnapshot().sequenceNumber())
        .as("The latest sequence number should be greater than that of the stale snapshot")
        .isEqualTo(stale1.currentSnapshot().sequenceNumber() + 1);
    CloseableIterable<FileScanTask> tasks = icebergTableWithPk.newScan().planFiles();
    List<DataFile> dataFiles =
        Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::file));
    Set<DeleteFile> deleteFiles =
        Lists.newArrayList(CloseableIterable.transform(tasks, FileScanTask::deletes)).stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    assertThat(dataFiles).hasSize(3);
    assertThat(deleteFiles).hasSize(1);
    assertThat(Iterables.getOnlyElement(deleteFiles).content())
        .isEqualTo(FileContent.EQUALITY_DELETES);
    shouldHaveDataAndFileSequenceNumbers(
        TABLE_NAME_WITH_PK,
        ImmutableList.of(Pair.of(1L, 1L), Pair.of(2L, 2L), Pair.of(3L, 3L), Pair.of(3L, 3L)));

    Assertions.assertThatThrownBy(
            () ->
                Actions.forTable(stale1)
                    .rewriteDataFiles()
                    .useStartingSequenceNumber(false)
                    .execute(),
            "Rewrite using new sequence number should fail")
        .isInstanceOf(ValidationException.class);

    // Rewrite using the starting sequence number should succeed
    RewriteDataFilesActionResult result =
        Actions.forTable(stale2).rewriteDataFiles().useStartingSequenceNumber(true).execute();

    // Should not rewrite files from the new commit
    assertThat(result.deletedDataFiles()).hasSize(2);
    assertThat(result.addedDataFiles()).hasSize(1);
    // The 2 older files with file-sequence-number <= 2 should be rewritten into a new file.
    // The new file is the one with file-sequence-number == 4.
    // The new file should use rewrite's starting-sequence-number 2 as its data-sequence-number.
    shouldHaveDataAndFileSequenceNumbers(
        TABLE_NAME_WITH_PK, ImmutableList.of(Pair.of(3L, 3L), Pair.of(3L, 3L), Pair.of(2L, 4L)));

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(
        icebergTableWithPk,
        Lists.newArrayList(
            SimpleDataUtil.createRecord(1, "hi"), SimpleDataUtil.createRecord(2, "world")));
  }

  /**
   * Assert that data files and delete files in the table should have expected data sequence numbers
   * and file sequence numbers
   *
   * @param tableName table name
   * @param expectedSequenceNumbers list of {@link Pair}'s. Each {@link Pair} contains
   *     (expectedDataSequenceNumber, expectedFileSequenceNumber) of a file.
   */
  private void shouldHaveDataAndFileSequenceNumbers(
      String tableName, List<Pair<Long, Long>> expectedSequenceNumbers) {
    // "status < 2" for added or existing entries
    List<Row> liveEntries = sql("SELECT * FROM %s$entries WHERE status < 2", tableName);

    List<Pair<Long, Long>> actualSequenceNumbers =
        liveEntries.stream()
            .map(
                row ->
                    Pair.<Long, Long>of(
                        row.getFieldAs("sequence_number"), row.getFieldAs("file_sequence_number")))
            .collect(Collectors.toList());
    assertThat(actualSequenceNumbers).hasSameElementsAs(expectedSequenceNumbers);
  }
}
