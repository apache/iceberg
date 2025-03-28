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

import static org.apache.hadoop.hdfs.server.namenode.TestEditLog.getConf;
import static org.apache.iceberg.TestHelpers.waitUntilAfter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.CatalogTestBase;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestCleanOrphanFilesAction extends CatalogTestBase {

  private static final String TABLE_NAME_UNPARTITIONED = "test_table_unpartitioned";
  private static final String TABLE_NAME_PARTITIONED = "test_table_partitioned";

  @Parameter(index = 2)
  private FileFormat format;

  @Parameter(index = 3)
  private int formatVersion;

  private Table icebergTableUnPartitioned;
  private Table icebergTablePartitioned;

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv().getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
    return super.getTableEnv();
  }

  @Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, formatVersion={3}")
  public static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET}) {
      for (Object[] catalogParams : CatalogTestBase.parameters()) {
        for (int version : Arrays.asList(2, 3)) {
          String catalogName = (String) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(new Object[] {catalogName, baseNamespace, format, version});
        }
      }
    }
    return parameters;
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    sql(
        "CREATE TABLE %s (id int, data varchar) with ('write.format.default'='%s', '%s'='%s')",
        TABLE_NAME_UNPARTITIONED, format.name(), TableProperties.FORMAT_VERSION, formatVersion);
    icebergTableUnPartitioned =
        validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_UNPARTITIONED));

    sql(
        "CREATE TABLE %s (id int, data varchar,spec varchar) "
            + " PARTITIONED BY (data,spec) with ('write.format.default'='%s', '%s'='%s')",
        TABLE_NAME_PARTITIONED, format.name(), TableProperties.FORMAT_VERSION, formatVersion);
    icebergTablePartitioned =
        validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME_PARTITIONED));
  }

  @Override
  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_UNPARTITIONED);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME_PARTITIONED);
    dropDatabase(flinkDatabase, true);
    super.clean();
  }

  @TestTemplate
  void testAllValidFilesAreKept() {
    sql("INSERT INTO %s SELECT 1, 'hello'", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'world'", TABLE_NAME_UNPARTITIONED);

    icebergTableUnPartitioned.refresh();

    waitUntilAfter(System.currentTimeMillis());

    CleanOrphanFilesAction action =
        Actions.forTable(icebergTableUnPartitioned)
            .cleanOrphanDataFiles()
            .olderThan(System.currentTimeMillis());

    List<String> result = getResult(action);

    assertThat(result.isEmpty()).as("No valid files should be deleted.").isTrue();
    List<Row> rows = sql("select * from %s", TABLE_NAME_UNPARTITIONED);
    assertThat(rows).as("All data should be read.").hasSize(2);
  }

  @TestTemplate
  void testOrphanedFileRemovedWithParallelTasks() throws IOException {
    sql("INSERT INTO %s SELECT 1, 'hello', 'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'world', 'b'", TABLE_NAME_PARTITIONED);
    icebergTablePartitioned.refresh();

    String invalidFilePath1 = icebergTablePartitioned.location() + "/data=invalid/invalid/";
    SimpleDataUtil.writeFile(
        icebergTablePartitioned,
        icebergTablePartitioned.schema(),
        icebergTablePartitioned.spec(),
        getConf(),
        invalidFilePath1,
        "invalid-file-1.parquet",
        Lists.newArrayList(
            GenericRowData.of(1, StringData.fromString("hello"), StringData.fromString("a"))));

    String invalidFilePath2 = icebergTablePartitioned.location() + "/data=world/spec=invalid/";
    SimpleDataUtil.writeFile(
        icebergTablePartitioned,
        icebergTablePartitioned.schema(),
        icebergTablePartitioned.spec(),
        getConf(),
        invalidFilePath2,
        "invalid-file-2.parquet",
        Lists.newArrayList(
            GenericRowData.of(2, StringData.fromString("world"), StringData.fromString("b"))));

    String invalidFilePath3 = icebergTablePartitioned.location() + "/data=hello/spec=a/invalid/";
    SimpleDataUtil.writeFile(
        icebergTablePartitioned,
        icebergTablePartitioned.schema(),
        icebergTablePartitioned.spec(),
        getConf(),
        invalidFilePath3,
        "invalid-file-3.parquet",
        Lists.newArrayList(
            GenericRowData.of(3, StringData.fromString("hello"), StringData.fromString("a"))));

    Set<String> deletedFiles = ConcurrentHashMap.newKeySet();
    Set<String> deleteThreads = ConcurrentHashMap.newKeySet();
    AtomicInteger deleteThreadsIndex = new AtomicInteger(0);

    ExecutorService executorService =
        Executors.newFixedThreadPool(
            3,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("remove-orphan-" + deleteThreadsIndex.getAndIncrement());
              thread.setDaemon(true);
              return thread;
            });

    CleanOrphanFilesAction action =
        Actions.forTable(icebergTablePartitioned)
            .cleanOrphanDataFiles()
            .executeDeleteWith(executorService)
            .olderThan(System.currentTimeMillis()) // 设置清理阈值
            .deleteWith(
                file -> {
                  deleteThreads.add(Thread.currentThread().getName());
                  deletedFiles.add(file);
                });

    List<String> result = getResult(action);

    assertThat(deleteThreads)
        .containsExactlyInAnyOrder("remove-orphan-0", "remove-orphan-1", "remove-orphan-2");

    assertThat(result).as("Three orphan files should be deleted.").hasSize(3);

    List<Row> rows = sql("select * from %s", TABLE_NAME_PARTITIONED);
    assertThat(rows).as("All data should be read.").hasSize(2);
  }

  @TestTemplate
  void testOlderThanTimestamp() throws IOException {
    sql("INSERT INTO %s SELECT 1, 'hello', 'a'", TABLE_NAME_PARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'world', 'b'", TABLE_NAME_PARTITIONED);
    icebergTablePartitioned.refresh();

    String invalidFilePath1 = icebergTablePartitioned.location() + "/data=invalid/invalid/";
    SimpleDataUtil.writeFile(
        icebergTablePartitioned,
        icebergTablePartitioned.schema(),
        icebergTablePartitioned.spec(),
        getConf(),
        invalidFilePath1,
        "invalid-file-1.parquet",
        Lists.newArrayList(
            GenericRowData.of(1, StringData.fromString("hello"), StringData.fromString("a"))));

    CleanOrphanFilesAction.NormalizedPath normalizedPath =
        CleanOrphanFilesAction.normalizePath(invalidFilePath1 + "invalid-file-1.parquet");
    Files.setLastModifiedTime(
        Paths.get(normalizedPath.getPath()),
        FileTime.from(Instant.now().minus(2, ChronoUnit.DAYS)));

    String invalidFilePath3 = icebergTablePartitioned.location() + "/data=hello/spec=a/invalid/";
    SimpleDataUtil.writeFile(
        icebergTablePartitioned,
        icebergTablePartitioned.schema(),
        icebergTablePartitioned.spec(),
        getConf(),
        invalidFilePath3,
        "invalid-file-3.parquet",
        Lists.newArrayList(
            GenericRowData.of(3, StringData.fromString("hello"), StringData.fromString("a"))));

    long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);

    CleanOrphanFilesAction action =
        Actions.forTable(icebergTablePartitioned)
            .cleanOrphanDataFiles()
            .olderThan(olderThanTimestamp);

    List<String> result = getResult(action);
    assertThat(result).as("One orphan file should be deleted.").hasSize(1);

    List<Row> rows = sql("select * from %s", TABLE_NAME_PARTITIONED);
    assertThat(rows).as("All data should be read.").hasSize(2);
  }

  @TestTemplate
  void testRemoveUnreachableMetadataVersionFiles() throws IOException {
    icebergTableUnPartitioned
        .updateProperties()
        .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1")
        .commit();

    sql("INSERT INTO %s SELECT 1, 'hello'", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'world'", TABLE_NAME_UNPARTITIONED);
    icebergTableUnPartitioned.refresh();

    sql("INSERT INTO %s SELECT 3, 'data1'", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s SELECT 4, 'data2'", TABLE_NAME_UNPARTITIONED);
    icebergTableUnPartitioned.refresh();

    waitUntilAfter(System.currentTimeMillis());

    CleanOrphanFilesAction action =
        Actions.forTable(icebergTableUnPartitioned)
            .cleanOrphanDataFiles()
            .olderThan(System.currentTimeMillis());

    List<String> result = getResult(action);

    assertThat(result.size() > 0).isTrue();
    List<Row> rows = sql("select * from %s", TABLE_NAME_UNPARTITIONED);
    assertThat(rows).as("All data should be read.").hasSize(4);
  }

  @TestTemplate
  void testManyTopLevelPartitions() throws IOException {
    for (int i = 0; i < 50; i++) {
      sql("INSERT INTO %s SELECT %d, 'data%d', 'data%d'", TABLE_NAME_PARTITIONED, i, i, i);
    }
    icebergTablePartitioned.refresh();

    String invalidFilePath1 = icebergTablePartitioned.location() + "/data=hello/spec=a/";
    SimpleDataUtil.writeFile(
        icebergTablePartitioned,
        icebergTablePartitioned.schema(),
        icebergTablePartitioned.spec(),
        getConf(),
        invalidFilePath1,
        "invalid-file-1.parquet",
        Lists.newArrayList(
            GenericRowData.of(1, StringData.fromString("hello"), StringData.fromString("a"))));

    waitUntilAfter(System.currentTimeMillis());
    CleanOrphanFilesAction action =
        Actions.forTable(icebergTablePartitioned)
            .cleanOrphanDataFiles()
            .olderThan(System.currentTimeMillis());
    List<String> result = getResult(action);

    assertThat(result).as("One orphan files in leaf partitions should be deleted.").hasSize(1);
    List<Row> rows = sql("select * from %s", TABLE_NAME_PARTITIONED);
    assertThat(rows).as("All data should be read.").hasSize(50);
  }

  @TestTemplate
  void testManyLeafPartitions() throws IOException {
    for (int i = 0; i < 50; i++) {
      sql("INSERT INTO %s SELECT %d, 'data%d', 'data%d'", TABLE_NAME_PARTITIONED, i, i % 3, i);
    }
    icebergTablePartitioned.refresh();

    String invalidFilePath1 = icebergTablePartitioned.location() + "/data=hello/spec=a/";
    SimpleDataUtil.writeFile(
        icebergTablePartitioned,
        icebergTablePartitioned.schema(),
        icebergTablePartitioned.spec(),
        getConf(),
        invalidFilePath1,
        "invalid-file-1.parquet",
        Lists.newArrayList(
            GenericRowData.of(1, StringData.fromString("hello"), StringData.fromString("a"))));

    waitUntilAfter(System.currentTimeMillis());
    CleanOrphanFilesAction action =
        Actions.forTable(icebergTablePartitioned)
            .cleanOrphanDataFiles()
            .olderThan(System.currentTimeMillis());
    List<String> result = getResult(action);

    assertThat(result).as("One orphan files in leaf partitions should be deleted.").hasSize(1);

    List<Row> rows = sql("select * from %s", TABLE_NAME_PARTITIONED);
    assertThat(rows).hasSize(50);
  }

  @TestTemplate
  void testRemoveOrphanFilesWithStatisticFiles() throws Exception {
    sql("INSERT INTO %s SELECT 1, 'hello'", TABLE_NAME_UNPARTITIONED);
    sql("INSERT INTO %s SELECT 2, 'world'", TABLE_NAME_UNPARTITIONED);
    icebergTableUnPartitioned.refresh();

    long snapshotId = icebergTableUnPartitioned.currentSnapshot().snapshotId();
    long snapshotSequenceNumber = icebergTableUnPartitioned.currentSnapshot().sequenceNumber();
    File statsLocation =
        new File(new URI(icebergTableUnPartitioned.location()))
            .toPath()
            .resolve("data")
            .resolve("some-stats-file")
            .toFile();
    StatisticsFile statisticsFile;
    try (PuffinWriter puffinWriter =
        Puffin.write(org.apache.iceberg.Files.localOutput(statsLocation)).build()) {
      puffinWriter.add(
          new Blob(
              "some-blob-type",
              ImmutableList.of(1),
              snapshotId,
              snapshotSequenceNumber,
              ByteBuffer.wrap("blob content".getBytes(StandardCharsets.UTF_8))));
      puffinWriter.finish();
      statisticsFile =
          new GenericStatisticsFile(
              snapshotId,
              statsLocation.toString(),
              puffinWriter.fileSize(),
              puffinWriter.footerSize(),
              puffinWriter.writtenBlobsMetadata().stream()
                  .map(GenericBlobMetadata::from)
                  .collect(ImmutableList.toImmutableList()));
    }

    Transaction transaction = icebergTableUnPartitioned.newTransaction();
    transaction.updateStatistics().setStatistics(statisticsFile).commit();
    transaction.commitTransaction();
    Actions.forTable(icebergTableUnPartitioned)
        .cleanOrphanDataFiles()
        .olderThan(System.currentTimeMillis())
        .execute();
    assertThat(statsLocation).as("stats file should exist").exists();
    assertThat(statsLocation.length())
        .as("stats file length")
        .isEqualTo(statisticsFile.fileSizeInBytes());

    transaction = icebergTableUnPartitioned.newTransaction();
    transaction.updateStatistics().removeStatistics(statisticsFile.snapshotId()).commit();
    transaction.commitTransaction();

    CleanOrphanFilesAction action =
        Actions.forTable(icebergTableUnPartitioned)
            .cleanOrphanDataFiles()
            .olderThan(System.currentTimeMillis());
    List<String> orphanFileLocations = getResult(action);
    assertThat(orphanFileLocations).hasSize(1).containsExactly(statsLocation.toURI().toString());
    assertThat(statsLocation).as("stats file should be deleted").doesNotExist();
  }

  @TestTemplate
  void testHiddenPathsIgnored() throws IOException {
    String testHiddenPathTable = "test_hidden_path_table";
    sql(
        "CREATE TABLE %s (id INT, _dt STRING, data STRING) "
            + "PARTITIONED BY (_dt, data) WITH ("
            + "  'format-version'='%s',"
            + "  'partition-projection.enabled'='true'"
            + ")",
        testHiddenPathTable, formatVersion);

    Table hiddenPathTable =
        validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, testHiddenPathTable));

    sql("INSERT INTO %s SELECT 1, '2023-01-01', 'valid'", testHiddenPathTable);
    hiddenPathTable.refresh();

    // should be deleted.
    String hiddenPath = hiddenPathTable.location() + "/_dt=2023-01-01/";
    SimpleDataUtil.writeFile(
        hiddenPathTable,
        hiddenPathTable.schema(),
        hiddenPathTable.spec(),
        getConf(),
        hiddenPath,
        "hidden-file-2.parquet",
        Lists.newArrayList(
            GenericRowData.of(
                3, StringData.fromString("2023-01-02"), StringData.fromString("orphan"))));

    // should be deleted.
    String hiddenPath1 = hiddenPathTable.location() + "/_dt2=2023-01-01/";
    SimpleDataUtil.writeFile(
        hiddenPathTable,
        hiddenPathTable.schema(),
        hiddenPathTable.spec(),
        getConf(),
        hiddenPath1,
        "hidden-file-2.parquet",
        Lists.newArrayList(
            GenericRowData.of(
                3, StringData.fromString("2023-01-02"), StringData.fromString("orphan"))));

    CleanOrphanFilesAction action =
        Actions.forTable(hiddenPathTable)
            .cleanOrphanDataFiles()
            .olderThan(System.currentTimeMillis());

    List<String> orphanFiles = getResult(action);

    assertThat(orphanFiles)
        .as("Should delete 1 files.")
        .hasSize(1)
        .containsExactly(
            "file:"
                + CleanOrphanFilesAction.normalizePath(hiddenPath + "hidden-file-2.parquet")
                    .getPath());

    List<Row> result = sql("SELECT id FROM %s", testHiddenPathTable);
    assertThat(result).extracting(r -> r.getField(0)).containsExactly(1);

    sql("DROP TABLE %s", testHiddenPathTable);
  }

  @TestTemplate
  void testPathsWithValidFileHavingNoAuthority() throws Exception {
    List<String> validFiles = Lists.newArrayList("hdfs:///dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("hdfs://servicename/dir1/dir2/file1");
    executeTest(validFiles, actualFiles, Lists.newArrayList());
  }

  @TestTemplate
  void testPathsWithActualFileHavingNoAuthority() throws Exception {
    List<String> validFiles = Lists.newArrayList("hdfs://servicename/dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("hdfs:///dir1/dir2/file1");
    executeTest(validFiles, actualFiles, Lists.newArrayList());
  }

  @TestTemplate
  void testPathsWithEqualSchemes() throws Exception {
    List<String> validFiles = Lists.newArrayList("scheme1://bucket1/dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("scheme2://bucket1/dir1/dir2/file1");
    assertThatThrownBy(
            () ->
                executeTest(
                    validFiles,
                    actualFiles,
                    Lists.newArrayList(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    DeleteOrphanFiles.PrefixMismatchMode.ERROR))
        .isInstanceOf(ValidationException.class)
        .hasMessageEndingWith("Conflicting authorities/schemes: [(scheme1, scheme2)].");

    Map<String, String> equalSchemes = Maps.newHashMap();
    equalSchemes.put("scheme1", "scheme");
    equalSchemes.put("scheme2", "scheme");
    executeTest(
        validFiles,
        actualFiles,
        Lists.newArrayList(),
        equalSchemes,
        ImmutableMap.of(),
        DeleteOrphanFiles.PrefixMismatchMode.ERROR);
  }

  @TestTemplate
  void testPathsWithEqualAuthorities() throws Exception {
    List<String> validFiles = Lists.newArrayList("hdfs://servicename1/dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("hdfs://servicename2/dir1/dir2/file1");
    assertThatThrownBy(
            () ->
                executeTest(
                    validFiles,
                    actualFiles,
                    Lists.newArrayList(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    DeleteOrphanFiles.PrefixMismatchMode.ERROR))
        .isInstanceOf(ValidationException.class)
        .hasMessageEndingWith("Conflicting authorities/schemes: [(servicename1, servicename2)].");

    Map<String, String> equalAuthorities = Maps.newHashMap();
    equalAuthorities.put("servicename1", "servicename");
    equalAuthorities.put("servicename2", "servicename");
    executeTest(
        validFiles,
        actualFiles,
        Lists.newArrayList(),
        ImmutableMap.of(),
        equalAuthorities,
        DeleteOrphanFiles.PrefixMismatchMode.ERROR);
  }

  @TestTemplate
  void testRemoveOrphanFileActionWithDeleteMode() throws Exception {
    List<String> validFiles = Lists.newArrayList("hdfs://servicename1/dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("hdfs://servicename2/dir1/dir2/file1");

    executeTest(
        validFiles,
        actualFiles,
        Lists.newArrayList("hdfs://servicename2/dir1/dir2/file1"),
        ImmutableMap.of(),
        ImmutableMap.of(),
        DeleteOrphanFiles.PrefixMismatchMode.DELETE);
  }

  private void executeTest(
      List<String> validFiles, List<String> actualFiles, List<String> expectedOrphanFiles)
      throws Exception {
    executeTest(
        validFiles,
        actualFiles,
        expectedOrphanFiles,
        ImmutableMap.of(),
        ImmutableMap.of(),
        DeleteOrphanFiles.PrefixMismatchMode.IGNORE);
  }

  private void executeTest(
      List<String> validFiles,
      List<String> actualFiles,
      List<String> expectedOrphanFiles,
      Map<String, String> equalSchemes,
      Map<String, String> equalAuthorities,
      DeleteOrphanFiles.PrefixMismatchMode mode)
      throws Exception {

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
    Set<CleanOrphanFilesAction.NormalizedPath> validNormalizedPaths = Sets.newHashSet();
    for (String actualFile : validFiles) {
      CleanOrphanFilesAction.NormalizedPath normalizedPath =
          CleanOrphanFilesAction.normalizePath(equalSchemes, equalAuthorities, actualFile);
      validNormalizedPaths.add(normalizedPath);
    }

    Set<CleanOrphanFilesAction.NormalizedPath> actualNormalizedPaths = Sets.newHashSet();
    for (String actualFile : actualFiles) {
      CleanOrphanFilesAction.NormalizedPath normalizedPath =
          CleanOrphanFilesAction.normalizePath(equalSchemes, equalAuthorities, actualFile);
      actualNormalizedPaths.add(normalizedPath);
    }
    List<String> orphanFiles =
        CleanOrphanFilesAction.findOrphanFiles(
            env.fromCollection(validNormalizedPaths),
            env.fromCollection(actualNormalizedPaths),
            mode);
    assertThat(orphanFiles).isEqualTo(expectedOrphanFiles);
  }

  private List<String> getResult(CleanOrphanFilesAction action) {
    DeleteOrphanFiles.Result result = action.execute();
    return StreamSupport.stream(result.orphanFileLocations().spliterator(), false)
        .collect(Collectors.toList());
  }
}
