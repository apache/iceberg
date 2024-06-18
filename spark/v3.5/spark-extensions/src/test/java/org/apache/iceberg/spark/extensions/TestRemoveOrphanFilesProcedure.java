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

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.WRITE_AUDIT_PUBLISH_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.FilePathLastModifiedRecord;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestRemoveOrphanFilesProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s PURGE", tableName);
    sql("DROP TABLE IF EXISTS p PURGE");
  }

  @TestTemplate
  public void testRemoveOrphanFilesInEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    List<Object[]> output =
        sql("CALL %s.system.remove_orphan_files('%s')", catalogName, tableIdent);
    assertEquals("Should be no orphan files", ImmutableList.of(), output);

    assertEquals("Should have no rows", ImmutableList.of(), sql("SELECT * FROM %s", tableName));
  }

  @TestTemplate
  public void testRemoveOrphanFilesInDataFolder() throws IOException {
    if (catalogName.equals("testhadoop")) {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    } else {
      // give a fresh location to Hive tables as Spark will not clean up the table location
      // correctly while dropping tables through spark_catalog
      sql(
          "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, java.nio.file.Files.createTempDirectory(temp, "junit"));
    }
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    String metadataLocation = table.location() + "/metadata";
    String dataLocation = table.location() + "/data";

    // produce orphan files in the data location using parquet
    sql("CREATE TABLE p (id bigint) USING parquet LOCATION '%s'", dataLocation);
    sql("INSERT INTO TABLE p VALUES (1)");

    // wait to ensure files are old enough
    waitUntilAfter(System.currentTimeMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    // check for orphans in the metadata folder
    List<Object[]> output1 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s',"
                + "location => '%s')",
            catalogName, tableIdent, currentTimestamp, metadataLocation);
    assertEquals("Should be no orphan files in the metadata folder", ImmutableList.of(), output1);

    // check for orphans in the table location
    List<Object[]> output2 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output2).as("Should be orphan files in the data folder").hasSize(1);

    // the previous call should have deleted all orphan files
    List<Object[]> output3 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output3).as("Should be no more orphan files in the data folder").hasSize(0);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testRemoveOrphanFilesDryRun() throws IOException {
    if (catalogName.equals("testhadoop")) {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    } else {
      // give a fresh location to Hive tables as Spark will not clean up the table location
      // correctly while dropping tables through spark_catalog
      sql(
          "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, java.nio.file.Files.createTempDirectory(temp, "junit"));
    }

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    // produce orphan files in the table location using parquet
    sql("CREATE TABLE p (id bigint) USING parquet LOCATION '%s'", table.location());
    sql("INSERT INTO TABLE p VALUES (1)");

    // wait to ensure files are old enough
    waitUntilAfter(System.currentTimeMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    // check for orphans without deleting
    List<Object[]> output1 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s',"
                + "dry_run => true)",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output1).as("Should be one orphan files").hasSize(1);

    // actually delete orphans
    List<Object[]> output2 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output2).as("Should be one orphan files").hasSize(1);

    // the previous call should have deleted all orphan files
    List<Object[]> output3 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output3).as("Should be no more orphan files").hasSize(0);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testRemoveOrphanFilesGCDisabled() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'false')", tableName, GC_ENABLED);

    assertThatThrownBy(
            () -> sql("CALL %s.system.remove_orphan_files('%s')", catalogName, tableIdent))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Cannot delete orphan files: GC is disabled (deleting files may corrupt other tables)");

    // reset the property to enable the table purging in removeTable.
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, GC_ENABLED);
  }

  @TestTemplate
  public void testRemoveOrphanFilesWap() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    spark.conf().set("spark.wap.id", "1");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    List<Object[]> output =
        sql("CALL %s.system.remove_orphan_files('%s')", catalogName, tableIdent);
    assertEquals("Should be no orphan files", ImmutableList.of(), output);
  }

  @TestTemplate
  public void testInvalidRemoveOrphanFilesCases() {
    assertThatThrownBy(
            () -> sql("CALL %s.system.remove_orphan_files('n', table => 't')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Named and positional arguments cannot be mixed");

    assertThatThrownBy(() -> sql("CALL %s.custom.remove_orphan_files('n', 't')", catalogName))
        .isInstanceOf(NoSuchProcedureException.class)
        .hasMessage("Procedure custom.remove_orphan_files not found");

    assertThatThrownBy(() -> sql("CALL %s.system.remove_orphan_files()", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [table]");

    assertThatThrownBy(() -> sql("CALL %s.system.remove_orphan_files('n', 2.2)", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Wrong arg type for older_than");

    assertThatThrownBy(() -> sql("CALL %s.system.remove_orphan_files('')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument table");
  }

  @TestTemplate
  public void testConcurrentRemoveOrphanFiles() throws IOException {
    if (catalogName.equals("testhadoop")) {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    } else {
      // give a fresh location to Hive tables as Spark will not clean up the table location
      // correctly while dropping tables through spark_catalog
      sql(
          "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, java.nio.file.Files.createTempDirectory(temp, "junit"));
    }

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    String metadataLocation = table.location() + "/metadata";
    String dataLocation = table.location() + "/data";

    // produce orphan files in the data location using parquet
    sql("CREATE TABLE p (id bigint) USING parquet LOCATION '%s'", dataLocation);
    sql("INSERT INTO TABLE p VALUES (1)");
    sql("INSERT INTO TABLE p VALUES (10)");
    sql("INSERT INTO TABLE p VALUES (100)");
    sql("INSERT INTO TABLE p VALUES (1000)");

    // wait to ensure files are old enough
    waitUntilAfter(System.currentTimeMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    // check for orphans in the table location
    List<Object[]> output =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "max_concurrent_deletes => %s,"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, 4, currentTimestamp);
    assertThat(output).as("Should be orphan files in the data folder").hasSize(4);

    // the previous call should have deleted all orphan files
    List<Object[]> output3 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "max_concurrent_deletes => %s,"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, 4, currentTimestamp);
    assertThat(output3).as("Should be no more orphan files in the data folder").hasSize(0);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testConcurrentRemoveOrphanFilesWithInvalidInput() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.remove_orphan_files(table => '%s', max_concurrent_deletes => %s)",
                    catalogName, tableIdent, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("max_concurrent_deletes should have value > 0, value: 0");

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.remove_orphan_files(table => '%s', max_concurrent_deletes => %s)",
                    catalogName, tableIdent, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("max_concurrent_deletes should have value > 0, value: -1");

    String tempViewName = "file_list_test";
    spark.emptyDataFrame().createOrReplaceTempView(tempViewName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.remove_orphan_files(table => '%s', file_list_view => '%s')",
                    catalogName, tableIdent, tempViewName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("file_path does not exist. Available: ");

    spark
        .createDataset(Lists.newArrayList(), Encoders.tuple(Encoders.INT(), Encoders.TIMESTAMP()))
        .toDF("file_path", "last_modified")
        .createOrReplaceTempView(tempViewName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.remove_orphan_files(table => '%s', file_list_view => '%s')",
                    catalogName, tableIdent, tempViewName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file_path column: IntegerType is not a string");

    spark
        .createDataset(Lists.newArrayList(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
        .toDF("file_path", "last_modified")
        .createOrReplaceTempView(tempViewName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.remove_orphan_files(table => '%s', file_list_view => '%s')",
                    catalogName, tableIdent, tempViewName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid last_modified column: StringType is not a timestamp");
  }

  @TestTemplate
  public void testRemoveOrphanFilesWithDeleteFiles() throws Exception {
    sql(
        "CREATE TABLE %s (id int, data string) USING iceberg TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();
    sql("DELETE FROM %s WHERE id=1", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    assertThat(TestHelpers.deleteManifests(table)).as("Should have 1 delete manifest").hasSize(1);
    assertThat(TestHelpers.deleteFiles(table)).as("Should have 1 delete file").hasSize(1);
    Path deleteManifestPath = new Path(TestHelpers.deleteManifests(table).iterator().next().path());
    Path deleteFilePath =
        new Path(String.valueOf(TestHelpers.deleteFiles(table).iterator().next().path()));

    // wait to ensure files are old enough
    waitUntilAfter(System.currentTimeMillis());
    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    // delete orphans
    List<Object[]> output =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output).as("Should be no orphan files").hasSize(0);

    FileSystem localFs = FileSystem.getLocal(new Configuration());
    assertThat(localFs.exists(deleteManifestPath))
        .as("Delete manifest should still exist")
        .isTrue();
    assertThat(localFs.exists(deleteFilePath)).as("Delete file should still exist").isTrue();

    records.remove(new SimpleRecord(1, "a"));
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableName);
    List<SimpleRecord> actualRecords =
        resultDF.as(Encoders.bean(SimpleRecord.class)).collectAsList();
    assertThat(actualRecords).as("Rows must match").isEqualTo(records);
  }

  @TestTemplate
  public void testRemoveOrphanFilesWithStatisticFiles() throws Exception {
    sql(
        "CREATE TABLE %s USING iceberg "
            + "TBLPROPERTIES('format-version'='2') "
            + "AS SELECT 10 int, 'abc' data",
        tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);

    String statsFileName = "stats-file-" + UUID.randomUUID();
    File statsLocation =
        new File(new URI(table.location()))
            .toPath()
            .resolve("data")
            .resolve(statsFileName)
            .toFile();
    StatisticsFile statisticsFile;
    try (PuffinWriter puffinWriter = Puffin.write(Files.localOutput(statsLocation)).build()) {
      long snapshotId = table.currentSnapshot().snapshotId();
      long snapshotSequenceNumber = table.currentSnapshot().sequenceNumber();
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

    Transaction transaction = table.newTransaction();
    transaction
        .updateStatistics()
        .setStatistics(statisticsFile.snapshotId(), statisticsFile)
        .commit();
    transaction.commitTransaction();

    // wait to ensure files are old enough
    waitUntilAfter(System.currentTimeMillis());
    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    List<Object[]> output =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output).as("Should be no orphan files").isEmpty();

    assertThat(statsLocation).exists();
    assertThat(statsLocation).hasSize(statisticsFile.fileSizeInBytes());

    transaction = table.newTransaction();
    transaction.updateStatistics().removeStatistics(statisticsFile.snapshotId()).commit();
    transaction.commitTransaction();

    output =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output).as("Should be orphan files").hasSize(1);
    assertThat(Iterables.getOnlyElement(output))
        .as("Deleted files")
        .containsExactly(statsLocation.toURI().toString());
    assertThat(statsLocation).doesNotExist();
  }

  @TestTemplate
  public void testRemoveOrphanFilesWithPartitionStatisticFiles() throws Exception {
    sql(
        "CREATE TABLE %s USING iceberg "
            + "TBLPROPERTIES('format-version'='2') "
            + "AS SELECT 10 int, 'abc' data",
        tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);

    String partitionStatsLocation = ProcedureUtil.statsFileLocation(table.location());
    PartitionStatisticsFile partitionStatisticsFile =
        ProcedureUtil.writePartitionStatsFile(
            table.currentSnapshot().snapshotId(), partitionStatsLocation, table.io());

    commitPartitionStatsTxn(table, partitionStatisticsFile);

    // wait to ensure files are old enough
    waitUntilAfter(System.currentTimeMillis());
    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    List<Object[]> output =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output).as("Should be no orphan files").isEmpty();

    assertThat(new File(partitionStatsLocation)).as("partition stats file should exist").exists();

    removePartitionStatsTxn(table, partitionStatisticsFile);

    output =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    assertThat(output).as("Should be orphan files").hasSize(1);
    assertThat(Iterables.getOnlyElement(output))
        .as("Deleted files")
        .containsExactly("file:" + partitionStatsLocation);
    assertThat(new File(partitionStatsLocation))
        .as("partition stats file should be deleted")
        .doesNotExist();
  }

  private static void removePartitionStatsTxn(
      Table table, PartitionStatisticsFile partitionStatisticsFile) {
    Transaction transaction = table.newTransaction();
    transaction
        .updatePartitionStatistics()
        .removePartitionStatistics(partitionStatisticsFile.snapshotId())
        .commit();
    transaction.commitTransaction();
  }

  private static void commitPartitionStatsTxn(
      Table table, PartitionStatisticsFile partitionStatisticsFile) {
    Transaction transaction = table.newTransaction();
    transaction
        .updatePartitionStatistics()
        .setPartitionStatistics(partitionStatisticsFile)
        .commit();
    transaction.commitTransaction();
  }

  @TestTemplate
  public void testRemoveOrphanFilesProcedureWithPrefixMode()
      throws NoSuchTableException, ParseException, IOException {
    if (catalogName.equals("testhadoop")) {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    } else {
      sql(
          "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, java.nio.file.Files.createTempDirectory(temp, "junit"));
    }
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    String location = table.location();
    Path originalPath = new Path(location);

    URI uri = originalPath.toUri();
    Path newParentPath = new Path("file1", uri.getAuthority(), uri.getPath());

    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(new Path(newParentPath, "path/to/data-a.parquet").toString())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(new Path(newParentPath, "path/to/data-b.parquet").toString())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    table.newFastAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    Timestamp lastModifiedTimestamp = new Timestamp(10000);

    List<FilePathLastModifiedRecord> allFiles =
        Lists.newArrayList(
            new FilePathLastModifiedRecord(
                new Path(originalPath, "path/to/data-a.parquet").toString(), lastModifiedTimestamp),
            new FilePathLastModifiedRecord(
                new Path(originalPath, "path/to/data-b.parquet").toString(), lastModifiedTimestamp),
            new FilePathLastModifiedRecord(
                ReachableFileUtil.versionHintLocation(table), lastModifiedTimestamp));

    for (String file : ReachableFileUtil.metadataFileLocations(table, true)) {
      allFiles.add(new FilePathLastModifiedRecord(file, lastModifiedTimestamp));
    }

    for (ManifestFile manifest : TestHelpers.dataManifests(table)) {
      allFiles.add(new FilePathLastModifiedRecord(manifest.path(), lastModifiedTimestamp));
    }

    Dataset<Row> compareToFileList =
        spark
            .createDataFrame(allFiles, FilePathLastModifiedRecord.class)
            .withColumnRenamed("filePath", "file_path")
            .withColumnRenamed("lastModified", "last_modified");
    String fileListViewName = "files_view";
    compareToFileList.createOrReplaceTempView(fileListViewName);
    List<Object[]> orphanFiles =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "equal_schemes => map('file1', 'file'),"
                + "file_list_view => '%s')",
            catalogName, tableIdent, fileListViewName);
    assertThat(orphanFiles).isEmpty();

    // Test with no equal schemes
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.remove_orphan_files("
                        + "table => '%s',"
                        + "file_list_view => '%s')",
                    catalogName, tableIdent, fileListViewName))
        .isInstanceOf(ValidationException.class)
        .hasMessageEndingWith("Conflicting authorities/schemes: [(file1, file)].");

    // Drop table in afterEach has purge and fails due to invalid scheme "file1" used in this test
    // Dropping the table here
    sql("DROP TABLE %s", tableName);
  }

  @TestTemplate
  public void testRemoveOrphanFilesProcedureWithEqualAuthorities()
      throws NoSuchTableException, ParseException, IOException {
    if (catalogName.equals("testhadoop")) {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    } else {
      sql(
          "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, java.nio.file.Files.createTempDirectory(temp, "junit"));
    }
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Path originalPath = new Path(table.location());

    URI uri = originalPath.toUri();
    String originalAuthority = uri.getAuthority() == null ? "" : uri.getAuthority();
    Path newParentPath = new Path(uri.getScheme(), "localhost", uri.getPath());

    DataFile dataFile1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(new Path(newParentPath, "path/to/data-a.parquet").toString())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(new Path(newParentPath, "path/to/data-b.parquet").toString())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    table.newFastAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    Timestamp lastModifiedTimestamp = new Timestamp(10000);

    List<FilePathLastModifiedRecord> allFiles =
        Lists.newArrayList(
            new FilePathLastModifiedRecord(
                new Path(originalPath, "path/to/data-a.parquet").toString(), lastModifiedTimestamp),
            new FilePathLastModifiedRecord(
                new Path(originalPath, "path/to/data-b.parquet").toString(), lastModifiedTimestamp),
            new FilePathLastModifiedRecord(
                ReachableFileUtil.versionHintLocation(table), lastModifiedTimestamp));

    for (String file : ReachableFileUtil.metadataFileLocations(table, true)) {
      allFiles.add(new FilePathLastModifiedRecord(file, lastModifiedTimestamp));
    }

    for (ManifestFile manifest : TestHelpers.dataManifests(table)) {
      allFiles.add(new FilePathLastModifiedRecord(manifest.path(), lastModifiedTimestamp));
    }

    Dataset<Row> compareToFileList =
        spark
            .createDataFrame(allFiles, FilePathLastModifiedRecord.class)
            .withColumnRenamed("filePath", "file_path")
            .withColumnRenamed("lastModified", "last_modified");
    String fileListViewName = "files_view";
    compareToFileList.createOrReplaceTempView(fileListViewName);
    List<Object[]> orphanFiles =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "equal_authorities => map('localhost', '%s'),"
                + "file_list_view => '%s')",
            catalogName, tableIdent, originalAuthority, fileListViewName);
    assertThat(orphanFiles).isEmpty();

    // Test with no equal authorities
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.remove_orphan_files("
                        + "table => '%s',"
                        + "file_list_view => '%s')",
                    catalogName, tableIdent, fileListViewName))
        .isInstanceOf(ValidationException.class)
        .hasMessageEndingWith("Conflicting authorities/schemes: [(localhost, null)].");

    // Drop table in afterEach has purge and fails due to invalid authority "localhost"
    // Dropping the table here
    sql("DROP TABLE %s", tableName);
  }
}
