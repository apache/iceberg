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

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestRemoveOrphanFilesProcedure extends SparkExtensionsTestBase {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public TestRemoveOrphanFilesProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s PURGE", tableName);
    sql("DROP TABLE IF EXISTS p PURGE");
  }

  @Test
  public void testRemoveOrphanFilesInEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    List<Object[]> output =
        sql("CALL %s.system.remove_orphan_files('%s')", catalogName, tableIdent);
    assertEquals("Should be no orphan files", ImmutableList.of(), output);

    assertEquals("Should have no rows", ImmutableList.of(), sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testRemoveOrphanFilesInDataFolder() throws IOException {
    if (catalogName.equals("testhadoop")) {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    } else {
      // give a fresh location to Hive tables as Spark will not clean up the table location
      // correctly while dropping tables through spark_catalog
      sql(
          "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, temp.newFolder());
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
    Assert.assertEquals("Should be orphan files in the data folder", 1, output2.size());

    // the previous call should have deleted all orphan files
    List<Object[]> output3 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    Assert.assertEquals("Should be no more orphan files in the data folder", 0, output3.size());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testRemoveOrphanFilesDryRun() throws IOException {
    if (catalogName.equals("testhadoop")) {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    } else {
      // give a fresh location to Hive tables as Spark will not clean up the table location
      // correctly while dropping tables through spark_catalog
      sql(
          "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, temp.newFolder());
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
    Assert.assertEquals("Should be one orphan files", 1, output1.size());

    // actually delete orphans
    List<Object[]> output2 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    Assert.assertEquals("Should be one orphan files", 1, output2.size());

    // the previous call should have deleted all orphan files
    List<Object[]> output3 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, currentTimestamp);
    Assert.assertEquals("Should be no more orphan files", 0, output3.size());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testRemoveOrphanFilesGCDisabled() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'false')", tableName, GC_ENABLED);

    AssertHelpers.assertThrows(
        "Should reject call",
        ValidationException.class,
        "Cannot delete orphan files: GC is disabled",
        () -> sql("CALL %s.system.remove_orphan_files('%s')", catalogName, tableIdent));

    // reset the property to enable the table purging in removeTable.
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, GC_ENABLED);
  }

  @Test
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

  @Test
  public void testInvalidRemoveOrphanFilesCases() {
    AssertHelpers.assertThrows(
        "Should not allow mixed args",
        AnalysisException.class,
        "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.remove_orphan_files('n', table => 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class,
        "not found",
        () -> sql("CALL %s.custom.remove_orphan_files('n', 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls without all required args",
        AnalysisException.class,
        "Missing required parameters",
        () -> sql("CALL %s.system.remove_orphan_files()", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls with invalid arg types",
        AnalysisException.class,
        "Wrong arg type",
        () -> sql("CALL %s.system.remove_orphan_files('n', 2.2)", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls with empty table identifier",
        IllegalArgumentException.class,
        "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.remove_orphan_files('')", catalogName));
  }

  @Test
  public void testConcurrentRemoveOrphanFiles() throws IOException {
    if (catalogName.equals("testhadoop")) {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    } else {
      // give a fresh location to Hive tables as Spark will not clean up the table location
      // correctly while dropping tables through spark_catalog
      sql(
          "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, temp.newFolder());
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
    Assert.assertEquals("Should be orphan files in the data folder", 4, output.size());

    // the previous call should have deleted all orphan files
    List<Object[]> output3 =
        sql(
            "CALL %s.system.remove_orphan_files("
                + "table => '%s',"
                + "max_concurrent_deletes => %s,"
                + "older_than => TIMESTAMP '%s')",
            catalogName, tableIdent, 4, currentTimestamp);
    Assert.assertEquals("Should be no more orphan files in the data folder", 0, output3.size());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testConcurrentRemoveOrphanFilesWithInvalidInput() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    AssertHelpers.assertThrows(
        "Should throw an error when max_concurrent_deletes = 0",
        IllegalArgumentException.class,
        "max_concurrent_deletes should have value > 0",
        () ->
            sql(
                "CALL %s.system.remove_orphan_files(table => '%s', max_concurrent_deletes => %s)",
                catalogName, tableIdent, 0));

    AssertHelpers.assertThrows(
        "Should throw an error when max_concurrent_deletes < 0 ",
        IllegalArgumentException.class,
        "max_concurrent_deletes should have value > 0",
        () ->
            sql(
                "CALL %s.system.remove_orphan_files(table => '%s', max_concurrent_deletes => %s)",
                catalogName, tableIdent, -1));

    String tempViewName = "file_list_test";
    spark.emptyDataFrame().createOrReplaceTempView(tempViewName);

    AssertHelpers.assertThrows(
        "Should throw an error if file_list_view is missing required columns",
        IllegalArgumentException.class,
        "does not exist. Available:",
        () ->
            sql(
                "CALL %s.system.remove_orphan_files(table => '%s', file_list_view => '%s')",
                catalogName, tableIdent, tempViewName));

    spark
        .createDataset(Lists.newArrayList(), Encoders.tuple(Encoders.INT(), Encoders.TIMESTAMP()))
        .toDF("file_path", "last_modified")
        .createOrReplaceTempView(tempViewName);

    AssertHelpers.assertThrows(
        "Should throw an error if file_path has wrong type",
        IllegalArgumentException.class,
        "Invalid file_path column",
        () ->
            sql(
                "CALL %s.system.remove_orphan_files(table => '%s', file_list_view => '%s')",
                catalogName, tableIdent, tempViewName));

    spark
        .createDataset(Lists.newArrayList(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
        .toDF("file_path", "last_modified")
        .createOrReplaceTempView(tempViewName);

    AssertHelpers.assertThrows(
        "Should throw an error if last_modified has wrong type",
        IllegalArgumentException.class,
        "Invalid last_modified column",
        () ->
            sql(
                "CALL %s.system.remove_orphan_files(table => '%s', file_list_view => '%s')",
                catalogName, tableIdent, tempViewName));
  }

  @Test
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
    Assert.assertEquals(
        "Should have 1 delete manifest", 1, TestHelpers.deleteManifests(table).size());
    Assert.assertEquals("Should have 1 delete file", 1, TestHelpers.deleteFiles(table).size());
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
    Assert.assertEquals("Should be no orphan files", 0, output.size());

    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Assert.assertTrue("Delete manifest should still exist", localFs.exists(deleteManifestPath));
    Assert.assertTrue("Delete file should still exist", localFs.exists(deleteFilePath));

    records.remove(new SimpleRecord(1, "a"));
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableName);
    List<SimpleRecord> actualRecords =
        resultDF.as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }
}
