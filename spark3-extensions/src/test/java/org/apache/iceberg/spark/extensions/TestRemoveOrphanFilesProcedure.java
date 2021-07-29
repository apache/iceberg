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

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.MockFileSystem;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.WRITE_AUDIT_PUBLISH_ENABLED;

public class TestRemoveOrphanFilesProcedure extends SparkExtensionsTestBase {

  private SparkSession session = null;
  private String tmpLocation = null;

  public TestRemoveOrphanFilesProcedure(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @BeforeClass
  public static void startMetastoreAndSpark() {
    Configuration conf = new Configuration();
    conf.setClass("fs.mock.impl", MockFileSystem.class, FileSystem.class);
    SparkTestBase.metastore = new TestHiveMetastore();
    metastore.start(conf);
    SparkTestBase.hiveConf = metastore.hiveConf();

    SparkTestBase.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.testing", "true")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .config("spark.hadoop." + "fs.mock.impl", MockFileSystem.class.getName())
        .config("spark.sql.shuffle.partitions", "4")
        .enableHiveSupport()
        .getOrCreate();

    SparkTestBase.catalog = (HiveCatalog)
        CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String getTableLocation(boolean mockSchema, boolean mockAuthority) throws IOException {
    tmpLocation = temp.newFolder().getAbsolutePath();
    if (!mockSchema) {
      return tmpLocation;
    } else {
      String path;
      if (mockAuthority) {
        path = "mock://localhost:9000" + tmpLocation;
      } else {
        path = "mock:" + tmpLocation;
      }
      return path;
    }
  }

  private Catalog setupHadoopCatalog(SparkSession sparkSession, String catalogName, String tableLocation) {
    Catalog hadoopCatalog = new HadoopCatalog(sparkSession.sessionState().newHadoopConf(), tableLocation);
    sparkSession.conf().set("spark.sql.catalog." + catalogName, SparkCatalog.class.getName());
    sparkSession.conf().set("spark.sql.catalog." + catalogName + ".type", "hadoop");
    sparkSession.conf().set("spark.sql.catalog." + catalogName + ".warehouse", tableLocation);
    sql(sparkSession, "CREATE NAMESPACE IF NOT EXISTS default");
    return hadoopCatalog;
  }

  protected List<Object[]> sql(SparkSession sparkSession, String query, Object... args) {
    List<Row> rows = sparkSession.sql(String.format(query, args)).collectAsList();
    if (rows.size() < 1) {
      return ImmutableList.of();
    }

    return rowsToJava(rows);
  }

  @After
  public void removeTable() throws IOException {
    if (session == null) {
      session = spark;
    }
    sql(session, "DROP TABLE IF EXISTS %s", tableName);
    sql(session, "DROP TABLE IF EXISTS p", tableName);
    if (tmpLocation != null) {
      Path path = new Path(tmpLocation);
      FileSystem fs = path.getFileSystem(session.sessionState().newHadoopConf());
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      tmpLocation = null;
    }
    session = null;
  }

  @Test
  public void testRemoveOrphanFilesInEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    List<Object[]> output = sql(
        "CALL %s.system.remove_orphan_files('%s')",
        catalogName, tableIdent);
    assertEquals("Should be no orphan files", ImmutableList.of(), output);

    assertEquals("Should have no rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));
  }

  private void testRemoveOrphanFilesInDataFolder(boolean mockSchema, boolean mockAuthority) throws IOException {
    session = spark;
    String tableLocation = getTableLocation(mockSchema, mockAuthority);
    Catalog catalog = validationCatalog;
    if (catalogName.equals("testhadoop")) {
      // HadoopCatalog doesn't support reset warehouse location, so we create a new SparkSession.
      session = spark.newSession();
      catalog = setupHadoopCatalog(session, catalogName, tableLocation);
      sql(session, "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
      session.sql(String.format("select * from %s.manifests", tableName)).show();
    } else {
      // give a fresh location to Hive tables as Spark will not clean up the table location
      // correctly while dropping tables through spark_catalog
      sql(session, "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, temp.newFolder());
    }

    sql(session, "INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql(session, "INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = catalog.loadTable(tableIdent);

    String metadataLocation = table.location() + "/metadata";
    String dataLocation = table.location() + "/data";

    // produce orphan files in the data location using parquet
    sql("CREATE TABLE p (id bigint) USING parquet LOCATION '%s'", dataLocation);
    sql("INSERT INTO TABLE p VALUES (1)");

    // wait to ensure files are old enough
    waitUntilAfter(System.currentTimeMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    // check for orphans in the metadata folder
    List<Object[]> output1 = sql(session,
        "CALL %s.system.remove_orphan_files(" +
            "table => '%s'," +
            "older_than => TIMESTAMP '%s'," +
            "location => '%s')",
        catalogName, tableIdent, currentTimestamp, metadataLocation);
    assertEquals("Should be no orphan files in the metadata folder", ImmutableList.of(), output1);

    // check for orphans in the table location
    List<Object[]> output2 = sql(session,
        "CALL %s.system.remove_orphan_files(" +
            "table => '%s'," +
            "older_than => TIMESTAMP '%s')",
        catalogName, tableIdent, currentTimestamp);
    Assert.assertEquals("Should be orphan files in the data folder", 1, output2.size());

    // the previous call should have deleted all orphan files
    List<Object[]> output3 = sql(session,
        "CALL %s.system.remove_orphan_files(" +
            "table => '%s'," +
            "older_than => TIMESTAMP '%s')",
        catalogName, tableIdent, currentTimestamp);
    Assert.assertEquals("Should be no more orphan files in the data folder", 0, output3.size());

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql(session, "SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testRemoveOrphanFilesInDataFolderNormalPath() throws IOException {
    testRemoveOrphanFilesInDataFolder(false, false);
  }

  @Test
  public void testRemoveOrphanFilesInDataFolderWithSchema() throws IOException {
    testRemoveOrphanFilesInDataFolder(true, false);
  }

  @Test
  public void testRemoveOrphanFilesInDataFolderWithSchemaAndAuthority() throws IOException {
    testRemoveOrphanFilesInDataFolder(true, true);
  }

  private void testRemoveOrphanFilesDryRun(boolean mockSchema, boolean mockAuthority) throws IOException {
    session = spark;
    String tableLocation = getTableLocation(mockSchema, mockAuthority);
    Catalog catalog = validationCatalog;
    if (catalogName.equals("testhadoop")) {
      // HadoopCatalog doesn't support reset warehouse location, so we create a new SparkSession.
      session = spark.newSession();
      catalog = setupHadoopCatalog(session, catalogName, tableLocation);
      sql(session, "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    } else {
      // give a fresh location to Hive tables as Spark will not clean up the table location
      // correctly while dropping tables through spark_catalog
      sql(session, "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
          tableName, tableLocation);
    }

    sql(session, "INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql(session, "INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = catalog.loadTable(tableIdent);

    // produce orphan files in the table location using parquet
    sql(session, "CREATE TABLE p (id bigint) USING parquet LOCATION '%s'", table.location());
    sql(session, "INSERT INTO TABLE p VALUES (1)");

    // wait to ensure files are old enough
    waitUntilAfter(System.currentTimeMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    // check for orphans without deleting
    List<Object[]> output1 = sql(session,
        "CALL %s.system.remove_orphan_files(" +
            "table => '%s'," +
            "older_than => TIMESTAMP '%s'," +
            "dry_run => true)",
        catalogName, tableIdent, currentTimestamp);
    Assert.assertEquals("Should be one orphan files", 1, output1.size());

    // actually delete orphans
    List<Object[]> output2 = sql(session,
        "CALL %s.system.remove_orphan_files(" +
            "table => '%s'," +
            "older_than => TIMESTAMP '%s')",
        catalogName, tableIdent, currentTimestamp);
    Assert.assertEquals("Should be one orphan files", 1, output2.size());

    // the previous call should have deleted all orphan files
    List<Object[]> output3 = sql(session,
        "CALL %s.system.remove_orphan_files(" +
            "table => '%s'," +
            "older_than => TIMESTAMP '%s')",
        catalogName, tableIdent, currentTimestamp);
    Assert.assertEquals("Should be no more orphan files", 0, output3.size());

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql(session, "SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testRemoveOrphanFilesDryRunNormalPath() throws IOException {
    testRemoveOrphanFilesDryRun(false, false);
  }

  @Test
  public void testRemoveOrphanFilesDryRunPathWithSchema() throws IOException {
    testRemoveOrphanFilesDryRun(true, false);
  }

  @Test
  public void testRemoveOrphanFilesDryRunPathWithSchemaAndAuthority() throws IOException {
    testRemoveOrphanFilesDryRun(true, true);
  }

  @Test
  public void testRemoveOrphanFilesGCDisabled() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'false')", tableName, GC_ENABLED);

    AssertHelpers.assertThrows("Should reject call",
        ValidationException.class, "Cannot remove orphan files: GC is disabled",
        () -> sql("CALL %s.system.remove_orphan_files('%s')", catalogName, tableIdent));
  }

  @Test
  public void testRemoveOrphanFilesWap() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    spark.conf().set("spark.wap.id", "1");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    List<Object[]> output = sql(
        "CALL %s.system.remove_orphan_files('%s')", catalogName, tableIdent);
    assertEquals("Should be no orphan files", ImmutableList.of(), output);
  }

  @Test
  public void testInvalidRemoveOrphanFilesCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.remove_orphan_files('n', table => 't')", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.remove_orphan_files('n', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.remove_orphan_files()", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        AnalysisException.class, "Wrong arg type",
        () -> sql("CALL %s.system.remove_orphan_files('n', 2.2)", catalogName));

    AssertHelpers.assertThrows("Should reject calls with empty table identifier",
        IllegalArgumentException.class, "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.remove_orphan_files('')", catalogName));
  }
}
