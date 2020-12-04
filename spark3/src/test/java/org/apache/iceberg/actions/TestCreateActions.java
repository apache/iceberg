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

package org.apache.iceberg.actions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;
import scala.Some;
import scala.collection.Seq;

public class TestCreateActions extends SparkCatalogTestBase {
  private static final String CREATE_PARTITIONED_PARQUET = "CREATE TABLE %s (id INT, data STRING) " +
      "using parquet PARTITIONED BY (id) LOCATION '%s'";
  private static final String CREATE_PARQUET = "CREATE TABLE %s (id INT, data STRING) " +
      "using parquet LOCATION '%s'";
  private static final String CREATE_HIVE_EXTERNAL_PARQUET = "CREATE EXTERNAL TABLE %s (data STRING) " +
      "PARTITIONED BY (id INT) STORED AS parquet LOCATION '%s'";
  private static final String CREATE_HIVE_PARQUET = "CREATE TABLE %s (data STRING) " +
      "PARTITIONED BY (id INT) STORED AS parquet";

  private static final String NAMESPACE = "default";

  @Parameterized.Parameters(name = "Catalog Name {0} - Options {2}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"spark_catalog", SparkSessionCatalog.class.getName(), ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
        )},
        new Object[] {"spark_catalog", SparkSessionCatalog.class.getName(), ImmutableMap.of(
            "type", "hadoop",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
        )},
        new Object[] { "testhive", SparkCatalog.class.getName(), ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"
        )},
        new Object[] { "testhadoop", SparkCatalog.class.getName(), ImmutableMap.of(
            "type", "hadoop",
            "default-namespace", "default"
        )}
    };
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private String baseTableName = "baseTable";
  private File tableDir;
  private String tableLocation;
  private final String type;
  private final TableCatalog catalog;

  public TestCreateActions(
      String catalogName,
      String implementation,
      Map<String, String> config) {
    super(catalogName, implementation, config);
    this.catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    this.type = config.get("type");
  }

  @Before
  public void before() {
    try {
      this.tableDir = temp.newFolder();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.tableLocation = tableDir.toURI().toString();

    spark.conf().set("hive.exec.dynamic.partition", "true");
    spark.conf().set("hive.exec.dynamic.partition.mode", "nonstrict");
    spark.sql(String.format("DROP TABLE IF EXISTS %s", baseTableName));

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").orderBy("data").write()
        .mode("append")
        .option("path", tableLocation)
        .saveAsTable(baseTableName);
  }

  @After
  public void after() throws IOException {
    // Drop the hive table.
    spark.sql(String.format("DROP TABLE IF EXISTS %s", baseTableName));
  }

  @Test
  public void testMigratePartitioned() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
    String source = sourceName("test_migrate_partitioned_table");
    String dest = source;
    createSourceTable(CREATE_PARTITIONED_PARQUET, source);
    assertMigratedFileCount(Actions.migrate(source), source, dest);
  }

  @Test
  public void testMigrateUnpartitioned() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
    String source = sourceName("test_migrate_unpartitioned_table");
    String dest = source;
    createSourceTable(CREATE_PARQUET, source);
    assertMigratedFileCount(Actions.migrate(source), source, dest);
  }

  @Test
  public void testSnapshotPartitioned() throws Exception {
    Assume.assumeTrue("Cannot snapshot with arbitrary location in a hadoop based catalog",
        !type.equals("hadoop"));
    File location = temp.newFolder();
    String source = sourceName("test_snapshot_partitioned_table");
    String dest = destName("iceberg_snapshot_partitioned");
    createSourceTable(CREATE_PARTITIONED_PARQUET, source);
    assertMigratedFileCount(Actions.snapshot(source, dest).withLocation(location.toString()), source, dest);
    assertIsolatedSnapshot(source, dest);
  }

  @Test
  public void testSnapshotUnpartitioned() throws Exception {
    Assume.assumeTrue("Cannot snapshot with arbitrary location in a hadoop based catalog",
        !type.equals("hadoop"));
    File location = temp.newFolder();
    String source = sourceName("test_snapshot_unpartitioned_table");
    String dest = destName("iceberg_snapshot_unpartitioned");
    createSourceTable(CREATE_PARQUET, source);
    assertMigratedFileCount(Actions.snapshot(source, dest).withLocation(location.toString()), source, dest);
    assertIsolatedSnapshot(source, dest);
  }

  @Test
  public void testSnapshotHiveTable() throws Exception {
    Assume.assumeTrue("Cannot snapshot with arbitrary location in a hadoop based catalog",
        !type.equals("hadoop"));
    File location = temp.newFolder();
    String source = sourceName("snapshot_hive_table");
    String dest = destName("iceberg_snapshot_hive_table");
    createSourceTable(CREATE_HIVE_EXTERNAL_PARQUET, source);
    assertMigratedFileCount(Actions.snapshot(source, dest).withLocation(location.toString()), source, dest);
    assertIsolatedSnapshot(source, dest);
  }

  @Test
  public void testMigrateHiveTable() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    String source = sourceName("migrate_hive_table");
    String dest = source;
    createSourceTable(CREATE_HIVE_EXTERNAL_PARQUET, source);
    assertMigratedFileCount(Actions.migrate(source), source, dest);
  }

  @Test
  public void testSnapshotManagedHiveTable() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    File location = temp.newFolder();
    String source = sourceName("snapshot_managed_hive_table");
    String dest = destName("iceberg_snapshot_managed_hive_table");
    createSourceTable(CREATE_HIVE_PARQUET, source);
    assertMigratedFileCount(Actions.snapshot(source, dest).withLocation(location.toString()), source, dest);
    assertIsolatedSnapshot(source, dest);
  }

  @Test
  public void testMigrateManagedHiveTable() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    File location = temp.newFolder();
    String source = sourceName("migrate_managed_hive_table");
    String dest = destName("iceberg_migrate_managed_hive_table");
    createSourceTable(CREATE_HIVE_PARQUET, source);
    assertMigratedFileCount(Actions.snapshot(source, dest).withLocation(location.toString()), source, dest);
  }

  @Test
  public void testProperties() throws Exception {
    String source = sourceName("test_properties_table");
    String dest = destName("iceberg_properties");
    Map<String, String> props = Maps.newHashMap();
    props.put("city", "New Orleans");
    props.put("note", "Jazz");
    createSourceTable(CREATE_PARQUET, source);
    for (Map.Entry<String, String> keyValue : props.entrySet()) {
      spark.sql(String.format("ALTER TABLE %s SET TBLPROPERTIES (\"%s\" = \"%s\")",
          source, keyValue.getKey(), keyValue.getValue()));
    }
    assertMigratedFileCount(Actions.snapshot(source, dest).withProperty("dogs", "sundance"), source, dest);
    SparkTable table = loadTable(dest);

    Map<String, String> expectedProps = Maps.newHashMap();
    expectedProps.putAll(props);
    expectedProps.put("dogs", "sundance");

    for (Map.Entry<String, String> entry : expectedProps.entrySet()) {
      Assert.assertTrue(
          "Created table missing property " + entry.getKey(),
          table.properties().containsKey(entry.getKey()));
      Assert.assertEquals("Property value is not the expected value",
          entry.getValue(), table.properties().get(entry.getKey()));
    }
  }

  @Test
  public void testSnapshotDefaultLocation() throws Exception {
    String source = sourceName("test_snapshot_default");
    String dest = destName("iceberg_snapshot_default");
    createSourceTable(CREATE_PARTITIONED_PARQUET, source);
    assertMigratedFileCount(Actions.snapshot(source, dest), source, dest);
    assertIsolatedSnapshot(source, dest);
  }

  private SparkTable loadTable(String name) throws NoSuchTableException, ParseException {
    return (SparkTable) catalog.loadTable(Spark3Util.catalogAndIdentifier(spark, name).identifier());
  }

  private CatalogTable loadSessionTable(String name)
      throws NoSuchTableException, NoSuchDatabaseException, ParseException {
    Identifier identifier = Spark3Util.catalogAndIdentifier(spark, name).identifier();
    Some<String> namespace = Some.apply(identifier.namespace()[0]);
    return spark.sessionState().catalog().getTableMetadata(new TableIdentifier(identifier.name(), namespace));
  }

  private void createSourceTable(String createStatement, String tableName)
      throws IOException, NoSuchTableException, NoSuchDatabaseException, ParseException {
    File location = temp.newFolder();
    spark.sql(String.format(createStatement, tableName, location));
    CatalogTable table = loadSessionTable(tableName);
    Seq<String> partitionColumns = table.partitionColumnNames();
    String format = table.provider().get();
    spark.table(baseTableName).write().mode(SaveMode.Append).format(format).partitionBy(partitionColumns)
        .saveAsTable(tableName);
  }

  // Counts the number of files in the source table, makes sure the same files exist in the destination table
  private void assertMigratedFileCount(CreateAction migrateAction, String source, String dest)
      throws NoSuchTableException, NoSuchDatabaseException, ParseException {
    CatalogTable sourceTable = loadSessionTable(source);
    Path sourceDir = Paths.get(sourceTable.location());
    List<File> files = FileUtils.listFiles(sourceDir.toFile(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
        .stream()
        .filter(file -> !file.toString().endsWith("crc") && !file.toString().contains("_SUCCESS"))
        .collect(Collectors.toList());
    long expectedMigratedFiles = files.size();
    List<Row> expected = spark.table(source).collectAsList();

    long migratedFiles = migrateAction.execute();

    SparkTable destTable = loadTable(dest);
    Assert.assertEquals("Provider should be iceberg", "iceberg",
        destTable.properties().get(TableCatalog.PROP_PROVIDER));
    Assert.assertEquals("Expected number of migrated files", expectedMigratedFiles, migratedFiles);
    List<Row> actual = spark.table(dest).collectAsList();
    Assert.assertTrue(String.format("Rows in migrated table did not match\nExpected :%s rows \nFound    :%s",
        expected, actual), expected.containsAll(actual) && actual.containsAll(expected));
  }

  // Inserts records into the destination, makes sure those records exist and source table is unchanged
  private void assertIsolatedSnapshot(String source, String dest) {
    List<Row> expected = spark.sql(String.format("SELECT * FROM %s", source)).collectAsList();

    List<SimpleRecord> extraData = Lists.newArrayList(
        new SimpleRecord(4, "d")
    );
    Dataset<Row> df = spark.createDataFrame(extraData, SimpleRecord.class);
    df.write().format("iceberg").mode("append").saveAsTable(dest);

    List<Row> result = spark.sql(String.format("SELECT * FROM %s", source)).collectAsList();
    Assert.assertEquals("No additional rows should be added to the original table", expected.size(),
        result.size());

    List<Row> snapshot = spark.sql(String.format("SELECT * FROM %s WHERE id = 4 AND data = 'd'", dest)).collectAsList();
    Assert.assertEquals("Added row not found in snapshot", 1, snapshot.size());
  }

  private String sourceName(String source) {
    return NAMESPACE + "." + catalog.name() + "_" + type + "_" + source;
  }

  private String destName(String dest) {
    if (catalog.name().equals("spark_catalog")) {
      return NAMESPACE + "." + catalog.name() + "_" + type + "_" + dest;
    } else {
      return catalog.name() + "." + NAMESPACE + "." + catalog.name() + "_" + type + "_" + dest;
    }
  }
}
