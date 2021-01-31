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
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;
import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
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
  public void testPartitionedTableWithUnRecoveredPartitions() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
    String source = sourceName("test_unrecovered_partitions");
    String dest = source;
    File location = temp.newFolder();
    sql(CREATE_PARTITIONED_PARQUET, source, location);

    // Data generation and partition addition
    spark.range(5)
        .selectExpr("id", "cast(id as STRING) as data")
        .write()
        .partitionBy("id").mode(SaveMode.Overwrite)
        .parquet(location.toURI().toString());
    sql("ALTER TABLE %s ADD PARTITION(id=0)", source);

    assertMigratedFileCount(Actions.migrate(source), source, dest);
  }

  @Test
  public void testPartitionedTableWithCustomPartitions() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
    String source = sourceName("test_custom_parts");
    String dest = source;
    File tblLocation = temp.newFolder();
    File partitionDataLoc = temp.newFolder();

    // Data generation and partition addition
    spark.sql(String.format(CREATE_PARTITIONED_PARQUET, source, tblLocation));
    spark.range(10)
        .selectExpr("cast(id as STRING) as data")
        .write()
        .mode(SaveMode.Overwrite).parquet(partitionDataLoc.toURI().toString());
    sql("ALTER TABLE %s ADD PARTITION(id=0) LOCATION '%s'", source, partitionDataLoc.toURI().toString());
    assertMigratedFileCount(Actions.migrate(source), source, dest);
  }

  @Test
  public void testAddColumnOnMigratedTableAtEnd() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
    String source = sourceName("test_add_column_migrated_table");
    String dest = source;
    createSourceTable(CREATE_PARQUET, source);
    List<Object[]> expected1 = sql("select *, null from %s order by id", source);
    List<Object[]> expected2 = sql("select *, null, null from %s order by id", source);

    // migrate table
    Actions.migrate(source).execute();
    SparkTable sparkTable = loadTable(dest);
    Table table = sparkTable.table();

    // test column addition on migrated table
    Schema beforeSchema = table.schema();
    String newCol1 = "newCol1";
    sparkTable.table().updateSchema().addColumn(newCol1, Types.IntegerType.get()).commit();
    Schema afterSchema = table.schema();
    Assert.assertNull(beforeSchema.findField(newCol1));
    Assert.assertNotNull(afterSchema.findField(newCol1));

    // reads should succeed without any exceptions
    List<Object[]> results1 = sql("select * from %s order by id", dest);
    Assert.assertTrue(results1.size() > 0);
    assertEquals("Output must match", results1, expected1);

    String newCol2 = "newCol2";
    sql("ALTER TABLE %s ADD COLUMN %s INT", dest, newCol2);
    StructType schema = spark.table(dest).schema();
    Assert.assertTrue(Arrays.asList(schema.fieldNames()).contains(newCol2));

    // reads should succeed without any exceptions
    List<Object[]> results2 = sql("select * from %s order by id", dest);
    Assert.assertTrue(results2.size() > 0);
    assertEquals("Output must match", results2, expected2);
  }

  @Test
  public void testAddColumnOnMigratedTableAtMiddle() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
    String source = sourceName("test_add_column_migrated_table_middle");
    String dest = source;
    createSourceTable(CREATE_PARQUET, source);

    // migrate table
    Actions.migrate(source).execute();
    SparkTable sparkTable = loadTable(dest);
    Table table = sparkTable.table();
    List<Object[]> expected = sql("select id, null, data from %s order by id", source);

    // test column addition on migrated table
    Schema beforeSchema = table.schema();
    String newCol1 = "newCol";
    sparkTable.table().updateSchema().addColumn("newCol", Types.IntegerType.get())
        .moveAfter(newCol1, "id")
        .commit();
    Schema afterSchema = table.schema();
    Assert.assertNull(beforeSchema.findField(newCol1));
    Assert.assertNotNull(afterSchema.findField(newCol1));

    // reads should succeed
    List<Object[]> results = sql("select * from %s order by id", dest);
    Assert.assertTrue(results.size() > 0);
    assertEquals("Output must match", results, expected);
  }

  @Test
  public void removeColumnsAtEnd() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
    String source = sourceName("test_remove_column_migrated_table");
    String dest = source;

    String colName1 = "newCol1";
    String colName2 = "newCol2";
    File location = temp.newFolder();
    spark.range(10).selectExpr("cast(id as INT)", "CAST(id as INT) " + colName1, "CAST(id as INT) " + colName2)
        .write()
        .mode(SaveMode.Overwrite).saveAsTable(dest);
    List<Object[]> expected1 = sql("select id, %s from %s order by id", colName1, source);
    List<Object[]> expected2 = sql("select id from %s order by id", source);

    // migrate table
    Actions.migrate(source).execute();
    SparkTable sparkTable = loadTable(dest);
    Table table = sparkTable.table();

    // test column removal on migrated table
    Schema beforeSchema = table.schema();
    sparkTable.table().updateSchema().deleteColumn(colName1).commit();
    Schema afterSchema = table.schema();
    Assert.assertNotNull(beforeSchema.findField(colName1));
    Assert.assertNull(afterSchema.findField(colName1));

    // reads should succeed without any exceptions
    List<Object[]> results1 = sql("select * from %s order by id", dest);
    Assert.assertTrue(results1.size() > 0);
    assertEquals("Output must match", expected1, results1);

    sql("ALTER TABLE %s DROP COLUMN %s", dest, colName2);
    StructType schema = spark.table(dest).schema();
    Assert.assertFalse(Arrays.asList(schema.fieldNames()).contains(colName2));

    // reads should succeed without any exceptions
    List<Object[]> results2 = sql("select * from %s order by id", dest);
    Assert.assertTrue(results2.size() > 0);
    assertEquals("Output must match", expected2, results2);
  }

  @Test
  public void removeColumnFromMiddle() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
    String source = sourceName("test_remove_column_migrated_table_from_middle");
    String dest = source;
    String dropColumnName = "col1";

    spark.range(10).selectExpr("cast(id as INT)", "CAST(id as INT) as " +
        dropColumnName, "CAST(id as INT) as col2").write().mode(SaveMode.Overwrite).saveAsTable(dest);
    List<Object[]> expected = sql("select id, col2 from %s order by id", source);

    // migrate table
    Actions.migrate(source).execute();

    // drop column
    sql("ALTER TABLE %s DROP COLUMN %s", dest, "col1");
    StructType schema = spark.table(dest).schema();
    Assert.assertFalse(Arrays.asList(schema.fieldNames()).contains(dropColumnName));

    // reads should return same output as that of non-iceberg table
    List<Object[]> results = sql("select * from %s order by id", dest);
    Assert.assertTrue(results.size() > 0);
    assertEquals("Output must match", expected, results);
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

  @Test
  public void schemaEvolutionTestWithSparkAPI() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));

    File location = temp.newFolder();
    String tblName = sourceName("schema_evolution_test");

    // Data generation and partition addition
    spark.range(0, 5)
        .selectExpr("CAST(id as INT) as col0", "CAST(id AS FLOAT) col2", "CAST(id AS LONG) col3")
        .write()
        .mode(SaveMode.Append)
        .parquet(location.toURI().toString());
    Dataset<Row> rowDataset = spark.range(6, 10)
        .selectExpr("CAST(id as INT) as col0", "CAST(id AS STRING) col1",
            "CAST(id AS FLOAT) col2", "CAST(id AS LONG) col3");
    rowDataset
        .write()
        .mode(SaveMode.Append)
        .parquet(location.toURI().toString());
    spark.read()
        .schema(rowDataset.schema())
        .parquet(location.toURI().toString()).write().saveAsTable(tblName);
    List<Object[]> expectedBeforeAddColumn = sql("SELECT * FROM %s ORDER BY col0", tblName);
    List<Object[]> expectedAfterAddColumn = sql("SELECT col0, null, col1, col2, col3 FROM %s ORDER BY col0",
        tblName);

    // Migrate table
    Actions.migrate(tblName).execute();

    // check if iceberg and non-iceberg output
    List<Object[]> afterMigarteBeforeAddResults = sql("SELECT * FROM %s ORDER BY col0", tblName);
    assertEquals("Output must match", expectedBeforeAddColumn, afterMigarteBeforeAddResults);

    // Update schema and check output correctness
    SparkTable sparkTable = loadTable(tblName);
    sparkTable.table().updateSchema().addColumn("newCol", Types.IntegerType.get())
        .moveAfter("newCol", "col0")
        .commit();
    List<Object[]> afterMigarteAfterAddResults = sql("SELECT * FROM %s ORDER BY col0", tblName);
    assertEquals("Output must match", expectedAfterAddColumn, afterMigarteAfterAddResults);
  }

  @Test
  public void schemaEvolutionTestWithSparkSQL() throws Exception {
    Assume.assumeTrue("Cannot migrate to a hadoop based catalog", !type.equals("hadoop"));
    Assume.assumeTrue("Can only migrate from Spark Session Catalog", catalog.name().equals("spark_catalog"));
    String tblName = sourceName("schema_evolution_test_sql");

    // Data generation and partition addition
    spark.range(0, 5)
        .selectExpr("CAST(id as INT) col0", "CAST(id AS FLOAT) col1", "CAST(id AS STRING) col2")
        .write()
        .mode(SaveMode.Append)
        .saveAsTable(tblName);
    sql("ALTER TABLE %s ADD COLUMN col3 INT", tblName);
    spark.range(6, 10)
        .selectExpr("CAST(id AS INT) col0", "CAST(id AS FLOAT) col1", "CAST(id AS STRING) col2", "CAST(id AS INT) col3")
        .registerTempTable("tempdata");
    sql("INSERT INTO TABLE %s SELECT * FROM tempdata", tblName);
    List<Object[]> expectedBeforeAddColumn = sql("SELECT * FROM %s ORDER BY col0", tblName);
    List<Object[]> expectedAfterAddColumn = sql("SELECT col0, null, col1, col2, col3 FROM %s ORDER BY col0",
        tblName);

    // Migrate table
    Actions.migrate(tblName).execute();

    // check if iceberg and non-iceberg output
    List<Object[]> afterMigarteBeforeAddResults = sql("SELECT * FROM %s ORDER BY col0", tblName);
    assertEquals("Output must match", expectedBeforeAddColumn, afterMigarteBeforeAddResults);

    // Update schema and check output correctness
    SparkTable sparkTable = loadTable(tblName);
    sparkTable.table().updateSchema().addColumn("newCol", Types.IntegerType.get())
        .moveAfter("newCol", "col0")
        .commit();
    List<Object[]> afterMigarteAfterAddResults = sql("SELECT * FROM %s ORDER BY col0", tblName);
    assertEquals("Output must match", expectedAfterAddColumn, afterMigarteAfterAddResults);
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
    List<URI> uris;
    if (sourceTable.partitionColumnNames().size() == 0) {
      uris = new ArrayList<>();
      uris.add(sourceTable.location());
    } else {
      Seq<CatalogTablePartition> catalogTablePartitionSeq =
          spark.sessionState().catalog().listPartitions(sourceTable.identifier(), Option.apply(null));
      uris = JavaConverters.seqAsJavaList(catalogTablePartitionSeq)
          .stream()
          .map(CatalogTablePartition::location)
          .collect(Collectors.toList());
    }
    long expectedMigratedFiles = uris.stream()
        .flatMap(uri ->
            FileUtils.listFiles(Paths.get(uri).toFile(),
                TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).stream())
        .filter(file -> !file.toString().endsWith("crc") && !file.toString().contains("_SUCCESS")).count();

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
