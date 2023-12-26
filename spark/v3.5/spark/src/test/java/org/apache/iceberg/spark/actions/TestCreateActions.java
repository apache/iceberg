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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.MigrateTable;
import org.apache.iceberg.actions.SnapshotTable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.api.java.JavaSparkContext;
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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class TestCreateActions extends CatalogTestBase {
  private static final String CREATE_PARTITIONED_PARQUET =
      "CREATE TABLE %s (id INT, data STRING) " + "using parquet PARTITIONED BY (id) LOCATION '%s'";
  private static final String CREATE_PARQUET =
      "CREATE TABLE %s (id INT, data STRING) " + "using parquet LOCATION '%s'";
  private static final String CREATE_HIVE_EXTERNAL_PARQUET =
      "CREATE EXTERNAL TABLE %s (data STRING) "
          + "PARTITIONED BY (id INT) STORED AS parquet LOCATION '%s'";
  private static final String CREATE_HIVE_PARQUET =
      "CREATE TABLE %s (data STRING) " + "PARTITIONED BY (id INT) STORED AS parquet";

  private static final String NAMESPACE = "default";

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, type = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled",
                "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
        "hive"
      },
      new Object[] {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hadoop",
            "default-namespace", "default",
            "parquet-enabled", "true",
            "cache-enabled",
                "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
        "hadoop"
      },
      new Object[] {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"),
        "hive"
      },
      new Object[] {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hadoop",
            "default-namespace", "default"),
        "hadoop"
      }
    };
  }

  private String baseTableName = "baseTable";
  private File tableDir;
  private String tableLocation;

  @Parameter(index = 3)
  private String type;

  private TableCatalog catalog;

  @BeforeEach
  @Override
  public void before() {
    super.before();
    try {
      this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.tableLocation = tableDir.toURI().toString();
    this.catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);

    spark.conf().set("hive.exec.dynamic.partition", "true");
    spark.conf().set("hive.exec.dynamic.partition.mode", "nonstrict");
    spark.conf().set("spark.sql.parquet.writeLegacyFormat", false);
    spark.conf().set("spark.sql.parquet.writeLegacyFormat", false);
    spark.sql(String.format("DROP TABLE IF EXISTS %s", baseTableName));

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data")
        .orderBy("data")
        .write()
        .mode("append")
        .option("path", tableLocation)
        .saveAsTable(baseTableName);
  }

  @AfterEach
  public void after() throws IOException {
    // Drop the hive table.
    spark.sql(String.format("DROP TABLE IF EXISTS %s", baseTableName));
  }

  @TestTemplate
  public void testMigratePartitioned() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");
    String source = sourceName("test_migrate_partitioned_table");
    String dest = source;
    createSourceTable(CREATE_PARTITIONED_PARQUET, source);
    assertMigratedFileCount(SparkActions.get().migrateTable(source), source, dest);
  }

  @TestTemplate
  public void testPartitionedTableWithUnRecoveredPartitions() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");
    String source = sourceName("test_unrecovered_partitions");
    String dest = source;
    File location = Files.createTempDirectory(temp, "junit").toFile();
    sql(CREATE_PARTITIONED_PARQUET, source, location);

    // Data generation and partition addition
    spark
        .range(5)
        .selectExpr("id", "cast(id as STRING) as data")
        .write()
        .partitionBy("id")
        .mode(SaveMode.Overwrite)
        .parquet(location.toURI().toString());
    sql("ALTER TABLE %s ADD PARTITION(id=0)", source);

    assertMigratedFileCount(SparkActions.get().migrateTable(source), source, dest);
  }

  @TestTemplate
  public void testPartitionedTableWithCustomPartitions() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");
    String source = sourceName("test_custom_parts");
    String dest = source;
    File tblLocation = Files.createTempDirectory(temp, "junit").toFile();
    File partitionDataLoc = Files.createTempDirectory(temp, "junit").toFile();

    // Data generation and partition addition
    spark.sql(String.format(CREATE_PARTITIONED_PARQUET, source, tblLocation));
    spark
        .range(10)
        .selectExpr("cast(id as STRING) as data")
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(partitionDataLoc.toURI().toString());
    sql(
        "ALTER TABLE %s ADD PARTITION(id=0) LOCATION '%s'",
        source, partitionDataLoc.toURI().toString());
    assertMigratedFileCount(SparkActions.get().migrateTable(source), source, dest);
  }

  @TestTemplate
  public void testAddColumnOnMigratedTableAtEnd() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");
    String source = sourceName("test_add_column_migrated_table");
    String dest = source;
    createSourceTable(CREATE_PARQUET, source);
    List<Object[]> expected1 = sql("select *, null from %s order by id", source);
    List<Object[]> expected2 = sql("select *, null, null from %s order by id", source);

    // migrate table
    SparkActions.get().migrateTable(source).execute();
    SparkTable sparkTable = loadTable(dest);
    Table table = sparkTable.table();

    // test column addition on migrated table
    Schema beforeSchema = table.schema();
    String newCol1 = "newCol1";
    sparkTable.table().updateSchema().addColumn(newCol1, Types.IntegerType.get()).commit();
    Schema afterSchema = table.schema();
    assertThat(beforeSchema.findField(newCol1)).isNull();
    assertThat(afterSchema.findField(newCol1)).isNotNull();

    // reads should succeed without any exceptions
    List<Object[]> results1 = sql("select * from %s order by id", dest);
    assertThat(results1).isNotEmpty();
    assertEquals("Output must match", results1, expected1);

    String newCol2 = "newCol2";
    sql("ALTER TABLE %s ADD COLUMN %s INT", dest, newCol2);
    StructType schema = spark.table(dest).schema();
    assertThat(schema.fieldNames()).contains(newCol2);

    // reads should succeed without any exceptions
    List<Object[]> results2 = sql("select * from %s order by id", dest);
    assertThat(results2).isNotEmpty();
    assertEquals("Output must match", results2, expected2);
  }

  @TestTemplate
  public void testAddColumnOnMigratedTableAtMiddle() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");
    String source = sourceName("test_add_column_migrated_table_middle");
    String dest = source;
    createSourceTable(CREATE_PARQUET, source);

    // migrate table
    SparkActions.get().migrateTable(source).execute();
    SparkTable sparkTable = loadTable(dest);
    Table table = sparkTable.table();
    List<Object[]> expected = sql("select id, null, data from %s order by id", source);

    // test column addition on migrated table
    Schema beforeSchema = table.schema();
    String newCol1 = "newCol";
    sparkTable
        .table()
        .updateSchema()
        .addColumn("newCol", Types.IntegerType.get())
        .moveAfter(newCol1, "id")
        .commit();
    Schema afterSchema = table.schema();
    assertThat(beforeSchema.findField(newCol1)).isNull();
    assertThat(afterSchema.findField(newCol1)).isNotNull();

    // reads should succeed
    List<Object[]> results = sql("select * from %s order by id", dest);
    assertThat(results).isNotEmpty();
    assertEquals("Output must match", results, expected);
  }

  @TestTemplate
  public void removeColumnsAtEnd() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");
    String source = sourceName("test_remove_column_migrated_table");
    String dest = source;

    String colName1 = "newCol1";
    String colName2 = "newCol2";
    File location = Files.createTempDirectory(temp, "junit").toFile();
    spark
        .range(10)
        .selectExpr("cast(id as INT)", "CAST(id as INT) " + colName1, "CAST(id as INT) " + colName2)
        .write()
        .mode(SaveMode.Overwrite)
        .saveAsTable(dest);
    List<Object[]> expected1 = sql("select id, %s from %s order by id", colName1, source);
    List<Object[]> expected2 = sql("select id from %s order by id", source);

    // migrate table
    SparkActions.get().migrateTable(source).execute();
    SparkTable sparkTable = loadTable(dest);
    Table table = sparkTable.table();

    // test column removal on migrated table
    Schema beforeSchema = table.schema();
    sparkTable.table().updateSchema().deleteColumn(colName1).commit();
    Schema afterSchema = table.schema();
    assertThat(beforeSchema.findField(colName1)).isNotNull();
    assertThat(afterSchema.findField(colName1)).isNull();

    // reads should succeed without any exceptions
    List<Object[]> results1 = sql("select * from %s order by id", dest);
    assertThat(results1).isNotEmpty();
    assertEquals("Output must match", expected1, results1);

    sql("ALTER TABLE %s DROP COLUMN %s", dest, colName2);
    StructType schema = spark.table(dest).schema();
    assertThat(schema.fieldNames()).doesNotContain(colName2);

    // reads should succeed without any exceptions
    List<Object[]> results2 = sql("select * from %s order by id", dest);
    assertThat(results2).isNotEmpty();
    assertEquals("Output must match", expected2, results2);
  }

  @TestTemplate
  public void removeColumnFromMiddle() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");
    String source = sourceName("test_remove_column_migrated_table_from_middle");
    String dest = source;
    String dropColumnName = "col1";

    spark
        .range(10)
        .selectExpr(
            "cast(id as INT)", "CAST(id as INT) as " + dropColumnName, "CAST(id as INT) as col2")
        .write()
        .mode(SaveMode.Overwrite)
        .saveAsTable(dest);
    List<Object[]> expected = sql("select id, col2 from %s order by id", source);

    // migrate table
    SparkActions.get().migrateTable(source).execute();

    // drop column
    sql("ALTER TABLE %s DROP COLUMN %s", dest, "col1");
    StructType schema = spark.table(dest).schema();
    assertThat(schema.fieldNames()).doesNotContain(dropColumnName);

    // reads should return same output as that of non-iceberg table
    List<Object[]> results = sql("select * from %s order by id", dest);
    assertThat(results).isNotEmpty();
    assertEquals("Output must match", expected, results);
  }

  @TestTemplate
  public void testMigrateUnpartitioned() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");
    String source = sourceName("test_migrate_unpartitioned_table");
    String dest = source;
    createSourceTable(CREATE_PARQUET, source);
    assertMigratedFileCount(SparkActions.get().migrateTable(source), source, dest);
  }

  @TestTemplate
  public void testSnapshotPartitioned() throws Exception {
    assumeThat(type)
        .as("Cannot snapshot with arbitrary location in a hadoop based catalog")
        .isNotEqualTo("hadoop");
    File location = Files.createTempDirectory(temp, "junit").toFile();
    String source = sourceName("test_snapshot_partitioned_table");
    String dest = destName("iceberg_snapshot_partitioned");
    createSourceTable(CREATE_PARTITIONED_PARQUET, source);
    assertSnapshotFileCount(
        SparkActions.get().snapshotTable(source).as(dest).tableLocation(location.toString()),
        source,
        dest);
    assertIsolatedSnapshot(source, dest);
  }

  @TestTemplate
  public void testSnapshotUnpartitioned() throws Exception {
    assumeThat(type)
        .as("Cannot snapshot with arbitrary location in a hadoop based catalog")
        .isNotEqualTo("hadoop");
    File location = Files.createTempDirectory(temp, "junit").toFile();
    String source = sourceName("test_snapshot_unpartitioned_table");
    String dest = destName("iceberg_snapshot_unpartitioned");
    createSourceTable(CREATE_PARQUET, source);
    assertSnapshotFileCount(
        SparkActions.get().snapshotTable(source).as(dest).tableLocation(location.toString()),
        source,
        dest);
    assertIsolatedSnapshot(source, dest);
  }

  @TestTemplate
  public void testSnapshotHiveTable() throws Exception {
    assumeThat(type)
        .as("Cannot snapshot with arbitrary location in a hadoop based catalog")
        .isNotEqualTo("hadoop");
    File location = Files.createTempDirectory(temp, "junit").toFile();
    String source = sourceName("snapshot_hive_table");
    String dest = destName("iceberg_snapshot_hive_table");
    createSourceTable(CREATE_HIVE_EXTERNAL_PARQUET, source);
    assertSnapshotFileCount(
        SparkActions.get().snapshotTable(source).as(dest).tableLocation(location.toString()),
        source,
        dest);
    assertIsolatedSnapshot(source, dest);
  }

  @TestTemplate
  public void testMigrateHiveTable() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    String source = sourceName("migrate_hive_table");
    String dest = source;
    createSourceTable(CREATE_HIVE_EXTERNAL_PARQUET, source);
    assertMigratedFileCount(SparkActions.get().migrateTable(source), source, dest);
  }

  @TestTemplate
  public void testSnapshotManagedHiveTable() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    File location = Files.createTempDirectory(temp, "junit").toFile();
    String source = sourceName("snapshot_managed_hive_table");
    String dest = destName("iceberg_snapshot_managed_hive_table");
    createSourceTable(CREATE_HIVE_PARQUET, source);
    assertSnapshotFileCount(
        SparkActions.get().snapshotTable(source).as(dest).tableLocation(location.toString()),
        source,
        dest);
    assertIsolatedSnapshot(source, dest);
  }

  @TestTemplate
  public void testMigrateManagedHiveTable() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    File location = Files.createTempDirectory(temp, "junit").toFile();
    String source = sourceName("migrate_managed_hive_table");
    String dest = destName("iceberg_migrate_managed_hive_table");
    createSourceTable(CREATE_HIVE_PARQUET, source);
    assertSnapshotFileCount(
        SparkActions.get().snapshotTable(source).as(dest).tableLocation(location.toString()),
        source,
        dest);
  }

  @TestTemplate
  public void testProperties() throws Exception {
    String source = sourceName("test_properties_table");
    String dest = destName("iceberg_properties");
    Map<String, String> props = Maps.newHashMap();
    props.put("city", "New Orleans");
    props.put("note", "Jazz");
    createSourceTable(CREATE_PARQUET, source);
    for (Map.Entry<String, String> keyValue : props.entrySet()) {
      spark.sql(
          String.format(
              "ALTER TABLE %s SET TBLPROPERTIES (\"%s\" = \"%s\")",
              source, keyValue.getKey(), keyValue.getValue()));
    }
    assertSnapshotFileCount(
        SparkActions.get().snapshotTable(source).as(dest).tableProperty("dogs", "sundance"),
        source,
        dest);
    SparkTable table = loadTable(dest);

    Map<String, String> expectedProps = Maps.newHashMap();
    expectedProps.putAll(props);
    expectedProps.put("dogs", "sundance");

    for (Map.Entry<String, String> entry : expectedProps.entrySet()) {
      assertThat(table.properties())
          .as("Created table missing property " + entry.getKey())
          .containsKey(entry.getKey());
      assertThat(table.properties().get(entry.getKey()))
          .as("Property value is not the expected value")
          .isEqualTo(entry.getValue());
    }
  }

  @TestTemplate
  public void testSparkTableReservedProperties() throws Exception {
    String destTableName = "iceberg_reserved_properties";
    String source = sourceName("test_reserved_properties_table");
    String dest = destName(destTableName);
    createSourceTable(CREATE_PARQUET, source);
    SnapshotTableSparkAction action = SparkActions.get().snapshotTable(source).as(dest);
    action.tableProperty(TableProperties.FORMAT_VERSION, "1");
    assertSnapshotFileCount(action, source, dest);
    SparkTable table = loadTable(dest);
    // set sort orders
    table.table().replaceSortOrder().asc("id").desc("data").commit();

    String[] keys = {"provider", "format", "current-snapshot-id", "location", "sort-order"};

    for (String entry : keys) {
      assertThat(table.properties())
          .as("Created table missing reserved property " + entry)
          .containsKey(entry);
    }

    assertThat(table.properties().get("provider")).as("Unexpected provider").isEqualTo("iceberg");
    assertThat(table.properties().get("format"))
        .as("Unexpected provider")
        .isEqualTo("iceberg/parquet");
    assertThat(table.properties().get("current-snapshot-id"))
        .as("No current-snapshot-id found")
        .isNotEqualTo("none");
    assertThat(table.properties().get("location"))
        .as("Location isn't correct")
        .endsWith(destTableName);

    assertThat(table.properties().get("format-version"))
        .as("Unexpected format-version")
        .isEqualTo("1");
    table.table().updateProperties().set("format-version", "2").commit();
    assertThat(table.properties().get("format-version"))
        .as("Unexpected format-version")
        .isEqualTo("2");

    assertThat(table.properties().get("sort-order"))
        .as("Sort-order isn't correct")
        .isEqualTo("id ASC NULLS FIRST, data DESC NULLS LAST");
    assertThat(table.properties().get("identifier-fields"))
        .as("Identifier fields should be null")
        .isNull();

    table
        .table()
        .updateSchema()
        .allowIncompatibleChanges()
        .requireColumn("id")
        .setIdentifierFields("id")
        .commit();
    assertThat(table.properties().get("identifier-fields"))
        .as("Identifier fields aren't correct")
        .isEqualTo("[id]");
  }

  @TestTemplate
  public void testSnapshotDefaultLocation() throws Exception {
    String source = sourceName("test_snapshot_default");
    String dest = destName("iceberg_snapshot_default");
    createSourceTable(CREATE_PARTITIONED_PARQUET, source);
    assertSnapshotFileCount(SparkActions.get().snapshotTable(source).as(dest), source, dest);
    assertIsolatedSnapshot(source, dest);
  }

  @TestTemplate
  public void schemaEvolutionTestWithSparkAPI() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");

    File location = Files.createTempDirectory(temp, "junit").toFile();
    String tblName = sourceName("schema_evolution_test");

    // Data generation and partition addition
    spark
        .range(0, 5)
        .selectExpr("CAST(id as INT) as col0", "CAST(id AS FLOAT) col2", "CAST(id AS LONG) col3")
        .write()
        .mode(SaveMode.Append)
        .parquet(location.toURI().toString());
    Dataset<Row> rowDataset =
        spark
            .range(6, 10)
            .selectExpr(
                "CAST(id as INT) as col0",
                "CAST(id AS STRING) col1",
                "CAST(id AS FLOAT) col2",
                "CAST(id AS LONG) col3");
    rowDataset.write().mode(SaveMode.Append).parquet(location.toURI().toString());
    spark
        .read()
        .schema(rowDataset.schema())
        .parquet(location.toURI().toString())
        .write()
        .saveAsTable(tblName);
    List<Object[]> expectedBeforeAddColumn = sql("SELECT * FROM %s ORDER BY col0", tblName);
    List<Object[]> expectedAfterAddColumn =
        sql("SELECT col0, null, col1, col2, col3 FROM %s ORDER BY col0", tblName);

    // Migrate table
    SparkActions.get().migrateTable(tblName).execute();

    // check if iceberg and non-iceberg output
    List<Object[]> afterMigarteBeforeAddResults = sql("SELECT * FROM %s ORDER BY col0", tblName);
    assertEquals("Output must match", expectedBeforeAddColumn, afterMigarteBeforeAddResults);

    // Update schema and check output correctness
    SparkTable sparkTable = loadTable(tblName);
    sparkTable
        .table()
        .updateSchema()
        .addColumn("newCol", Types.IntegerType.get())
        .moveAfter("newCol", "col0")
        .commit();
    List<Object[]> afterMigarteAfterAddResults = sql("SELECT * FROM %s ORDER BY col0", tblName);
    assertEquals("Output must match", expectedAfterAddColumn, afterMigarteAfterAddResults);
  }

  @TestTemplate
  public void schemaEvolutionTestWithSparkSQL() throws Exception {
    assumeThat(type).as("Cannot migrate to a hadoop based catalog").isNotEqualTo("hadoop");
    assumeThat(catalog.name())
        .as("Can only migrate from Spark Session Catalog")
        .isEqualTo("spark_catalog");
    String tblName = sourceName("schema_evolution_test_sql");

    // Data generation and partition addition
    spark
        .range(0, 5)
        .selectExpr("CAST(id as INT) col0", "CAST(id AS FLOAT) col1", "CAST(id AS STRING) col2")
        .write()
        .mode(SaveMode.Append)
        .saveAsTable(tblName);
    sql("ALTER TABLE %s ADD COLUMN col3 INT", tblName);
    spark
        .range(6, 10)
        .selectExpr(
            "CAST(id AS INT) col0",
            "CAST(id AS FLOAT) col1",
            "CAST(id AS STRING) col2",
            "CAST(id AS INT) col3")
        .registerTempTable("tempdata");
    sql("INSERT INTO TABLE %s SELECT * FROM tempdata", tblName);
    List<Object[]> expectedBeforeAddColumn = sql("SELECT * FROM %s ORDER BY col0", tblName);
    List<Object[]> expectedAfterAddColumn =
        sql("SELECT col0, null, col1, col2, col3 FROM %s ORDER BY col0", tblName);

    // Migrate table
    SparkActions.get().migrateTable(tblName).execute();

    // check if iceberg and non-iceberg output
    List<Object[]> afterMigarteBeforeAddResults = sql("SELECT * FROM %s ORDER BY col0", tblName);
    assertEquals("Output must match", expectedBeforeAddColumn, afterMigarteBeforeAddResults);

    // Update schema and check output correctness
    SparkTable sparkTable = loadTable(tblName);
    sparkTable
        .table()
        .updateSchema()
        .addColumn("newCol", Types.IntegerType.get())
        .moveAfter("newCol", "col0")
        .commit();
    List<Object[]> afterMigarteAfterAddResults = sql("SELECT * FROM %s ORDER BY col0", tblName);
    assertEquals("Output must match", expectedAfterAddColumn, afterMigarteAfterAddResults);
  }

  @TestTemplate
  public void testHiveStyleThreeLevelList() throws Exception {
    threeLevelList(true);
  }

  @TestTemplate
  public void testThreeLevelList() throws Exception {
    threeLevelList(false);
  }

  @TestTemplate
  public void testHiveStyleThreeLevelListWithNestedStruct() throws Exception {
    threeLevelListWithNestedStruct(true);
  }

  @TestTemplate
  public void testThreeLevelListWithNestedStruct() throws Exception {
    threeLevelListWithNestedStruct(false);
  }

  @TestTemplate
  public void testHiveStyleThreeLevelLists() throws Exception {
    threeLevelLists(true);
  }

  @TestTemplate
  public void testThreeLevelLists() throws Exception {
    threeLevelLists(false);
  }

  @TestTemplate
  public void testHiveStyleStructOfThreeLevelLists() throws Exception {
    structOfThreeLevelLists(true);
  }

  @TestTemplate
  public void testStructOfThreeLevelLists() throws Exception {
    structOfThreeLevelLists(false);
  }

  @TestTemplate
  public void testTwoLevelList() throws IOException {
    spark.conf().set("spark.sql.parquet.writeLegacyFormat", true);

    String tableName = sourceName("testTwoLevelList");
    File location = Files.createTempDirectory(temp, "junit").toFile();

    StructType sparkSchema =
        new StructType(
            new StructField[] {
              new StructField(
                  "col1",
                  new ArrayType(
                      new StructType(
                          new StructField[] {
                            new StructField("col2", DataTypes.IntegerType, false, Metadata.empty())
                          }),
                      false),
                  true,
                  Metadata.empty())
            });

    // even though this list looks like three level list, it is actually a 2-level list where the
    // items are
    // structs with 1 field.
    String expectedParquetSchema =
        "message spark_schema {\n"
            + "  optional group col1 (LIST) {\n"
            + "    repeated group array {\n"
            + "      required int32 col2;\n"
            + "    }\n"
            + "  }\n"
            + "}\n";

    // generate parquet file with required schema
    List<String> testData = Collections.singletonList("{\"col1\": [{\"col2\": 1}]}");
    spark
        .read()
        .schema(sparkSchema)
        .json(JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(testData))
        .coalesce(1)
        .write()
        .format("parquet")
        .mode(SaveMode.Append)
        .save(location.getPath());

    File parquetFile =
        Arrays.stream(
                Preconditions.checkNotNull(
                    location.listFiles(
                        new FilenameFilter() {
                          @Override
                          public boolean accept(File dir, String name) {
                            return name.endsWith("parquet");
                          }
                        })))
            .findAny()
            .get();

    // verify generated parquet file has expected schema
    ParquetFileReader pqReader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(
                new Path(parquetFile.getPath()), spark.sessionState().newHadoopConf()));
    MessageType schema = pqReader.getFooter().getFileMetaData().getSchema();
    assertThat(schema).isEqualTo(MessageTypeParser.parseMessageType(expectedParquetSchema));

    // create sql table on top of it
    sql(
        "CREATE EXTERNAL TABLE %s (col1 ARRAY<STRUCT<col2 INT>>)"
            + " STORED AS parquet"
            + " LOCATION '%s'",
        tableName, location);
    List<Object[]> expected = sql("select array(struct(1))");

    // migrate table
    SparkActions.get().migrateTable(tableName).execute();

    // check migrated table is returning expected result
    List<Object[]> results = sql("SELECT * FROM %s", tableName);
    assertThat(results).isNotEmpty();
    assertEquals("Output must match", expected, results);
  }

  private void threeLevelList(boolean useLegacyMode) throws Exception {
    spark.conf().set("spark.sql.parquet.writeLegacyFormat", useLegacyMode);

    String tableName = sourceName(String.format("threeLevelList_%s", useLegacyMode));
    File location = Files.createTempDirectory(temp, "junit").toFile();
    sql(
        "CREATE TABLE %s (col1 ARRAY<STRUCT<col2 INT>>)" + " STORED AS parquet" + " LOCATION '%s'",
        tableName, location);

    int testValue = 12345;
    sql("INSERT INTO %s VALUES (ARRAY(STRUCT(%s)))", tableName, testValue);
    List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));

    // migrate table
    SparkActions.get().migrateTable(tableName).execute();

    // check migrated table is returning expected result
    List<Object[]> results = sql("SELECT * FROM %s", tableName);
    assertThat(results).isNotEmpty();
    assertEquals("Output must match", expected, results);
  }

  private void threeLevelListWithNestedStruct(boolean useLegacyMode) throws Exception {
    spark.conf().set("spark.sql.parquet.writeLegacyFormat", useLegacyMode);

    String tableName =
        sourceName(String.format("threeLevelListWithNestedStruct_%s", useLegacyMode));
    File location = Files.createTempDirectory(temp, "junit").toFile();
    sql(
        "CREATE TABLE %s (col1 ARRAY<STRUCT<col2 STRUCT<col3 INT>>>)"
            + " STORED AS parquet"
            + " LOCATION '%s'",
        tableName, location);

    int testValue = 12345;
    sql("INSERT INTO %s VALUES (ARRAY(STRUCT(STRUCT(%s))))", tableName, testValue);
    List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));

    // migrate table
    SparkActions.get().migrateTable(tableName).execute();

    // check migrated table is returning expected result
    List<Object[]> results = sql("SELECT * FROM %s", tableName);
    assertThat(results).isNotEmpty();
    assertEquals("Output must match", expected, results);
  }

  private void threeLevelLists(boolean useLegacyMode) throws Exception {
    spark.conf().set("spark.sql.parquet.writeLegacyFormat", useLegacyMode);

    String tableName = sourceName(String.format("threeLevelLists_%s", useLegacyMode));
    File location = Files.createTempDirectory(temp, "junit").toFile();
    sql(
        "CREATE TABLE %s (col1 ARRAY<STRUCT<col2 INT>>, col3 ARRAY<STRUCT<col4 INT>>)"
            + " STORED AS parquet"
            + " LOCATION '%s'",
        tableName, location);

    int testValue1 = 12345;
    int testValue2 = 987654;
    sql(
        "INSERT INTO %s VALUES (ARRAY(STRUCT(%s)), ARRAY(STRUCT(%s)))",
        tableName, testValue1, testValue2);
    List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));

    // migrate table
    SparkActions.get().migrateTable(tableName).execute();

    // check migrated table is returning expected result
    List<Object[]> results = sql("SELECT * FROM %s", tableName);
    assertThat(results).isNotEmpty();
    assertEquals("Output must match", expected, results);
  }

  private void structOfThreeLevelLists(boolean useLegacyMode) throws Exception {
    spark.conf().set("spark.sql.parquet.writeLegacyFormat", useLegacyMode);

    String tableName = sourceName(String.format("structOfThreeLevelLists_%s", useLegacyMode));
    File location = Files.createTempDirectory(temp, "junit").toFile();
    sql(
        "CREATE TABLE %s (col1 STRUCT<col2 ARRAY<STRUCT<col3 INT>>>)"
            + " STORED AS parquet"
            + " LOCATION '%s'",
        tableName, location);

    int testValue1 = 12345;
    sql("INSERT INTO %s VALUES (STRUCT(STRUCT(ARRAY(STRUCT(%s)))))", tableName, testValue1);
    List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));

    // migrate table
    SparkActions.get().migrateTable(tableName).execute();

    // check migrated table is returning expected result
    List<Object[]> results = sql("SELECT * FROM %s", tableName);
    assertThat(results).isNotEmpty();
    assertEquals("Output must match", expected, results);
  }

  private SparkTable loadTable(String name) throws NoSuchTableException, ParseException {
    return (SparkTable)
        catalog.loadTable(Spark3Util.catalogAndIdentifier(spark, name).identifier());
  }

  private CatalogTable loadSessionTable(String name)
      throws NoSuchTableException, NoSuchDatabaseException, ParseException {
    Identifier identifier = Spark3Util.catalogAndIdentifier(spark, name).identifier();
    Some<String> namespace = Some.apply(identifier.namespace()[0]);
    return spark
        .sessionState()
        .catalog()
        .getTableMetadata(new TableIdentifier(identifier.name(), namespace));
  }

  private void createSourceTable(String createStatement, String tableName)
      throws IOException, NoSuchTableException, NoSuchDatabaseException, ParseException {
    File location = Files.createTempDirectory(temp, "junit").toFile();
    spark.sql(String.format(createStatement, tableName, location));
    CatalogTable table = loadSessionTable(tableName);
    String format = table.provider().get();
    spark
        .table(baseTableName)
        .selectExpr(table.schema().names())
        .write()
        .mode(SaveMode.Append)
        .format(format)
        .insertInto(tableName);
  }

  // Counts the number of files in the source table, makes sure the same files exist in the
  // destination table
  private void assertMigratedFileCount(MigrateTable migrateAction, String source, String dest)
      throws NoSuchTableException, NoSuchDatabaseException, ParseException {
    long expectedFiles = expectedFilesCount(source);
    MigrateTable.Result migratedFiles = migrateAction.execute();
    validateTables(source, dest);
    assertThat(migratedFiles.migratedDataFilesCount())
        .as("Expected number of migrated files")
        .isEqualTo(expectedFiles);
  }

  // Counts the number of files in the source table, makes sure the same files exist in the
  // destination table
  private void assertSnapshotFileCount(SnapshotTable snapshotTable, String source, String dest)
      throws NoSuchTableException, NoSuchDatabaseException, ParseException {
    long expectedFiles = expectedFilesCount(source);
    SnapshotTable.Result snapshotTableResult = snapshotTable.execute();
    validateTables(source, dest);
    assertThat(snapshotTableResult.importedDataFilesCount())
        .as("Expected number of imported snapshot files")
        .isEqualTo(expectedFiles);
  }

  private void validateTables(String source, String dest)
      throws NoSuchTableException, ParseException {
    List<Row> expected = spark.table(source).collectAsList();
    SparkTable destTable = loadTable(dest);
    assertThat(destTable.properties().get(TableCatalog.PROP_PROVIDER))
        .as("Provider should be iceberg")
        .isEqualTo("iceberg");
    List<Row> actual = spark.table(dest).collectAsList();
    assertThat(actual)
        .as(
            String.format(
                "Rows in migrated table did not match\nExpected :%s rows \nFound    :%s",
                expected, actual))
        .containsAll(expected);
    assertThat(expected)
        .as(
            String.format(
                "Rows in migrated table did not match\nExpected :%s rows \nFound    :%s",
                expected, actual))
        .containsAll(actual);
  }

  private long expectedFilesCount(String source)
      throws NoSuchDatabaseException, NoSuchTableException, ParseException {
    CatalogTable sourceTable = loadSessionTable(source);
    List<URI> uris;
    if (sourceTable.partitionColumnNames().isEmpty()) {
      uris = Lists.newArrayList();
      uris.add(sourceTable.location());
    } else {
      Seq<CatalogTablePartition> catalogTablePartitionSeq =
          spark
              .sessionState()
              .catalog()
              .listPartitions(sourceTable.identifier(), Option.apply(null));
      uris =
          JavaConverters.seqAsJavaList(catalogTablePartitionSeq).stream()
              .map(CatalogTablePartition::location)
              .collect(Collectors.toList());
    }
    return uris.stream()
        .flatMap(
            uri ->
                FileUtils.listFiles(
                    Paths.get(uri).toFile(), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
                    .stream())
        .filter(file -> !file.toString().endsWith("crc") && !file.toString().contains("_SUCCESS"))
        .count();
  }

  // Insert records into the destination, makes sure those records exist and source table is
  // unchanged
  private void assertIsolatedSnapshot(String source, String dest) {
    List<Row> expected = spark.sql(String.format("SELECT * FROM %s", source)).collectAsList();

    List<SimpleRecord> extraData = Lists.newArrayList(new SimpleRecord(4, "d"));
    Dataset<Row> df = spark.createDataFrame(extraData, SimpleRecord.class);
    df.write().format("iceberg").mode("append").saveAsTable(dest);

    List<Row> result = spark.sql(String.format("SELECT * FROM %s", source)).collectAsList();
    assertThat(result)
        .as("No additional rows should be added to the original table")
        .hasSameSizeAs(expected);

    List<Row> snapshot =
        spark
            .sql(String.format("SELECT * FROM %s WHERE id = 4 AND data = 'd'", dest))
            .collectAsList();
    assertThat(snapshot).as("Added row not found in snapshot").hasSize(1);
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
