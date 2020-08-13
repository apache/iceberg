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

package org.apache.iceberg.spark.source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveTableBaseTest;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTableUtil.SparkPartition;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.TableProperties.PARQUET_VECTORIZATION_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestSparkTableUtil extends HiveTableBaseTest {
  private static final Configuration CONF = HiveTableBaseTest.hiveConf;
  private static final String tableName = "hive_table";
  private static final String dbName = HiveTableBaseTest.DB_NAME;
  private static final String qualifiedTableName = String.format("%s.%s", dbName, tableName);
  private static final Path tableLocationPath = HiveTableBaseTest.getTableLocationPath(tableName);
  private static final String tableLocationStr = tableLocationPath.toString();
  private static SparkSession spark = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();


  @BeforeClass
  public static void startSpark() {
    String metastoreURI = CONF.get(HiveConf.ConfVars.METASTOREURIS.varname);

    // Create a spark session.
    TestSparkTableUtil.spark = SparkSession.builder().master("local[2]")
            .enableHiveSupport()
            .config("spark.hadoop.hive.metastore.uris", metastoreURI)
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
            .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkTableUtil.spark;
    // Stop the spark session.
    TestSparkTableUtil.spark = null;
    currentSpark.stop();
  }

  @Before
  public void before() {

    // Create a hive table.
    SQLContext sc = new SQLContext(TestSparkTableUtil.spark);

    sc.sql(String.format(
                    "CREATE TABLE %s (\n" +
                    "    id int COMMENT 'unique id'\n" +
                    ")\n" +
                    " PARTITIONED BY (data string)\n" +
                    " LOCATION '%s'", qualifiedTableName, tableLocationStr)
    );

    List<SimpleRecord> expected = Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").orderBy("data").write()
            .mode("append")
            .insertInto(qualifiedTableName);
  }

  @After
  public void after() throws IOException {
    // Drop the hive table.
    SQLContext sc = new SQLContext(TestSparkTableUtil.spark);
    sc.sql(String.format("DROP TABLE IF EXISTS %s", qualifiedTableName));

    // Delete the data corresponding to the table.
    tableLocationPath.getFileSystem(CONF).delete(tableLocationPath, true);
  }

  @Test
  public void testPartitionScan() {
    List<SparkPartition> partitions = SparkTableUtil.getPartitions(spark, qualifiedTableName);
    Assert.assertEquals("There should be 3 partitions", 3, partitions.size());

    Dataset<Row> partitionDF = SparkTableUtil.partitionDF(spark, qualifiedTableName);
    Assert.assertEquals("There should be 3 partitions", 3, partitionDF.count());
  }

  @Test
  public void testPartitionScanByFilter() {
    List<SparkPartition> partitions = SparkTableUtil.getPartitionsByFilter(spark, qualifiedTableName, "data = 'a'");
    Assert.assertEquals("There should be 1 matching partition", 1, partitions.size());

    Dataset<Row> partitionDF = SparkTableUtil.partitionDFByFilter(spark, qualifiedTableName, "data = 'a'");
    Assert.assertEquals("There should be 1 matching partition", 1, partitionDF.count());
  }

  @Test
  public void testImportPartitionedTable() throws Exception {
    File location = temp.newFolder("partitioned_table");
    spark.table(qualifiedTableName).write().mode("overwrite").partitionBy("data").format("parquet")
            .saveAsTable("test_partitioned_table");
    TableIdentifier source = spark.sessionState().sqlParser()
            .parseTableIdentifier("test_partitioned_table");
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table table = tables.create(SparkSchemaUtil.schemaForTable(spark, qualifiedTableName),
            SparkSchemaUtil.specForTable(spark, qualifiedTableName),
            ImmutableMap.of(),
            location.getCanonicalPath());
    File stagingDir = temp.newFolder("staging-dir");
    SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
    long count = spark.read().format("iceberg").load(location.toString()).count();
    Assert.assertEquals("three values ", 3, count);
  }

  @Test
  public void testImportUnpartitionedTable() throws Exception {
    File location = temp.newFolder("unpartitioned_table");
    spark.table(qualifiedTableName).write().mode("overwrite").format("parquet")
            .saveAsTable("test_unpartitioned_table");
    TableIdentifier source = spark.sessionState().sqlParser()
            .parseTableIdentifier("test_unpartitioned_table");
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table table = tables.create(SparkSchemaUtil.schemaForTable(spark, qualifiedTableName),
            SparkSchemaUtil.specForTable(spark, qualifiedTableName),
            ImmutableMap.of(),
            location.getCanonicalPath());
    File stagingDir = temp.newFolder("staging-dir");
    SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
    long count = spark.read().format("iceberg").load(location.toString()).count();
    Assert.assertEquals("three values ", 3, count);
  }

  @Test
  public void testImportAsHiveTable() throws Exception {
    spark.table(qualifiedTableName).write().mode("overwrite").format("parquet")
            .saveAsTable("unpartitioned_table");
    TableIdentifier source = new TableIdentifier("unpartitioned_table");
    Table table = catalog.createTable(
            org.apache.iceberg.catalog.TableIdentifier.of(DB_NAME, "test_unpartitioned_table"),
            SparkSchemaUtil.schemaForTable(spark, "unpartitioned_table"),
            SparkSchemaUtil.specForTable(spark, "unpartitioned_table"));
    File stagingDir = temp.newFolder("staging-dir");
    SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
    long count1 = spark.read().format("iceberg").load(DB_NAME + ".test_unpartitioned_table").count();
    Assert.assertEquals("three values ", 3, count1);

    spark.table(qualifiedTableName).write().mode("overwrite").partitionBy("data").format("parquet")
            .saveAsTable("partitioned_table");
    source = new TableIdentifier("partitioned_table");
    table = catalog.createTable(
            org.apache.iceberg.catalog.TableIdentifier.of(DB_NAME, "test_partitioned_table"),
            SparkSchemaUtil.schemaForTable(spark, "partitioned_table"),
            SparkSchemaUtil.specForTable(spark, "partitioned_table"));

    SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
    long count2 = spark.read().format("iceberg").load(DB_NAME + ".test_partitioned_table").count();
    Assert.assertEquals("three values ", 3, count2);
  }

  @Test
  public void testImportWithNameMapping() throws Exception {
    spark.table(qualifiedTableName).write().mode("overwrite").format("parquet")
        .saveAsTable("original_table");

    // The field is different so that it will project with name mapping
    Schema filteredSchema = new Schema(
        optional(1, "data", Types.StringType.get())
    );

    NameMapping nameMapping = MappingUtil.create(filteredSchema);

    TableIdentifier source = new TableIdentifier("original_table");
    Table table = catalog.createTable(
        org.apache.iceberg.catalog.TableIdentifier.of(DB_NAME, "target_table"),
        filteredSchema,
        SparkSchemaUtil.specForTable(spark, "original_table"));

    table.updateProperties().set(DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping)).commit();

    File stagingDir = temp.newFolder("staging-dir");
    SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());

    // The filter invoke the metric/dictionary row group filter in which it project schema
    // with name mapping again to match the metric read from footer.
    List<String> actual = spark.read().format("iceberg").load(DB_NAME + ".target_table")
        .select("data")
        .sort("data")
        .filter("data >= 'b'")
        .as(Encoders.STRING())
        .collectAsList();

    List<String> expected = Lists.newArrayList("b", "c");

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testImportWithNameMappingForVectorizedParquetReader() throws Exception {
    spark.table(qualifiedTableName).write().mode("overwrite").format("parquet")
        .saveAsTable("original_table");

    // The field is different so that it will project with name mapping
    Schema filteredSchema = new Schema(
        optional(1, "data", Types.StringType.get())
    );

    NameMapping nameMapping = MappingUtil.create(filteredSchema);

    TableIdentifier source = new TableIdentifier("original_table");
    Table table = catalog.createTable(
        org.apache.iceberg.catalog.TableIdentifier.of(DB_NAME, "target_table_for_vectorization"),
        filteredSchema,
        SparkSchemaUtil.specForTable(spark, "original_table"));

    table.updateProperties()
        .set(DEFAULT_NAME_MAPPING, NameMappingParser.toJson(nameMapping))
        .set(PARQUET_VECTORIZATION_ENABLED, "true")
        .commit();

    File stagingDir = temp.newFolder("staging-dir");
    SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());

    // The filter invoke the metric/dictionary row group filter in which it project schema
    // with name mapping again to match the metric read from footer.
    List<String> actual = spark.read().format("iceberg")
        .load(DB_NAME + ".target_table_for_vectorization")
        .select("data")
        .sort("data")
        .filter("data >= 'b'")
        .as(Encoders.STRING())
        .collectAsList();

    List<String> expected = Lists.newArrayList("b", "c");

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testImportPartitionedWithWhitespace() throws Exception {
    String partitionCol = "dAtA sPaced";
    String spacedTableName = "whitespacetable";
    String whiteSpaceKey = "some key value";

    List<SimpleRecord> spacedRecords = Lists.newArrayList(new SimpleRecord(1, whiteSpaceKey));

    File icebergLocation = temp.newFolder("partitioned_table");

    spark.createDataFrame(spacedRecords, SimpleRecord.class)
        .withColumnRenamed("data", partitionCol)
        .write().mode("overwrite").partitionBy(partitionCol).format("parquet")
        .saveAsTable(spacedTableName);

    TableIdentifier source = spark.sessionState().sqlParser()
        .parseTableIdentifier(spacedTableName);
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table table = tables.create(SparkSchemaUtil.schemaForTable(spark, spacedTableName),
        SparkSchemaUtil.specForTable(spark, spacedTableName),
        ImmutableMap.of(),
        icebergLocation.getCanonicalPath());
    File stagingDir = temp.newFolder("staging-dir");
    SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
    List<SimpleRecord> results = spark.read().format("iceberg").load(icebergLocation.toString())
        .withColumnRenamed(partitionCol, "data")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Data should match", spacedRecords, results);
  }

  @Test
  public void testImportUnpartitionedWithWhitespace() throws Exception {
    String spacedTableName = "whitespacetable";
    String whiteSpaceKey = "some key value";

    List<SimpleRecord> spacedRecords = Lists.newArrayList(new SimpleRecord(1, whiteSpaceKey));

    File whiteSpaceOldLocation = temp.newFolder("white space location");
    File icebergLocation = temp.newFolder("partitioned_table");

    spark.createDataFrame(spacedRecords, SimpleRecord.class)
        .write().mode("overwrite").parquet(whiteSpaceOldLocation.getPath());

    spark.catalog().createExternalTable(spacedTableName, whiteSpaceOldLocation.getPath());

    TableIdentifier source = spark.sessionState().sqlParser()
        .parseTableIdentifier(spacedTableName);
    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    Table table = tables.create(SparkSchemaUtil.schemaForTable(spark, spacedTableName),
        SparkSchemaUtil.specForTable(spark, spacedTableName),
        ImmutableMap.of(),
        icebergLocation.getCanonicalPath());
    File stagingDir = temp.newFolder("staging-dir");
    SparkTableUtil.importSparkTable(spark, source, table, stagingDir.toString());
    List<SimpleRecord> results = spark.read().format("iceberg").load(icebergLocation.toString())
        .as(Encoders.bean(SimpleRecord.class)).collectAsList();

    Assert.assertEquals("Data should match", spacedRecords, results);
  }
}
