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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

public class TestSparkCatalogWithSQL {

  private static final String PROVIDER = "foo";
  private static final String SPARK_CATALOG_NAME = "iceberg_catalog";
  private static final String DB_NAME = "test_db";
  private static final String NAMESPACE_STR = "test_namespace";
  private static  File tmpSparkWarehouse;
  private static String partitionOverwriteModeKey = "spark.sql.sources.partitionOverwriteMode";
  private static String partitionOverwriteModeValue = "dynamic";
  private static Map<String, String> properties = ImmutableMap.of("status", "staging", "owner", "andrew");
  // table 0
  private static final String TABLE_NAME = "test_table";
  private static final String DB_TABLE = Joiner.on(".").join(DB_NAME, TABLE_NAME);
  private static final String CATALOG_DB_TABLE = Joiner.on(".").join(SPARK_CATALOG_NAME, DB_TABLE);

  // table 1
  private static final String TABLE_NAME_1 = "test_table_1";
  private static final String DB_TABLE_1 = Joiner.on(".").join(DB_NAME, TABLE_NAME_1);
  private static final String CATALOG_DB_TABLE_1 = Joiner.on(".").join(SPARK_CATALOG_NAME, DB_TABLE_1);

  private static final Namespace NAMESPACE = Namespace.of(DB_NAME);
  private static final Map<String, String> PROPERTIES_TO_VERIFY = new HashMap<String, String>() {
    {
      put("provider", PROVIDER);
      put("status", "staging");
      put("owner", "andrew");
    }
  };
  private static final String ASSERT_MESSAGE =
      "Table created by SQL should be the same as the one loaded by catalog";

  private static SparkSession spark;
  private static TestHiveMetastore metastore;
  private static HiveClientPool clients;
  private static HiveConf hiveConf;
  private static SparkCatalog icebergCatalog;

  private static SparkCatalog catalog(String name) {
    CatalogPlugin catPlug = spark.sessionState().catalogManager().catalog(name);
    if (catPlug instanceof SparkCatalog) {
      return (SparkCatalog) catPlug;
    }
    throw new NoClassDefFoundError();
  }

  @BeforeClass
  public static void startMetastoreAndSpark() throws TException, InterruptedException, IOException {
    metastore = new TestHiveMetastore();
    metastore.start();

    hiveConf = metastore.hiveConf();
    clients = new HiveClientPool(1, hiveConf);

    // Create DB in Hive
    String dbPath = metastore.getDatabasePath(NAMESPACE.level(0));
    Database db = new Database(NAMESPACE.level(0), "desc", dbPath, new HashMap<>());
    clients.run(client -> {
      client.createDatabase(db);
      return null;
    });

    tmpSparkWarehouse = createTempDirectory("spark-warehouse",
        asFileAttribute(fromString("rwxrwxrwx"))).toFile();
    spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME, SparkCatalog.class.getName())
        .config("spark.sql.warehouse.dir", tmpSparkWarehouse.getAbsolutePath())
        .getOrCreate();

    icebergCatalog = catalog(SPARK_CATALOG_NAME);
  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    clients.close();
    clients = null;

    metastore.stop();
    metastore = null;

    spark.stop();
    spark = null;
    if (tmpSparkWarehouse != null) {
      tmpSparkWarehouse.delete();
    }
  }

  @Before
  @After
  public void dropTable() {
    spark.sql("DROP TABLE IF EXISTS " + CATALOG_DB_TABLE);
    spark.sql("DROP TABLE IF EXISTS " + CATALOG_DB_TABLE_1);
  }

  @Test
  public void testCreateUnpartitionedTable() throws NoSuchTableException {
    // Create table by Spark SQL
    spark.sql("CREATE TABLE IF NOT EXISTS " + CATALOG_DB_TABLE +
        " (id bigint, data string)" +
        " USING " + PROVIDER +
        " TBLPROPERTIES ('status'='staging', 'owner'='andrew')");

    // Load table by catalog
    Table table = icebergCatalog.loadTable(Identifier.of(NAMESPACE.levels(), TABLE_NAME));

    // Verify
    Assert.assertEquals(ASSERT_MESSAGE, CATALOG_DB_TABLE, table.name());
    verifyPartitioning(CATALOG_DB_TABLE, false);  // un-partitioned
    Assert.assertEquals(ASSERT_MESSAGE, PROPERTIES_TO_VERIFY, table.properties());
    StructType schema = new StructType(new StructField[]{
        new StructField("id", DataTypes.LongType, true, Metadata.empty()),
        new StructField("data", DataTypes.StringType, true, Metadata.empty())});
    Assert.assertEquals(ASSERT_MESSAGE, schema, table.schema());
  }

  @Test
  public void testCreatePartitionedTable() throws NoSuchTableException {
    // Create table by Spark SQL
    spark.sql("CREATE TABLE IF NOT EXISTS " + CATALOG_DB_TABLE +
        " (id bigint, data string, time timestamp, zone string)" +
        " USING " + PROVIDER +
        " PARTITIONED BY (id, days(time))");

    // Load table by catalog
    Table table = icebergCatalog.loadTable(Identifier.of(NAMESPACE.levels(), TABLE_NAME));

    // Verify
    Assert.assertEquals(ASSERT_MESSAGE, CATALOG_DB_TABLE, table.name());
    verifyPartitioning(CATALOG_DB_TABLE, true, "id", "days(time)");
    StructType schema = new StructType(new StructField[]{
        new StructField("id", DataTypes.LongType, true, Metadata.empty()),
        new StructField("data", DataTypes.StringType, true, Metadata.empty()),
        new StructField("time", DataTypes.TimestampType, true, Metadata.empty()),
        new StructField("zone", DataTypes.StringType, true, Metadata.empty()),
    });
    Assert.assertEquals(ASSERT_MESSAGE, schema, table.schema());
  }

  @Test
  public void testInsert() {
    // Create table by Spark SQL
    spark.sql("CREATE TABLE " + CATALOG_DB_TABLE +
        " (id INT, data STRING)" +
        " USING " + PROVIDER +
        " PARTITIONED BY (bucket(2, id))");

    // Base INSERT
    spark.sql("INSERT INTO " + CATALOG_DB_TABLE + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    verifyData(Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"))
    );

    // INSERT
    spark.sql("INSERT INTO " + CATALOG_DB_TABLE + " VALUES (1, 'a'), (4, 'd')");
    verifyData(Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"))
    );
  }

  @Test
  public void testInsertOverwrite() {
    // Create table by Spark SQL
    spark.sql("CREATE TABLE " + CATALOG_DB_TABLE +
        " (id INT, data STRING)" +
        " USING " + PROVIDER +
        " PARTITIONED BY (id)");

    // Base INSERT
    spark.sql("INSERT INTO " + CATALOG_DB_TABLE + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    verifyData(Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"))
    );

    spark.conf().set(partitionOverwriteModeKey, partitionOverwriteModeValue);
    spark.sql("INSERT OVERWRITE " + CATALOG_DB_TABLE + " VALUES (1, 'aa'), (4, 'd')");

    // Verify
    verifyData(Lists.newArrayList(
        new SimpleRecord(1, "aa"),  // data of id 1 is overwritten to "aa" from "a"
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "d"))
    );
  }

  @Test
  public void testCTASAndRTASUnpartitionedTable() {
    // Create source table and base insert
    spark.sql("CREATE TABLE " + CATALOG_DB_TABLE + " (id INT, data STRING)" + " USING " + PROVIDER);
    spark.sql("INSERT INTO " + CATALOG_DB_TABLE + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    verifyData(Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"))
    );

    // Create table as select
    spark.sql("CREATE TABLE " + CATALOG_DB_TABLE_1 + " USING " + PROVIDER +
        " AS SELECT id, data FROM " + CATALOG_DB_TABLE + " WHERE id <= 2");

    // Verify partitioning
    verifyPartitioning(CATALOG_DB_TABLE_1, false);  // un-partitioned

    // Verify data
    verifyData(CATALOG_DB_TABLE_1, Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"))
    );

    // Replace table as select
    spark.sql("REPLACE TABLE " + CATALOG_DB_TABLE_1 + " USING " + PROVIDER +
        " AS SELECT id, data FROM " + CATALOG_DB_TABLE + " WHERE id <> 1 AND id <> 2");

    // Verify partition spec
    verifyPartitioning(CATALOG_DB_TABLE_1, false);  // un-partitioned

    // Verify data
    verifyData(CATALOG_DB_TABLE_1, Lists.newArrayList(new SimpleRecord(3, "c")));
  }

  @Test
  public void testCTASAndRTASPartitionedTable() {
    // Create source table and base insert
    spark.sql("CREATE TABLE " + CATALOG_DB_TABLE + " (id INT, date DATE)" + " USING " + PROVIDER);
    spark.sql("INSERT INTO " + CATALOG_DB_TABLE +
        " VALUES (1, date('2017-01-01')), (2, date('2018-02-02')), (3, date('2019-03-03'))");
    List<String> actual = spark.sql("SELECT date from " + CATALOG_DB_TABLE + " ORDER BY id")
        .collectAsList().stream().map(row -> row.getDate(0).toString()).collect(Collectors.toList());
    Assert.assertEquals(Arrays.asList("2017-01-01", "2018-02-02", "2019-03-03"), actual);

    // Create table as select
    spark.sql("CREATE TABLE " + CATALOG_DB_TABLE_1 + " USING " + PROVIDER + " PARTITIONED BY (years(date))" +
        " AS SELECT id, date FROM " + CATALOG_DB_TABLE + " WHERE id <= 2");

    // Verify partitioning
    verifyPartitioning(CATALOG_DB_TABLE_1, true, "years(date)");

    // Verify data
    actual = spark.sql("SELECT date from " + CATALOG_DB_TABLE_1 + " ORDER BY id")
        .collectAsList().stream().map(row -> row.getDate(0).toString()).collect(Collectors.toList());
    Assert.assertEquals(Arrays.asList("2017-01-01", "2018-02-02"), actual);

    // Replace table as select
    spark.sql("REPLACE TABLE " + CATALOG_DB_TABLE_1 + " USING " + PROVIDER + " PARTITIONED BY (id, days(date))" +
        " AS SELECT id, date FROM " + CATALOG_DB_TABLE + " WHERE id <>1 AND id <> 2");

    // Verify partitioning
    verifyPartitioning(CATALOG_DB_TABLE_1, true, "id", "days(date)");

    // Verify data
    actual = spark.sql("SELECT date from " + CATALOG_DB_TABLE_1 + " ORDER BY id")
        .collectAsList().stream().map(row -> row.getDate(0).toString()).collect(Collectors.toList());
    Assert.assertEquals(Arrays.asList("2019-03-03"), actual);
  }

  @Test
  public void testCreateNameSpaceWithHiveCatalog() throws NoSuchNamespaceException {
    String[] str = {NAMESPACE_STR};
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + "." +
        NAMESPACE_STR + " LOCATION '" + tmpSparkWarehouse +
        "' WITH DBPROPERTIES ('status'='staging', 'owner'='andrew')");
    Map<String, String> meta = icebergCatalog.loadNamespaceMetadata(str);
    Assert.assertEquals("Location is  is expected", tmpSparkWarehouse.toString(), meta.get("location"));
    Assert.assertEquals("status is  is expected", properties.get("status"), meta.get("status"));
    Assert.assertEquals("owner is  is expected", properties.get("owner"), meta.get("owner"));
  }

  @Test
  public void testListNameSpaceWithHiveCatalog() {
    List<String> dbs = spark.sql("SHOW NAMESPACES IN " +
        SPARK_CATALOG_NAME).collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    Assert.assertTrue("namespace test_db is expected", dbs.contains("test_db"));
    List<String> db = spark.sql("SHOW NAMESPACES IN " +
        SPARK_CATALOG_NAME + ".test_db").collectAsList().stream().map(row -> row.getString(0))
        .collect(Collectors.toList());
    Assert.assertTrue("namespace test_db is expected", db.contains("test_db"));
  }

  @Test (expected = NoSuchNamespaceException.class)
  public void testDropNameSpaceWithHiveCatalog() throws NoSuchNamespaceException {
    String[] str = {NAMESPACE_STR};
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + SPARK_CATALOG_NAME + "." +
        NAMESPACE_STR + " LOCATION '" + tmpSparkWarehouse +
        "' WITH DBPROPERTIES ('status'='staging', 'owner'='andrew')");
    spark.sql("DROP NAMESPACE " + SPARK_CATALOG_NAME + "." + NAMESPACE_STR);
    icebergCatalog.loadNamespaceMetadata(str);
  }

  private void verifyData(String tableIdent, List<SimpleRecord> expected) {
    List<String> actual = spark.sql("SELECT data FROM " + tableIdent + " ORDER BY id")
        .collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    Assert.assertEquals(expected.stream().map(record -> record.getData()).collect(Collectors.toList()), actual);
  }

  private void verifyData(List<SimpleRecord> expected) {
    verifyData(CATALOG_DB_TABLE, expected);
  }

  private void verifyId(String... expected) {
    List<String> actual = spark.sql("SELECT id FROM " + CATALOG_DB_TABLE + " ORDER BY id")
        .collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    Assert.assertEquals(Arrays.asList(expected), actual);
  }

  private void verifyPartitioning(String tableIdent, boolean partitioned, String... expectedSpecs) {
    List<String> actual = spark.sql("DESCRIBE EXTENDED " + tableIdent)
        .filter(new Column("col_name").startsWith("Part ")).orderBy("col_name").select("data_type")
        .collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());
    if (partitioned) {  // partitioned table
      Assert.assertEquals("Partition spec mismatch", Arrays.asList(expectedSpecs), actual);
    } else {  // un-partitioned table
      Assert.assertEquals("Un-partitioned table is expected", 0, actual.size());
    }
  }
}
