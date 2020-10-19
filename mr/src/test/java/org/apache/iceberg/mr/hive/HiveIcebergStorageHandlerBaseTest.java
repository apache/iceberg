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

package org.apache.iceberg.mr.hive;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(StandaloneHiveRunner.class)
public abstract class HiveIcebergStorageHandlerBaseTest {

  @HiveSQL(files = {}, autoStart = false)
  private HiveShell shell;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static final Schema CUSTOMER_SCHEMA = new Schema(
          required(1, "customer_id", Types.LongType.get()),
          required(2, "first_name", Types.StringType.get())
  );

  private static final List<Record> CUSTOMER_RECORDS = TestHelper.RecordsBuilder.newInstance(CUSTOMER_SCHEMA)
          .add(0L, "Alice")
          .add(1L, "Bob")
          .add(2L, "Trudy")
          .build();

  private static final Schema ORDER_SCHEMA = new Schema(
          required(1, "order_id", Types.LongType.get()),
          required(2, "customer_id", Types.LongType.get()),
          required(3, "total", Types.DoubleType.get()));

  private static final List<Record> ORDER_RECORDS = TestHelper.RecordsBuilder.newInstance(ORDER_SCHEMA)
          .add(100L, 0L, 11.11d)
          .add(101L, 0L, 22.22d)
          .add(102L, 1L, 33.33d)
          .build();

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private static final PartitionSpec IDENTITY_SPEC =
      PartitionSpec.builderFor(CUSTOMER_SCHEMA).identity("customer_id").build();

  private static final Set<String> IGNORED_PARAMS =
      ImmutableSet.of("bucketing_version", StatsSetupConst.ROW_COUNT,
          StatsSetupConst.RAW_DATA_SIZE, StatsSetupConst.TOTAL_SIZE, StatsSetupConst.NUM_FILES);

  private static final int METASTORE_POOL_SIZE = 15;

  // before variables
  protected static TestHiveMetastore metastore;

  private TestTables testTables;

  public abstract TestTables testTables(Configuration conf, TemporaryFolder tmp) throws IOException;


  @BeforeClass
  public static void beforeClass() {
    metastore = new TestHiveMetastore();
    // We need to use increased pool size in these tests. See: #1620
    metastore.start(METASTORE_POOL_SIZE);
  }

  @AfterClass
  public static void afterClass() {
    metastore.stop();
    metastore = null;
  }

  @Before
  public void before() throws IOException {
    String metastoreUris = metastore.hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS);

    testTables = testTables(metastore.hiveConf(), temp);

    for (Map.Entry<String, String> property : testTables.properties().entrySet()) {
      shell.setHiveConfValue(property.getKey(), property.getValue());
    }

    shell.setHiveConfValue(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUris);
    String metastoreWarehouse = metastore.hiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    shell.setHiveConfValue(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, metastoreWarehouse);

    // Notification uses another HMSClient which we do not use in the tests, so we turn this off.
    shell.setHiveConfValue("hive.notification.event.poll.interval", "-1");
    shell.start();
  }

  @After
  public void after() throws Exception {
    metastore.reset();
    // HiveServer2 thread pools are using thread local Hive -> HMSClient objects. These are not cleaned up when the
    // HiveServer2 is stopped. Only Finalizer closes the HMS connections.
    System.gc();
  }

  // PARQUET

  @Test
  public void testScanEmptyTableParquet() throws IOException {
    testScanEmptyTable(FileFormat.PARQUET);
  }

  @Test
  public void testScanTableParquet() throws IOException {
    testScanTable(FileFormat.PARQUET);
  }

  @Test
  public void testJoinTablesParquet() throws IOException {
    testJoinTables(FileFormat.PARQUET);
  }

  // ORC

  @Test
  public void testScanEmptyTableORC() throws IOException {
    testScanEmptyTable(FileFormat.ORC);
  }

  @Test
  public void testScanTableORC() throws IOException {
    testScanTable(FileFormat.ORC);
  }

  @Test
  public void testJoinTablesORC() throws IOException {
    testJoinTables(FileFormat.ORC);
  }

  // AVRO

  @Test
  public void testScanEmptyTableAvro() throws IOException {
    testScanEmptyTable(FileFormat.AVRO);
  }

  @Test
  public void testScanTableAvro() throws IOException {
    testScanTable(FileFormat.AVRO);
  }

  @Test
  public void testJoinTablesAvro() throws IOException {
    testJoinTables(FileFormat.AVRO);
  }

  public void testScanEmptyTable(FileFormat format) throws IOException {
    Schema emptySchema = new Schema(required(1, "empty", Types.StringType.get()));
    createTable("empty", emptySchema, format, ImmutableList.of());

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.empty");
    Assert.assertEquals(0, rows.size());
  }

  public void testScanTable(FileFormat format) throws IOException {
    createTable("customers", CUSTOMER_SCHEMA, format, CUSTOMER_RECORDS);

    // Single fetch task: no MR job.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, rows.get(2));

    // Adding the ORDER BY clause will cause Hive to spawn a local MR job this time.
    List<Object[]> descRows = shell.executeStatement("SELECT * FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, descRows.get(2));
  }

  public void testJoinTables(FileFormat format) throws IOException {
    createTable("customers", CUSTOMER_SCHEMA, format, CUSTOMER_RECORDS);
    createTable("orders", ORDER_SCHEMA, format, ORDER_RECORDS);

    List<Object[]> rows = shell.executeStatement(
            "SELECT c.customer_id, c.first_name, o.order_id, o.total " +
                    "FROM default.customers c JOIN default.orders o ON c.customer_id = o.customer_id " +
                    "ORDER BY c.customer_id, o.order_id"
    );

    Assert.assertArrayEquals(new Object[] {0L, "Alice", 100L, 11.11d}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {0L, "Alice", 101L, 22.22d}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", 102L, 33.33d}, rows.get(2));
  }

  @Test
  public void testCreateDropTable() throws TException, IOException, InterruptedException {
    // We need the location for HadoopTable based tests only
    String location = locationForCreateTable(temp.getRoot().getPath(), "customers");
    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        (location != null ? "LOCATION '" + location + "' " : "") +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" + SchemaParser.toJson(CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.PARTITION_SPEC + "'='" + PartitionSpecParser.toJson(IDENTITY_SPEC) + "', " +
        "'dummy'='test')");

    Properties properties = new Properties();
    properties.put(Catalogs.NAME, TableIdentifier.of("default", "customers").toString());
    if (location != null) {
      properties.put(Catalogs.LOCATION, location);
    }

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = Catalogs.loadTable(shell.getHiveConf(), properties);
    Assert.assertEquals(CUSTOMER_SCHEMA.asStruct(), icebergTable.schema().asStruct());
    Assert.assertEquals(IDENTITY_SPEC, icebergTable.spec());

    // Check the HMS table parameters
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        metastore.clientPool().run(client -> client.getTable("default", "customers"));

    Map<String, String> hmsParams = hmsTable.getParameters();
    IGNORED_PARAMS.forEach(hmsParams::remove);

    // This is only set for HiveCatalog based tables. Check the value, then remove it so the other checks can be general
    if (Catalogs.hiveCatalog(shell.getHiveConf())) {
      Assert.assertTrue(hmsParams.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
          .startsWith(icebergTable.location()));
      hmsParams.remove(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    }

    // General metadata checks
    Assert.assertEquals(6, hmsParams.size());
    Assert.assertEquals("test", hmsParams.get("dummy"));
    Assert.assertEquals("TRUE", hmsParams.get(InputFormatConfig.EXTERNAL_TABLE_PURGE));
    Assert.assertEquals("TRUE", hmsParams.get("EXTERNAL"));
    Assert.assertNotNull(hmsParams.get(hive_metastoreConstants.DDL_TIME));
    Assert.assertEquals(HiveIcebergStorageHandler.class.getName(),
        hmsTable.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE));
    Assert.assertEquals(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(),
        hmsTable.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));

    if (!Catalogs.hiveCatalog(shell.getHiveConf())) {
      Assert.assertEquals(Collections.singletonMap("dummy", "test"), icebergTable.properties());

      shell.executeStatement("DROP TABLE customers");

      // Check if the table was really dropped even from the Catalog
      AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
          "Table does not exist", () -> {
            Catalogs.loadTable(shell.getHiveConf(), properties);
          }
      );
    } else {
      Map<String, String> expectedIcebergProperties = new HashMap<>(2);
      expectedIcebergProperties.put("dummy", "test");
      expectedIcebergProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
      Assert.assertEquals(expectedIcebergProperties, icebergTable.properties());

      // Check the HMS table parameters
      hmsTable = metastore.clientPool().run(client -> client.getTable("default", "customers"));
      Path hmsTableLocation = new Path(hmsTable.getSd().getLocation());

      // Drop the table
      shell.executeStatement("DROP TABLE customers");

      // Check if we drop an exception when trying to load the table
      AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
          "Table does not exist", () -> {
            Catalogs.loadTable(shell.getHiveConf(), properties);
          }
      );

      // Check if the files are removed
      FileSystem fs = Util.getFs(hmsTableLocation, shell.getHiveConf());
      Assert.assertEquals(1, fs.listStatus(hmsTableLocation).length);
      Assert.assertEquals(0, fs.listStatus(new Path(hmsTableLocation, "metadata")).length);
    }
  }

  @Test
  public void testCreateTableWithoutSpec() throws TException, InterruptedException {
    // We need the location for HadoopTable based tests only
    String location = locationForCreateTable(temp.getRoot().getPath(), "customers");
    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        (location != null ? "LOCATION '" + location + "' " : "") +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" + SchemaParser.toJson(CUSTOMER_SCHEMA) + "')");

    Properties properties = new Properties();
    properties.put(Catalogs.NAME, TableIdentifier.of("default", "customers").toString());
    if (location != null) {
      properties.put(Catalogs.LOCATION, location);
    }

    // Check the Iceberg table partition data
    org.apache.iceberg.Table icebergTable = Catalogs.loadTable(shell.getHiveConf(), properties);
    Assert.assertEquals(SPEC, icebergTable.spec());

    // Check the HMS table parameters
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        metastore.clientPool().run(client -> client.getTable("default", "customers"));

    Map<String, String> hmsParams = hmsTable.getParameters();
    IGNORED_PARAMS.forEach(hmsParams::remove);

    // Just check that the PartitionSpec is not set in the metadata
    Assert.assertNull(hmsParams.get(InputFormatConfig.PARTITION_SPEC));

    if (Catalogs.hiveCatalog(shell.getHiveConf())) {
      Assert.assertEquals(6, hmsParams.size());
    } else {
      Assert.assertEquals(5, hmsParams.size());
    }
  }

  @Test
  public void testCreateTableWithUnpartitionedSpec() throws TException, InterruptedException {
    // We need the location for HadoopTable based tests only
    String location = locationForCreateTable(temp.getRoot().getPath(), "customers");
    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        (location != null ? "LOCATION '" + location + "' " : "") +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" + SchemaParser.toJson(CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.PARTITION_SPEC + "'='" + PartitionSpecParser.toJson(SPEC) + "')");

    Properties properties = new Properties();
    properties.put(Catalogs.NAME, TableIdentifier.of("default", "customers").toString());
    if (location != null) {
      properties.put(Catalogs.LOCATION, location);
    }

    // Check the Iceberg table partition data
    org.apache.iceberg.Table icebergTable = Catalogs.loadTable(shell.getHiveConf(), properties);
    Assert.assertEquals(SPEC, icebergTable.spec());

    // Check the HMS table parameters
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        metastore.clientPool().run(client -> client.getTable("default", "customers"));

    Map<String, String> hmsParams = hmsTable.getParameters();
    IGNORED_PARAMS.forEach(hmsParams::remove);

    // Just check that the PartitionSpec is not set in the metadata
    Assert.assertNull(hmsParams.get(InputFormatConfig.PARTITION_SPEC));
    if (Catalogs.hiveCatalog(shell.getHiveConf())) {
      Assert.assertEquals(6, hmsParams.size());
    } else {
      Assert.assertEquals(5, hmsParams.size());
    }
  }

  @Test
  public void testDeleteBackingTable() throws TException, IOException, InterruptedException {
    // We need the location for HadoopTable based tests only
    String location = locationForCreateTable(temp.getRoot().getPath(), "customers");
    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        (location != null ? "LOCATION '" + location + "' " : "") +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" + SchemaParser.toJson(CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.EXTERNAL_TABLE_PURGE + "'='FALSE')");

    Properties properties = new Properties();
    properties.put(Catalogs.NAME, TableIdentifier.of("default", "customers").toString());
    if (location != null) {
      properties.put(Catalogs.LOCATION, location);
    }

    if (!Catalogs.hiveCatalog(shell.getHiveConf())) {
      shell.executeStatement("DROP TABLE customers");

      // Check if the table remains
      Catalogs.loadTable(shell.getHiveConf(), properties);
    } else {
      // Check the HMS table parameters
      org.apache.hadoop.hive.metastore.api.Table hmsTable =
          metastore.clientPool().run(client -> client.getTable("default", "customers"));
      Path hmsTableLocation = new Path(hmsTable.getSd().getLocation());

      // Drop the table
      shell.executeStatement("DROP TABLE customers");

      // Check if we drop an exception when trying to drop the table
      AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
          "Table does not exist", () -> {
            Catalogs.loadTable(shell.getHiveConf(), properties);
          }
      );

      // Check if the files are kept
      FileSystem fs = Util.getFs(hmsTableLocation, shell.getHiveConf());
      Assert.assertEquals(1, fs.listStatus(hmsTableLocation).length);
      Assert.assertEquals(1, fs.listStatus(new Path(hmsTableLocation, "metadata")).length);
    }
  }

  @Test
  public void testCreateTableError() {
    String location = locationForCreateTable(temp.getRoot().getPath(), "customers");

    // Wrong schema
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Unrecognized token 'WrongSchema'", () -> {
          shell.executeQuery("CREATE EXTERNAL TABLE withShell2 " +
              "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
              (location != null ? "LOCATION '" + location + "' " : "") +
              "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='WrongSchema')");
        }
    );

    // Missing schema, we try to get the schema from the table and fail
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Please provide an existing table or a valid schema", () -> {
          shell.executeQuery("CREATE EXTERNAL TABLE withShell2 " +
              "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
              (location != null ? "LOCATION '" + location + "' " : ""));
        }
    );

    if (location != null) {
      // Missing location
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "Table location not set", () -> {
            shell.executeQuery("CREATE EXTERNAL TABLE withShell2 " +
                "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
                "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
                SchemaParser.toJson(CUSTOMER_SCHEMA) + "')");
          }
      );
    }
  }

  @Test
  public void testCreateTableAboveExistingTable() throws TException, IOException, InterruptedException {
    // Create the Iceberg table
    createIcebergTable("customers", CUSTOMER_SCHEMA, FileFormat.PARQUET, Collections.emptyList());

    if (Catalogs.hiveCatalog(shell.getHiveConf())) {

      // In HiveCatalog we just expect an exception since the table is already exists
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "customers already exists", () -> {
            shell.executeQuery("CREATE EXTERNAL TABLE customers " +
                "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
                "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
                SchemaParser.toJson(CUSTOMER_SCHEMA) + "')");
          }
      );
    } else {
      // We need the location for HadoopTable based tests only
      String location = locationForCreateTable(temp.getRoot().getPath(), "customers");

      shell.executeStatement("CREATE EXTERNAL TABLE customers " +
          "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
          (location != null ? "LOCATION '" + location + "'" : ""));

      Properties properties = new Properties();
      properties.put(Catalogs.NAME, TableIdentifier.of("default", "customers").toString());
      if (location != null) {
        properties.put(Catalogs.LOCATION, location);
      }

      // Check the HMS table parameters
      org.apache.hadoop.hive.metastore.api.Table hmsTable =
          metastore.clientPool().run(client -> client.getTable("default", "customers"));

      Map<String, String> hmsParams = hmsTable.getParameters();
      IGNORED_PARAMS.forEach(hmsParams::remove);

      Assert.assertEquals(4, hmsParams.size());
      Assert.assertEquals("TRUE", hmsParams.get("EXTERNAL"));
      Assert.assertNotNull(hmsParams.get(hive_metastoreConstants.DDL_TIME));
      Assert.assertEquals(HiveIcebergStorageHandler.class.getName(),
          hmsTable.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE));
      Assert.assertEquals(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(),
          hmsTable.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
    }
  }

  protected void createTable(String tableName, Schema schema, FileFormat format, List<Record> records)
          throws IOException {
    Table table = createIcebergTable(tableName, schema, format, records);
    createHiveTable(tableName, table.location());
  }

  protected Table createIcebergTable(String tableName, Schema schema, FileFormat format, List<Record> records)
          throws IOException {
    String identifier = testTables.identifier("default." + tableName);
    TestHelper helper = new TestHelper(
            metastore.hiveConf(), testTables.tables(), identifier, schema, SPEC, format, temp);
    Table table = helper.createTable();

    if (!records.isEmpty()) {
      helper.appendToTable(helper.writeFile(null, records));
    }

    return table;
  }

  protected void createHiveTable(String tableName, String location) {
    shell.execute(String.format(
            "CREATE TABLE default.%s " +
            "STORED BY '%s' " +
            "LOCATION '%s'",
            tableName, HiveIcebergStorageHandler.class.getName(), location));
  }

  protected String locationForCreateTable(String tempDirName, String tableName) {
    return null;
  }
}
