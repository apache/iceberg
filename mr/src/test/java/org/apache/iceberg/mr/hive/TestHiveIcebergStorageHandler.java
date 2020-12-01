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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Type;
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
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHiveIcebergStorageHandler {
  private static final FileFormat[] FILE_FORMATS =
      new FileFormat[] {FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET};

  private static final String[] EXECUTION_ENGINES = new String[] {"mr", "tez"};

  private static final Schema CUSTOMER_SCHEMA = new Schema(
          optional(1, "customer_id", Types.LongType.get()),
          optional(2, "first_name", Types.StringType.get()),
          optional(3, "last_name", Types.StringType.get())
  );

  private static final List<Record> CUSTOMER_RECORDS = TestHelper.RecordsBuilder.newInstance(CUSTOMER_SCHEMA)
          .add(0L, "Alice", "Brown")
          .add(1L, "Bob", "Green")
          .add(2L, "Trudy", "Pink")
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

  private static final Schema COMPLEX_SCHEMA = new Schema(
      optional(1, "id", Types.LongType.get()),
      optional(2, "name", Types.StringType.get()),
      optional(3, "employee_info", Types.StructType.of(
          optional(7, "employer", Types.StringType.get()),
          optional(8, "id", Types.LongType.get()),
          optional(9, "address", Types.StringType.get())
      )),
      optional(4, "places_lived", Types.ListType.ofOptional(10, Types.StructType.of(
          optional(11, "street", Types.StringType.get()),
          optional(12, "city", Types.StringType.get()),
          optional(13, "country", Types.StringType.get())
      ))),
      optional(5, "memorable_moments", Types.MapType.ofOptional(14, 15,
          Types.StringType.get(),
          Types.StructType.of(
              optional(16, "year", Types.IntegerType.get()),
              optional(17, "place", Types.StringType.get()),
              optional(18, "details", Types.StringType.get())
          ))),
      optional(6, "current_address", Types.StructType.of(
          optional(19, "street_address", Types.StructType.of(
              optional(22, "street_number", Types.IntegerType.get()),
              optional(23, "street_name", Types.StringType.get()),
              optional(24, "street_type", Types.StringType.get())
          )),
          optional(20, "country", Types.StringType.get()),
          optional(21, "postal_code", Types.StringType.get())
      ))
  );

  private static final Set<String> IGNORED_PARAMS =
      ImmutableSet.of("bucketing_version", StatsSetupConst.ROW_COUNT,
          StatsSetupConst.RAW_DATA_SIZE, StatsSetupConst.TOTAL_SIZE, StatsSetupConst.NUM_FILES, "numFilesErasureCoded");

  private static final List<Type> SUPPORTED_TYPES =
          ImmutableList.of(Types.BooleanType.get(), Types.IntegerType.get(), Types.LongType.get(),
                  Types.FloatType.get(), Types.DoubleType.get(), Types.DateType.get(), Types.TimestampType.withZone(),
                  Types.TimestampType.withoutZone(), Types.StringType.get(), Types.BinaryType.get(),
                  Types.DecimalType.of(3, 1));

  @Parameters(name = "fileFormat={0}, engine={1}, catalog={2}")
  public static Collection<Object[]> parameters() {
    String javaVersion = System.getProperty("java.specification.version");

    Collection<Object[]> testParams = new ArrayList<>();
    for (FileFormat fileFormat : FILE_FORMATS) {
      for (String engine : EXECUTION_ENGINES) {
        // include Tez tests only for Java 8
        if (javaVersion.equals("1.8") || "mr".equals(engine)) {
          for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
            testParams.add(new Object[] {fileFormat, engine, testTableType});
          }
        }
      }
    }

    return testParams;
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(0)
  public FileFormat fileFormat;

  @Parameter(1)
  public String executionEngine;

  @Parameter(2)
  public TestTables.TestTableType testTableType;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() {
    shell = new TestHiveShell();
    shell.setHiveConfValue("hive.notification.event.poll.interval", "-1");
    shell.setHiveConfValue("hive.tez.exec.print.summary", "true");
    shell.start();
  }

  @AfterClass
  public static void afterClass() {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    shell.openSession();
    testTables = testTableType.instance(shell.metastore().hiveConf(), temp);
    for (Map.Entry<String, String> property : testTables.properties().entrySet()) {
      shell.setHiveSessionValue(property.getKey(), property.getValue());
    }
    shell.setHiveSessionValue("hive.execution.engine", executionEngine);
    shell.setHiveSessionValue("hive.jar.directory", temp.getRoot().getAbsolutePath());
    shell.setHiveSessionValue("tez.staging-dir", temp.getRoot().getAbsolutePath());
    // temporarily disabling vectorization in Tez, since it doesn't work with projection pruning (fix: TEZ-4248)
    // TODO: remove this once TEZ-4248 has been released and the Tez dependencies updated here
    if (executionEngine.equals("tez")) {
      shell.setHiveSessionValue("hive.vectorized.execution.enabled", "false");
    }
  }

  @After
  public void after() throws Exception {
    shell.closeSession();
    shell.metastore().reset();
    // HiveServer2 thread pools are using thread local Hive -> HMSClient objects. These are not cleaned up when the
    // HiveServer2 is stopped. Only Finalizer closes the HMS connections.
    System.gc();
  }

  @Test
  public void testScanEmptyTable() throws IOException {
    Schema emptySchema = new Schema(required(1, "empty", Types.StringType.get()));
    testTables.createTable(shell, "empty", emptySchema, fileFormat, ImmutableList.of());

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.empty");
    Assert.assertEquals(0, rows.size());
  }

  @Test
  public void testScanTable() throws IOException {
    testTables.createTable(shell, "customers", CUSTOMER_SCHEMA, fileFormat, CUSTOMER_RECORDS);

    // Single fetch task: no MR job.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, rows.get(2));

    // Adding the ORDER BY clause will cause Hive to spawn a local MR job this time.
    List<Object[]> descRows =
        shell.executeStatement("SELECT first_name, customer_id FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {"Trudy", 2L}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", 1L}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {"Alice", 0L}, descRows.get(2));
  }

  @Test
  public void testJoinTables() throws IOException {
    testTables.createTable(shell, "customers", CUSTOMER_SCHEMA, fileFormat, CUSTOMER_RECORDS);
    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

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
  public void testDecimalTableWithPredicateLiterals() throws IOException {
    Schema schema = new Schema(required(1, "decimal_field", Types.DecimalType.of(7, 2)));
    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
            .add(new BigDecimal("85.00"))
            .add(new BigDecimal("100.56"))
            .add(new BigDecimal("100.57"))
            .build();
    testTables.createTable(shell, "dec_test", schema, fileFormat, records);

    // Use integer literal in predicate
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field >= 85");
    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {"85.00"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {"100.56"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {"100.57"}, rows.get(2));

    // Use decimal literal in predicate with smaller scale than schema type definition
    rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field > 99.1");
    Assert.assertEquals(2, rows.size());
    Assert.assertArrayEquals(new Object[] {"100.56"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {"100.57"}, rows.get(1));

    // Use decimal literal in predicate with higher scale than schema type definition
    rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field > 100.565");
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {"100.57"}, rows.get(0));

    // Use decimal literal in predicate with the same scale as schema type definition
    rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field > 640.34");
    Assert.assertEquals(0, rows.size());
  }

  @Test
  public void testJoinTablesSupportedTypes() throws IOException {
    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);
      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> records = TestHelper.generateRandomRecords(schema, 1, 0L);

      testTables.createTable(shell, tableName, schema, fileFormat, records);
      List<Object[]> queryResult = shell.executeStatement("select s." + columnName + ", h." + columnName +
              " from default." + tableName + " s join default." + tableName + " h on h." + columnName + "=s." +
              columnName);
      Assert.assertEquals("Non matching record count for table " + tableName + " with type " + type,
              1, queryResult.size());
    }
  }

  @Test
  public void testSelectDistinctFromTable() throws IOException {
    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);
      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> records = TestHelper.generateRandomRecords(schema, 4, 0L);
      int size = records.stream().map(r -> r.getField(columnName)).collect(Collectors.toSet()).size();
      testTables.createTable(shell, tableName, schema, fileFormat, records);
      List<Object[]> queryResult = shell.executeStatement("select count(distinct(" + columnName +
              ")) from default." + tableName);
      int distincIds = ((Long) queryResult.get(0)[0]).intValue();
      Assert.assertEquals(tableName, size, distincIds);
    }
  }

  @Test
  public void testCreateDropTable() throws TException, IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" + SchemaParser.toJson(CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.PARTITION_SPEC + "'='" + PartitionSpecParser.toJson(IDENTITY_SPEC) + "', " +
        "'dummy'='test')");

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(CUSTOMER_SCHEMA.asStruct(), icebergTable.schema().asStruct());
    Assert.assertEquals(IDENTITY_SPEC, icebergTable.spec());

    // Check the HMS table parameters
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        shell.metastore().clientPool().run(client -> client.getTable("default", "customers"));

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
            testTables.loadTable(identifier);
          }
      );
    } else {
      Map<String, String> expectedIcebergProperties = new HashMap<>(2);
      expectedIcebergProperties.put("dummy", "test");
      expectedIcebergProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
      Assert.assertEquals(expectedIcebergProperties, icebergTable.properties());

      // Check the HMS table parameters
      hmsTable = shell.metastore().clientPool().run(client -> client.getTable("default", "customers"));
      Path hmsTableLocation = new Path(hmsTable.getSd().getLocation());

      // Drop the table
      shell.executeStatement("DROP TABLE customers");

      // Check if we drop an exception when trying to load the table
      AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
          "Table does not exist", () -> {
            testTables.loadTable(identifier);
          }
      );

      // Check if the files are removed
      FileSystem fs = Util.getFs(hmsTableLocation, shell.getHiveConf());
      if (fs.exists(hmsTableLocation)) {
        // if table directory has been deleted, we're good. This is the expected behavior in Hive4.
        // if table directory exists, its contents should have been cleaned up, save for an empty metadata dir (Hive3).
        Assert.assertEquals(1, fs.listStatus(hmsTableLocation).length);
        Assert.assertEquals(0, fs.listStatus(new Path(hmsTableLocation, "metadata")).length);
      }
    }
  }

  @Test
  public void testCreateTableWithoutSpec() throws TException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" + SchemaParser.toJson(CUSTOMER_SCHEMA) + "')");

    // Check the Iceberg table partition data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(SPEC, icebergTable.spec());

    // Check the HMS table parameters
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        shell.metastore().clientPool().run(client -> client.getTable("default", "customers"));

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
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    // We need the location for HadoopTable based tests only
    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" + SchemaParser.toJson(CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.PARTITION_SPEC + "'='" + PartitionSpecParser.toJson(SPEC) + "')");

    // Check the Iceberg table partition data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(SPEC, icebergTable.spec());

    // Check the HMS table parameters
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        shell.metastore().clientPool().run(client -> client.getTable("default", "customers"));

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
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" + SchemaParser.toJson(CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.EXTERNAL_TABLE_PURGE + "'='FALSE')");

    if (!Catalogs.hiveCatalog(shell.getHiveConf())) {
      shell.executeStatement("DROP TABLE customers");

      // Check if the table remains
      testTables.loadTable(identifier);
    } else {
      // Check the HMS table parameters
      org.apache.hadoop.hive.metastore.api.Table hmsTable =
          shell.metastore().clientPool().run(client -> client.getTable("default", "customers"));
      Path hmsTableLocation = new Path(hmsTable.getSd().getLocation());

      // Drop the table
      shell.executeStatement("DROP TABLE customers");

      // Check if we drop an exception when trying to drop the table
      AssertHelpers.assertThrows("should throw exception", NoSuchTableException.class,
          "Table does not exist", () -> {
            testTables.loadTable(identifier);
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
    TableIdentifier identifier = TableIdentifier.of("default", "withShell2");

    // Wrong schema
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Unrecognized token 'WrongSchema'", () -> {
          shell.executeStatement("CREATE EXTERNAL TABLE withShell2 " +
              "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
              testTables.locationForCreateTableSQL(identifier) +
              "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='WrongSchema')");
        }
    );

    // Missing schema, we try to get the schema from the table and fail
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Please provide ", () -> {
          shell.executeStatement("CREATE EXTERNAL TABLE withShell2 " +
              "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
              testTables.locationForCreateTableSQL(identifier));
        }
    );

    if (!testTables.locationForCreateTableSQL(identifier).isEmpty()) {
      // Only test this if the location is required
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "Table location not set", () -> {
            shell.executeStatement("CREATE EXTERNAL TABLE withShell2 " +
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
    testTables.createIcebergTable(shell.getHiveConf(), "customers", COMPLEX_SCHEMA, FileFormat.PARQUET,
        Collections.emptyList());

    if (Catalogs.hiveCatalog(shell.getHiveConf())) {

      // In HiveCatalog we just expect an exception since the table is already exists
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "customers already exists", () -> {
            shell.executeStatement("CREATE EXTERNAL TABLE customers " +
                "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
                "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
                SchemaParser.toJson(CUSTOMER_SCHEMA) + "')");
          }
      );
    } else {
      shell.executeStatement("CREATE EXTERNAL TABLE customers " +
          "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
          testTables.locationForCreateTableSQL(TableIdentifier.of("default", "customers")));

      // Check the HMS table parameters
      org.apache.hadoop.hive.metastore.api.Table hmsTable =
          shell.metastore().clientPool().run(client -> client.getTable("default", "customers"));

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

  @Test
  public void testColumnSelection() throws IOException {
    testTables.createTable(shell, "customers", CUSTOMER_SCHEMA, fileFormat, CUSTOMER_RECORDS);

    List<Object[]> outOfOrderColumns = shell
            .executeStatement("SELECT first_name, customer_id, last_name FROM default.customers");

    Assert.assertEquals(3, outOfOrderColumns.size());
    Assert.assertArrayEquals(new Object[] {"Alice", 0L, "Brown"}, outOfOrderColumns.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", 1L, "Green"}, outOfOrderColumns.get(1));
    Assert.assertArrayEquals(new Object[] {"Trudy", 2L, "Pink"}, outOfOrderColumns.get(2));

    List<Object[]> allButFirstColumn = shell.executeStatement("SELECT first_name, last_name FROM default.customers");

    Assert.assertEquals(3, allButFirstColumn.size());
    Assert.assertArrayEquals(new Object[] {"Alice", "Brown"}, allButFirstColumn.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", "Green"}, allButFirstColumn.get(1));
    Assert.assertArrayEquals(new Object[] {"Trudy", "Pink"}, allButFirstColumn.get(2));

    List<Object[]> allButMiddleColumn = shell.executeStatement("SELECT customer_id, last_name FROM default.customers");

    Assert.assertEquals(3, allButMiddleColumn.size());
    Assert.assertArrayEquals(new Object[] {0L, "Brown"}, allButMiddleColumn.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Green"}, allButMiddleColumn.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Pink"}, allButMiddleColumn.get(2));

    List<Object[]> allButLastColumn = shell.executeStatement("SELECT customer_id, first_name FROM default.customers");

    Assert.assertEquals(3, allButLastColumn.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, allButLastColumn.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, allButLastColumn.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, allButLastColumn.get(2));
  }

  @Test
  public void selectSameColumnTwice() throws IOException {
    testTables.createTable(shell, "customers", CUSTOMER_SCHEMA, fileFormat, CUSTOMER_RECORDS);

    List<Object[]> columns = shell.executeStatement("SELECT first_name, first_name FROM default.customers");

    Assert.assertEquals(3, columns.size());
    Assert.assertArrayEquals(new Object[] {"Alice", "Alice"}, columns.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", "Bob"}, columns.get(1));
    Assert.assertArrayEquals(new Object[] {"Trudy", "Trudy"}, columns.get(2));
  }

  @Test
  public void testCreateTableWithColumnSpecification() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers (customer_id BIGINT, first_name STRING, last_name STRING)" +
        " STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier));

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(CUSTOMER_SCHEMA.asStruct(), icebergTable.schema().asStruct());
    Assert.assertEquals(SPEC, icebergTable.spec());

    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, null, CUSTOMER_RECORDS);

    List<Object[]> descRows = shell.executeStatement("SELECT * FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, descRows.get(2));
  }

  @Test
  public void testCreateTableWithColumnSpecificationPartitioned() {
    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "currently not supported", () -> {
          shell.executeStatement("CREATE EXTERNAL TABLE customers (customer_id BIGINT) " +
              "PARTITIONED BY (first_name STRING) " +
              "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
              testTables.locationForCreateTableSQL(TableIdentifier.of("default", "customers")));
        }
    );
  }

  @Test
  public void testCreatePartitionedTable() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    PartitionSpec spec = PartitionSpec.builderFor(CUSTOMER_SCHEMA).identity("first_name").build();

    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.PARTITION_SPEC + "'='" + PartitionSpecParser.toJson(spec) + "', " +
        "'" + InputFormatConfig.TABLE_SCHEMA + "'='" + SchemaParser.toJson(CUSTOMER_SCHEMA) + "')");

    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(CUSTOMER_SCHEMA.asStruct(), icebergTable.schema().asStruct());
    Assert.assertEquals(spec, icebergTable.spec());

    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, Row.of("Alice"),
        Arrays.asList(CUSTOMER_RECORDS.get(0)));
    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, Row.of("Bob"),
        Arrays.asList(CUSTOMER_RECORDS.get(1)));
    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, Row.of("Trudy"),
        Arrays.asList(CUSTOMER_RECORDS.get(2)));

    List<Object[]> descRows = shell.executeStatement("SELECT * FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, descRows.get(2));
  }

  @Test
  public void testCreateTableWithColumnSpecificationHierarchy() {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers (" +
        "id BIGINT, name STRING, " +
        "employee_info STRUCT < employer: STRING, id: BIGINT, address: STRING >, " +
        "places_lived ARRAY < STRUCT <street: STRING, city: STRING, country: STRING >>, " +
        "memorable_moments MAP < STRING, STRUCT < year: INT, place: STRING, details: STRING >>, " +
        "current_address STRUCT < street_address: STRUCT " +
        "<street_number: INT, street_name: STRING, street_type: STRING>, country: STRING, postal_code: STRING >) " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier));

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(COMPLEX_SCHEMA.asStruct(), icebergTable.schema().asStruct());
  }

  @Test
  public void testCreateTableWithAllSupportedTypes() {
    TableIdentifier identifier = TableIdentifier.of("default", "all_types");
    Schema allSupportedSchema = new Schema(
        optional(1, "t_float", Types.FloatType.get()),
        optional(2, "t_double", Types.DoubleType.get()),
        optional(3, "t_boolean", Types.BooleanType.get()),
        optional(4, "t_int", Types.IntegerType.get()),
        optional(5, "t_bigint", Types.LongType.get()),
        optional(6, "t_binary", Types.BinaryType.get()),
        optional(7, "t_string", Types.StringType.get()),
        optional(8, "t_timestamp", Types.TimestampType.withoutZone()),
        optional(9, "t_date", Types.DateType.get()),
        optional(10, "t_decimal", Types.DecimalType.of(3, 2))
    );

    // Intentionally adding some mixed letters to test that we handle them correctly
    shell.executeStatement("CREATE EXTERNAL TABLE all_types (" +
        "t_Float FLOaT, t_dOuble DOUBLE, t_boolean BOOLEAN, t_int INT, t_bigint BIGINT, t_binary BINARY, " +
        "t_string STRING, t_timestamp TIMESTAMP, t_date DATE, t_decimal DECIMAL(3,2)) " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier));

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(allSupportedSchema.asStruct(), icebergTable.schema().asStruct());
  }

  @Test
  public void testCreateTableWithNotSupportedTypes() {
    TableIdentifier identifier = TableIdentifier.of("default", "not_supported_types");
    // Can not create INTERVAL types from normal create table, so leave them out from this test
    String[] notSupportedTypes = new String[] { "TINYINT", "SMALLINT", "VARCHAR(1)", "CHAR(1)" };

    for (String notSupportedType : notSupportedTypes) {
      AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
          "Unsupported Hive type", () -> {
            shell.executeStatement("CREATE EXTERNAL TABLE not_supported_types " +
                "(not_supported " + notSupportedType + ") " +
                "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
                testTables.locationForCreateTableSQL(identifier));
          }
      );
    }
  }

  @Test
  public void testArrayOfPrimitivesInTable() throws IOException {
    Schema schema =
            new Schema(required(1, "arrayofprimitives", Types.ListType.ofRequired(2, Types.IntegerType.get())));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "arraytable", schema, fileFormat, 1);
    // access a single element from the array
    for (int i = 0; i < records.size(); i++) {
      List<?> expectedList = (List<?>) records.get(i).getField("arrayofprimitives");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(
                String.format("SELECT arrayofprimitives[%d] FROM default.arraytable " + "LIMIT 1 OFFSET %d", j, i));
        Assert.assertEquals(expectedList.get(j), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testArrayOfArraysInTable() throws IOException {
    Schema schema =
            new Schema(
                    required(1, "arrayofarrays",
                            Types.ListType.ofRequired(2, Types.ListType.ofRequired(3, Types.DateType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "arraytable", schema, fileFormat, 1);
    // access an element from a matrix
    for (int i = 0; i < records.size(); i++) {
      List<?> expectedList = (List<?>) records.get(i).getField("arrayofarrays");
      for (int j = 0; j < expectedList.size(); j++) {
        List<?> expectedInnerList = (List<?>) expectedList.get(j);
        for (int k = 0; k < expectedInnerList.size(); k++) {
          List<Object[]> queryResult = shell.executeStatement(
                  String.format("SELECT arrayofarrays[%d][%d] FROM default.arraytable " + "LIMIT 1 OFFSET %d",
                          j, k, i));
          Assert.assertEquals(expectedInnerList.get(k).toString(), queryResult.get(0)[0]);
        }
      }
    }
  }

  @Test
  public void testArrayOfMapsInTable() throws IOException {
    Schema schema =
            new Schema(required(1, "arrayofmaps", Types.ListType
                    .ofRequired(2, Types.MapType.ofRequired(3, 4, Types.StringType.get(),
                            Types.BooleanType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "arraytable", schema, fileFormat, 1);
    // access an element from a map in an array
    for (int i = 0; i < records.size(); i++) {
      List<?> expectedList = (List<?>) records.get(i).getField("arrayofmaps");
      for (int j = 0; j < expectedList.size(); j++) {
        Map<?, ?> expectedMap = (Map<?, ?>) expectedList.get(j);
        for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
          List<Object[]> queryResult = shell.executeStatement(String
                  .format("SELECT arrayofmaps[%d][\"%s\"] FROM default.arraytable LIMIT 1 OFFSET %d", j,
                          entry.getKey(), i));
          Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
        }
      }
    }
  }

  @Test
  public void testArrayOfStructsInTable() throws IOException {
    Schema schema =
            new Schema(
                    required(1, "arrayofstructs", Types.ListType.ofRequired(2, Types.StructType
                            .of(required(3, "something", Types.DoubleType.get()), required(4, "someone",
                                    Types.LongType.get()), required(5, "somewhere", Types.StringType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "arraytable", schema, fileFormat, 1);
    // access an element from a struct in an array
    for (int i = 0; i < records.size(); i++) {
      List<?> expectedList = (List<?>) records.get(i).getField("arrayofstructs");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(String.format("SELECT arrayofstructs[%d].something, " +
                "arrayofstructs[%d].someone, arrayofstructs[%d].somewhere FROM default.arraytable LIMIT 1 " +
                "OFFSET %d", j, j, j, i));
        GenericRecord genericRecord = (GenericRecord) expectedList.get(j);
        Assert.assertEquals(genericRecord.getField("something"), queryResult.get(0)[0]);
        Assert.assertEquals(genericRecord.getField("someone"), queryResult.get(0)[1]);
        Assert.assertEquals(genericRecord.getField("somewhere"), queryResult.get(0)[2]);
      }
    }
  }

  @Test
  public void testMapOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "mapofprimitives", Types.MapType.ofRequired(2, 3, Types.StringType.get(),
                    Types.IntegerType.get())));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "maptable", schema, fileFormat, 1);
    // access a single value from the map
    for (int i = 0; i < records.size(); i++) {
      Map<?, ?> expectedMap = (Map<?, ?>) records.get(i).getField("mapofprimitives");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String
                .format("SELECT mapofprimitives[\"%s\"] " + "FROM default.maptable LIMIT 1 OFFSET %d", entry.getKey(),
                        i));
        Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testMapOfArraysInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "mapofarrays",
                    Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.ListType.ofRequired(4,
                            Types.DateType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "maptable", schema, fileFormat, 1);
    // access a single element from a list in a map
    for (int i = 0; i < records.size(); i++) {
      Map<?, ?> expectedMap = (Map<?, ?>) records.get(i).getField("mapofarrays");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<?> expectedList = (List<?>) entry.getValue();
        for (int j = 0; j < expectedList.size(); j++) {
          List<Object[]> queryResult = shell.executeStatement(String
                  .format("SELECT mapofarrays[\"%s\"]" + "[%d] FROM maptable LIMIT 1 OFFSET %d", entry.getKey(), j, i));
          Assert.assertEquals(expectedList.get(j).toString(), queryResult.get(0)[0]);
        }
      }
    }
  }

  @Test
  public void testMapOfMapsInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "mapofmaps", Types.MapType.ofRequired(2, 3, Types.StringType.get(),
                    Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.StringType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "maptable", schema, fileFormat, 1);
    // access a single element from a map in a map
    for (int i = 0; i < records.size(); i++) {
      Map<?, ?> expectedMap = (Map<?, ?>) records.get(i).getField("mapofmaps");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        Map<?, ?> expectedInnerMap = (Map<?, ?>) entry.getValue();
        for (Map.Entry<?, ?> innerEntry : expectedInnerMap.entrySet()) {
          List<Object[]> queryResult = shell.executeStatement(String
                  .format("SELECT mapofmaps[\"%s\"]" + "[\"%s\"] FROM maptable LIMIT 1 OFFSET %d", entry.getKey(),
                          innerEntry.getKey(), i));
          Assert.assertEquals(innerEntry.getValue(), queryResult.get(0)[0]);
        }
      }
    }
  }

  @Test
  public void testMapOfStructsInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "mapofstructs", Types.MapType.ofRequired(2, 3, Types.StringType.get(),
                    Types.StructType.of(required(4, "something", Types.DoubleType.get()),
                            required(5, "someone", Types.LongType.get()),
                            required(6, "somewhere", Types.StringType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "maptable", schema, fileFormat, 1);
    // access a single element from a struct in a map
    for (int i = 0; i < records.size(); i++) {
      Map<?, ?> expectedMap = (Map<?, ?>) records.get(i).getField("mapofstructs");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String.format("SELECT mapofstructs[\"%s\"].something, " +
                "mapofstructs[\"%s\"].someone, mapofstructs[\"%s\"].somewhere FROM default.maptable LIMIT 1 " +
                "OFFSET %d", entry.getKey(), entry.getKey(), entry.getKey(), i));
        GenericRecord genericRecord = (GenericRecord) entry.getValue();
        Assert.assertEquals(genericRecord.getField("something"), queryResult.get(0)[0]);
        Assert.assertEquals(genericRecord.getField("someone"), queryResult.get(0)[1]);
        Assert.assertEquals(genericRecord.getField("somewhere"), queryResult.get(0)[2]);
      }
    }
  }

  @Test
  public void testStructOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(required(1, "structofprimitives",
            Types.StructType.of(required(2, "key", Types.StringType.get()), required(3, "value",
                    Types.IntegerType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access a single value in a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofprimitives");
      List<Object[]> queryResult = shell.executeStatement(String.format(
              "SELECT structofprimitives.key, structofprimitives.value FROM default.structtable LIMIT 1 OFFSET %d", i));
      Assert.assertEquals(expectedStruct.getField("key"), queryResult.get(0)[0]);
      Assert.assertEquals(expectedStruct.getField("value"), queryResult.get(0)[1]);
    }
  }

  @Test
  public void testStructOfArraysInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "structofarrays", Types.StructType
                    .of(required(2, "names", Types.ListType.ofRequired(3, Types.StringType.get())),
                            required(4, "birthdays", Types.ListType.ofRequired(5,
                                    Types.DateType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access an element of an array inside a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofarrays");
      List<?> expectedList = (List<?>) expectedStruct.getField("names");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(
                String.format("SELECT structofarrays.names[%d] FROM default.structtable LIMIT 1 OFFSET %d", j, i));
        Assert.assertEquals(expectedList.get(j), queryResult.get(0)[0]);
      }
      expectedList = (List<?>) expectedStruct.getField("birthdays");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(
                String.format("SELECT structofarrays.birthdays[%d] FROM default.structtable LIMIT 1 OFFSET %d", j, i));
        Assert.assertEquals(expectedList.get(j).toString(), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testStructOfMapsInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "structofmaps", Types.StructType
                    .of(required(2, "map1", Types.MapType.ofRequired(3, 4,
                            Types.StringType.get(), Types.StringType.get())), required(5, "map2",
                            Types.MapType.ofRequired(6, 7, Types.StringType.get(),
                                    Types.IntegerType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access a map entry inside a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofmaps");
      Map<?, ?> expectedMap = (Map<?, ?>) expectedStruct.getField("map1");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String
                .format("SELECT structofmaps.map1[\"%s\"] from default.structtable LIMIT 1 OFFSET %d", entry.getKey(),
                        i));
        Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
      }
      expectedMap = (Map<?, ?>) expectedStruct.getField("map2");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String
                .format("SELECT structofmaps.map2[\"%s\"] from default.structtable LIMIT 1 OFFSET %d", entry.getKey(),
                        i));
        Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testStructOfStructsInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "structofstructs", Types.StructType.of(required(2, "struct1", Types.StructType
                    .of(required(3, "key", Types.StringType.get()), required(4, "value",
                            Types.IntegerType.get()))))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access a struct element inside a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofstructs");
      GenericRecord expectedInnerStruct = (GenericRecord) expectedStruct.getField("struct1");
      List<Object[]> queryResult = shell.executeStatement(String.format(
              "SELECT structofstructs.struct1.key, structofstructs.struct1.value FROM default.structtable " +
                      "LIMIT 1 OFFSET %d", i));
      Assert.assertEquals(expectedInnerStruct.getField("key"), queryResult.get(0)[0]);
      Assert.assertEquals(expectedInnerStruct.getField("value"), queryResult.get(0)[1]);
    }
  }
}
