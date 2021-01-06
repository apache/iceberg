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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
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
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHiveIcebergStorageHandlerNoScan {
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

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

  @Parameters(name = "catalog={0}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = new ArrayList<>();
    for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
      testParams.add(new Object[] {testTableType});
    }

    return testParams;
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(0)
  public TestTables.TestTableType testTableType;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    // Uses spark as an engine so we can detect if we unintentionally try to use any execution engines
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "spark");
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @Test
  public void testCreateDropTable() throws TException, IOException, InterruptedException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.PARTITION_SPEC + "'='" +
        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()) + "', " +
        "'dummy'='test')");

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.asStruct(),
        icebergTable.schema().asStruct());
    Assert.assertEquals(PartitionSpec.unpartitioned(), icebergTable.spec());

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

    // verify that storage descriptor is filled out with inputformat/outputformat/serde
    Assert.assertEquals(HiveIcebergInputFormat.class.getName(), hmsTable.getSd().getInputFormat());
    Assert.assertEquals(HiveIcebergOutputFormat.class.getName(), hmsTable.getSd().getOutputFormat());
    Assert.assertEquals(HiveIcebergSerDe.class.getName(), hmsTable.getSd().getSerdeInfo().getSerializationLib());

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
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "')");

    // Check the Iceberg table partition data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(PartitionSpec.unpartitioned(), icebergTable.spec());

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
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "', " +
        "'" + InputFormatConfig.PARTITION_SPEC + "'='" +
        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()) + "')");

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
        "TBLPROPERTIES ('" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "', " +
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
                SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "')");
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
                SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "')");
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
  public void testCreatePartitionedTableWithPropertiesAndWithColumnSpecification() {
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).identity("last_name").build();

    AssertHelpers.assertThrows("should throw exception", IllegalArgumentException.class,
        "Provide only one of the following", () -> {
          shell.executeStatement("CREATE EXTERNAL TABLE customers (customer_id BIGINT) " +
              "PARTITIONED BY (first_name STRING) " +
              "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
              testTables.locationForCreateTableSQL(TableIdentifier.of("default", "customers")) +
              " TBLPROPERTIES ('" + InputFormatConfig.PARTITION_SPEC + "'='" +
              PartitionSpecParser.toJson(spec) + "')");
        }
    );
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
}
