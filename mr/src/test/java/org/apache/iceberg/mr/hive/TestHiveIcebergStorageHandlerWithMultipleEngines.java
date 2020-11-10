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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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
public class TestHiveIcebergStorageHandlerWithMultipleEngines {
  private static final FileFormat[] fileFormats =
      new FileFormat[] {FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET};

  private static final String[] executionEngines = new String[] {"mr", "tez"};

  private static final Schema CUSTOMER_SCHEMA = new Schema(
      optional(1, "customer_id", Types.LongType.get()),
      optional(2, "first_name", Types.StringType.get())
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

  private static final List<Type> SUPPORTED_TYPES =
      ImmutableList.of(Types.BooleanType.get(), Types.IntegerType.get(), Types.LongType.get(),
          Types.FloatType.get(), Types.DoubleType.get(), Types.DateType.get(), Types.TimestampType.withZone(),
          Types.TimestampType.withoutZone(), Types.StringType.get(), Types.BinaryType.get(),
          Types.DecimalType.of(3, 1));

  @Parameters(name = "fileFormat={0}, engine={1}, catalog={2}")
  public static Collection<Object[]> parameters() {
    String javaVersion = System.getProperty("java.specification.version");

    Collection<Object[]> testParams = new ArrayList<>();
    for (FileFormat fileFormat : fileFormats) {
      for (String engine : executionEngines) {
        // include Tez tests only for Java 8
        if (javaVersion.equals("1.8")) {
          for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
            testParams.add(new Object[] {fileFormat, engine, testTableType});
          }
        }
      }
    }

    return testParams;
  }

  @Parameter(0)
  public FileFormat fileFormat;

  @Parameter(1)
  public String executionEngine;

  @Parameter(2)
  public TestTables.TestTableType testTableType;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static TestHiveShell shell;

  private TestTables testTables;

  @BeforeClass
  public static void beforeClass() {
    shell = new TestHiveShell();
    shell.setHiveConfValue("hive.notification.event.poll.interval", "-1");
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
  }

  @After
  public void after() throws Exception {
    shell.closeSession();
    shell.metastore().reset();
  }

  @Test
  public void testScanTable() throws IOException {
    testTables.createTable(shell, "customers", CUSTOMER_SCHEMA, fileFormat, CUSTOMER_RECORDS);

    // Adding the ORDER BY clause will cause Hive to spawn a local MR job this time.
    List<Object[]> descRows = shell.executeStatement("SELECT * FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, descRows.get(2));
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
  public void testCreateTableWithColumnSpecification() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    shell.executeStatement("CREATE EXTERNAL TABLE customers (customer_id BIGINT, first_name STRING) " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier));

    // Check the Iceberg table data
    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(CUSTOMER_SCHEMA.asStruct(), icebergTable.schema().asStruct());
    Assert.assertEquals(PartitionSpec.unpartitioned(), icebergTable.spec());

    testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, null, CUSTOMER_RECORDS);

    List<Object[]> descRows = shell.executeStatement("SELECT * FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, descRows.get(2));
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
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, descRows.get(2));
  }
}
