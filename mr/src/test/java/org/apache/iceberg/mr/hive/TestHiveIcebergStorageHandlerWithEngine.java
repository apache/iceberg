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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
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
public class TestHiveIcebergStorageHandlerWithEngine {

  private static final String[] EXECUTION_ENGINES = new String[] {"tez", "mr"};

  private static final Schema ORDER_SCHEMA = new Schema(
          required(1, "order_id", Types.LongType.get()),
          required(2, "customer_id", Types.LongType.get()),
          required(3, "total", Types.DoubleType.get()),
          required(4, "product_id", Types.LongType.get())
  );

  private static final List<Record> ORDER_RECORDS = TestHelper.RecordsBuilder.newInstance(ORDER_SCHEMA)
          .add(100L, 0L, 11.11d, 1L)
          .add(101L, 0L, 22.22d, 2L)
          .add(102L, 1L, 33.33d, 3L)
          .build();

  private static final Schema PRODUCT_SCHEMA = new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "price", Types.DoubleType.get())
  );

  private static final List<Record> PRODUCT_RECORDS = TestHelper.RecordsBuilder.newInstance(PRODUCT_SCHEMA)
          .add(1L, "skirt", 11.11d)
          .add(2L, "tee", 22.22d)
          .add(3L, "watch", 33.33d)
          .build();

  private static final List<Type> SUPPORTED_TYPES =
          ImmutableList.of(Types.BooleanType.get(), Types.IntegerType.get(), Types.LongType.get(),
                  Types.FloatType.get(), Types.DoubleType.get(), Types.DateType.get(), Types.TimestampType.withZone(),
                  Types.TimestampType.withoutZone(), Types.StringType.get(), Types.BinaryType.get(),
                  Types.DecimalType.of(3, 1), Types.UUIDType.get(), Types.FixedType.ofLength(5),
                  Types.TimeType.get());

  @Parameters(name = "fileFormat={0}, engine={1}, catalog={2}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = new ArrayList<>();
    String javaVersion = System.getProperty("java.specification.version");

    // Run tests with every FileFormat for a single Catalog (HiveCatalog)
    for (FileFormat fileFormat : HiveIcebergStorageHandlerTestUtils.FILE_FORMATS) {
      for (String engine : EXECUTION_ENGINES) {
        // include Tez tests only for Java 8
        if (javaVersion.equals("1.8") || "mr".equals(engine)) {
          testParams.add(new Object[] {fileFormat, engine, TestTables.TestTableType.HIVE_CATALOG});
        }
      }
    }

    // Run tests for every Catalog for a single FileFormat (PARQUET) and execution engine (mr)
    // skip HiveCatalog tests as they are added before
    for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
      if (!TestTables.TestTableType.HIVE_CATALOG.equals(testTableType)) {
        testParams.add(new Object[]{FileFormat.PARQUET, "mr", testTableType});
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
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, executionEngine);
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
  public void testScanTable() throws IOException {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Adding the ORDER BY clause will cause Hive to spawn a local MR job this time.
    List<Object[]> descRows =
        shell.executeStatement("SELECT first_name, customer_id FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {"Trudy", 2L}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", 1L}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {"Alice", 0L}, descRows.get(2));
  }

  @Test
  public void testCBOWithSelectedColumnsNonOverlapJoin() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);

    testTables.createTable(shell, "products", PRODUCT_SCHEMA, fileFormat, PRODUCT_RECORDS);
    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    List<Object[]> rows = shell.executeStatement(
            "SELECT o.order_id, o.customer_id, o.total, p.name " +
                    "FROM default.orders o JOIN default.products p ON o.product_id = p.id ORDER BY o.order_id"
    );

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {100L, 0L, 11.11d, "skirt"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {101L, 0L, 22.22d, "tee"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {102L, 1L, 33.33d, "watch"}, rows.get(2));
  }

  @Test
  public void testCBOWithSelectedColumnsOverlapJoin() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);

    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    List<Object[]> rows = shell.executeStatement(
            "SELECT c.first_name, o.order_id " +
                    "FROM default.orders o JOIN default.customers c ON o.customer_id = c.customer_id " +
                    "ORDER BY o.order_id DESC"
    );

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {"Bob", 102L}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {"Alice", 101L}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {"Alice", 100L}, rows.get(2));
  }

  @Test
  public void testCBOWithSelfJoin() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);

    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    List<Object[]> rows = shell.executeStatement(
            "SELECT o1.order_id, o1.customer_id, o1.total " +
                    "FROM default.orders o1 JOIN default.orders o2 ON o1.order_id = o2.order_id ORDER BY o1.order_id"
    );

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {100L, 0L, 11.11d}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {101L, 0L, 22.22d}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {102L, 1L, 33.33d}, rows.get(2));
  }

  @Test
  public void testJoinTablesSupportedTypes() throws IOException {
    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);
      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() && fileFormat == FileFormat.PARQUET) {
        continue;
      }
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
      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() && fileFormat == FileFormat.PARQUET) {
        continue;
      }
      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> records = TestHelper.generateRandomRecords(schema, 4, 0L);
      int size = records.stream().map(r -> r.getField(columnName)).collect(Collectors.toSet()).size();
      testTables.createTable(shell, tableName, schema, fileFormat, records);
      List<Object[]> queryResult = shell.executeStatement("select count(distinct(" + columnName +
              ")) from default." + tableName);
      int distinctIds = ((Long) queryResult.get(0)[0]).intValue();
      Assert.assertEquals(tableName, size, distinctIds);
    }
  }
}
