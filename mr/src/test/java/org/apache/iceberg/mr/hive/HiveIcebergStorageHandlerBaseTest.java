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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
          required(2, "first_name", Types.StringType.get()),
          required(3, "last_name", Types.StringType.get())
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

  // before variables
  protected TestHiveMetastore metastore;
  private TestTables testTables;

  public abstract TestTables testTables(Configuration conf, TemporaryFolder tmp) throws IOException;

  @Before
  public void before() throws IOException {
    metastore = new TestHiveMetastore();
    metastore.start();

    testTables = testTables(metastore.hiveConf(), temp);

    for (Map.Entry<String, String> property : testTables.properties().entrySet()) {
      shell.setHiveConfValue(property.getKey(), property.getValue());
    }

    String metastoreUris = metastore.hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS);
    shell.setHiveConfValue(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUris);

    String metastoreWarehouse = metastore.hiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
    shell.setHiveConfValue(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, metastoreWarehouse);

    shell.start();
  }

  @After
  public void after() {
    metastore.stop();
    metastore = null;
  }

  @Test
  public void testScanEmptyTable() throws IOException {
    Schema emptySchema = new Schema(required(1, "empty", Types.StringType.get()));
    createTable("empty", emptySchema, ImmutableList.of());

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.empty");
    Assert.assertEquals(0, rows.size());
  }

  @Test
  public void testScanTable() throws IOException {
    createTable("customers", CUSTOMER_SCHEMA, CUSTOMER_RECORDS);

    // Single fetch task: no MR job.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, rows.get(2));

    // Adding the ORDER BY clause will cause Hive to spawn a local MR job this time.
    List<Object[]> descRows = shell.executeStatement("SELECT * FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, descRows.size());
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, descRows.get(2));
  }

  @Test
  public void testJoinTables() throws IOException {
    createTable("customers", CUSTOMER_SCHEMA, CUSTOMER_RECORDS);
    createTable("orders", ORDER_SCHEMA, ORDER_RECORDS);

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
  public void testColumnSelection() throws IOException {
    createTable("customers", CUSTOMER_SCHEMA, CUSTOMER_RECORDS);

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
    createTable("customers", CUSTOMER_SCHEMA, CUSTOMER_RECORDS);

    List<Object[]> columns = shell.executeStatement("SELECT first_name, first_name FROM default.customers");

    Assert.assertEquals(3, columns.size());
    Assert.assertArrayEquals(new Object[] {"Alice", "Alice"}, columns.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", "Bob"}, columns.get(1));
    Assert.assertArrayEquals(new Object[] {"Trudy", "Trudy"}, columns.get(2));
  }

  protected void createTable(String tableName, Schema schema, List<Record> records)
          throws IOException {
    Table table = createIcebergTable(tableName, schema, records);
    createHiveTable(tableName, table.location());
  }

  protected Table createIcebergTable(String tableName, Schema schema, List<Record> records)
          throws IOException {
    String identifier = testTables.identifier("default." + tableName);
    TestHelper helper = new TestHelper(
            metastore.hiveConf(), testTables.tables(), identifier, schema, SPEC, FileFormat.PARQUET, temp);
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
}
