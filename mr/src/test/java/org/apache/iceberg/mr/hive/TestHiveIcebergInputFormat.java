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
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.mapred.IcebergSerDe;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(StandaloneHiveRunner.class)
public class TestHiveIcebergInputFormat {

  @HiveSQL(files = {}, autoStart = true)
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

  // before variables
  private HadoopTables tables;
  private Table customerTable;
  private Table orderTable;

  @Before
  public void before() throws IOException {
    Configuration conf = new Configuration();
    tables = new HadoopTables(conf);

    File customerLocation = temp.newFolder("customers");
    Assert.assertTrue(customerLocation.delete());

    TestHelper customerHelper = new TestHelper(
            conf, tables, CUSTOMER_SCHEMA, PartitionSpec.unpartitioned(), FileFormat.PARQUET, temp, customerLocation);

    customerTable = customerHelper.createUnpartitionedTable();
    customerHelper.appendToTable(customerHelper.writeFile(null, CUSTOMER_RECORDS));

    File orderLocation = temp.newFolder("orders");
    Assert.assertTrue(orderLocation.delete());

    TestHelper orderHelper = new TestHelper(
            conf, tables, ORDER_SCHEMA, PartitionSpec.unpartitioned(), FileFormat.PARQUET, temp, orderLocation);

    orderTable = orderHelper.createUnpartitionedTable();
    orderHelper.appendToTable(orderHelper.writeFile(null, ORDER_RECORDS));
  }

  @Test
  public void testScanEmptyTable() throws IOException {
    File emptyLocation = temp.newFolder("empty");
    Assert.assertTrue(emptyLocation.delete());

    Schema emptySchema = new Schema(required(1, "empty", Types.StringType.get()));
    Table emptyTable = tables.create(
            emptySchema, PartitionSpec.unpartitioned(), Collections.emptyMap(), emptyLocation.toString());
    createHiveTable("empty", emptyTable.location());

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.empty");
    Assert.assertEquals(0, rows.size());
  }

  @Test
  public void testScanTable() {
    createHiveTable("customers", customerTable.location());

    // Single fetch task: no MR job.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, rows.get(2));

    // Adding the ORDER BY clause will cause Hive to spawn a local MR job this time.
    List<Object[]> descRows = shell.executeStatement("SELECT * FROM default.customers ORDER BY customer_id DESC");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, descRows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, descRows.get(1));
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, descRows.get(2));
  }

  @Test
  public void testJoinTables() {
    createHiveTable("customers", customerTable.location());
    createHiveTable("orders", orderTable.location());

    List<Object[]> rows = shell.executeStatement(
            "SELECT c.customer_id, c.first_name, o.order_id, o.total " +
                    "FROM default.customers c JOIN default.orders o ON c.customer_id = o.customer_id " +
                    "ORDER BY c.customer_id, o.order_id"
    );

    Assert.assertArrayEquals(new Object[] {0L, "Alice", 100L, 11.11d}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {0L, "Alice", 101L, 22.22d}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", 102L, 33.33d}, rows.get(2));
  }

  private void createHiveTable(String table, String location) {
    shell.execute(String.format(
            "CREATE TABLE default.%s " +
            "ROW FORMAT SERDE '%s' " +
            "STORED AS " +
                "INPUTFORMAT '%s' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat' " +
            "LOCATION '%s'",
            table, IcebergSerDe.class.getName(), HiveIcebergInputFormat.class.getName(), location));
  }
}
