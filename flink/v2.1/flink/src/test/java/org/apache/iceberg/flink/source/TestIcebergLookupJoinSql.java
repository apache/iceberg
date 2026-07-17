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
package org.apache.iceberg.flink.source;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests SQL lookup join with Iceberg table source. */
public class TestIcebergLookupJoinSql extends TestSqlBase {
  private static final Schema DIM_SCHEMA =
      new Schema(
          required(1, "user_id", Types.LongType.get()),
          required(2, "name", Types.StringType.get()),
          required(3, "city", Types.StringType.get()));

  @BeforeEach
  @Override
  public void before() throws IOException {
    setUpTableEnv(getTableEnv());
    setUpTableEnv(getStreamingTableEnv());
  }

  @AfterEach
  public void after() throws IOException {
    CATALOG_EXTENSION.catalog().dropTable(TestFixtures.TABLE_IDENTIFIER);
  }

  @Test
  public void testLookupJoinReturnsMatchedAndUnmatchedRows() throws Exception {
    createDimTable(
        dimRecord(1L, "alice", "beijing"),
        dimRecord(2L, "bob", "shanghai"),
        dimRecord(3L, "carol", "guangzhou"));

    TableEnvironment streamEnv = getStreamingTableEnv();
    createOrdersTable(streamEnv, "orders", 5);

    String joinSql =
        String.format(
            "SELECT o.order_id, o.user_id, u.name, u.city\n"
                + "FROM orders AS o\n"
                + "LEFT JOIN iceberg_catalog.`%s`.`%s` FOR SYSTEM_TIME AS OF o.proc_time AS u\n"
                + "  ON o.user_id = u.user_id",
            TestFixtures.DATABASE, TestFixtures.TABLE);

    assertThat(SqlHelpers.sql(streamEnv, joinSql))
        .containsExactlyInAnyOrder(
            Row.of(1L, 1L, "alice", "beijing"),
            Row.of(2L, 2L, "bob", "shanghai"),
            Row.of(3L, 3L, "carol", "guangzhou"),
            Row.of(4L, 4L, null, null),
            Row.of(5L, 5L, null, null));
  }

  @Test
  public void testLookupJoinWithFilterConditionInJoin() throws Exception {
    createDimTable(
        dimRecord(1L, "alice", "beijing"),
        dimRecord(2L, "bob", "shanghai"),
        dimRecord(3L, "carol", "beijing"));

    TableEnvironment streamEnv = getStreamingTableEnv();
    createOrdersTable(streamEnv, "orders_with_filter", 4);

    String joinSql =
        String.format(
            "SELECT o.order_id, o.user_id, u.name, u.city\n"
                + "FROM orders_with_filter AS o\n"
                + "LEFT JOIN iceberg_catalog.`%s`.`%s` FOR SYSTEM_TIME AS OF o.proc_time AS u\n"
                + "  ON o.user_id = u.user_id AND u.city = 'beijing'",
            TestFixtures.DATABASE, TestFixtures.TABLE);

    assertThat(SqlHelpers.sql(streamEnv, joinSql))
        .containsExactlyInAnyOrder(
            Row.of(1L, 1L, "alice", "beijing"),
            Row.of(2L, 2L, null, null),
            Row.of(3L, 3L, "carol", "beijing"),
            Row.of(4L, 4L, null, null));
  }

  @Test
  public void testLookupJoinWithPartialCacheReturnsMatchedAndUnmatchedRows() throws Exception {
    createDimTable(
        dimRecord(1L, "alice", "beijing"),
        dimRecord(2L, "bob", "shanghai"),
        dimRecord(3L, "carol", "guangzhou"));

    TableEnvironment streamEnv = getStreamingTableEnv();
    createOrdersTable(streamEnv, "orders_with_partial_cache_no_filter", 5);

    String joinSql =
        String.format(
            "SELECT o.order_id, o.user_id, u.name, u.city\n"
                + "FROM orders_with_partial_cache_no_filter AS o\n"
                + "LEFT JOIN iceberg_catalog.`%s`.`%s` "
                + "/*+ OPTIONS('lookup.cache'='PARTIAL', 'lookup.partial-cache.max-rows'='10') */ "
                + "FOR SYSTEM_TIME AS OF o.proc_time AS u\n"
                + "  ON o.user_id = u.user_id",
            TestFixtures.DATABASE, TestFixtures.TABLE);

    assertThat(SqlHelpers.sql(streamEnv, joinSql))
        .containsExactlyInAnyOrder(
            Row.of(1L, 1L, "alice", "beijing"),
            Row.of(2L, 2L, "bob", "shanghai"),
            Row.of(3L, 3L, "carol", "guangzhou"),
            Row.of(4L, 4L, null, null),
            Row.of(5L, 5L, null, null));
  }

  @Test
  public void testLookupJoinWithPartialCacheAndFilterConditionInJoin() throws Exception {
    createDimTable(
        dimRecord(1L, "alice", "beijing"),
        dimRecord(2L, "bob", "shanghai"),
        dimRecord(3L, "carol", "beijing"));

    TableEnvironment streamEnv = getStreamingTableEnv();
    createOrdersTable(streamEnv, "orders_with_partial_cache", 4);

    String joinSql =
        String.format(
            "SELECT o.order_id, o.user_id, u.name, u.city\n"
                + "FROM orders_with_partial_cache AS o\n"
                + "LEFT JOIN iceberg_catalog.`%s`.`%s` "
                + "/*+ OPTIONS('lookup.cache'='PARTIAL', 'lookup.partial-cache.max-rows'='10') */ "
                + "FOR SYSTEM_TIME AS OF o.proc_time AS u\n"
                + "  ON o.user_id = u.user_id AND u.city = 'beijing'",
            TestFixtures.DATABASE, TestFixtures.TABLE);

    assertThat(SqlHelpers.sql(streamEnv, joinSql))
        .containsExactlyInAnyOrder(
            Row.of(1L, 1L, "alice", "beijing"),
            Row.of(2L, 2L, null, null),
            Row.of(3L, 3L, "carol", "beijing"),
            Row.of(4L, 4L, null, null));
  }

  private void createDimTable(Record... records) throws IOException {
    Table dimTable =
        CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, DIM_SCHEMA);
    GenericAppenderHelper helper =
        new GenericAppenderHelper(dimTable, FileFormat.PARQUET, temporaryFolder);
    DataFile dataFile = helper.writeFile(Lists.newArrayList(records));
    helper.appendToTable(dataFile);
  }

  private static void createOrdersTable(
      TableEnvironment tableEnvironment, String tableName, int numberOfRows) {
    SqlHelpers.sql(
        tableEnvironment,
        "CREATE TEMPORARY TABLE %s (\n"
            + "  order_id BIGINT,\n"
            + "  user_id  BIGINT,\n"
            + "  proc_time AS PROCTIME()\n"
            + ") WITH (\n"
            + "  'connector' = 'datagen',\n"
            + "  'number-of-rows' = '%d',\n"
            + "  'fields.order_id.kind' = 'sequence',\n"
            + "  'fields.order_id.start' = '1',\n"
            + "  'fields.order_id.end' = '%d',\n"
            + "  'fields.user_id.kind' = 'sequence',\n"
            + "  'fields.user_id.start' = '1',\n"
            + "  'fields.user_id.end' = '%d'\n"
            + ")",
        tableName,
        numberOfRows,
        numberOfRows,
        numberOfRows);
  }

  private static Record dimRecord(long userId, String name, String city) {
    Record record = GenericRecord.create(DIM_SCHEMA);
    record.setField("user_id", userId);
    record.setField("name", name);
    record.setField("city", city);
    return record;
  }

  private static void setUpTableEnv(TableEnvironment tableEnvironment) {
    Configuration tableConf = tableEnvironment.getConfig().getConfiguration();
    tableConf.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE, true);
    tableConf.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);

    tableEnvironment.getConfig().set("table.exec.resource.default-parallelism", "1");
    SqlHelpers.sql(
        tableEnvironment,
        "create catalog iceberg_catalog with ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_EXTENSION.warehouse());
    SqlHelpers.sql(tableEnvironment, "use catalog iceberg_catalog");

    tableConf.set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
  }
}
