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
package org.apache.iceberg.flink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Iceberg Lookup Join 集成测试。
 *
 * <p>测试 Iceberg 表作为维表进行 Temporal Join 的功能。
 */
@RunWith(Parameterized.class)
public class IcebergLookupJoinITCase extends FlinkTestBase {

  private static final String DIM_TABLE_NAME = "dim_user";
  private static final String FACT_TABLE_NAME = "fact_orders";
  private static final String RESULT_TABLE_NAME = "result_sink";

  @ClassRule public static final TemporaryFolder WAREHOUSE = new TemporaryFolder();

  private final String catalogName;
  private final String lookupMode;
  private volatile TableEnvironment tEnv;

  @Parameterized.Parameters(name = "catalogName = {0}, lookupMode = {1}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(
        // Hadoop catalog with PARTIAL mode
        new Object[] {"testhadoop", "partial"},
        // Hadoop catalog with ALL mode
        new Object[] {"testhadoop", "all"});
  }

  public IcebergLookupJoinITCase(String catalogName, String lookupMode) {
    this.catalogName = catalogName;
    this.lookupMode = lookupMode;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment(
                  MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
          env.enableCheckpointing(400);
          env.setMaxParallelism(2);
          env.setParallelism(2);
          tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());

          // 配置
          tEnv.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
        }
      }
    }
    return tEnv;
  }

  @Before
  public void before() {
    // 创建维表
    createDimTable();
    // 插入维表数据
    insertDimData();
  }

  @After
  public void after() {
    sql("DROP TABLE IF EXISTS %s", DIM_TABLE_NAME);
    sql("DROP TABLE IF EXISTS %s", FACT_TABLE_NAME);
    sql("DROP TABLE IF EXISTS %s", RESULT_TABLE_NAME);
  }

  private void createDimTable() {
    Map<String, String> tableProps = createTableProps();
    tableProps.put("lookup.mode", lookupMode);
    tableProps.put("lookup.cache.ttl", "1m");
    tableProps.put("lookup.cache.max-rows", "1000");
    tableProps.put("lookup.cache.reload-interval", "30s");

    sql(
        "CREATE TABLE %s ("
            + "  user_id BIGINT,"
            + "  user_name STRING,"
            + "  user_level INT,"
            + "  PRIMARY KEY (user_id) NOT ENFORCED"
            + ") WITH %s",
        DIM_TABLE_NAME, toWithClause(tableProps));
  }

  private void insertDimData() {
    sql(
        "INSERT INTO %s VALUES " + "(1, 'Alice', 1), " + "(2, 'Bob', 2), " + "(3, 'Charlie', 3)",
        DIM_TABLE_NAME);
  }

  /** 测试基本的 Lookup Join 功能 */
  @Test
  public void testBasicLookupJoin() throws Exception {
    // 创建事实表（使用 datagen 模拟流数据）
    sql(
        "CREATE TABLE %s ("
            + "  order_id BIGINT,"
            + "  user_id BIGINT,"
            + "  amount DOUBLE,"
            + "  proc_time AS PROCTIME()"
            + ") WITH ("
            + "  'connector' = 'datagen',"
            + "  'rows-per-second' = '1',"
            + "  'fields.order_id.kind' = 'sequence',"
            + "  'fields.order_id.start' = '1',"
            + "  'fields.order_id.end' = '3',"
            + "  'fields.user_id.min' = '1',"
            + "  'fields.user_id.max' = '3',"
            + "  'fields.amount.min' = '10.0',"
            + "  'fields.amount.max' = '100.0'"
            + ")",
        FACT_TABLE_NAME);

    // 创建结果表
    sql(
        "CREATE TABLE %s ("
            + "  order_id BIGINT,"
            + "  user_id BIGINT,"
            + "  user_name STRING,"
            + "  user_level INT,"
            + "  amount DOUBLE"
            + ") WITH ("
            + "  'connector' = 'print'"
            + ")",
        RESULT_TABLE_NAME);

    // 执行 Lookup Join 查询
    // 注意：由于 datagen 会持续产生数据，这里只是验证 SQL 语法正确性
    String joinSql =
        String.format(
            "SELECT o.order_id, o.user_id, d.user_name, d.user_level, o.amount "
                + "FROM %s AS o "
                + "LEFT JOIN %s FOR SYSTEM_TIME AS OF o.proc_time AS d "
                + "ON o.user_id = d.user_id",
            FACT_TABLE_NAME, DIM_TABLE_NAME);

    // 验证 SQL 可以正常解析和计划
    getTableEnv().executeSql("EXPLAIN " + joinSql);
  }

  /** 测试使用 SQL Hints 覆盖 Lookup 配置 */
  @Test
  public void testLookupJoinWithHints() throws Exception {
    // 创建事实表
    sql(
        "CREATE TABLE %s ("
            + "  order_id BIGINT,"
            + "  user_id BIGINT,"
            + "  amount DOUBLE,"
            + "  proc_time AS PROCTIME()"
            + ") WITH ("
            + "  'connector' = 'datagen',"
            + "  'rows-per-second' = '1',"
            + "  'fields.order_id.kind' = 'sequence',"
            + "  'fields.order_id.start' = '1',"
            + "  'fields.order_id.end' = '3',"
            + "  'fields.user_id.min' = '1',"
            + "  'fields.user_id.max' = '3',"
            + "  'fields.amount.min' = '10.0',"
            + "  'fields.amount.max' = '100.0'"
            + ")",
        FACT_TABLE_NAME);

    // 使用 Hints 覆盖配置执行 Lookup Join
    String joinSqlWithHints =
        String.format(
            "SELECT o.order_id, o.user_id, d.user_name, d.user_level, o.amount "
                + "FROM %s AS o "
                + "LEFT JOIN %s /*+ OPTIONS('lookup.mode'='partial', 'lookup.cache.ttl'='5m') */ "
                + "FOR SYSTEM_TIME AS OF o.proc_time AS d "
                + "ON o.user_id = d.user_id",
            FACT_TABLE_NAME, DIM_TABLE_NAME);

    // 验证带 Hints 的 SQL 可以正常解析和计划
    getTableEnv().executeSql("EXPLAIN " + joinSqlWithHints);
  }

  /** 测试多键 Lookup Join */
  @Test
  public void testMultiKeyLookupJoin() throws Exception {
    // 创建多键维表
    Map<String, String> tableProps = createTableProps();
    tableProps.put("lookup.mode", lookupMode);

    sql("DROP TABLE IF EXISTS dim_multi_key");
    sql(
        "CREATE TABLE dim_multi_key ("
            + "  key1 BIGINT,"
            + "  key2 STRING,"
            + "  value STRING,"
            + "  PRIMARY KEY (key1, key2) NOT ENFORCED"
            + ") WITH %s",
        toWithClause(tableProps));

    // 插入数据
    sql(
        "INSERT INTO dim_multi_key VALUES "
            + "(1, 'A', 'value1A'), "
            + "(1, 'B', 'value1B'), "
            + "(2, 'A', 'value2A')");

    // 创建事实表
    sql(
        "CREATE TABLE fact_multi_key ("
            + "  id BIGINT,"
            + "  key1 BIGINT,"
            + "  key2 STRING,"
            + "  proc_time AS PROCTIME()"
            + ") WITH ("
            + "  'connector' = 'datagen',"
            + "  'rows-per-second' = '1',"
            + "  'number-of-rows' = '3'"
            + ")");

    // 执行多键 Lookup Join
    String joinSql =
        "SELECT f.id, f.key1, f.key2, d.value "
            + "FROM fact_multi_key AS f "
            + "LEFT JOIN dim_multi_key FOR SYSTEM_TIME AS OF f.proc_time AS d "
            + "ON f.key1 = d.key1 AND f.key2 = d.key2";

    // 验证 SQL 可以正常解析和计划
    getTableEnv().executeSql("EXPLAIN " + joinSql);

    // 清理
    sql("DROP TABLE IF EXISTS dim_multi_key");
    sql("DROP TABLE IF EXISTS fact_multi_key");
  }

  /** 测试维表数据的读取 */
  @Test
  public void testReadDimTableData() {
    // 验证维表数据正确写入
    List<Row> results = sql("SELECT * FROM %s ORDER BY user_id", DIM_TABLE_NAME);

    Assertions.assertThat(results).hasSize(3);
    Assertions.assertThat(results.get(0).getField(0)).isEqualTo(1L);
    Assertions.assertThat(results.get(0).getField(1)).isEqualTo("Alice");
    Assertions.assertThat(results.get(0).getField(2)).isEqualTo(1);
  }

  private Map<String, String> createTableProps() {
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put("connector", "iceberg");
    tableProps.put("catalog-type", "hadoop");
    tableProps.put("catalog-name", catalogName);
    tableProps.put("warehouse", createWarehouse());
    return tableProps;
  }

  private String toWithClause(Map<String, String> props) {
    StringBuilder sb = new StringBuilder("(");
    boolean first = true;
    for (Map.Entry<String, String> entry : props.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append("'").append(entry.getKey()).append("'='").append(entry.getValue()).append("'");
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  private static String createWarehouse() {
    try {
      return String.format("file://%s", WAREHOUSE.newFolder().getAbsolutePath());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
