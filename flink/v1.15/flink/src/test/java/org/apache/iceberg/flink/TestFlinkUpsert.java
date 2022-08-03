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

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkUpsert extends FlinkCatalogTestBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private final boolean isStreamingJob;
  private final Map<String, String> tableUpsertProps = Maps.newHashMap();
  private TableEnvironment tEnv;

  public TestFlinkUpsert(
      String catalogName, Namespace baseNamespace, FileFormat format, Boolean isStreamingJob) {
    super(catalogName, baseNamespace);
    this.isStreamingJob = isStreamingJob;
    tableUpsertProps.put(TableProperties.FORMAT_VERSION, "2");
    tableUpsertProps.put(TableProperties.UPSERT_ENABLED, "true");
    tableUpsertProps.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());
  }

  @Parameterized.Parameters(
      name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        // Only test with one catalog as this is a file operation concern.
        // FlinkCatalogTestBase requires the catalog name start with testhadoop if using hadoop
        // catalog.
        String catalogName = "testhadoop";
        Namespace baseNamespace = Namespace.of("default");
        parameters.add(new Object[] {catalogName, baseNamespace, format, isStreaming});
      }
    }
    return parameters;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
        if (isStreamingJob) {
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env =
              StreamExecutionEnvironment.getExecutionEnvironment(
                  MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
          env.enableCheckpointing(400);
          env.setMaxParallelism(2);
          env.setParallelism(2);
          tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
        } else {
          settingsBuilder.inBatchMode();
          tEnv = TableEnvironment.create(settingsBuilder.build());
        }
      }
    }
    return tEnv;
  }

  @Override
  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE IF NOT EXISTS %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @Override
  @After
  public void clean() {
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testUpsertAndQuery() {
    String tableName = "test_upsert_query";
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220302 = LocalDate.of(2022, 3, 2);

    sql(
        "CREATE TABLE %s(id INT NOT NULL, province STRING NOT NULL, dt DATE, PRIMARY KEY(id,province) NOT ENFORCED) "
            + "PARTITIONED BY (province) WITH %s",
        tableName, toWithClause(tableUpsertProps));

    try {
      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'a', DATE '2022-03-01'),"
              + "(2, 'b', DATE '2022-03-01'),"
              + "(1, 'b', DATE '2022-03-01')",
          tableName);

      sql(
          "INSERT INTO %s VALUES "
              + "(4, 'a', DATE '2022-03-02'),"
              + "(5, 'b', DATE '2022-03-02'),"
              + "(1, 'b', DATE '2022-03-02')",
          tableName);

      List<Row> rowsOn20220301 =
          Lists.newArrayList(Row.of(2, "b", dt20220301), Row.of(1, "a", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt < '2022-03-02'", tableName), rowsOn20220301);

      List<Row> rowsOn20220302 =
          Lists.newArrayList(
              Row.of(1, "b", dt20220302), Row.of(4, "a", dt20220302), Row.of(5, "b", dt20220302));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", tableName), rowsOn20220302);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @Test
  public void testUpsertOptions() {
    String tableName = "test_upsert_options";
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220302 = LocalDate.of(2022, 3, 2);

    Map<String, String> optionsUpsertProps = Maps.newHashMap(tableUpsertProps);
    optionsUpsertProps.remove(TableProperties.UPSERT_ENABLED);
    sql(
        "CREATE TABLE %s(id INT NOT NULL, province STRING NOT NULL, dt DATE, PRIMARY KEY(id,province) NOT ENFORCED) "
            + "PARTITIONED BY (province) WITH %s",
        tableName, toWithClause(optionsUpsertProps));

    try {
      sql(
          "INSERT INTO %s /*+ OPTIONS('upsert-enabled'='true')*/  VALUES "
              + "(1, 'a', DATE '2022-03-01'),"
              + "(2, 'b', DATE '2022-03-01'),"
              + "(1, 'b', DATE '2022-03-01')",
          tableName);

      sql(
          "INSERT INTO %s /*+ OPTIONS('upsert-enabled'='true')*/  VALUES "
              + "(4, 'a', DATE '2022-03-02'),"
              + "(5, 'b', DATE '2022-03-02'),"
              + "(1, 'b', DATE '2022-03-02')",
          tableName);

      List<Row> rowsOn20220301 =
          Lists.newArrayList(Row.of(2, "b", dt20220301), Row.of(1, "a", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt < '2022-03-02'", tableName), rowsOn20220301);

      List<Row> rowsOn20220302 =
          Lists.newArrayList(
              Row.of(1, "b", dt20220302), Row.of(4, "a", dt20220302), Row.of(5, "b", dt20220302));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", tableName), rowsOn20220302);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @Test
  public void testPrimaryKeyEqualToPartitionKey() {
    // This is an SQL based reproduction of TestFlinkIcebergSinkV2#testUpsertOnDataKey
    String tableName = "upsert_on_data_key";
    try {
      sql(
          "CREATE TABLE %s(id INT NOT NULL, data STRING NOT NULL, PRIMARY KEY(data) NOT ENFORCED) "
              + "PARTITIONED BY (data) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql("INSERT INTO %s VALUES " + "(1, 'aaa')," + "(2, 'aaa')," + "(3, 'bbb')", tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(2, "aaa"), Row.of(3, "bbb")));

      sql("INSERT INTO %s VALUES " + "(4, 'aaa')," + "(5, 'bbb')", tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(4, "aaa"), Row.of(5, "bbb")));

      sql("INSERT INTO %s VALUES " + "(6, 'aaa')," + "(7, 'bbb')", tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(6, "aaa"), Row.of(7, "bbb")));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @Test
  public void testPrimaryKeyFieldsAtBeginningOfSchema() {
    String tableName = "upsert_on_pk_at_schema_start";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    try {
      sql(
          "CREATE TABLE %s(data STRING NOT NULL, dt DATE NOT NULL, id INT, PRIMARY KEY(data,dt) NOT ENFORCED) "
              + "PARTITIONED BY (data) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql(
          "INSERT INTO %s VALUES "
              + "('aaa', DATE '2022-03-01', 1),"
              + "('aaa', DATE '2022-03-01', 2),"
              + "('bbb', DATE '2022-03-01', 3)",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("aaa", dt, 2), Row.of("bbb", dt, 3)));

      sql(
          "INSERT INTO %s VALUES "
              + "('aaa', DATE '2022-03-01', 4),"
              + "('bbb', DATE '2022-03-01', 5)",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("aaa", dt, 4), Row.of("bbb", dt, 5)));

      sql(
          "INSERT INTO %s VALUES "
              + "('aaa', DATE '2022-03-01', 6),"
              + "('bbb', DATE '2022-03-01', 7)",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("aaa", dt, 6), Row.of("bbb", dt, 7)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @Test
  public void testPrimaryKeyFieldsAtEndOfTableSchema() {
    // This is the same test case as testPrimaryKeyFieldsAtBeginningOfSchema, but the primary key
    // fields
    // are located at the end of the flink schema.
    String tableName = "upsert_on_pk_at_schema_end";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    try {
      sql(
          "CREATE TABLE %s(id INT, data STRING NOT NULL, dt DATE NOT NULL, PRIMARY KEY(data,dt) NOT ENFORCED) "
              + "PARTITIONED BY (data) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'aaa', DATE '2022-03-01'),"
              + "(2, 'aaa', DATE '2022-03-01'),"
              + "(3, 'bbb', DATE '2022-03-01')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(2, "aaa", dt), Row.of(3, "bbb", dt)));

      sql(
          "INSERT INTO %s VALUES "
              + "(4, 'aaa', DATE '2022-03-01'),"
              + "(5, 'bbb', DATE '2022-03-01')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(4, "aaa", dt), Row.of(5, "bbb", dt)));

      sql(
          "INSERT INTO %s VALUES "
              + "(6, 'aaa', DATE '2022-03-01'),"
              + "(7, 'bbb', DATE '2022-03-01')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(6, "aaa", dt), Row.of(7, "bbb", dt)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }
}
