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
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkUpsert extends CatalogTestBase {

  @Parameter(index = 2)
  private FileFormat format;

  @Parameter(index = 3)
  private boolean isStreamingJob;

  private final Map<String, String> tableUpsertProps = Maps.newHashMap();
  private TableEnvironment tEnv;

  @Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static List<Object[]> parameters() {
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
                  MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
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
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE IF NOT EXISTS %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    tableUpsertProps.put(TableProperties.FORMAT_VERSION, "2");
    tableUpsertProps.put(TableProperties.UPSERT_ENABLED, "true");
    tableUpsertProps.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());
  }

  @Override
  @AfterEach
  public void clean() {
    dropDatabase(flinkDatabase, true);
    super.clean();
  }

  @TestTemplate
  public void testUpsertAndQuery() {
    String tableName = "test_upsert_query";
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220302 = LocalDate.of(2022, 3, 2);

    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING NOT NULL, dt DATE, PRIMARY KEY(id,dt) NOT ENFORCED) "
            + "PARTITIONED BY (dt) WITH %s",
        tableName, toWithClause(tableUpsertProps));

    try {
      sql(
          "INSERT INTO %s VALUES "
              + "(1, 'Bill', DATE '2022-03-01'),"
              + "(1, 'Jane', DATE '2022-03-01'),"
              + "(2, 'Jane', DATE '2022-03-01')",
          tableName);

      sql(
          "INSERT INTO %s VALUES "
              + "(2, 'Bill', DATE '2022-03-01'),"
              + "(1, 'Jane', DATE '2022-03-02'),"
              + "(2, 'Jane', DATE '2022-03-02')",
          tableName);

      List<Row> rowsOn20220301 =
          Lists.newArrayList(Row.of(1, "Jane", dt20220301), Row.of(2, "Bill", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt < '2022-03-02'", tableName), rowsOn20220301);

      List<Row> rowsOn20220302 =
          Lists.newArrayList(Row.of(1, "Jane", dt20220302), Row.of(2, "Jane", dt20220302));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", tableName), rowsOn20220302);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testUpsertOptions() {
    String tableName = "test_upsert_options";
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220302 = LocalDate.of(2022, 3, 2);

    Map<String, String> optionsUpsertProps = Maps.newHashMap(tableUpsertProps);
    optionsUpsertProps.remove(TableProperties.UPSERT_ENABLED);
    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING NOT NULL, dt DATE, PRIMARY KEY(id,dt) NOT ENFORCED) "
            + "PARTITIONED BY (dt) WITH %s",
        tableName, toWithClause(optionsUpsertProps));

    try {
      sql(
          "INSERT INTO %s /*+ OPTIONS('upsert-enabled'='true')*/  VALUES "
              + "(1, 'Bill', DATE '2022-03-01'),"
              + "(1, 'Jane', DATE '2022-03-01'),"
              + "(2, 'Jane', DATE '2022-03-01')",
          tableName);

      sql(
          "INSERT INTO %s /*+ OPTIONS('upsert-enabled'='true')*/  VALUES "
              + "(2, 'Bill', DATE '2022-03-01'),"
              + "(1, 'Jane', DATE '2022-03-02'),"
              + "(2, 'Jane', DATE '2022-03-02')",
          tableName);

      List<Row> rowsOn20220301 =
          Lists.newArrayList(Row.of(1, "Jane", dt20220301), Row.of(2, "Bill", dt20220301));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt < '2022-03-02'", tableName), rowsOn20220301);

      List<Row> rowsOn20220302 =
          Lists.newArrayList(Row.of(1, "Jane", dt20220302), Row.of(2, "Jane", dt20220302));
      TestHelpers.assertRows(
          sql("SELECT * FROM %s WHERE dt = '2022-03-02'", tableName), rowsOn20220302);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Iterables.concat(rowsOn20220301, rowsOn20220302)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testPrimaryKeyEqualToPartitionKey() {
    // This is an SQL based reproduction of TestFlinkIcebergSinkV2#testUpsertOnDataKey
    String tableName = "upsert_on_id_key";
    try {
      sql(
          "CREATE TABLE %s(id INT NOT NULL, name STRING NOT NULL, PRIMARY KEY(id) NOT ENFORCED) "
              + "PARTITIONED BY (id) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql("INSERT INTO %s VALUES " + "(1, 'Bill')," + "(1, 'Jane')," + "(2, 'Bill')", tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(1, "Jane"), Row.of(2, "Bill")));

      sql("INSERT INTO %s VALUES " + "(1, 'Bill')," + "(2, 'Jane')", tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(1, "Bill"), Row.of(2, "Jane")));

      sql("INSERT INTO %s VALUES " + "(3, 'Bill')," + "(4, 'Jane')", tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(
              Row.of(1, "Bill"), Row.of(2, "Jane"), Row.of(3, "Bill"), Row.of(4, "Jane")));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testPrimaryKeyFieldsAtBeginningOfSchema() {
    String tableName = "upsert_on_pk_at_schema_start";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    try {
      sql(
          "CREATE TABLE %s(id INT, dt DATE NOT NULL, name STRING NOT NULL, PRIMARY KEY(id,dt) NOT ENFORCED) "
              + "PARTITIONED BY (dt) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql(
          "INSERT INTO %s VALUES "
              + "(1, DATE '2022-03-01', 'Andy'),"
              + "(1, DATE '2022-03-01', 'Bill'),"
              + "(2, DATE '2022-03-01', 'Jane')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(1, dt, "Bill"), Row.of(2, dt, "Jane")));

      sql(
          "INSERT INTO %s VALUES "
              + "(1, DATE '2022-03-01', 'Jane'),"
              + "(2, DATE '2022-03-01', 'Bill')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of(1, dt, "Jane"), Row.of(2, dt, "Bill")));

      sql(
          "INSERT INTO %s VALUES "
              + "(3, DATE '2022-03-01', 'Duke'),"
              + "(4, DATE '2022-03-01', 'Leon')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(
              Row.of(1, dt, "Jane"),
              Row.of(2, dt, "Bill"),
              Row.of(3, dt, "Duke"),
              Row.of(4, dt, "Leon")));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @TestTemplate
  public void testPrimaryKeyFieldsAtEndOfTableSchema() {
    // This is the same test case as testPrimaryKeyFieldsAtBeginningOfSchema, but the primary key
    // fields
    // are located at the end of the flink schema.
    String tableName = "upsert_on_pk_at_schema_end";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    try {
      sql(
          "CREATE TABLE %s(name STRING NOT NULL, id INT, dt DATE NOT NULL, PRIMARY KEY(id,dt) NOT ENFORCED) "
              + "PARTITIONED BY (dt) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql(
          "INSERT INTO %s VALUES "
              + "('Andy', 1, DATE '2022-03-01'),"
              + "('Bill', 1, DATE '2022-03-01'),"
              + "('Jane', 2, DATE '2022-03-01')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("Bill", 1, dt), Row.of("Jane", 2, dt)));

      sql(
          "INSERT INTO %s VALUES "
              + "('Jane', 1, DATE '2022-03-01'),"
              + "('Bill', 2, DATE '2022-03-01')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("Jane", 1, dt), Row.of("Bill", 2, dt)));

      sql(
          "INSERT INTO %s VALUES "
              + "('Duke', 3, DATE '2022-03-01'),"
              + "('Leon', 4, DATE '2022-03-01')",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(
              Row.of("Jane", 1, dt),
              Row.of("Bill", 2, dt),
              Row.of("Duke", 3, dt),
              Row.of("Leon", 4, dt)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }
}
