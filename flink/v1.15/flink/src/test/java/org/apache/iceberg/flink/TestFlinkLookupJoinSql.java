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

import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
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
public class TestFlinkLookupJoinSql extends FlinkCatalogTestBase {
  private static final String DIM_TABLE_NAME = "dim_table";
  private static final String SOURCE_TABLE_NAME = "source_table";

  public static final TableSchema SOURCE_SCHEMA =
      TableSchema.builder().field("id", DataTypes.INT()).field("data", DataTypes.STRING()).build();
  private final Map<String, String> tableProps = Maps.newHashMap();
  private TableEnvironment tEnv;
  private final boolean isStreamingJob;

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  public TestFlinkLookupJoinSql(
      String catalogName, Namespace baseNamespace, FileFormat format, boolean isStreamingJob) {
    super(catalogName, baseNamespace);
    this.isStreamingJob = isStreamingJob;
    tableProps.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());
  }

  @Parameterized.Parameters(
      name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
          String catalogName = (String) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(new Object[] {catalogName, baseNamespace, format, isStreaming});
        }
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
    // create source table
    getTableEnv()
        .createTemporaryView(
            SOURCE_TABLE_NAME,
            getTableEnv()
                .fromValues(
                    SOURCE_SCHEMA.toRowDataType(),
                    Expressions.row(1, "hello"),
                    Expressions.row(2, "world"),
                    Expressions.row(3, "lee"),
                    Expressions.row(2, "leeoe"),
                    Expressions.row(4, "bar")));
  }

  @Override
  @After
  public void clean() {
    sql("DROP DATABASE IF EXISTS %s;", flinkDatabase);
    super.clean();
  }

  @Test
  public void testLookupJoinAsynAndSync() {
    // create sink table
    String sinkTable1 = "sinkTable1";
    String sinkTable2 = "sinkTable2";
    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING, dt STRING, data STRING) ",
        sinkTable1, toWithClause(tableProps));
    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING, dt STRING, data STRING) ",
        sinkTable2, toWithClause(tableProps));

    // create dimension table
    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING, dt STRING, PRIMARY KEY(id) NOT ENFORCED) ",
        DIM_TABLE_NAME, toWithClause(tableProps));

    sql("ALTER TABLE %s SET ('lookup.async.enabled' = 'true')", DIM_TABLE_NAME);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'Bill', '2022-03-01'),"
            + "(3, 'Jane', '2022-03-01'),"
            + "(2, 'Jack', '2022-03-01')",
        DIM_TABLE_NAME);

    sql(
        "INSERT INTO %s select s.id, d.name, d.dt, s.data from (select *, PROCTIME() as proctime from %s) as s "
            + "left join %s FOR SYSTEM_TIME AS OF s.proctime as d ON s.id = d.id",
        sinkTable1, SOURCE_TABLE_NAME, DIM_TABLE_NAME);

    TestHelpers.assertRows(
        sql("SELECT * FROM %s", sinkTable1),
        Lists.newArrayList(
            Row.of(1, "Bill", "2022-03-01", "hello"),
            Row.of(2, "Jack", "2022-03-01", "world"),
            Row.of(3, "Jane", "2022-03-01", "lee"),
            Row.of(2, "Jack", "2022-03-01", "leeoe"),
            Row.of(4, null, null, "bar")));

    sql("ALTER TABLE %s SET ('lookup.async.enabled' = 'false')", DIM_TABLE_NAME);
    sql(
        "INSERT INTO %s select s.id, d.name, d.dt, s.data from (select *, PROCTIME() as proctime from %s) as s "
            + "left join %s FOR SYSTEM_TIME AS OF s.proctime as d ON s.id = d.id",
        sinkTable2, SOURCE_TABLE_NAME, DIM_TABLE_NAME);
    TestHelpers.assertRows(
        sql("SELECT * FROM %s", sinkTable2),
        Lists.newArrayList(
            Row.of(1, "Bill", "2022-03-01", "hello"),
            Row.of(2, "Jack", "2022-03-01", "world"),
            Row.of(3, "Jane", "2022-03-01", "lee"),
            Row.of(2, "Jack", "2022-03-01", "leeoe"),
            Row.of(4, null, null, "bar")));

    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, sinkTable1);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, sinkTable2);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, DIM_TABLE_NAME);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, SOURCE_TABLE_NAME);
  }

  @Test
  public void testLookupJoinAllAndLRU() {
    // create sink table
    String sinkTable1 = "sinkTable1";
    String sinkTable2 = "sinkTable2";
    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING, dt STRING, data STRING) ",
        sinkTable1, toWithClause(tableProps));
    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING, dt STRING, data STRING) ",
        sinkTable2, toWithClause(tableProps));

    // create dimension table
    sql(
        "CREATE TABLE %s(id INT NOT NULL, name STRING, dt STRING, PRIMARY KEY(id) NOT ENFORCED) ",
        DIM_TABLE_NAME, toWithClause(tableProps));

    sql("ALTER TABLE %s SET ('lookup.cache.type' = 'all')", DIM_TABLE_NAME);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'Bill', '2022-03-01'),"
            + "(3, 'Jane', '2022-03-01'),"
            + "(2, 'Jack', '2022-03-01')",
        DIM_TABLE_NAME);

    sql(
        "INSERT INTO %s select s.id, d.name, d.dt, s.data from (select *, PROCTIME() as proctime from %s) as s "
            + "left join %s FOR SYSTEM_TIME AS OF s.proctime as d ON s.id = d.id",
        sinkTable1, SOURCE_TABLE_NAME, DIM_TABLE_NAME);

    TestHelpers.assertRows(
        sql("SELECT * FROM %s", sinkTable1),
        Lists.newArrayList(
            Row.of(1, "Bill", "2022-03-01", "hello"),
            Row.of(2, "Jack", "2022-03-01", "world"),
            Row.of(3, "Jane", "2022-03-01", "lee"),
            Row.of(2, "Jack", "2022-03-01", "leeoe"),
            Row.of(4, null, null, "bar")));

    sql("ALTER TABLE %s SET ('lookup.cache.type' = 'lru')", DIM_TABLE_NAME);
    sql(
        "INSERT INTO %s select s.id, d.name, d.dt, s.data from (select *, PROCTIME() as proctime from %s) as s "
            + "left join %s FOR SYSTEM_TIME AS OF s.proctime as d ON s.id = d.id",
        sinkTable2, SOURCE_TABLE_NAME, DIM_TABLE_NAME);
    TestHelpers.assertRows(
        sql("SELECT * FROM %s", sinkTable2),
        Lists.newArrayList(
            Row.of(1, "Bill", "2022-03-01", "hello"),
            Row.of(2, "Jack", "2022-03-01", "world"),
            Row.of(3, "Jane", "2022-03-01", "lee"),
            Row.of(2, "Jack", "2022-03-01", "leeoe"),
            Row.of(4, null, null, "bar")));

    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, sinkTable1);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, sinkTable2);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, DIM_TABLE_NAME);
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, SOURCE_TABLE_NAME);
  }
}
