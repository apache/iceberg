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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.BoundedTableFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkTableSinkCompaction extends CatalogTestBase {

  private static final String TABLE_NAME = "test_table";
  private TableEnvironment tEnv;
  private Table icebergTable;
  private static final String TABLE_PROPERTIES =
      "'flink-maintenance.rewrite.lock.type'='jdbc','flink-maintenance.rewrite.lock.jdbc.uri'='jdbc:sqlite:file::memory:?ic','flink-maintenance.rewrite.lock.jdbc.init-lock-table'='true','flink-maintenance.rewrite.rewrite-all'='true','flink-maintenance.rewrite.schedule.data-file-size'='1'";

  @Parameter(index = 2)
  private boolean userSqlHint = true;

  @Parameters(name = "catalogName={0}, baseNamespace={1}, userSqlHint={2}")
  public static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();

    for (Boolean userSqlHint : new Boolean[] {true, false}) {
      String catalogName = "testhadoop_basenamespace";
      Namespace baseNamespace = Namespace.of("l0", "l1");
      parameters.add(new Object[] {catalogName, baseNamespace, userSqlHint});
    }
    return parameters;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
        settingsBuilder.inStreamingMode();
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
        env.enableCheckpointing(100);
        tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
      }
    }

    tEnv.getConfig()
        .getConfiguration()
        .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK, true)
        .set(FlinkWriteOptions.COMPACTION_ENABLE, true);

    return tEnv;
  }

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    if (userSqlHint) {

      sql("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);

    } else {
      sql("CREATE TABLE %s (id int, data varchar) with (%s)", TABLE_NAME, TABLE_PROPERTIES);
    }
    icebergTable = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
  }

  @Override
  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    dropDatabase(flinkDatabase, true);
    BoundedTableFactory.clearDataSets();
    super.clean();
  }

  @TestTemplate
  public void testSQLCompactionE2e() throws Exception {
    // Register the rows into a temporary table.
    getTableEnv()
        .createTemporaryView(
            "sourceTable",
            getTableEnv()
                .fromValues(
                    SimpleDataUtil.FLINK_SCHEMA.toRowDataType(),
                    Expressions.row(1, "hello"),
                    Expressions.row(2, "world"),
                    Expressions.row(3, (String) null),
                    Expressions.row(null, "bar")));

    // Redirect the records from source table to destination table.
    if (userSqlHint) {
      sql(
          "INSERT INTO %s /*+ OPTIONS(%s) */ SELECT id,data from sourceTable",
          TABLE_NAME, TABLE_PROPERTIES);
    } else {
      sql("INSERT INTO %s SELECT id,data from sourceTable", TABLE_NAME);
    }

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(
        icebergTable,
        Lists.newArrayList(
            SimpleDataUtil.createRecord(1, "hello"),
            SimpleDataUtil.createRecord(2, "world"),
            SimpleDataUtil.createRecord(3, null),
            SimpleDataUtil.createRecord(null, "bar")));
  }
}
