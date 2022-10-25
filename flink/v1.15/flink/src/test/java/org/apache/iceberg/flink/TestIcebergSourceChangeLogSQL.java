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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergSourceChangeLogSQL extends FlinkCatalogTestBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private final boolean isStreamingJob;
  private final Map<String, String> tableUpsertProps = Maps.newHashMap();
  private TableEnvironment tEnv;

  public TestIcebergSourceChangeLogSQL(
      String catalogName,
      Namespace baseNamespace,
      FileFormat format,
      Boolean isStreamingJob,
      String formatVersion) {
    super(catalogName, baseNamespace);
    this.isStreamingJob = isStreamingJob;
    tableUpsertProps.put(TableProperties.FORMAT_VERSION, formatVersion);
    tableUpsertProps.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());
  }

  @Parameterized.Parameters(
      name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}, formatVersion={4}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET}) {
      // Only test with one catalog as this is a file operation concern.
      // FlinkCatalogTestBase requires the catalog name start with testhadoop if using hadoop
      // catalog.
      String catalogName = "testhadoop";
      Namespace baseNamespace = Namespace.of("default");
      parameters.add(new Object[] {catalogName, baseNamespace, format, false, "1"});
      parameters.add(new Object[] {catalogName, baseNamespace, format, false, "2"});
    }
    return parameters;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        Configuration configuration = new Configuration();
        configuration.setBoolean(
            FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE.key(), true);
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
        settingsBuilder.withConfiguration(configuration);
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
  public void testChangeLogRead() {
    Assume.assumeFalse("  ", isStreamingJob);

    String tableName = "test_upsert_query";
    LocalDate dt20220301 = LocalDate.of(2022, 3, 1);
    LocalDate dt20220302 = LocalDate.of(2022, 3, 2);

    sql(
        "CREATE TABLE %s(id INT NOT NULL, data STRING NOT NULL, dt DATE) "
            + "PARTITIONED BY (dt) WITH %s",
        tableName, toWithClause(tableUpsertProps));

    try {
      sql(
          "INSERT INTO %s VALUES " + "(1, 'a', DATE '2022-03-01')," + "(3, 'c', DATE '2022-03-02')",
          tableName);

      sql("INSERT OVERWRITE %s VALUES " + "(1, 'a2', DATE '2022-03-01')", tableName);

      List<Row> rowsOn20220301 =
          Lists.newArrayList(
              Row.ofKind(RowKind.INSERT, 1, "a2", dt20220301),
              Row.ofKind(RowKind.DELETE, 1, "a", dt20220301),
              Row.ofKind(RowKind.INSERT, 1, "a", dt20220301),
              Row.ofKind(RowKind.INSERT, 3, "c", dt20220302));
      TestHelpers.assertRows(
          sql(
              "SELECT * FROM %s /*+ OPTIONS('scan-mode'='CHANGELOG_SCAN')*/ order by id",
              tableName),
          rowsOn20220301);
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }
}
