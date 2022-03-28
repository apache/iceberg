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
public class TestFlinkMultipleUpsertsOnNonPKField extends FlinkCatalogTestBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private final boolean isStreamingJob;
  private final Map<String, String> tableUpsertProps = Maps.newHashMap();
  private TableEnvironment tEnv;

  public TestFlinkMultipleUpsertsOnNonPKField(String catalogName, Namespace baseNamespace, FileFormat format, Boolean isStreamingJob) {
    super(catalogName, baseNamespace);
    this.isStreamingJob = isStreamingJob;
    tableUpsertProps.put(TableProperties.FORMAT_VERSION, "2");
    tableUpsertProps.put(TableProperties.UPSERT_ENABLED, "true");
    tableUpsertProps.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());
  }

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        // Only test with one catalog as this is a file operation concern.
        // FlinkCatalogTestBase requires the catalog name start with testhadoop if using hadoop catalog.
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
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
            .newInstance();
        if (isStreamingJob) {
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env = StreamExecutionEnvironment
              .getExecutionEnvironment(MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG);
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
  public void  testMultipleUpsertsToOneRowWithNonPKFieldChanging_fails() {
    String tableName = "test_multiple_upserts_on_one_row_fails";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    try {
      sql("CREATE TABLE %s(data STRING NOT NULL, dt DATE NOT NULL, id INT NOT NULL, bool BOOLEAN NOT NULL, " +
              "PRIMARY KEY(data,dt) NOT ENFORCED) " +
              "PARTITIONED BY (data) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql("INSERT INTO %s VALUES " +
          "('aaa', TO_DATE('2022-03-01'), 1, false)," +
          "('aaa', TO_DATE('2022-03-01'), 2, false)," +
          "('bbb', TO_DATE('2022-03-01'), 3, false)",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("aaa", dt, 2, false), Row.of("bbb", dt, 3, false)));

      // Process several duplicates of the same record with PK ('aaa', TO_DATE('2022-03-01')).
      // Depending on the number of times that records are inserted for that row, one of the
      // rows 2 back will be used instead.
      //
      // Indicating possibly an issue with insertedRowMap checking and/or the positional delete
      // writer.
      sql("INSERT INTO %s VALUES " +
          "('aaa', TO_DATE('2022-03-01'), 6, false)," +
          "('aaa', TO_DATE('2022-03-01'), 6, true)," +
          "('aaa', TO_DATE('2022-03-01'), 6, false)," +
          "('aaa', TO_DATE('2022-03-01'), 6, false)",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("aaa", dt, 6, false), Row.of("bbb", dt, 3, false)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  /**
   * This test is the same as the one above, except the non-PK field `id` has increasing
   * values instead of the same value throughout.
   *
   * This one passes while the other one does not.
   */
  @Test
  public void  testMultipleUpsertsToOneRowWithNonPKFieldChanging_succeeds() {
    String tableName = "test_multiple_upserts_on_one_row_succeeds";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    try {
      sql("CREATE TABLE %s(data STRING NOT NULL, dt DATE NOT NULL, id INT NOT NULL, bool BOOLEAN NOT NULL, " +
              "PRIMARY KEY(data,dt) NOT ENFORCED) " +
              "PARTITIONED BY (data) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql("INSERT INTO %s VALUES " +
          "('aaa', TO_DATE('2022-03-01'), 1, false)," +
          "('aaa', TO_DATE('2022-03-01'), 2, false)," +
          "('bbb', TO_DATE('2022-03-01'), 3, false)",
          tableName);

      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("aaa", dt, 2, false), Row.of("bbb", dt, 3, false)));

      // Process several duplicates of the same record with PK ('aaa', TO_DATE('2022-03-01')).
      // Depending on the number of times that records are inserted for that row, one of the
      // rows 2 back will be used instead.
      //
      // Indicating possibly an issue with insertedRowMap checking and/or the positional delete
      // writer.
      sql("INSERT INTO %s VALUES " +
          "('aaa', TO_DATE('2022-03-01'), 1, false)," +
          "('aaa', TO_DATE('2022-03-01'), 2, false)," +
          "('aaa', TO_DATE('2022-03-01'), 3, false)," +
          "('aaa', TO_DATE('2022-03-01'), 6, false)",
          tableName);
      TestHelpers.assertRows(
          sql("SELECT * FROM %s", tableName),
          Lists.newArrayList(Row.of("aaa", dt, 6, false), Row.of("bbb", dt, 3, false)));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }
}

