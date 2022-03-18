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
import org.apache.iceberg.flink.source.BoundedTableFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
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

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private TableEnvironment tEnv;

  private final FileFormat format;
  private final boolean isStreamingJob;
  private final Map<String, String> tableUpsertProps = Maps.newHashMap();

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, format={2}, isStreaming={3}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format : new FileFormat[] {FileFormat.PARQUET, FileFormat.AVRO, FileFormat.ORC}) {
      for (Boolean isStreaming : new Boolean[] {true, false}) {
        for (Object[] catalogParams : FlinkCatalogTestBase.parameters()) {
          String catalogName = (String) catalogParams[0];
          Namespace baseNamespace = (Namespace) catalogParams[1];
          parameters.add(new Object[] {catalogName, baseNamespace, format, isStreaming });
        }
      }
    }
    return parameters;
  }

  public TestFlinkUpsert(String catalogName, Namespace baseNamespace, FileFormat format, Boolean isStreamingJob) {
    super(catalogName, baseNamespace);
    this.format = format;
    this.isStreamingJob = isStreamingJob;
    tableUpsertProps.put(TableProperties.FORMAT_VERSION, "2");
    tableUpsertProps.put(TableProperties.UPSERT_ENABLED, "true");
    tableUpsertProps.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());
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
    BoundedTableFactory.clearDataSets();
    super.clean();
  }

  @Test
  public void testUpsertAndQuery() {
    Assume.assumeTrue("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    String tableName = "test_upsert_query";

    sql("CREATE TABLE %s(id INT NOT NULL, province STRING NOT NULL, dt DATE, PRIMARY KEY(id,province) NOT ENFORCED) " +
            "PARTITIONED BY (province) WITH %s",
        tableName, toWithClause(tableUpsertProps));

    try {
      sql("INSERT INTO %s VALUES " +
          "(1, 'a', TO_DATE('2022-03-01'))," +
          "(2, 'b', TO_DATE('2022-03-01'))," +
          "(1, 'b', TO_DATE('2022-03-01'))",
          tableName);

      sql("INSERT INTO %s VALUES " +
          "(4, 'a', TO_DATE('2022-03-02'))," +
          "(5, 'b', TO_DATE('2022-03-02'))," +
          "(1, 'b', TO_DATE('2022-03-02'))",
          tableName);

      List<Row> result = sql("SELECT * FROM %s WHERE dt < '2022-03-02'", tableName);

      Assert.assertEquals("result should have 2 rows!", 2, result.size());

      result = sql("SELECT * FROM %s WHERE dt < '2022-03-03'", tableName);

      Assert.assertEquals("result should have 5 rows!", 5, result.size());
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  // This is an SQL based reproduction of TestFlinkIcebergSinkV2#testUpsertOnDataKey
  //
  // This test case fails when updating the equality delete filter in RowDataTaskWriterFactory
  // to be TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds)) - which makes the above
  // test pass.
  @Test
  public void testUpsertWhenPartitionFieldSourceIdsAreEqualToEqualityDeleteFields() {
    Assume.assumeTrue("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    String tableName = "upsert_on_data_key";
    try {
      sql("CREATE TABLE %s(id INT NOT NULL, data STRING NOT NULL, PRIMARY KEY(data) NOT ENFORCED) " +
            "PARTITIONED BY (data) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql("INSERT INTO %s VALUES " +
          "(1, 'aaa')," +
          "(2, 'aaa')," +
          "(3, 'bbb')",
          tableName);

      List<Row> result = sql("SELECT * FROM %s", tableName);

      Assert.assertEquals("result should have 2 rows!", 2, result.size());
      Assert.assertEquals("result should have the correct rows",
          Sets.newHashSet(Row.of(2, "aaa"), Row.of(3, "bbb")), Sets.newHashSet(result));

      sql("INSERT INTO %s VALUES " +
          "(4, 'aaa')," +
          "(5, 'bbb')",
          tableName);

      result = sql("SELECT * FROM %s", tableName);

      // Assert.assertEquals("result should have 2 rows!", 2, result.size());
      Assert.assertEquals("result should have the correct rows",
          Sets.newHashSet(Row.of(4, "aaa"), Row.of(5, "bbb")), Sets.newHashSet(result));

      sql("INSERT INTO %s VALUES " +
          "(6, 'aaa')," +
          "(7, 'bbb')",
          tableName);

      result = sql("SELECT * FROM %s", tableName);

      Assert.assertEquals("result should have 2 rows!", 2, result.size());
      Assert.assertEquals("result should have the correct rows",
          Sets.newHashSet(Row.of(6, "aaa"), Row.of(7, "bbb")), Sets.newHashSet(result));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  @Test
  public void testUpsertEqualityFieldsAreSuperSetOfPartitionFieldsWithPartitionFieldAtEnd() {
    Assume.assumeTrue("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    String tableName = "upsert_on_super_of_partition_ids";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    try {
      sql("CREATE TABLE %s(id INT, data STRING NOT NULL, dt DATE NOT NULL, PRIMARY KEY(data,dt) NOT ENFORCED) " +
              "PARTITIONED BY (data) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql("INSERT INTO %s VALUES " +
          "(1, 'aaa', TO_DATE('2022-03-01'))," +
          "(2, 'aaa', TO_DATE('2022-03-01'))," +
          "(3, 'bbb', TO_DATE('2022-03-01'))",
          tableName);

      List<Row> result = sql("SELECT * FROM %s", tableName);

      // Assert.assertEquals("result should have 2 rows!", 2, result.size());
      Assert.assertEquals("result should have the correct rows",
          Sets.newHashSet(Row.of(2, "aaa", dt), Row.of(3, "bbb", dt)), Sets.newHashSet(result));

      sql("INSERT INTO %s VALUES " +
          "(4, 'aaa', TO_DATE('2022-03-01'))," +
          "(5, 'bbb', TO_DATE('2022-03-01'))",
          tableName);

      result = sql("SELECT * FROM %s", tableName);

      // Assert.assertEquals("result should have 2 rows!", 2, result.size());
      Assert.assertEquals("result should have the correct rows",
          Sets.newHashSet(Row.of(4, "aaa", dt), Row.of(5, "bbb", dt)), Sets.newHashSet(result));

      sql("INSERT INTO %s VALUES " +
          "(6, 'aaa', TO_DATE('2022-03-01'))," +
          "(7, 'bbb', TO_DATE('2022-03-01'))",
          tableName);

      result = sql("SELECT * FROM %s", tableName);

      // Assert.assertEquals("result should have 2 rows!", 2, result.size());
      Assert.assertEquals("result should have the correct rows",
          Sets.newHashSet(Row.of(6, "aaa", dt), Row.of(7, "bbb", dt)), Sets.newHashSet(result));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }

  // For some reason this test, which is similar to the test above, wants the deletion schema to be the subset
  // of equality field ids, but the one above it (which is the same but the fields are in a different order)
  // wants to use the full schema.
  //
  // Either way, the deletion manifest statistics are WRONG. =(
  @Test
  public void testUpsertEqualityFieldsAreSuperSetOfPartitionFieldsWithPartitionFieldAtBeginning() {
    Assume.assumeTrue("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    String tableName = "upsert_on_super_of_partition_ids_other_order";
    LocalDate dt = LocalDate.of(2022, 3, 1);
    try {
      sql("CREATE TABLE %s(data STRING NOT NULL, dt DATE NOT NULL, id INT, PRIMARY KEY(data,dt) NOT ENFORCED) " +
              "PARTITIONED BY (data) WITH %s",
          tableName, toWithClause(tableUpsertProps));

      sql("INSERT INTO %s VALUES " +
          "('aaa', TO_DATE('2022-03-01'), 1)," +
          "('aaa', TO_DATE('2022-03-01'), 2)," +
          "('bbb', TO_DATE('2022-03-01'), 3)",
          tableName);

      List<Row> result = sql("SELECT * FROM %s", tableName);

      // Assert.assertEquals("result should have 2 rows!", 2, result.size());
      Assert.assertEquals("result should have the correct rows",
          Sets.newHashSet(Row.of("aaa", dt, 2), Row.of("bbb", dt, 3)), Sets.newHashSet(result));

      sql("INSERT INTO %s VALUES " +
          "('aaa', TO_DATE('2022-03-01'), 4)," +
          "('bbb', TO_DATE('2022-03-01'), 5)",
          tableName);

      result = sql("SELECT * FROM %s", tableName);

      // Assert.assertEquals("result should have 2 rows!", 2, result.size());
      Assert.assertEquals("result should have the correct rows",
          Sets.newHashSet(Row.of("aaa", dt, 4), Row.of("bbb", dt, 5)), Sets.newHashSet(result));

      sql("INSERT INTO %s VALUES " +
          "('aaa', TO_DATE('2022-03-01'), 6)," +
          "('bbb', TO_DATE('2022-03-01'), 7)",
          tableName);

      result = sql("SELECT * FROM %s", tableName);

      // Assert.assertEquals("result should have 2 rows!", 2, result.size());
      Assert.assertEquals("result should have the correct rows",
          Sets.newHashSet(Row.of("aaa", dt, 6), Row.of("bbb", dt, 7)), Sets.newHashSet(result));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, tableName);
    }
  }
}
