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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkTableSink extends AbstractTestBase {
  private static final Configuration CONF = new Configuration();

  private static final String TABLE_NAME = "flink_table";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String tablePath;
  private TableEnvironment tEnv;

  private final FileFormat format;
  private final int parallelism;
  private final boolean isStreamingJob;

  @Parameterized.Parameters(name = "{index}: format={0}, parallelism={2}, isStreamingJob={3}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[] {"avro", 1, false},
        new Object[] {"avro", 1, true},
        new Object[] {"avro", 2, false},
        new Object[] {"avro", 2, true},
        new Object[] {"orc", 1, false},
        new Object[] {"orc", 1, true},
        new Object[] {"orc", 2, false},
        new Object[] {"orc", 2, true},
        new Object[] {"parquet", 1, false},
        new Object[] {"parquet", 1, true},
        new Object[] {"parquet", 2, false},
        new Object[] {"parquet", 2, true}
    );
  }

  public TestFlinkTableSink(String format, int parallelism, boolean isStreamingJob) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.parallelism = parallelism;
    this.isStreamingJob = isStreamingJob;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    String warehouse = folder.getAbsolutePath();

    tablePath = warehouse.concat("/default/").concat(TABLE_NAME);
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdirs());

    Map<String, String> properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    Catalog catalog = new HadoopCatalog(CONF, warehouse);

    EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
        .newInstance()
        .inBatchMode()
        .useBlinkPlanner();

    if (isStreamingJob) {
      settingsBuilder.inStreamingMode();
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
      env.enableCheckpointing(400);
      env.setParallelism(parallelism);
      tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
    } else {
      settingsBuilder.inBatchMode();
      tEnv = TableEnvironment.create(settingsBuilder.build());
    }

    tEnv.executeSql(String.format("create catalog iceberg_catalog with (" +
        "'type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", warehouse));
    tEnv.executeSql("use catalog iceberg_catalog");

    catalog.createTable(TableIdentifier.parse("default." + TABLE_NAME),
        SimpleDataUtil.SCHEMA,
        PartitionSpec.unpartitioned(),
        properties);
  }

  @Test
  public void testStreamSQL() throws Exception {
    List<RowData> expected = Lists.newArrayList(
        SimpleDataUtil.createRowData(1, "hello"),
        SimpleDataUtil.createRowData(2, "world"),
        SimpleDataUtil.createRowData(3, "foo"),
        SimpleDataUtil.createRowData(4, "bar")
    );

    // Register the rows into a temporary table.
    Table sourceTable = tEnv.fromValues(SimpleDataUtil.FLINK_SCHEMA.toRowDataType(),
        Expressions.row(1, "hello"),
        Expressions.row(2, "world"),
        Expressions.row(3, "foo"),
        Expressions.row(4, "bar")
    );
    tEnv.createTemporaryView("sourceTable", sourceTable);

    // Redirect the records from source table to destination table.
    String insertSQL = String.format("INSERT INTO %s SELECT id,data from sourceTable", TABLE_NAME);
    executeSQLAndWaitResult(tEnv, insertSQL);

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRows(tablePath, expected);
  }

  @Test
  public void testOverwriteTable() throws Exception {
    Assume.assumeFalse("Flink unbounded streaming does not support overwrite operation", isStreamingJob);

    executeSQLAndWaitResult(tEnv, String.format("INSERT INTO %s SELECT 1, 'hello'", TABLE_NAME));
    SimpleDataUtil.assertTableRows(tablePath, Lists.newArrayList(SimpleDataUtil.createRowData(1, "hello")));

    executeSQLAndWaitResult(tEnv, String.format("INSERT OVERWRITE %s SELECT 2, 'world'", TABLE_NAME));
    SimpleDataUtil.assertTableRows(tablePath, Lists.newArrayList(SimpleDataUtil.createRowData(2, "world")));
    org.apache.iceberg.Table table = new HadoopTables().load(tablePath);
    Assert.assertEquals("overwrite", table.currentSnapshot().operation());
  }

  private static void executeSQLAndWaitResult(TableEnvironment tEnv, String statement) {
    tEnv.executeSql(statement).getJobClient().ifPresent(jobClient -> {
      try {
        jobClient.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
