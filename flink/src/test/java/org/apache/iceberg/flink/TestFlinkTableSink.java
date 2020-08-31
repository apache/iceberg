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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.flink.table.api.Expressions.$;

@RunWith(Parameterized.class)
public class TestFlinkTableSink extends AbstractTestBase {
  private static final Configuration CONF = new Configuration();
  private static final DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
      SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

  private static final String TABLE_NAME = "flink_table";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private String tablePath;
  private String warehouse;
  private Map<String, String> properties;
  private Catalog catalog;
  private StreamExecutionEnvironment env;
  private StreamTableEnvironment tEnv;

  private final FileFormat format;
  private final int parallelism;

  @Parameterized.Parameters(name = "{index}: format={0}, parallelism={2}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[] {"avro", 1},
        new Object[] {"avro", 2},
        new Object[] {"orc", 1},
        new Object[] {"orc", 2},
        new Object[] {"parquet", 1},
        new Object[] {"parquet", 2}
    );
  }

  public TestFlinkTableSink(String format, int parallelism) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.parallelism = parallelism;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    warehouse = folder.getAbsolutePath();

    tablePath = warehouse.concat("/default/").concat(TABLE_NAME);
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdirs());

    properties = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    catalog = new HadoopCatalog(CONF, warehouse);

    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(400);
    env.setParallelism(parallelism);

    EnvironmentSettings settings = EnvironmentSettings
        .newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build();
    tEnv = StreamTableEnvironment.create(env, settings);
    tEnv.executeSql(String.format("create catalog iceberg_catalog with (" +
        "'type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", warehouse));
    tEnv.executeSql("use catalog iceberg_catalog");
    tEnv.getConfig().getConfiguration().set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);

    catalog.createTable(TableIdentifier.parse("default." + TABLE_NAME),
        SimpleDataUtil.SCHEMA,
        PartitionSpec.unpartitioned(),
        properties);
  }

  private DataStream<RowData> generateInputStream(List<Row> rows) {
    TypeInformation<Row> typeInformation = new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());
    return env.addSource(new FiniteTestSource<>(rows), typeInformation)
        .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));
  }

  private Pair<List<Row>, List<Record>> generateData() {
    String[] worlds = new String[] {"hello", "world", "foo", "bar", "apache", "foundation"};
    List<Row> rows = Lists.newArrayList();
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < worlds.length; i++) {
      rows.add(Row.of(i + 1, worlds[i]));
      Record record = SimpleDataUtil.createRecord(i + 1, worlds[i]);
      expected.add(record);
      expected.add(record);
    }
    return Pair.of(rows, expected);
  }

  @Test
  public void testStreamSQL() throws Exception {
    Pair<List<Row>, List<Record>> data = generateData();
    List<Row> rows = data.first();
    List<Record> expected = data.second();
    DataStream<RowData> stream = generateInputStream(rows);

    // Register the rows into a temporary table named 'sourceTable'.
    tEnv.createTemporaryView("sourceTable", tEnv.fromDataStream(stream, $("id"), $("data")));

    // Redirect the records from source table to destination table.
    String insertSQL = String.format("INSERT INTO %s SELECT id,data from sourceTable", TABLE_NAME);
    TableResult result = tEnv.executeSql(insertSQL);
    waitComplete(result);

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(tablePath, expected);
  }

  @Test
  public void testBatchSQL() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings
        .newInstance()
        .inBatchMode()
        .useBlinkPlanner()
        .build();
    TableEnvironment batchEnv = TableEnvironment.create(settings);
    batchEnv.executeSql(String.format("create catalog batch_catalog with (" +
        "'type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", warehouse));
    batchEnv.executeSql("use catalog batch_catalog");
    batchEnv.getConfig().getConfiguration().set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);

    // Create source table.
    catalog.createTable(TableIdentifier.parse("default.sourceTable"),
        SimpleDataUtil.SCHEMA,
        PartitionSpec.unpartitioned(),
        properties);

    TableResult result;
    String[] words = new String[] {"hello", "world", "apache"};
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < words.length; i++) {
      expected.add(SimpleDataUtil.createRecord(i, words[i]));
      result = batchEnv.executeSql(String.format("INSERT INTO sourceTable SELECT %d, '%s'", i, words[i]));
      waitComplete(result);
    }

    // Assert the table records as expected.
    SimpleDataUtil.assertTableRecords(warehouse.concat("/default/sourceTable"), expected);
  }

  private static void waitComplete(TableResult result) {
    result.getJobClient().ifPresent(jobClient -> {
      try {
        jobClient.getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
