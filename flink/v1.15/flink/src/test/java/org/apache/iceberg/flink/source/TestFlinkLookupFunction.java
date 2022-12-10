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
package org.apache.iceberg.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkLookupFunction {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());
  private static final DataFormatConverters.RowConverter CONVERTER =
      new DataFormatConverters.RowConverter(SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

  private Table table;
  private StreamExecutionEnvironment env;
  private TableLoader tableLoader;

  private final FileFormat format;
  private final int parallelism;
  private final boolean partitioned;
  private FlinkLookupFunction flinkLookupFunction;

  @Parameterized.Parameters(name = "format={0}, parallelism = {1}, partitioned = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"avro", 1, true},
      {"avro", 1, false},
      {"avro", 2, true},
      {"avro", 2, false},
      {"orc", 1, true},
      {"orc", 1, false},
      {"orc", 2, true},
      {"orc", 2, false},
      {"parquet", 1, true},
      {"parquet", 1, false},
      {"parquet", 2, true},
      {"parquet", 2, false}
    };
  }

  public TestFlinkLookupFunction(String format, int parallelism, boolean partitioned) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.parallelism = parallelism;
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    File folder = TEMPORARY_FOLDER.newFolder();
    String warehouse = folder.getAbsolutePath();

    String tablePath = warehouse.concat("/test");

    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdir());

    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(tablePath, props, partitioned);

    env =
        StreamExecutionEnvironment.getExecutionEnvironment()
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);

    tableLoader = TableLoader.fromHadoopTable(tablePath);
    tableLoader.open();
  }

  private BoundedTestSource<Row> createBoundedSource(List<Row> rows) {
    return new BoundedTestSource<>(rows.toArray(new Row[0]));
  }

  private void initFlinkLookupFunction(Map<String, String> properties) {
    String[] lookupKeys = {"id"};
    flinkLookupFunction =
        new FlinkLookupFunction(
            SimpleDataUtil.FLINK_SCHEMA, lookupKeys, tableLoader, properties, -1L, null);
  }

  @Test
  public void testFlinkLookupFunctionWithNoCache() throws Exception {
    Map<String, String> properties = Maps.newHashMap();
    Integer[] key = new Integer[] {1};
    initFlinkLookupFunction(properties);
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));

    RowData expectedRowData = SimpleDataUtil.createRowData(1, "hello");

    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .append();

    env.execute("Test Iceberg DataStream");

    flinkLookupFunction.open(null);
    flinkLookupFunction.setCollector(
        new Collector<RowData>() {
          @Override
          public void collect(RowData record) {
            Assert.assertEquals(
                "Should join the table data by key correctly. "
                    + "The joined 'id' value should be '1'",
                expectedRowData.getInt(0),
                record.getInt(0));
            Assert.assertEquals(
                "Should join the table data by key correctly. "
                    + "The joined 'data' value should be 'hello'\"",
                expectedRowData.getString(1),
                record.getString(1));
          }

          @Override
          public void close() {}
        });
    flinkLookupFunction.eval(key);
  }

  @Test
  public void testFlinkLookupFunctionWithCache() throws Exception {
    Integer[] key = new Integer[] {1};
    Map<String, String> properties = Maps.newHashMap();
    properties.put("lookup-join-cache-size", "3");
    properties.put("lookup-join-cache-ttl", "10000");
    initFlinkLookupFunction(properties);
    List<Row> rows1 = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));

    List<Row> rows2 =
        Lists.newArrayList(Row.of(1, "hello2"), Row.of(2, "world2"), Row.of(3, "foo2"));

    RowData expectedRowData = SimpleDataUtil.createRowData(1, "hello");
    insertTableValues(rows1);

    flinkLookupFunction.open(null);
    flinkLookupFunction.setCollector(
        new Collector<RowData>() {
          @Override
          public void collect(RowData record) {
            Assert.assertEquals(
                "Should join the table data by key correctly. "
                    + "The joined 'id' value should be '1'",
                expectedRowData.getInt(0),
                record.getInt(0));
            Assert.assertEquals(
                "Should join the table data by key correctly. "
                    + "The joined 'data' value should be 'hello'\"",
                expectedRowData.getString(1),
                record.getString(1));
          }

          @Override
          public void close() {}
        });
    flinkLookupFunction.eval(key);

    // Insert into table values "rows2", verify that the function of "cache" is in effect
    insertTableValues(rows2);
    flinkLookupFunction.eval(key);

    Thread.sleep(12000);
    List<RowData> arrayList = Lists.newArrayList();
    flinkLookupFunction.setCollector(
        new Collector<RowData>() {
          @Override
          public void collect(RowData record) {
            arrayList.add(record);
          }

          @Override
          public void close() {}
        });

    flinkLookupFunction.eval(key);
    Assert.assertEquals("Collect data size should be 2", 2, arrayList.size());
  }

  private void insertTableValues(List<Row> rows) throws Exception {
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .append();

    env.execute("Test Iceberg DataStream");
  }
}
