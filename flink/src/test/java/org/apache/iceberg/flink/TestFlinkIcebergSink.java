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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSink extends AbstractTestBase {
  private static final Configuration CONF = new Configuration();
  private static final DataFormatConverters.RowConverter CONVERTER = new DataFormatConverters.RowConverter(
      SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String warehouse;
  private String tablePath;
  private Table table;

  private final FileFormat format;
  private final int parallelism;
  private final boolean partitioned;

  @Parameterized.Parameters(name = "parallelism = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"avro", 1, true},
        new Object[] {"avro", 1, false},
        new Object[] {"avro", 2, true},
        new Object[] {"avro", 2, false},
    };
  }

  public TestFlinkIcebergSink(String format, int parallelism, boolean partitioned) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.parallelism = parallelism;
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    warehouse = folder.getAbsolutePath();

    tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdir());

    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(tablePath, props, partitioned);
  }

  private List<RowData> convertToRowData(List<Row> rows) {
    return rows.stream().map(CONVERTER::toInternal).collect(Collectors.toList());
  }

  private void writeWithStreamJob(StreamExecutionEnvironment env, List<Row> rows) throws Exception {
    TypeInformation<Row> typeInformation = new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());
    DataStream<RowData> dataStream = env.addSource(new FiniteTestSource<>(rows), typeInformation)
        .map(CONVERTER::toInternal, RowDataTypeInfo.of(SimpleDataUtil.ROW_TYPE));

    // Output the data stream to stdout.
    Map<String, String> options = ImmutableMap.of(
        FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop",
        FlinkCatalogFactory.HADOOP_WAREHOUSE_LOCATION, warehouse
    );

    IcebergSinkUtil.builder()
        .inputStream(dataStream)
        .config(CONF)
        .options(options)
        .fullTableName("test")
        .table(table)
        .flinkSchema(SimpleDataUtil.FLINK_SCHEMA)
        .build();

    // Execute the program.
    env.execute("Test Iceberg DataStream");
  }

  @Test
  public void testDataStream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Enable the checkpoint.
    env.enableCheckpointing(100);
    env.setParallelism(parallelism);

    List<Row> rows1 = Lists.newArrayList(
        Row.of(1, "hello"),
        Row.of(2, "world"),
        Row.of(3, "foo")
    );
    // Summit the first flink stream job to write those rows.
    writeWithStreamJob(env, rows1);

    // Assert the iceberg table's records. NOTICE: the FiniteTestSource will checkpoint the same rows twice, so it will
    // commit the same row list into iceberg twice.
    List<RowData> expectedRows = Lists.newArrayList(Iterables.concat(convertToRowData(rows1), convertToRowData(rows1)));
    SimpleDataUtil.assertTableRows(tablePath, expectedRows);

    List<Row> rows2 = Lists.newArrayList(
        Row.of(4, "bar"),
        Row.of(5, "apache")
    );
    // Submit the second flink stream job to write those rows.
    writeWithStreamJob(env, rows2);
    expectedRows.addAll(convertToRowData(rows2));
    expectedRows.addAll(convertToRowData(rows2));
    SimpleDataUtil.assertTableRows(tablePath, expectedRows);
  }
}
