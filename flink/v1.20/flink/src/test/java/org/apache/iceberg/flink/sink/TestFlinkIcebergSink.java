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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkIcebergSink extends TestFlinkIcebergSinkBase {
  @Parameter(index = 0)
  private FileFormat format;

  @Parameter(index = 1)
  private int parallelism;

  @Parameter(index = 2)
  private boolean partitioned;

  @Parameter(index = 3)
  private boolean useV2Sink;

  @Parameters(name = "format={0}, parallelism = {1}, partitioned = {2}, useV2Sink = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {FileFormat.AVRO, 1, true, false},
      {FileFormat.AVRO, 1, false, false},
      {FileFormat.AVRO, 2, true, false},
      {FileFormat.AVRO, 2, false, false},
      {FileFormat.ORC, 1, true, false},
      {FileFormat.ORC, 1, false, false},
      {FileFormat.ORC, 2, true, false},
      {FileFormat.ORC, 2, false, false},
      {FileFormat.PARQUET, 1, true, false},
      {FileFormat.PARQUET, 1, false, false},
      {FileFormat.PARQUET, 2, true, false},
      {FileFormat.PARQUET, 2, false, false},
      {FileFormat.AVRO, 2, true, true},
      {FileFormat.ORC, 2, true, true},
      {FileFormat.PARQUET, 1, true, true},
      {FileFormat.PARQUET, 1, false, true},
      {FileFormat.PARQUET, 2, true, true},
      {FileFormat.PARQUET, 2, false, true}
    };
  }

  @BeforeEach
  public void before() throws IOException {
    this.table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()));

    this.env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);

    this.tableLoader = CATALOG_EXTENSION.tableLoader();
  }

  @TestTemplate
  public void testWriteRowData() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    IcebergSinkBuilder.forRowData(dataStream, useV2Sink)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }

  @TestTemplate
  public void testWriteRow() throws Exception {
    testWriteRow(parallelism, null, DistributionMode.NONE, useV2Sink);
  }

  @TestTemplate
  public void testWriteRowWithTableSchema() throws Exception {
    testWriteRow(parallelism, SimpleDataUtil.FLINK_SCHEMA, DistributionMode.NONE, useV2Sink);
  }
}
