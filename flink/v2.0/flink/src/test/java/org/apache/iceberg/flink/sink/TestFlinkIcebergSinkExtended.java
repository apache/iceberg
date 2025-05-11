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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests the more extended features of Flink sink. Extract them separately since it is
 * unnecessary to test all the parameters combinations in {@link TestFlinkIcebergSink}. Each test
 * method in {@link TestFlinkIcebergSink} runs 12 combinations, which are expensive and slow.
 */
public class TestFlinkIcebergSinkExtended extends TestFlinkIcebergSinkBase {
  private final boolean partitioned = true;
  private final int parallelism = 2;
  private final FileFormat format = FileFormat.PARQUET;

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

  @Test
  public void testTwoSinksInDisjointedDAG() throws Exception {
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());

    Table leftTable =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TableIdentifier.of("left"),
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                props);
    TableLoader leftTableLoader =
        TableLoader.fromCatalog(CATALOG_EXTENSION.catalogLoader(), TableIdentifier.of("left"));

    Table rightTable =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TableIdentifier.of("right"),
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                props);
    TableLoader rightTableLoader =
        TableLoader.fromCatalog(CATALOG_EXTENSION.catalogLoader(), TableIdentifier.of("right"));

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);
    env.getConfig().disableAutoGeneratedUIDs();

    List<Row> leftRows = createRows("left-");
    DataStream<Row> leftStream =
        env.fromCollection(leftRows, ROW_TYPE_INFO)
            .name("leftCustomSource")
            .uid("leftCustomSource");
    FlinkSink.forRow(leftStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(leftTable)
        .tableLoader(leftTableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .distributionMode(DistributionMode.NONE)
        .uidPrefix("leftIcebergSink")
        .append();

    List<Row> rightRows = createRows("right-");
    DataStream<Row> rightStream =
        env.fromCollection(rightRows, ROW_TYPE_INFO)
            .name("rightCustomSource")
            .uid("rightCustomSource");
    FlinkSink.forRow(rightStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(rightTable)
        .tableLoader(rightTableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(parallelism)
        .distributionMode(DistributionMode.HASH)
        .uidPrefix("rightIcebergSink")
        .setSnapshotProperty("flink.test", TestFlinkIcebergSink.class.getName())
        .setSnapshotProperties(Collections.singletonMap("direction", "rightTable"))
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(leftTable, convertToRowData(leftRows));
    SimpleDataUtil.assertTableRows(rightTable, convertToRowData(rightRows));

    leftTable.refresh();
    assertThat(leftTable.currentSnapshot().summary()).doesNotContainKeys("flink.test", "direction");
    rightTable.refresh();
    assertThat(rightTable.currentSnapshot().summary())
        .containsEntry("flink.test", TestFlinkIcebergSink.class.getName())
        .containsEntry("direction", "rightTable");
  }

  @Test
  public void testOverrideWriteConfigWithUnknownFileFormat() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.WRITE_FORMAT.key(), "UNRECOGNIZED");

    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    FlinkSink.Builder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .setAll(newProps);

    assertThatThrownBy(builder::append)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file format: UNRECOGNIZED");
  }

  @Test
  public void testWriteRowWithTableRefreshInterval() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    Configuration flinkConf = new Configuration();
    flinkConf.setString(FlinkWriteOptions.TABLE_REFRESH_INTERVAL.key(), "100ms");

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .flinkConf(flinkConf)
        .writeParallelism(parallelism)
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }
}
