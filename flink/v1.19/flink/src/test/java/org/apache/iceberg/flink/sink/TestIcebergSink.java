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
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.IcebergSink.Builder;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIcebergSink extends TestFlinkIcebergSinkBase {

  private TableLoader tableLoader;

  @Parameter(index = 0)
  private FileFormat format;

  @Parameter(index = 1)
  private int parallelism;

  @Parameter(index = 2)
  private boolean partitioned;

  @Parameters(name = "format={0}, parallelism={1}, partitioned={2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {FileFormat.AVRO, 1, true},
      {FileFormat.AVRO, 1, false},
      {FileFormat.AVRO, 2, true},
      {FileFormat.AVRO, 2, false},
      {FileFormat.ORC, 1, true},
      {FileFormat.ORC, 1, false},
      {FileFormat.ORC, 2, true},
      {FileFormat.ORC, 2, false},
      {FileFormat.PARQUET, 1, true},
      {FileFormat.PARQUET, 1, false},
      {FileFormat.PARQUET, 2, true},
      {FileFormat.PARQUET, 2, false}
    };
  }

  @BeforeEach
  void before() throws IOException {
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()));

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);

    tableLoader = CATALOG_EXTENSION.tableLoader();
  }

  @TestTemplate
  void testWriteRowData() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    IcebergSink.forRowData(dataStream)
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
  void testWriteRow() throws Exception {
    testWriteRow(null, DistributionMode.NONE);
  }

  @TestTemplate
  void testWriteRowWithTableSchema() throws Exception {
    testWriteRow(SimpleDataUtil.FLINK_SCHEMA, DistributionMode.NONE);
  }

  @TestTemplate
  void testJobNoneDistributeMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(null, DistributionMode.NONE);

    if (parallelism > 1) {
      if (partitioned) {
        int files = partitionFiles("aaa") + partitionFiles("bbb") + partitionFiles("ccc");
        assertThat(files).as("Should have more than 3 files in iceberg table.").isGreaterThan(3);
      }
    }
  }

  @TestTemplate
  void testJobHashDistributionMode() {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    assertThatThrownBy(() -> testWriteRow(null, DistributionMode.RANGE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Flink does not support 'range' write distribution mode now.");
  }

  @TestTemplate
  void testJobNullDistributionMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(null, null);

    if (partitioned) {
      assertThat(partitionFiles("aaa"))
          .as("There should be only 1 data file in partition 'aaa'")
          .isEqualTo(1);
      assertThat(partitionFiles("bbb"))
          .as("There should be only 1 data file in partition 'bbb'")
          .isEqualTo(1);
      assertThat(partitionFiles("ccc"))
          .as("There should be only 1 data file in partition 'ccc'")
          .isEqualTo(1);
    }
  }

  @TestTemplate
  void testPartitionWriteMode() throws Exception {
    testWriteRow(null, DistributionMode.HASH);
    if (partitioned) {
      assertThat(partitionFiles("aaa"))
          .as("There should be only 1 data file in partition 'aaa'")
          .isEqualTo(1);
      assertThat(partitionFiles("bbb"))
          .as("There should be only 1 data file in partition 'bbb'")
          .isEqualTo(1);
      assertThat(partitionFiles("ccc"))
          .as("There should be only 1 data file in partition 'ccc'")
          .isEqualTo(1);
    }
  }

  @TestTemplate
  void testShuffleByPartitionWithSchema() throws Exception {
    testWriteRow(SimpleDataUtil.FLINK_SCHEMA, DistributionMode.HASH);
    if (partitioned) {
      assertThat(partitionFiles("aaa"))
          .as("There should be only 1 data file in partition 'aaa'")
          .isEqualTo(1);
      assertThat(partitionFiles("bbb"))
          .as("There should be only 1 data file in partition 'bbb'")
          .isEqualTo(1);
      assertThat(partitionFiles("ccc"))
          .as("There should be only 1 data file in partition 'ccc'")
          .isEqualTo(1);
    }
  }

  @TestTemplate
  void testTwoSinksInDisjointedDAG() throws Exception {
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
    IcebergSink.forRow(leftStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(leftTable)
        .tableLoader(leftTableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .distributionMode(DistributionMode.NONE)
        .uidSuffix("leftIcebergSink")
        .append();

    List<Row> rightRows = createRows("right-");
    DataStream<Row> rightStream =
        env.fromCollection(rightRows, ROW_TYPE_INFO)
            .name("rightCustomSource")
            .uid("rightCustomSource");
    IcebergSink.forRow(rightStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(rightTable)
        .tableLoader(rightTableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(parallelism)
        .distributionMode(DistributionMode.HASH)
        .uidSuffix("rightIcebergSink")
        .setSnapshotProperty("flink.test", TestIcebergSink.class.getName())
        .snapshotProperties(Collections.singletonMap("direction", "rightTable"))
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(leftTable, convertToRowData(leftRows));
    SimpleDataUtil.assertTableRows(rightTable, convertToRowData(rightRows));

    leftTable.refresh();

    assertThat(leftTable.currentSnapshot().summary().get("flink.test")).isNull();
    assertThat(leftTable.currentSnapshot().summary().get("direction")).isNull();

    assertThat(rightTable.currentSnapshot().summary().get("flink.test"))
        .isEqualTo(TestIcebergSink.class.getName());
    assertThat(rightTable.currentSnapshot().summary().get("direction")).isEqualTo("rightTable");
  }

  @TestTemplate
  void testOverrideWriteConfigWithUnknownDistributionMode() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.DISTRIBUTION_MODE.key(), "UNRECOGNIZED");

    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);
    IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .setAll(newProps)
        .append();

    assertThatThrownBy(() -> env.execute("Test Iceberg DataStream"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid distribution mode: UNRECOGNIZED");
  }

  @TestTemplate
  void testOverrideWriteConfigWithUnknownFileFormat() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.WRITE_FORMAT.key(), "UNRECOGNIZED");

    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    Builder builder =
        IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .setAll(newProps)
            .uidSuffix("ingestion");
    assertThatThrownBy(builder::append)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file format: UNRECOGNIZED");
  }

  @TestTemplate
  void testWriteRowWithTableRefreshInterval() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    Configuration flinkConf = new Configuration();
    flinkConf.setString(FlinkWriteOptions.TABLE_REFRESH_INTERVAL.key(), "100ms");

    IcebergSink.forRowData(dataStream)
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

  @TestTemplate
  void testOperatorsUidNameNoUidSuffix() throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO).uid("mySourceId");

    IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(parallelism)
        .distributionMode(DistributionMode.HASH)
        .append();

    Transformation firstTransformation = env.getTransformations().get(0);
    Transformation secondTransformation = env.getTransformations().get(1);
    assertThat(firstTransformation.getUid()).isEqualTo("Sink pre-writer mapper: hadoop.default.t");
    assertThat(firstTransformation.getName()).isEqualTo("Sink pre-writer mapper: hadoop.default.t");
    assertThat(secondTransformation.getUid()).isEqualTo("hadoop.default.t");
    assertThat(secondTransformation.getName()).isEqualTo("hadoop.default.t");
  }

  @TestTemplate
  void testOperatorsUidNameWitUidSuffix() throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO).uid("mySourceId");

    IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(parallelism)
        .distributionMode(DistributionMode.HASH)
        .uidSuffix("data-ingestion")
        .append();

    Transformation firstTransformation = env.getTransformations().get(0);
    Transformation secondTransformation = env.getTransformations().get(1);
    assertThat(firstTransformation.getUid()).isEqualTo("Sink pre-writer mapper: data-ingestion");
    assertThat(firstTransformation.getName()).isEqualTo("Sink pre-writer mapper: data-ingestion");
    assertThat(secondTransformation.getUid()).isEqualTo("data-ingestion");
    assertThat(secondTransformation.getName()).isEqualTo("data-ingestion");
  }

  private void testWriteRow(TableSchema tableSchema, DistributionMode distributionMode)
      throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO).uid("mySourceId");

    IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .tableSchema(tableSchema)
        .writeParallelism(parallelism)
        .distributionMode(distributionMode)
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }
}
