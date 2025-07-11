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

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
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
import org.apache.iceberg.types.Types;
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

  @Parameter(index = 3)
  private boolean isTableSchema;

  @Parameters(name = "format={0}, parallelism={1}, partitioned={2}, isTableSchema={3}")
  public static Object[][] parameters() {
    return new Object[][] {
      // Remove after the deprecation of TableSchema - BEGIN
      {FileFormat.AVRO, 1, true, true},
      {FileFormat.AVRO, 1, false, true},
      {FileFormat.AVRO, 2, true, true},
      {FileFormat.AVRO, 2, false, true},
      {FileFormat.ORC, 1, true, true},
      {FileFormat.ORC, 1, false, true},
      {FileFormat.ORC, 2, true, true},
      {FileFormat.ORC, 2, false, true},
      {FileFormat.PARQUET, 1, true, true},
      {FileFormat.PARQUET, 1, false, true},
      {FileFormat.PARQUET, 2, true, true},
      {FileFormat.PARQUET, 2, false, true},
      // Remove after the deprecation of TableSchema - END

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

    if (isTableSchema) {
      IcebergSink.forRow(leftStream, SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .table(leftTable)
          .tableLoader(leftTableLoader)
          .tableSchema(SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .distributionMode(DistributionMode.NONE)
          .uidSuffix("leftIcebergSink")
          .append();
    } else {
      IcebergSink.forRow(leftStream, SimpleDataUtil.FLINK_SCHEMA)
          .table(leftTable)
          .tableLoader(leftTableLoader)
          .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
          .distributionMode(DistributionMode.NONE)
          .uidSuffix("leftIcebergSink")
          .append();
    }

    List<Row> rightRows = createRows("right-");
    DataStream<Row> rightStream =
        env.fromCollection(rightRows, ROW_TYPE_INFO)
            .name("rightCustomSource")
            .uid("rightCustomSource");

    if (isTableSchema) {
      IcebergSink.forRow(rightStream, SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .table(rightTable)
          .tableLoader(rightTableLoader)
          .tableSchema(SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .writeParallelism(parallelism)
          .distributionMode(DistributionMode.HASH)
          .uidSuffix("rightIcebergSink")
          .setSnapshotProperty("flink.test", TestIcebergSink.class.getName())
          .snapshotProperties(Collections.singletonMap("direction", "rightTable"))
          .append();
    } else {
      IcebergSink.forRow(rightStream, SimpleDataUtil.FLINK_SCHEMA)
          .table(rightTable)
          .tableLoader(rightTableLoader)
          .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
          .writeParallelism(parallelism)
          .distributionMode(DistributionMode.HASH)
          .uidSuffix("rightIcebergSink")
          .setSnapshotProperty("flink.test", TestIcebergSink.class.getName())
          .snapshotProperties(Collections.singletonMap("direction", "rightTable"))
          .append();
    }

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
  void testOverrideWriteConfigWithUnknownFileFormat() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.WRITE_FORMAT.key(), "UNRECOGNIZED");

    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    Builder builder =
        isTableSchema
            ? IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_TABLE_SCHEMA)
                .table(table)
                .tableLoader(tableLoader)
                .writeParallelism(parallelism)
                .setAll(newProps)
                .uidSuffix("ingestion")
            : IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
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
  void testOperatorsUidNameNoUidSuffix() {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO).uid("mySourceId");

    if (isTableSchema) {
      IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .tableSchema(SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .writeParallelism(parallelism)
          .distributionMode(DistributionMode.HASH)
          .append();
    } else {
      IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
          .writeParallelism(parallelism)
          .distributionMode(DistributionMode.HASH)
          .append();
    }

    Transformation firstTransformation = env.getTransformations().get(0);
    Transformation secondTransformation = env.getTransformations().get(1);
    assertThat(firstTransformation.getUid()).isEqualTo("Sink pre-writer mapper: hadoop.default.t");
    assertThat(firstTransformation.getName()).isEqualTo("Sink pre-writer mapper: hadoop.default.t");
    assertThat(secondTransformation.getUid()).isEqualTo("hadoop.default.t");
    assertThat(secondTransformation.getName()).isEqualTo("hadoop.default.t");
  }

  @TestTemplate
  void testOperatorsUidNameWitUidSuffix() {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO).uid("mySourceId");

    if (isTableSchema) {
      IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .tableSchema(SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .writeParallelism(parallelism)
          .distributionMode(DistributionMode.HASH)
          .uidSuffix("data-ingestion")
          .append();
    } else {
      IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
          .writeParallelism(parallelism)
          .distributionMode(DistributionMode.HASH)
          .uidSuffix("data-ingestion")
          .append();
    }

    Transformation firstTransformation = env.getTransformations().get(0);
    Transformation secondTransformation = env.getTransformations().get(1);
    assertThat(firstTransformation.getUid()).isEqualTo("Sink pre-writer mapper: data-ingestion");
    assertThat(firstTransformation.getName()).isEqualTo("Sink pre-writer mapper: data-ingestion");
    assertThat(secondTransformation.getUid()).isEqualTo("data-ingestion");
    assertThat(secondTransformation.getName()).isEqualTo("data-ingestion");
  }

  @TestTemplate
  void testErrorOnNullForRequiredField() {
    assumeThat(format)
        .as("ORC file format supports null values even for required fields.")
        .isNotEqualTo(FileFormat.ORC);

    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id2", Types.IntegerType.get()),
            Types.NestedField.required(2, "data2", Types.StringType.get()));
    TableIdentifier tableIdentifier = TableIdentifier.of(DATABASE, "t2");
    Table table2 =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                tableIdentifier,
                icebergSchema,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()));

    // Null out a required field
    List<Row> rows = List.of(Row.of(42, null));

    env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Row> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO).uid("mySourceId");

    if (isTableSchema) {
      TableSchema flinkSchema = FlinkSchemaUtil.toSchema(icebergSchema);
      IcebergSink.forRow(dataStream, flinkSchema)
          .table(table2)
          .tableLoader(TableLoader.fromCatalog(CATALOG_EXTENSION.catalogLoader(), tableIdentifier))
          .tableSchema(flinkSchema)
          .writeParallelism(parallelism)
          .append();
    } else {
      ResolvedSchema flinkSchema = FlinkSchemaUtil.toResolvedSchema(icebergSchema);
      IcebergSink.forRow(dataStream, flinkSchema)
          .table(table2)
          .tableLoader(TableLoader.fromCatalog(CATALOG_EXTENSION.catalogLoader(), tableIdentifier))
          .resolvedSchema(flinkSchema)
          .writeParallelism(parallelism)
          .append();
    }

    assertThatThrownBy(() -> env.execute()).hasRootCauseInstanceOf(NullPointerException.class);
  }

  @TestTemplate
  void testDefaultWriteParallelism() {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO).uid("mySourceId");

    var sink =
        isTableSchema
            ? IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_TABLE_SCHEMA)
                .table(table)
                .tableLoader(tableLoader)
                .tableSchema(SimpleDataUtil.FLINK_TABLE_SCHEMA)
                .distributionMode(DistributionMode.NONE)
                .append()
            : IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
                .table(table)
                .tableLoader(tableLoader)
                .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
                .distributionMode(DistributionMode.NONE)
                .append();

    // since the sink write parallelism was null, it asserts that the default parallelism used was
    // the input source parallelism.
    // sink.getTransformation is referring to the SinkV2 Writer Operator associated to the
    // IcebergSink
    assertThat(sink.getTransformation().getParallelism()).isEqualTo(dataStream.getParallelism());
  }

  @TestTemplate
  void testWriteParallelism() {
    List<Row> rows = createRows("");

    // the parallelism of this input source is always 1, as this is a non-parallel source.
    DataStream<Row> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO).uid("mySourceId");

    var sink =
        isTableSchema
            ? IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_TABLE_SCHEMA)
                .table(table)
                .tableLoader(tableLoader)
                .tableSchema(SimpleDataUtil.FLINK_TABLE_SCHEMA)
                .distributionMode(DistributionMode.NONE)
                .writeParallelism(parallelism)
                .append()
            : IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
                .table(table)
                .tableLoader(tableLoader)
                .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
                .distributionMode(DistributionMode.NONE)
                .writeParallelism(parallelism)
                .append();

    // The parallelism has been properly specified when creating the IcebergSink, so this asserts
    // that its value is the same as the parallelism TestTemplate parameter
    // sink.getTransformation is referring to the SinkV2 Writer Operator associated to the
    // IcebergSink
    assertThat(sink.getTransformation().getParallelism()).isEqualTo(parallelism);
  }

  private void testWriteRow(ResolvedSchema resolvedSchema, DistributionMode distributionMode)
      throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO).uid("mySourceId");

    if (isTableSchema) {
      IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_TABLE_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .tableSchema(
              resolvedSchema == null ? null : TableSchema.fromResolvedSchema(resolvedSchema))
          .writeParallelism(parallelism)
          .distributionMode(distributionMode)
          .append();
    } else {
      IcebergSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
          .table(table)
          .tableLoader(tableLoader)
          .resolvedSchema(resolvedSchema)
          .writeParallelism(parallelism)
          .distributionMode(distributionMode)
          .append();
    }

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }
}
