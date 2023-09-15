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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogResource;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSink extends TestFlinkIcebergSinkBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopCatalogResource catalogResource =
      new HadoopCatalogResource(TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE);

  private TableLoader tableLoader;

  private final FileFormat format;
  private final int parallelism;
  private final boolean partitioned;
  private final boolean newSink;

  @Parameterized.Parameters(
      name = "format={0}, parallelism = {1}, partitioned = {2}, newSink = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"avro", 1, true, false},
      {"avro", 1, false, false},
      {"avro", 2, true, false},
      {"avro", 2, false, false},
      {"orc", 1, true, false},
      {"orc", 1, false, false},
      {"orc", 2, true, false},
      {"orc", 2, false, false},
      {"parquet", 1, true, false},
      {"parquet", 1, false, false},
      {"parquet", 2, true, false},
      {"parquet", 2, false, false},
      {"avro", 1, true, true},
      {"avro", 1, false, true},
      {"avro", 2, true, true},
      {"avro", 2, false, true},
      {"orc", 1, true, true},
      {"orc", 1, false, true},
      {"orc", 2, true, true},
      {"orc", 2, false, true},
      {"parquet", 1, true, true},
      {"parquet", 1, false, true},
      {"parquet", 2, true, true},
      {"parquet", 2, false, true}
    };
  }

  public TestFlinkIcebergSink(
      String format, int parallelism, boolean partitioned, boolean newSink) {
    this.format = FileFormat.fromString(format);
    this.parallelism = parallelism;
    this.partitioned = partitioned;
    this.newSink = newSink;
  }

  @Before
  public void before() throws IOException {
    table =
        catalogResource
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
                MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);

    tableLoader = catalogResource.tableLoader();
  }

  @Test
  public void testWriteRowData() throws Exception {
    List<Row> rows = Lists.newArrayList(Row.of(1, "hello"), Row.of(2, "world"), Row.of(3, "foo"));
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(rows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    getSink(newSink, dataStream, table, tableLoader, parallelism, null);

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }

  private void testWriteRow(TableSchema tableSchema, DistributionMode distributionMode)
      throws Exception {
    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    getSink(
        newSink,
        dataStream,
        SimpleDataUtil.FLINK_SCHEMA,
        table,
        tableLoader,
        tableSchema,
        parallelism,
        distributionMode,
        null,
        null,
        null,
        null);

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(table, convertToRowData(rows));
  }

  private int partitionFiles(String partition) throws IOException {
    return SimpleDataUtil.partitionDataFiles(table, ImmutableMap.of("data", partition)).size();
  }

  @Test
  public void testWriteRow() throws Exception {
    testWriteRow(null, DistributionMode.NONE);
  }

  @Test
  public void testWriteRowWithTableSchema() throws Exception {
    testWriteRow(SimpleDataUtil.FLINK_SCHEMA, DistributionMode.NONE);
  }

  @Test
  public void testJobNoneDistributeMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(null, DistributionMode.NONE);

    if (parallelism > 1) {
      if (partitioned) {
        int files = partitionFiles("aaa") + partitionFiles("bbb") + partitionFiles("ccc");
        Assert.assertTrue("Should have more than 3 files in iceberg table.", files > 3);
      }
    }
  }

  @Test
  public void testJobHashDistributionMode() {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    Assertions.assertThatThrownBy(() -> testWriteRow(null, DistributionMode.RANGE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Flink does not support 'range' write distribution mode now.");
  }

  @Test
  public void testJobNullDistributionMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(null, null);

    if (partitioned) {
      Assert.assertEquals(
          "There should be only 1 data file in partition 'aaa'", 1, partitionFiles("aaa"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'bbb'", 1, partitionFiles("bbb"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'ccc'", 1, partitionFiles("ccc"));
    }
  }

  @Test
  public void testPartitionWriteMode() throws Exception {
    testWriteRow(null, DistributionMode.HASH);
    if (partitioned) {
      Assert.assertEquals(
          "There should be only 1 data file in partition 'aaa'", 1, partitionFiles("aaa"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'bbb'", 1, partitionFiles("bbb"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'ccc'", 1, partitionFiles("ccc"));
    }
  }

  @Test
  public void testShuffleByPartitionWithSchema() throws Exception {
    testWriteRow(SimpleDataUtil.FLINK_SCHEMA, DistributionMode.HASH);
    if (partitioned) {
      Assert.assertEquals(
          "There should be only 1 data file in partition 'aaa'", 1, partitionFiles("aaa"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'bbb'", 1, partitionFiles("bbb"));
      Assert.assertEquals(
          "There should be only 1 data file in partition 'ccc'", 1, partitionFiles("ccc"));
    }
  }

  @Test
  public void testTwoSinksInDisjointedDAG() throws Exception {
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());

    Table leftTable =
        catalogResource
            .catalog()
            .createTable(
                TableIdentifier.of("left"),
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                props);
    TableLoader leftTableLoader =
        TableLoader.fromCatalog(catalogResource.catalogLoader(), TableIdentifier.of("left"));

    Table rightTable =
        catalogResource
            .catalog()
            .createTable(
                TableIdentifier.of("right"),
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                props);
    TableLoader rightTableLoader =
        TableLoader.fromCatalog(catalogResource.catalogLoader(), TableIdentifier.of("right"));

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);
    env.getConfig().disableAutoGeneratedUIDs();

    List<Row> leftRows = createRows("left-");
    DataStream<Row> leftStream =
        env.fromCollection(leftRows, ROW_TYPE_INFO)
            .name("leftCustomSource")
            .uid("leftCustomSource");
    getSink(
        newSink,
        leftStream,
        SimpleDataUtil.FLINK_SCHEMA,
        leftTable,
        leftTableLoader,
        SimpleDataUtil.FLINK_SCHEMA,
        null,
        DistributionMode.NONE,
        "leftIcebergSink",
        null,
        null,
        null);

    List<Row> rightRows = createRows("right-");
    DataStream<Row> rightStream =
        env.fromCollection(rightRows, ROW_TYPE_INFO)
            .name("rightCustomSource")
            .uid("rightCustomSource");

    getSink(
        newSink,
        rightStream,
        SimpleDataUtil.FLINK_SCHEMA,
        rightTable,
        rightTableLoader,
        SimpleDataUtil.FLINK_SCHEMA,
        parallelism,
        DistributionMode.HASH,
        "rightIcebergSink",
        "flink.test",
        TestFlinkIcebergSink.class.getName(),
        Collections.singletonMap("direction", "rightTable"));

    // Execute the program.
    env.execute("Test Iceberg DataStream.");

    SimpleDataUtil.assertTableRows(leftTable, convertToRowData(leftRows));
    SimpleDataUtil.assertTableRows(rightTable, convertToRowData(rightRows));

    leftTable.refresh();
    Assert.assertNull(leftTable.currentSnapshot().summary().get("flink.test"));
    Assert.assertNull(leftTable.currentSnapshot().summary().get("direction"));
    rightTable.refresh();
    Assert.assertEquals(
        TestFlinkIcebergSink.class.getName(),
        rightTable.currentSnapshot().summary().get("flink.test"));
    Assert.assertEquals("rightTable", rightTable.currentSnapshot().summary().get("direction"));
  }

  @Test
  public void testOverrideWriteConfigWithUnknownDistributionMode() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.DISTRIBUTION_MODE.key(), "UNRECOGNIZED");

    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    SinkBuilder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .setAll(newProps);

    Assertions.assertThatThrownBy(builder::append)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid distribution mode: UNRECOGNIZED");
  }

  @Test
  public void testOverrideWriteConfigWithUnknownFileFormat() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.WRITE_FORMAT.key(), "UNRECOGNIZED");

    List<Row> rows = createRows("");
    DataStream<Row> dataStream = env.addSource(createBoundedSource(rows), ROW_TYPE_INFO);

    SinkBuilder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .writeParallelism(parallelism)
            .setAll(newProps);

    Assertions.assertThatThrownBy(builder::append)
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

    getSink(newSink, dataStream, table, tableLoader, parallelism, flinkConf);
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

  private static DataStreamSink<?> getSink(
      boolean generateNewSink,
      DataStream<RowData> input,
      Table table,
      TableLoader loader,
      int writerNum,
      Configuration flinkConf) {
    if (generateNewSink) {
      SinkBuilder builder =
          IcebergSink.forRowData(input)
              .table(table)
              .tableLoader(loader)
              .writeParallelism(writerNum);
      if (flinkConf != null) {
        builder.flinkConf(flinkConf);
      }

      return builder.append();
    } else {
      SinkBuilder builder =
          FlinkSink.forRowData(input).table(table).tableLoader(loader).writeParallelism(writerNum);
      if (flinkConf != null) {
        builder.flinkConf(flinkConf);
      }

      return builder.append();
    }
  }

  private static DataStreamSink<?> getSink(
      boolean generateNewSink,
      DataStream<Row> input,
      TableSchema forRowSchema,
      Table table,
      TableLoader loader,
      TableSchema forBuilderSchema,
      Integer writerNum,
      DistributionMode distributionMode,
      String uidPrefix,
      String snapshotPropertyName,
      String snapshotPropertyValue,
      Map<String, String> snapshotPropertyMap) {
    if (generateNewSink) {
      SinkBuilder builder =
          IcebergSink.forRow(input, forRowSchema)
              .table(table)
              .tableLoader(loader)
              .tableSchema(forBuilderSchema)
              .distributionMode(distributionMode);
      if (writerNum != null) {
        builder.writeParallelism(writerNum);
      }
      if (uidPrefix != null) {
        builder.uidPrefix(uidPrefix);
      }
      if (snapshotPropertyName != null && snapshotPropertyValue != null) {
        builder.setSnapshotProperty(snapshotPropertyName, snapshotPropertyValue);
      }
      if (snapshotPropertyMap != null) {
        builder.setSnapshotProperties(snapshotPropertyMap);
      }
      return builder.append();
    } else {
      SinkBuilder builder =
          FlinkSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
              .table(table)
              .tableLoader(loader)
              .tableSchema(forBuilderSchema)
              .distributionMode(distributionMode);
      if (writerNum != null) {
        builder.writeParallelism(writerNum);
      }
      if (uidPrefix != null) {
        builder.uidPrefix(uidPrefix);
      }
      if (snapshotPropertyName != null && snapshotPropertyValue != null) {
        builder.setSnapshotProperty(snapshotPropertyName, snapshotPropertyValue);
      }
      if (snapshotPropertyMap != null) {
        builder.setSnapshotProperties(snapshotPropertyMap);
      }
      return builder.append();
    }
  }
}
