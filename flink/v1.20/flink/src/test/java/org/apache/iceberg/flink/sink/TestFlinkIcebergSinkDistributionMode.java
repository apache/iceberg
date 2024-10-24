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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.shuffle.StatisticsType;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * This tests the distribution mode of Flink sink. Extract them separately since it is unnecessary
 * to test different file formats (Avro, Orc, Parquet) like in {@link TestFlinkIcebergSink}.
 * Removing the file format dimension reduces the number of combinations from 12 to 4, which helps
 * reduce test run time.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkIcebergSinkDistributionMode extends TestFlinkIcebergSinkBase {

  @RegisterExtension
  public static MiniClusterExtension miniClusterResource =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  private final FileFormat format = FileFormat.PARQUET;

  @Parameter(index = 0)
  private int parallelism;

  @Parameter(index = 1)
  private boolean partitioned;

  @Parameters(name = "parallelism = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {1, true},
      {1, false},
      {2, true},
      {2, false}
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
  public void testShuffleByPartitionWithSchema() throws Exception {
    testWriteRow(parallelism, SimpleDataUtil.FLINK_SCHEMA, DistributionMode.HASH);
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
  public void testJobNoneDistributeMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(parallelism, null, DistributionMode.NONE);

    if (parallelism > 1) {
      if (partitioned) {
        int files = partitionFiles("aaa") + partitionFiles("bbb") + partitionFiles("ccc");
        assertThat(files).as("Should have more than 3 files in iceberg table.").isGreaterThan(3);
      }
    }
  }

  @TestTemplate
  public void testJobNullDistributionMode() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.HASH.modeName())
        .commit();

    testWriteRow(parallelism, null, null);

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
  public void testPartitionWriteMode() throws Exception {
    testWriteRow(parallelism, null, DistributionMode.HASH);
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
  public void testOverrideWriteConfigWithUnknownDistributionMode() {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.put(FlinkWriteOptions.DISTRIBUTION_MODE.key(), "UNRECOGNIZED");

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
        .hasMessage("Invalid distribution mode: UNRECOGNIZED");
  }

  @TestTemplate
  public void testRangeDistributionWithoutSortOrderUnpartitioned() throws Exception {
    assumeThat(partitioned).isFalse();

    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.RANGE.modeName())
        .commit();

    int numOfCheckpoints = 6;
    DataStream<Row> dataStream =
        env.addSource(
            createRangeDistributionBoundedSource(createCharRows(numOfCheckpoints, 10)),
            ROW_TYPE_INFO);
    FlinkSink.Builder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .writeParallelism(parallelism);

    // Range distribution requires either sort order or partition spec defined
    assertThatThrownBy(builder::append)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Invalid write distribution mode: range. Need to define sort order or partition spec.");
  }

  @TestTemplate
  public void testRangeDistributionWithoutSortOrderPartitioned() throws Exception {
    assumeThat(partitioned).isTrue();

    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.RANGE.modeName())
        .commit();

    int numOfCheckpoints = 6;
    DataStream<Row> dataStream =
        env.addSource(
            createRangeDistributionBoundedSource(createCharRows(numOfCheckpoints, 10)),
            ROW_TYPE_INFO);
    FlinkSink.Builder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .table(table)
            .tableLoader(tableLoader)
            .writeParallelism(parallelism);

    // sort based on partition columns
    builder.append();
    env.execute(getClass().getSimpleName());

    table.refresh();
    // ordered in reverse timeline from the newest snapshot to the oldest snapshot
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots().iterator());
    // only keep the snapshots with added data files
    snapshots =
        snapshots.stream()
            .filter(snapshot -> snapshot.addedDataFiles(table.io()).iterator().hasNext())
            .collect(Collectors.toList());

    // Sometimes we will have more checkpoints than the bounded source if we pass the
    // auto checkpoint interval. Thus producing multiple snapshots.
    assertThat(snapshots).hasSizeGreaterThanOrEqualTo(numOfCheckpoints);
  }

  @TestTemplate
  public void testRangeDistributionWithSortOrder() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.RANGE.modeName())
        .commit();
    table.replaceSortOrder().asc("data").commit();

    int numOfCheckpoints = 6;
    DataStream<Row> dataStream =
        env.addSource(
            createRangeDistributionBoundedSource(createCharRows(numOfCheckpoints, 10)),
            ROW_TYPE_INFO);
    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .rangeDistributionStatisticsType(StatisticsType.Map)
        .append();
    env.execute(getClass().getSimpleName());

    table.refresh();
    // ordered in reverse timeline from the newest snapshot to the oldest snapshot
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots().iterator());
    // only keep the snapshots with added data files
    snapshots =
        snapshots.stream()
            .filter(snapshot -> snapshot.addedDataFiles(table.io()).iterator().hasNext())
            .collect(Collectors.toList());

    // Sometimes we will have more checkpoints than the bounded source if we pass the
    // auto checkpoint interval. Thus producing multiple snapshots.
    assertThat(snapshots).hasSizeGreaterThanOrEqualTo(numOfCheckpoints);

    // It takes 2 checkpoint cycle for statistics collection and application
    // of the globally aggregated statistics in the range partitioner.
    // The last two checkpoints should have range shuffle applied
    List<Snapshot> rangePartitionedCycles =
        snapshots.subList(snapshots.size() - 2, snapshots.size());

    if (partitioned) {
      for (Snapshot snapshot : rangePartitionedCycles) {
        List<DataFile> addedDataFiles =
            Lists.newArrayList(snapshot.addedDataFiles(table.io()).iterator());
        // up to 26 partitions
        assertThat(addedDataFiles).hasSizeLessThanOrEqualTo(26);
      }
    } else {
      for (Snapshot snapshot : rangePartitionedCycles) {
        List<DataFile> addedDataFiles =
            Lists.newArrayList(snapshot.addedDataFiles(table.io()).iterator());
        // each writer task should only write one file for non-partition sort column
        assertThat(addedDataFiles).hasSize(parallelism);
        // verify there is no overlap in min-max stats range
        if (parallelism == 2) {
          assertIdColumnStatsNoRangeOverlap(addedDataFiles.get(0), addedDataFiles.get(1));
        }
      }
    }
  }

  @TestTemplate
  public void testRangeDistributionSketchWithSortOrder() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.RANGE.modeName())
        .commit();
    table.replaceSortOrder().asc("id").commit();

    int numOfCheckpoints = 6;
    DataStream<Row> dataStream =
        env.addSource(
            createRangeDistributionBoundedSource(createIntRows(numOfCheckpoints, 1_000)),
            ROW_TYPE_INFO);
    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .rangeDistributionStatisticsType(StatisticsType.Sketch)
        .append();
    env.execute(getClass().getSimpleName());

    table.refresh();
    // ordered in reverse timeline from the newest snapshot to the oldest snapshot
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots().iterator());
    // only keep the snapshots with added data files
    snapshots =
        snapshots.stream()
            .filter(snapshot -> snapshot.addedDataFiles(table.io()).iterator().hasNext())
            .collect(Collectors.toList());

    // Sometimes we will have more checkpoints than the bounded source if we pass the
    // auto checkpoint interval. Thus producing multiple snapshots.
    assertThat(snapshots).hasSizeGreaterThanOrEqualTo(numOfCheckpoints);

    // It takes 2 checkpoint cycle for statistics collection and application
    // of the globally aggregated statistics in the range partitioner.
    // The last two checkpoints should have range shuffle applied
    List<Snapshot> rangePartitionedCycles =
        snapshots.subList(snapshots.size() - 2, snapshots.size());

    // since the input has a single value for the data column,
    // it is always the same partition. Hence there is no difference
    // for partitioned or not
    for (Snapshot snapshot : rangePartitionedCycles) {
      List<DataFile> addedDataFiles =
          Lists.newArrayList(snapshot.addedDataFiles(table.io()).iterator());
      // each writer task should only write one file for non-partition sort column
      assertThat(addedDataFiles).hasSize(parallelism);
      // verify there is no overlap in min-max stats range
      if (parallelism == 2) {
        assertIdColumnStatsNoRangeOverlap(addedDataFiles.get(0), addedDataFiles.get(1));
      }
    }
  }

  /** Test migration from Map stats to Sketch stats */
  @TestTemplate
  public void testRangeDistributionStatisticsMigration() throws Exception {
    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.RANGE.modeName())
        .commit();
    table.replaceSortOrder().asc("id").commit();

    int numOfCheckpoints = 4;
    List<List<Row>> rowsPerCheckpoint = Lists.newArrayListWithCapacity(numOfCheckpoints);
    for (int checkpointId = 0; checkpointId < numOfCheckpoints; ++checkpointId) {
      // checkpointId 2 would emit 11_000 records which is larger than
      // the OPERATOR_SKETCH_SWITCH_THRESHOLD of 10_000.
      // This should trigger the stats migration.
      int maxId = checkpointId < 1 ? 1_000 : 11_000;
      List<Row> rows = Lists.newArrayListWithCapacity(maxId);
      for (int j = 0; j < maxId; ++j) {
        // fixed value "a" for the data (possible partition column)
        rows.add(Row.of(j, "a"));
      }

      rowsPerCheckpoint.add(rows);
    }

    DataStream<Row> dataStream =
        env.addSource(createRangeDistributionBoundedSource(rowsPerCheckpoint), ROW_TYPE_INFO);
    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .rangeDistributionStatisticsType(StatisticsType.Auto)
        .append();
    env.execute(getClass().getSimpleName());

    table.refresh();
    // ordered in reverse timeline from the newest snapshot to the oldest snapshot
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots().iterator());
    // only keep the snapshots with added data files
    snapshots =
        snapshots.stream()
            .filter(snapshot -> snapshot.addedDataFiles(table.io()).iterator().hasNext())
            .collect(Collectors.toList());

    // Sometimes we will have more checkpoints than the bounded source if we pass the
    // auto checkpoint interval. Thus producing multiple snapshots.
    assertThat(snapshots).hasSizeGreaterThanOrEqualTo(numOfCheckpoints);

    // It takes 2 checkpoint cycle for statistics collection and application
    // of the globally aggregated statistics in the range partitioner.
    // The last two checkpoints should have range shuffle applied
    List<Snapshot> rangePartitionedCycles =
        snapshots.subList(snapshots.size() - 2, snapshots.size());

    // since the input has a single value for the data column,
    // it is always the same partition. Hence there is no difference
    // for partitioned or not
    for (Snapshot snapshot : rangePartitionedCycles) {
      List<DataFile> addedDataFiles =
          Lists.newArrayList(snapshot.addedDataFiles(table.io()).iterator());
      // each writer task should only write one file for non-partition sort column
      // sometimes
      assertThat(addedDataFiles).hasSize(parallelism);
      // verify there is no overlap in min-max stats range
      if (parallelism == 2) {
        assertIdColumnStatsNoRangeOverlap(addedDataFiles.get(0), addedDataFiles.get(1));
      }
    }
  }

  private BoundedTestSource<Row> createRangeDistributionBoundedSource(
      List<List<Row>> rowsPerCheckpoint) {
    return new BoundedTestSource<>(rowsPerCheckpoint);
  }

  private List<List<Row>> createCharRows(int numOfCheckpoints, int countPerChar) {
    List<List<Row>> rowsPerCheckpoint = Lists.newArrayListWithCapacity(numOfCheckpoints);
    for (int checkpointId = 0; checkpointId < numOfCheckpoints; ++checkpointId) {
      List<Row> rows = Lists.newArrayListWithCapacity(26 * countPerChar);
      for (int j = 0; j < countPerChar; ++j) {
        for (char c = 'a'; c <= 'z'; ++c) {
          rows.add(Row.of(1, String.valueOf(c)));
        }
      }

      rowsPerCheckpoint.add(rows);
    }

    return rowsPerCheckpoint;
  }

  private List<List<Row>> createIntRows(int numOfCheckpoints, int maxId) {
    List<List<Row>> rowsPerCheckpoint = Lists.newArrayListWithCapacity(numOfCheckpoints);
    for (int checkpointId = 0; checkpointId < numOfCheckpoints; ++checkpointId) {
      List<Row> rows = Lists.newArrayListWithCapacity(maxId);
      for (int j = 0; j < maxId; ++j) {
        // fixed value "a" for the data (possible partition column)
        rows.add(Row.of(j, "a"));
      }

      rowsPerCheckpoint.add(rows);
    }

    return rowsPerCheckpoint;
  }

  private void assertIdColumnStatsNoRangeOverlap(DataFile file1, DataFile file2) {
    // id column has fieldId 1
    int file1LowerBound =
        Conversions.fromByteBuffer(Types.IntegerType.get(), file1.lowerBounds().get(1));
    int file1UpperBound =
        Conversions.fromByteBuffer(Types.IntegerType.get(), file1.upperBounds().get(1));
    int file2LowerBound =
        Conversions.fromByteBuffer(Types.IntegerType.get(), file2.lowerBounds().get(1));
    int file2UpperBound =
        Conversions.fromByteBuffer(Types.IntegerType.get(), file2.upperBounds().get(1));

    if (file1LowerBound < file2LowerBound) {
      assertThat(file1UpperBound).isLessThanOrEqualTo(file2LowerBound);
    } else {
      assertThat(file2UpperBound).isLessThanOrEqualTo(file1LowerBound);
    }
  }
}
