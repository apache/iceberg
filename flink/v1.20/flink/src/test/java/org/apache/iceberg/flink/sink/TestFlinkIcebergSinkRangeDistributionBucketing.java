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

import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Test range distribution with bucketing partition column. Compared to hash distribution, range
 * distribution is more general to handle bucketing column while achieving even distribution of
 * traffic to writer tasks.
 *
 * <ul>
 *   <li><a href="https://github.com/apache/iceberg/pull/4228">keyBy on low cardinality</a> (e.g.
 *       60) may not achieve balanced data distribution.
 *   <li>number of buckets (e.g. 60) is not divisible by the writer parallelism (e.g. 40).
 *   <li>number of buckets (e.g. 60) is smaller than the writer parallelism (e.g. 120).
 * </ul>
 */
@Timeout(value = 30)
public class TestFlinkIcebergSinkRangeDistributionBucketing {
  private static final Configuration DISABLE_CLASSLOADER_CHECK_CONFIG =
      new Configuration()
          // disable classloader check as Avro may cache class/object in the serializers.
          .set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

  // max supported parallelism is 16 (= 4 x 4)
  @RegisterExtension
  public static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(4)
              .setNumberSlotsPerTaskManager(4)
              .setConfiguration(DISABLE_CLASSLOADER_CHECK_CONFIG)
              .build());

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  private static final int NUM_BUCKETS = 4;
  private static final int NUM_OF_CHECKPOINTS = 6;
  private static final int ROW_COUNT_PER_CHECKPOINT = 200;
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(2, "uuid", Types.UUIDType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).hour("ts").bucket("uuid", NUM_BUCKETS).build();
  private static final RowType ROW_TYPE = FlinkSchemaUtil.convert(SCHEMA);
  // FIXME (before merge): should it be parameterized test or hardcoded value with some comment that
  //       it is for manual tests purpose?
  private static final boolean DO_NOT_USE_V2_SINK = false;

  private TableLoader tableLoader;
  private Table table;

  @BeforeEach
  public void before() throws IOException {
    this.tableLoader = CATALOG_EXTENSION.tableLoader();
    this.table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SCHEMA,
                SPEC,
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name()));

    table
        .updateProperties()
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, DistributionMode.RANGE.modeName())
        .commit();

    // Assuming ts is on ingestion/processing time. Writer only writes to 1 or 2 hours concurrently.
    // Only sort on the bucket column to avoid each writer task writes to 60 buckets/files
    // concurrently.
    table.replaceSortOrder().asc(bucket("uuid", NUM_BUCKETS)).commit();
  }

  @AfterEach
  public void after() throws Exception {
    CATALOG_EXTENSION.catalog().dropTable(TestFixtures.TABLE_IDENTIFIER);
  }

  /** number of buckets 4 matches writer parallelism of 4 */
  @Test
  public void testBucketNumberEqualsToWriterParallelism() throws Exception {
    testParallelism(4);
  }

  /** number of buckets 4 is less than writer parallelism of 6 */
  @Test
  public void testBucketNumberLessThanWriterParallelismNotDivisible() throws Exception {
    testParallelism(6);
  }

  /** number of buckets 4 is less than writer parallelism of 8 */
  @Test
  public void testBucketNumberLessThanWriterParallelismDivisible() throws Exception {
    testParallelism(8);
  }

  /** number of buckets 4 is greater than writer parallelism of 3 */
  @Test
  public void testBucketNumberHigherThanWriterParallelismNotDivisible() throws Exception {
    testParallelism(3);
  }

  /** number of buckets 4 is greater than writer parallelism of 2 */
  @Test
  public void testBucketNumberHigherThanWriterParallelismDivisible() throws Exception {
    testParallelism(2);
  }

  private void testParallelism(int parallelism) throws Exception {
    try (StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism)) {

      DataGeneratorSource<RowData> generatorSource =
          new DataGeneratorSource<>(
              new RowGenerator(),
              ROW_COUNT_PER_CHECKPOINT * NUM_OF_CHECKPOINTS,
              RateLimiterStrategy.perCheckpoint(ROW_COUNT_PER_CHECKPOINT),
              FlinkCompatibilityUtil.toTypeInfo(ROW_TYPE));
      DataStream<RowData> dataStream =
          env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "Data Generator");

      IcebergSinkBuilder.forRowData(dataStream, DO_NOT_USE_V2_SINK)
          .table(table)
          .tableLoader(tableLoader)
          .writeParallelism(parallelism)
          .append();
      env.execute(getClass().getSimpleName());

      table.refresh();
      // ordered in reverse timeline from the oldest snapshot to the newest snapshot
      List<Snapshot> snapshots = Lists.newArrayList(table.snapshots().iterator());
      // only keep the snapshots with added data files
      snapshots =
          snapshots.stream()
              .filter(snapshot -> snapshot.addedDataFiles(table.io()).iterator().hasNext())
              .collect(Collectors.toList());

      // Source rate limit per checkpoint cycle may not be super precise.
      // There could be more checkpoint cycles and commits than planned.
      assertThat(snapshots).hasSizeGreaterThanOrEqualTo(NUM_OF_CHECKPOINTS);

      // It takes 2 checkpoint cycle for statistics collection and application
      // of the globally aggregated statistics in the range partitioner.
      // The last two checkpoints should have range shuffle applied
      List<Snapshot> rangePartitionedCycles =
          snapshots.subList(snapshots.size() - 2, snapshots.size());

      for (Snapshot snapshot : rangePartitionedCycles) {
        List<DataFile> addedDataFiles =
            Lists.newArrayList(snapshot.addedDataFiles(table.io()).iterator());
        assertThat(addedDataFiles)
            .hasSizeLessThanOrEqualTo(maxAddedDataFilesPerCheckpoint(parallelism));
      }
    }
  }

  /**
   * Traffic is not perfectly balanced across all buckets in the small sample size Range
   * distribution of the bucket id may cross subtask boundary. Hence the number of committed data
   * files per checkpoint maybe larger than writer parallelism or the number of buckets. But it
   * should not be more than the sum of those two. Without range distribution, the number of data
   * files per commit can be 4x of parallelism (as the number of buckets is 4).
   */
  private int maxAddedDataFilesPerCheckpoint(int parallelism) {
    return NUM_BUCKETS + parallelism;
  }

  private static class RowGenerator implements GeneratorFunction<Long, RowData> {
    // use constant timestamp so that all rows go to the same hourly partition
    private final long ts = System.currentTimeMillis();

    @Override
    public RowData map(Long index) throws Exception {
      // random uuid should result in relatively balanced distribution across buckets
      UUID uuid = UUID.randomUUID();
      ByteBuffer uuidByteBuffer = ByteBuffer.allocate(16);
      uuidByteBuffer.putLong(uuid.getMostSignificantBits());
      uuidByteBuffer.putLong(uuid.getLeastSignificantBits());
      return GenericRowData.of(
          TimestampData.fromEpochMillis(ts),
          uuidByteBuffer.array(),
          StringData.fromString("row-" + index));
    }
  }
}
