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

import static org.apache.iceberg.flink.MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG;
import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.apache.iceberg.flink.TestFixtures.TABLE_IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.TestBucketPartitionerUtil.TableSchemaType;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestBucketPartitionerFlinkIcebergSink {

  private static final int NUMBER_TASK_MANAGERS = 1;
  private static final int SLOTS_PER_TASK_MANAGER = 8;

  @RegisterExtension
  private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
              .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
              .setConfiguration(DISABLE_CLASSLOADER_CHECK_CONFIG)
              .build());

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

  // Parallelism = 8 (parallelism > numBuckets) throughout the test suite
  private final int parallelism = NUMBER_TASK_MANAGERS * SLOTS_PER_TASK_MANAGER;
  private final FileFormat format = FileFormat.PARQUET;
  private final int numBuckets = 4;

  private Table table;
  private StreamExecutionEnvironment env;
  private TableLoader tableLoader;

  private void setupEnvironment(TableSchemaType tableSchemaType) {
    PartitionSpec partitionSpec = tableSchemaType.getPartitionSpec(numBuckets);
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                partitionSpec,
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()));
    env =
        StreamExecutionEnvironment.getExecutionEnvironment(DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism * 2);
    tableLoader = CATALOG_EXTENSION.tableLoader();
  }

  private void appendRowsToTable(List<RowData> allRows) throws Exception {
    DataFormatConverters.RowConverter converter =
        new DataFormatConverters.RowConverter(SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

    DataStream<RowData> dataStream =
        env.addSource(
                new BoundedTestSource<>(
                    allRows.stream().map(converter::toExternal).toArray(Row[]::new)),
                ROW_TYPE_INFO)
            .map(converter::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE))
            .partitionCustom(
                new BucketPartitioner(table.spec()),
                new BucketPartitionKeySelector(
                    table.spec(),
                    table.schema(),
                    FlinkSink.toFlinkRowType(table.schema(), SimpleDataUtil.FLINK_SCHEMA)));

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .distributionMode(DistributionMode.NONE)
        .append();

    env.execute("Test Iceberg DataStream");

    SimpleDataUtil.assertTableRows(table, allRows);
  }

  @ParameterizedTest
  @EnumSource(
      value = TableSchemaType.class,
      names = {"ONE_BUCKET", "IDENTITY_AND_BUCKET"})
  public void testSendRecordsToAllBucketsEvenly(TableSchemaType tableSchemaType) throws Exception {
    setupEnvironment(tableSchemaType);
    List<RowData> rows = generateTestDataRows();

    appendRowsToTable(rows);
    TableTestStats stats = extractPartitionResults(tableSchemaType);

    assertThat(stats.totalRowCount).isEqualTo(rows.size());
    // All 4 buckets should've been written to
    assertThat(stats.writersPerBucket.size()).isEqualTo(numBuckets);
    assertThat(stats.numFilesPerBucket.size()).isEqualTo(numBuckets);
    // Writer expectation (2 writers per bucket):
    // - Bucket0 -> Writers [0, 4]
    // - Bucket1 -> Writers [1, 5]
    // - Bucket2 -> Writers [2, 6]
    // - Bucket3 -> Writers [3, 7]
    for (int i = 0, j = numBuckets; i < numBuckets; i++, j++) {
      assertThat(stats.writersPerBucket.get(i)).hasSameElementsAs(Arrays.asList(i, j));
      // 2 files per bucket (one file is created by each writer)
      assertThat(stats.numFilesPerBucket.get(i)).isEqualTo(2);
      // 2 rows per file (total of 16 rows across 8 files)
      assertThat(stats.rowsPerWriter.get(i)).isEqualTo(2);
    }
  }

  /**
   * Generating 16 rows to be sent uniformly to all writers (round-robin across 8 writers -> 4
   * buckets)
   */
  private List<RowData> generateTestDataRows() {
    int totalNumRows = parallelism * 2;
    int numRowsPerBucket = totalNumRows / numBuckets;
    return TestBucketPartitionerUtil.generateRowsForBucketIdRange(numRowsPerBucket, numBuckets);
  }

  private TableTestStats extractPartitionResults(TableSchemaType tableSchemaType)
      throws IOException {
    int totalRecordCount = 0;
    Map<Integer, List<Integer>> writersPerBucket = Maps.newHashMap(); // <BucketId, List<WriterId>>
    Map<Integer, Integer> filesPerBucket = Maps.newHashMap(); // <BucketId, NumFiles>
    Map<Integer, Long> rowsPerWriter = Maps.newHashMap(); // <WriterId, NumRecords>

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      for (FileScanTask scanTask : fileScanTasks) {
        long recordCountInFile = scanTask.file().recordCount();

        String[] splitFilePath = scanTask.file().path().toString().split("/");
        // Filename example: 00007-0-a7d3a29a-33e9-4740-88f4-0f494397d60c-00001.parquet
        // Writer ID: .......^^^^^
        String filename = splitFilePath[splitFilePath.length - 1];
        int writerId = Integer.parseInt(filename.split("-")[0]);

        totalRecordCount += recordCountInFile;
        int bucketId =
            scanTask
                .file()
                .partition()
                .get(tableSchemaType.bucketPartitionColumnPosition(), Integer.class);
        writersPerBucket.computeIfAbsent(bucketId, k -> Lists.newArrayList());
        writersPerBucket.get(bucketId).add(writerId);
        filesPerBucket.put(bucketId, filesPerBucket.getOrDefault(bucketId, 0) + 1);
        rowsPerWriter.put(writerId, rowsPerWriter.getOrDefault(writerId, 0L) + recordCountInFile);
      }
    }

    return new TableTestStats(totalRecordCount, writersPerBucket, filesPerBucket, rowsPerWriter);
  }

  /** DTO to hold Test Stats */
  private static class TableTestStats {
    final int totalRowCount;
    final Map<Integer, List<Integer>> writersPerBucket;
    final Map<Integer, Integer> numFilesPerBucket;
    final Map<Integer, Long> rowsPerWriter;

    TableTestStats(
        int totalRecordCount,
        Map<Integer, List<Integer>> writersPerBucket,
        Map<Integer, Integer> numFilesPerBucket,
        Map<Integer, Long> rowsPerWriter) {
      this.totalRowCount = totalRecordCount;
      this.writersPerBucket = writersPerBucket;
      this.numFilesPerBucket = numFilesPerBucket;
      this.rowsPerWriter = rowsPerWriter;
    }
  }
}
