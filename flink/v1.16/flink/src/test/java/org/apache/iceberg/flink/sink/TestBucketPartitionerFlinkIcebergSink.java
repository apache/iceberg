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

import static org.apache.iceberg.flink.MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG;
import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.apache.iceberg.flink.TestFixtures.TABLE_IDENTIFIER;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.BucketUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestBucketPartitionerFlinkIcebergSink {

  private enum TableSchemaType {
    ONE_BUCKET,
    IDENTITY_AND_BUCKET,
    TWO_BUCKETS;
  }

  private static final int NUMBER_TASK_MANAGERS = 1;
  private static final int SLOTS_PER_TASK_MANAGER = 8;

  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
              .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
              .setConfiguration(DISABLE_CLASSLOADER_CHECK_CONFIG)
              .build());

  @RegisterExtension
  static HadoopCatalogExtension catalogExtension =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());
  private static final DataFormatConverters.RowConverter CONVERTER =
      new DataFormatConverters.RowConverter(SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

  // Parallelism = 8 throughout the test suite
  private final int parallelism = NUMBER_TASK_MANAGERS * SLOTS_PER_TASK_MANAGER;
  private final FileFormat format = FileFormat.PARQUET;
  private final int numBuckets = 4;

  private Table table;
  private StreamExecutionEnvironment env;
  private TableLoader tableLoader;

  private void setupEnvironment(TableSchemaType tableSchemaType) {
    table = getTable(tableSchemaType);
    env =
        StreamExecutionEnvironment.getExecutionEnvironment(DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism * 2);
    tableLoader = catalogExtension.tableLoader();
  }

  private Table getTable(TableSchemaType tableSchemaType) {
    PartitionSpec partitionSpec = null;

    switch (tableSchemaType) {
      case ONE_BUCKET:
        partitionSpec =
            PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).bucket("data", numBuckets).build();
        break;
      case IDENTITY_AND_BUCKET:
        partitionSpec =
            PartitionSpec.builderFor(SimpleDataUtil.SCHEMA)
                .identity("id")
                .bucket("data", numBuckets)
                .build();
        break;
      case TWO_BUCKETS:
        partitionSpec =
            PartitionSpec.builderFor(SimpleDataUtil.SCHEMA)
                .bucket("id", numBuckets)
                .bucket("data", numBuckets)
                .build();
        break;
    }

    return catalogExtension
        .catalog()
        .createTable(
            TABLE_IDENTIFIER,
            SimpleDataUtil.SCHEMA,
            partitionSpec,
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()));
  }

  private List<RowData> convertToRowData(List<Row> rows) {
    return rows.stream().map(CONVERTER::toInternal).collect(Collectors.toList());
  }

  private BoundedTestSource<Row> createBoundedSource(List<Row> rows) {
    return new BoundedTestSource<>(rows.toArray(new Row[0]));
  }

  private TableTestStats extractTableTestStats(TableSchemaType tableSchemaType) throws IOException {
    int totalRecordCount = 0;
    Map<Integer, List<Integer>> writersPerBucket = Maps.newHashMap(); // <BucketId, Set<WriterId>>
    Map<Integer, Integer> filesPerBucket = Maps.newHashMap(); // <BucketId, NumFiles>
    Map<Integer, Long> recordsPerFile = new TreeMap<>(); // <WriterId, NumRecords>

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      for (FileScanTask scanTask : fileScanTasks) {
        long recordCountInFile = scanTask.file().recordCount();

        String[] splitFilePath = scanTask.file().path().toString().split("/");
        String filename = splitFilePath[splitFilePath.length - 1];
        int writerId = Integer.parseInt(filename.split("-")[0]);

        totalRecordCount += recordCountInFile;
        int bucketId =
            scanTask
                .file()
                .partition()
                .get(tableSchemaType == TableSchemaType.ONE_BUCKET ? 0 : 1, Integer.class);
        writersPerBucket.computeIfAbsent(bucketId, k -> Lists.newArrayList());
        writersPerBucket.get(bucketId).add(writerId);
        filesPerBucket.put(bucketId, filesPerBucket.getOrDefault(bucketId, 0) + 1);
        recordsPerFile.put(writerId, recordsPerFile.getOrDefault(writerId, 0L) + recordCountInFile);
      }
    }

    for (int k : writersPerBucket.keySet()) {
      Collections.sort(writersPerBucket.get(k));
    }

    return new TableTestStats(totalRecordCount, writersPerBucket, filesPerBucket, recordsPerFile);
  }

  private void testWriteRowData(List<Row> allRows) throws Exception {
    DataStream<RowData> dataStream =
        env.addSource(createBoundedSource(allRows), ROW_TYPE_INFO)
            .map(CONVERTER::toInternal, FlinkCompatibilityUtil.toTypeInfo(SimpleDataUtil.ROW_TYPE));

    FlinkSink.forRowData(dataStream)
        .table(table)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .distributionMode(DistributionMode.HASH)
        .append();

    // Execute the program.
    env.execute("Test Iceberg DataStream");

    // Assert the iceberg table's records.
    SimpleDataUtil.assertTableRows(table, convertToRowData(allRows));
  }

  @ParameterizedTest
  @EnumSource(
      value = TableSchemaType.class,
      names = {"ONE_BUCKET", "IDENTITY_AND_BUCKET"})
  public void testSendRecordsToAllBucketsEvenly(TableSchemaType tableSchemaType) throws Exception {
    setupEnvironment(tableSchemaType);

    // Generating 16 records to be sent (8 writers -> 4 buckets), will be uniformly round-robin'd
    // to all writers
    int totalNumRows = parallelism * 2;
    int numRowsPerBucket = totalNumRows / numBuckets;
    List<Row> rows = Lists.newArrayListWithCapacity(totalNumRows);
    List<Row> rowsPerBucket = Lists.newArrayListWithCapacity(numBuckets);
    for (int i = 0; i < numBuckets; i++) {
      rowsPerBucket.addAll(generateRowsForBucketId(1, i));
    }
    for (int i = 0; i < numRowsPerBucket; i++) {
      rows.addAll(rowsPerBucket);
    }

    testWriteRowData(rows);
    TableTestStats stats = extractTableTestStats(tableSchemaType);

    Assertions.assertThat(stats.totalRowCount).isEqualTo(totalNumRows);
    // All 4 buckets should've been written to
    Assertions.assertThat(stats.writersPerBucket.size()).isEqualTo(numBuckets);
    Assertions.assertThat(stats.numFilesPerBucket.size()).isEqualTo(numBuckets);
    // Writer expectation (2 writers per bucket):
    // - Bucket0 -> Writers [0, 4]
    // - Bucket1 -> Writers [1, 5]
    // - Bucket2 -> Writers [2, 6]
    // - Bucket3 -> Writers [3, 7]
    for (int i = 0, j = numBuckets; i < numBuckets; i++, j++) {
      Assertions.assertThat(stats.writersPerBucket.get(i)).isEqualTo(Arrays.asList(i, j));
      // 2 files per bucket (one file is created by each writer)
      Assertions.assertThat((int) stats.numFilesPerBucket.get(i)).isEqualTo(2);
      // 2 rows per file (total of 16 rows across 8 files)
      Assertions.assertThat((long) stats.rowsPerFile.get(i)).isEqualTo(2);
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = TableSchemaType.class,
      names = {"TWO_BUCKETS"})
  public void testBucketPartitionerFail(TableSchemaType tableSchemaType) {
    setupEnvironment(tableSchemaType);

    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> new BucketPartitioner(table.spec()))
        .withMessageContaining(BucketPartitionerUtils.BAD_NUMBER_OF_BUCKETS_ERROR_MESSAGE);
  }

  @ParameterizedTest
  @EnumSource(
      value = TableSchemaType.class,
      names = {"TWO_BUCKETS"})
  public void testBucketPartitionKeySelectorFail(TableSchemaType tableSchemaType) {
    setupEnvironment(tableSchemaType);

    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> new BucketPartitionKeySelector(table.spec(), table.schema(), null))
        .withMessageContaining(BucketPartitionerUtils.BAD_NUMBER_OF_BUCKETS_ERROR_MESSAGE);
  }

  /**
   * Utility method to generate rows whose values will "hash" to a desired bucketId
   *
   * @param numRows the number of rows/requests to generate
   * @param bucketId the desired bucketId
   * @return the list of rows whose data "hashes" to the desired bucketId
   */
  private List<Row> generateRowsForBucketId(int numRows, int bucketId) {
    List<Row> rows = Lists.newArrayListWithCapacity(numRows);
    for (int i = 0; i < numRows; i++) {
      String value = generateValueForBucketId(bucketId, numBuckets);
      rows.add(Row.of(1, value));
    }
    return rows;
  }

  /**
   * Utility method to generate a UUID string that will "hash" to a desired bucketId
   *
   * @param bucketId the desired bucketId
   * @return the string data that "hashes" to the desired bucketId
   */
  public static String generateValueForBucketId(int bucketId, int numBuckets) {
    String value = "";
    while (true) {
      String uuid = UUID.randomUUID().toString();
      if (hash(numBuckets, uuid) == bucketId) {
        value = uuid;
        break;
      }
    }
    return value;
  }

  /**
   * Utility method that performs the same "hashing" mechanism used by Bucket.java
   *
   * @param numBuckets the maximum number of buckets
   * @param uuid the UUID string value to hash
   * @return the target BucketId
   */
  public static int hash(int numBuckets, String uuid) {
    return (BucketUtil.hash(uuid) & Integer.MAX_VALUE) % numBuckets;
  }

  /** DTO to hold Test Stats */
  private static class TableTestStats {
    final int totalRowCount;
    final Map<Integer, List<Integer>> writersPerBucket;
    final Map<Integer, Integer> numFilesPerBucket;
    final Map<Integer, Long> rowsPerFile;

    TableTestStats(
        int totalRecordCount,
        Map<Integer, List<Integer>> writersPerBucket,
        Map<Integer, Integer> numFilesPerBucket,
        Map<Integer, Long> rowsPerFile) {
      this.totalRowCount = totalRecordCount;
      this.writersPerBucket = writersPerBucket;
      this.numFilesPerBucket = numFilesPerBucket;
      this.rowsPerFile = rowsPerFile;
    }
  }
}
