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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.HadoopCatalogResource;
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
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestBucketPartitionerFlinkIcebergSink {

  private static final int NUMBER_TASK_MANAGERS = 1;
  private static final int SLOTS_PER_TASK_MANAGER = 8;

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
              .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
              .setConfiguration(DISABLE_CLASSLOADER_CHECK_CONFIG)
              .build());

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopCatalogResource catalogResource =
      new HadoopCatalogResource(TEMPORARY_FOLDER, DATABASE, TestFixtures.TABLE);

  private static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());
  private static final DataFormatConverters.RowConverter CONVERTER =
      new DataFormatConverters.RowConverter(SimpleDataUtil.FLINK_SCHEMA.getFieldDataTypes());

  private Table table;
  private StreamExecutionEnvironment env;
  private TableLoader tableLoader;

  // Simple parallelism = 8 throughout the test suite
  private final int parallelism = NUMBER_TASK_MANAGERS * SLOTS_PER_TASK_MANAGER;
  private final FileFormat format = FileFormat.fromString("parquet");
  private final int numBuckets = 4;

  @Before
  public void before() throws IOException {
    table =
        catalogResource
            .catalog()
            .createTable(
                TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).bucket("data", numBuckets).build(),
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()));

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism * 2);

    tableLoader = catalogResource.tableLoader();
  }

  private List<RowData> convertToRowData(List<Row> rows) {
    return rows.stream().map(CONVERTER::toInternal).collect(Collectors.toList());
  }

  private BoundedTestSource<Row> createBoundedSource(List<Row> rows) {
    return new BoundedTestSource<>(rows.toArray(new Row[0]));
  }

  private TableTestStats extractTableTestStats() throws IOException {
    // Assertions:
    //  - Total record count in table
    //  - All records belong to that bucket
    //  - Each bucket should've been written by valid writers
    //  - Each file should have the expected number of records

    int totalRecordCount = 0;
    Map<Integer, List<Integer>> writersPerBucket = Maps.newHashMap(); // <BucketId, Set<WriterId>>
    Map<Integer, Integer> filesPerBucket = Maps.newHashMap(); // <BucketId, NumFiles>
    Map<Integer, Long> recordsPerFile = new TreeMap<>(); // <WriterId, NumRecords>

    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      for (FileScanTask scanTask : fileScanTasks) {
        Optional<Integer> bucketIdOpt =
            BucketPartitionKeySelector.extractInteger(scanTask.file().partition().toString());

        long recordCountInFile = scanTask.file().recordCount();

        String[] splitFilePath = scanTask.file().path().toString().split("/");
        String filename = splitFilePath[splitFilePath.length - 1];
        int writerId = Integer.parseInt(filename.split("-")[0]);

        totalRecordCount += recordCountInFile;
        int bucketId = bucketIdOpt.get();
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

  // Test 1A: N records sent to the same bucket across all partitions
  //  - 24 rows (3 batches w/overlap), 4 buckets
  @Test
  public void testSendToBucket0A() throws Exception {
    // Data generation
    // We make the number of requests a factor of the parallelism for an easier verification
    int totalNumRows = parallelism * 3; // 24 rows
    TableTestStats stats = sendAndAssertBucket0(totalNumRows);
    // Number of records per file
    Assert.assertEquals(16, stats.recordsPerFile.get(0).intValue());
    Assert.assertEquals(8, stats.recordsPerFile.get(4).intValue());
  }

  // Test 1B: N records sent to the same bucket across all partitions
  //  - 30 rows (4 batches w/o overlap, uneven first bucket), 4 buckets
  @Test
  public void testSendToBucket0B() throws Exception {
    // Data generation
    // We make the number of requests a factor of the parallelism for an easier verification
    int totalNumRows = 30;
    TableTestStats stats = sendAndAssertBucket0(totalNumRows);
    // Number of records per file
    Assert.assertEquals(16, stats.recordsPerFile.get(0).intValue());
    Assert.assertEquals(14, stats.recordsPerFile.get(4).intValue());
  }

  @NotNull
  private TableTestStats sendAndAssertBucket0(int totalNumRows) throws Exception {
    List<Row> rows = generateRowsForBucketId(totalNumRows, 0);
    // Writing to the table
    testWriteRowData(rows);
    // Extracting stats
    TableTestStats stats = extractTableTestStats();

    // Assertions
    // All the records were received
    Assert.assertEquals(totalNumRows, stats.totalRecordCount);
    // Only bucketId = 0 was written
    Assert.assertEquals(1, stats.writersPerBucket.size());
    Assert.assertEquals(1, stats.filesPerBucket.size());
    Assert.assertTrue(stats.writersPerBucket.containsKey(0));
    Assert.assertTrue(stats.filesPerBucket.containsKey(0));
    // Valid writers
    List<Integer> validWriters = Arrays.asList(0, 4);
    Assert.assertEquals(validWriters, stats.writersPerBucket.get(0));
    // Num writers = num created files
    Assert.assertEquals(validWriters.size(), stats.filesPerBucket.get(0).intValue());
    // TODO: verify all the records actually hash to bucketId = 0
    return stats;
  }

  // Test 2: N records of each bucket value, to all buckets, through the same partitions
  //  - 16 rows (2 writers per bucket, uneven last bucket), 4 buckets
  @Test
  public void testSendRecordsToAllBucketsEvenly() throws Exception {
    // Data generation
    // We make the number of requests a factor of the parallelism for an easier verification
    int totalNumRows = parallelism * 2;
    int numRowsPerBucket = totalNumRows / numBuckets;
    List<Row> rows = Lists.newArrayListWithCapacity(totalNumRows);
    List<Row> templateRows = Lists.newArrayListWithCapacity(numBuckets);
    for (int i = 0; i < numBuckets; i++) {
      templateRows.addAll(generateRowsForBucketId(1, i));
    }
    for (int i = 0; i < numRowsPerBucket; i++) {
      rows.addAll(templateRows);
    }

    // Writing to the table
    testWriteRowData(rows);
    // Extracting stats
    TableTestStats stats = extractTableTestStats();

    // Assertions
    // All the records were received
    Assert.assertEquals(totalNumRows, stats.totalRecordCount);
    // Only bucketId = 0 was written
    Assert.assertEquals(numBuckets, stats.writersPerBucket.size());
    Assert.assertEquals(numBuckets, stats.filesPerBucket.size());
    for (int i = 0, j = 4; i < numBuckets; i++, j++) {
      Assert.assertEquals(Arrays.asList(i, j), stats.writersPerBucket.get(i));
      Assert.assertTrue(stats.filesPerBucket.get(i) == 2);
    }
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
      rows.add(Row.of(i, value));
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
    final int totalRecordCount;
    final Map<Integer, List<Integer>> writersPerBucket; // <BucketId, Set<WriterId>>
    final Map<Integer, Integer> filesPerBucket; // <BucketId, NumFiles>
    final Map<Integer, Long> recordsPerFile; // <WriterId, NumRecords>

    TableTestStats(
        int totalRecordCount,
        Map<Integer, List<Integer>> writersPerBucket,
        Map<Integer, Integer> filesPerBucket,
        Map<Integer, Long> recordsPerFile) {
      this.totalRecordCount = totalRecordCount;
      this.writersPerBucket = writersPerBucket;
      this.filesPerBucket = filesPerBucket;
      this.recordsPerFile = recordsPerFile;
    }
  }
}
