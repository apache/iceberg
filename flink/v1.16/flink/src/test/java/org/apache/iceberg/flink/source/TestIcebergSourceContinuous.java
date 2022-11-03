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
package org.apache.iceberg.flink.source;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceContinuous {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopTableResource tableResource =
      new HadoopTableResource(
          TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private final AtomicLong randomSeed = new AtomicLong(0L);

  @Test
  public void testTableScanThenIncremental() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, tableResource.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);
      tableResource.table().currentSnapshot().snapshotId();

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);
      tableResource.table().currentSnapshot().snapshotId();

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  @Test
  public void testEarliestSnapshot() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot0
    List<Record> batch0 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> result1 = waitForResult(iter, 4);
      List<Record> combinedBatch0AndBatch1 = Lists.newArrayList(batch0);
      combinedBatch0AndBatch1.addAll(batch1);
      TestHelpers.assertRecords(result1, combinedBatch0AndBatch1, tableResource.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  @Test
  public void testLatestSnapshot() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot0
    List<Record> batch0 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      // we want to make sure job is running first so that enumerator can
      // start from the latest snapshot before inserting the next batch2 below.
      waitUntilJobIsRunning(MINI_CLUSTER_RESOURCE.getClusterClient());

      // inclusive behavior for starting snapshot
      List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, tableResource.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  @Test
  public void testSpecificSnapshotId() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot0
    List<Record> batch0 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);
    long snapshot0 = tableResource.table().currentSnapshot().snapshotId();

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);
    long snapshot1 = tableResource.table().currentSnapshot().snapshotId();

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID)
            .startSnapshotId(snapshot1)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, tableResource.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  @Test
  public void testSpecificSnapshotTimestamp() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot0
    List<Record> batch0 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);
    long snapshot0Timestamp = tableResource.table().currentSnapshot().timestampMillis();

    // sleep for 2 ms to make sure snapshot1 has a higher timestamp value
    Thread.sleep(2);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);
    long snapshot1Timestamp = tableResource.table().currentSnapshot().timestampMillis();

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP)
            .startSnapshotTimestamp(snapshot1Timestamp)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      // consume data from snapshot1
      List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, tableResource.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, tableResource.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());
    }
  }

  private DataStream<Row> createStream(ScanContext scanContext) throws Exception {
    // start the source and collect output
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<Row> stream =
        env.fromSource(
                IcebergSource.forRowData()
                    .tableLoader(tableResource.tableLoader())
                    .assignerFactory(new SimpleSplitAssignerFactory())
                    .streaming(scanContext.isStreaming())
                    .streamingStartingStrategy(scanContext.streamingStartingStrategy())
                    .startSnapshotTimestamp(scanContext.startSnapshotTimestamp())
                    .startSnapshotId(scanContext.startSnapshotId())
                    .monitorInterval(Duration.ofMillis(10L))
                    .build(),
                WatermarkStrategy.noWatermarks(),
                "icebergSource",
                TypeInformation.of(RowData.class))
            .map(new RowDataToRowMapper(FlinkSchemaUtil.convert(tableResource.table().schema())));
    return stream;
  }

  public static List<Row> waitForResult(CloseableIterator<Row> iter, int limit) {
    List<Row> results = Lists.newArrayListWithCapacity(limit);
    while (results.size() < limit) {
      if (iter.hasNext()) {
        results.add(iter.next());
      } else {
        break;
      }
    }
    return results;
  }

  public static void waitUntilJobIsRunning(ClusterClient<?> client) throws Exception {
    while (getRunningJobs(client).isEmpty()) {
      Thread.sleep(10);
    }
  }

  public static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
    Collection<JobStatusMessage> statusMessages = client.listJobs().get();
    return statusMessages.stream()
        .filter(status -> status.getJobState() == JobStatus.RUNNING)
        .map(JobStatusMessage::getJobId)
        .collect(Collectors.toList());
  }
}
