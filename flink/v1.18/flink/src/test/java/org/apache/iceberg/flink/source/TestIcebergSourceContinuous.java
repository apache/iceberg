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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.data.RowDataToRowMapper;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class TestIcebergSourceContinuous {

  public static final InMemoryReporter METRIC_REPORTER = InMemoryReporter.create();

  @TempDir protected Path temporaryFolder;

  @RegisterExtension
  public static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled(METRIC_REPORTER);

  @RegisterExtension
  private static final HadoopTableExtension TABLE_EXTENSION =
      new HadoopTableExtension(TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private final AtomicLong randomSeed = new AtomicLong(0L);

  @Test
  public void testTableScanThenIncremental() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
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
      TestHelpers.assertRecords(result1, batch1, TABLE_EXTENSION.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);
      TABLE_EXTENSION.table().currentSnapshot().snapshotId();

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, TABLE_EXTENSION.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);
      TABLE_EXTENSION.table().currentSnapshot().snapshotId();

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, TABLE_EXTENSION.table().schema());

      assertThatIcebergEnumeratorMetricsExist();
    }
  }

  @Test
  public void testTableScanThenIncrementalAfterExpiration() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);
    long snapshotId = TABLE_EXTENSION.table().currentSnapshot().snapshotId();

    // snapshot2
    List<Record> batch2 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch2);

    TABLE_EXTENSION.table().expireSnapshots().expireSnapshotId(snapshotId).commit();

    assertThat(TABLE_EXTENSION.table().history()).hasSize(1);

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();

    assertThat(FlinkSplitPlanner.checkScanMode(scanContext))
        .isEqualTo(FlinkSplitPlanner.ScanMode.BATCH);

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> result1 = waitForResult(iter, 4);
      List<Record> initialRecords = Lists.newArrayList();
      initialRecords.addAll(batch1);
      initialRecords.addAll(batch2);
      TestHelpers.assertRecords(result1, initialRecords, TABLE_EXTENSION.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);
      TABLE_EXTENSION.table().currentSnapshot().snapshotId();

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, TABLE_EXTENSION.table().schema());

      assertThatIcebergEnumeratorMetricsExist();
    }
  }

  @Test
  public void testEarliestSnapshot() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // snapshot0
    List<Record> batch0 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
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
      TestHelpers.assertRecords(result1, combinedBatch0AndBatch1, TABLE_EXTENSION.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, TABLE_EXTENSION.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, TABLE_EXTENSION.table().schema());

      assertThatIcebergEnumeratorMetricsExist();
    }
  }

  @Test
  public void testLatestSnapshot(@InjectClusterClient ClusterClient<?> clusterClient)
      throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // snapshot0
    List<Record> batch0 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
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
      waitUntilJobIsRunning(clusterClient);

      // inclusive behavior for starting snapshot
      List<Row> result1 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result1, batch1, TABLE_EXTENSION.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, TABLE_EXTENSION.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, TABLE_EXTENSION.table().schema());

      assertThatIcebergEnumeratorMetricsExist();
    }
  }

  @Test
  public void testSpecificSnapshotId() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // snapshot0
    List<Record> batch0 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);
    long snapshot0 = TABLE_EXTENSION.table().currentSnapshot().snapshotId();

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);
    long snapshot1 = TABLE_EXTENSION.table().currentSnapshot().snapshotId();

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
      TestHelpers.assertRecords(result1, batch1, TABLE_EXTENSION.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, TABLE_EXTENSION.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, TABLE_EXTENSION.table().schema());

      assertThatIcebergEnumeratorMetricsExist();
    }
  }

  @Test
  public void testSpecificSnapshotTimestamp() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    // snapshot0
    List<Record> batch0 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch0);
    long snapshot0Timestamp = TABLE_EXTENSION.table().currentSnapshot().timestampMillis();

    // sleep for 2 ms to make sure snapshot1 has a higher timestamp value
    Thread.sleep(2);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);
    long snapshot1Timestamp = TABLE_EXTENSION.table().currentSnapshot().timestampMillis();

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
      TestHelpers.assertRecords(result1, batch1, TABLE_EXTENSION.table().schema());

      // snapshot2
      List<Record> batch2 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch2);

      List<Row> result2 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result2, batch2, TABLE_EXTENSION.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, TABLE_EXTENSION.table().schema());

      assertThatIcebergEnumeratorMetricsExist();
    }
  }

  @Test
  public void testReadingFromBranch() throws Exception {
    String branch = "b1";
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);

    List<Record> batchBase =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batchBase);

    // create branch
    TABLE_EXTENSION
        .table()
        .manageSnapshots()
        .createBranch(branch, TABLE_EXTENSION.table().currentSnapshot().snapshotId())
        .commit();

    // snapshot1 to branch
    List<Record> batch1 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(branch, batch1);

    // snapshot2 to branch
    List<Record> batch2 =
        RandomGenericData.generate(
            TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(branch, batch2);

    List<Record> branchExpectedRecords = Lists.newArrayList();
    branchExpectedRecords.addAll(batchBase);
    branchExpectedRecords.addAll(batch1);
    branchExpectedRecords.addAll(batch2);
    // reads from branch: it should contain the first snapshot (before the branch creation) followed
    // by the next 2 snapshots added
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .useBranch(branch)
            .build();

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> resultMain = waitForResult(iter, 6);
      TestHelpers.assertRecords(
          resultMain, branchExpectedRecords, TABLE_EXTENSION.table().schema());

      // snapshot3 to branch
      List<Record> batch3 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(branch, batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, TABLE_EXTENSION.table().schema());

      // snapshot4 to branch
      List<Record> batch4 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(branch, batch4);

      List<Row> result4 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result4, batch4, TABLE_EXTENSION.table().schema());
    }

    // read only from main branch. Should contain only the first snapshot
    scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();
    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> resultMain = waitForResult(iter, 2);
      TestHelpers.assertRecords(resultMain, batchBase, TABLE_EXTENSION.table().schema());

      List<Record> batchMain2 =
          RandomGenericData.generate(
              TABLE_EXTENSION.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batchMain2);
      resultMain = waitForResult(iter, 2);
      TestHelpers.assertRecords(resultMain, batchMain2, TABLE_EXTENSION.table().schema());
    }
  }

  @Test
  public void testValidation() {
    assertThatThrownBy(
            () ->
                IcebergSource.forRowData()
                    .tableLoader(TABLE_EXTENSION.tableLoader())
                    .assignerFactory(new SimpleSplitAssignerFactory())
                    .streaming(true)
                    .endTag("tag")
                    .build())
        .hasMessage("Cannot set end-tag option for streaming reader")
        .isInstanceOf(IllegalArgumentException.class);
  }

  private DataStream<Row> createStream(ScanContext scanContext) throws Exception {
    // start the source and collect output
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    DataStream<Row> stream =
        env.fromSource(
                IcebergSource.forRowData()
                    .tableLoader(TABLE_EXTENSION.tableLoader())
                    .assignerFactory(new SimpleSplitAssignerFactory())
                    .streaming(scanContext.isStreaming())
                    .streamingStartingStrategy(scanContext.streamingStartingStrategy())
                    .startSnapshotTimestamp(scanContext.startSnapshotTimestamp())
                    .startSnapshotId(scanContext.startSnapshotId())
                    .monitorInterval(Duration.ofMillis(10L))
                    .branch(scanContext.branch())
                    .build(),
                WatermarkStrategy.noWatermarks(),
                "icebergSource",
                TypeInformation.of(RowData.class))
            .map(new RowDataToRowMapper(FlinkSchemaUtil.convert(TABLE_EXTENSION.table().schema())));
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

  public static void waitUntilJobIsRunning(ClusterClient<?> client) {
    Awaitility.await("job should be running")
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(10))
        .untilAsserted(() -> assertThat(getRunningJobs(client)).isNotEmpty());
  }

  public static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
    Collection<JobStatusMessage> statusMessages = client.listJobs().get();
    return statusMessages.stream()
        .filter(status -> status.getJobState() == JobStatus.RUNNING)
        .map(JobStatusMessage::getJobId)
        .collect(Collectors.toList());
  }

  private static void assertThatIcebergEnumeratorMetricsExist() {
    assertThatIcebergSourceMetricExists(
        "enumerator", "coordinator.enumerator.elapsedSecondsSinceLastSplitDiscovery");
    assertThatIcebergSourceMetricExists("enumerator", "coordinator.enumerator.unassignedSplits");
    assertThatIcebergSourceMetricExists("enumerator", "coordinator.enumerator.pendingRecords");
  }

  private static void assertThatIcebergSourceMetricExists(
      String metricGroupPattern, String metricName) {
    Optional<MetricGroup> groups = METRIC_REPORTER.findGroup(metricGroupPattern);
    assertThat(groups).isPresent();
    assertThat(
            METRIC_REPORTER.getMetricsByGroup(groups.get()).keySet().stream()
                .map(name -> groups.get().getMetricIdentifier(name)))
        .satisfiesOnlyOnce(
            fullMetricName -> assertThat(fullMetricName).containsSubsequence(metricName));
  }
}
