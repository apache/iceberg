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
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceContinuous {

  public static final InMemoryReporter METRIC_REPORTER = InMemoryReporter.create();

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled(METRIC_REPORTER);

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

      assertThatIcebergEnumeratorMetricsExist();
    }
  }

  @Test
  public void testTableScanThenIncrementalAfterExpiration() throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    // snapshot1
    List<Record> batch1 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch1);
    long snapshotId = tableResource.table().currentSnapshot().snapshotId();

    // snapshot2
    List<Record> batch2 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batch2);

    tableResource.table().expireSnapshots().expireSnapshotId(snapshotId).commit();

    Assert.assertEquals(1, tableResource.table().history().size());

    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .monitorInterval(Duration.ofMillis(10L))
            .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            .build();

    Assert.assertEquals(
        FlinkSplitPlanner.ScanMode.BATCH, FlinkSplitPlanner.checkScanMode(scanContext));

    try (CloseableIterator<Row> iter =
        createStream(scanContext).executeAndCollect(getClass().getSimpleName())) {
      List<Row> result1 = waitForResult(iter, 4);
      List<Record> initialRecords = Lists.newArrayList();
      initialRecords.addAll(batch1);
      initialRecords.addAll(batch2);
      TestHelpers.assertRecords(result1, initialRecords, tableResource.table().schema());

      // snapshot3
      List<Record> batch3 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batch3);
      tableResource.table().currentSnapshot().snapshotId();

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());

      assertThatIcebergEnumeratorMetricsExist();
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

      assertThatIcebergEnumeratorMetricsExist();
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

      assertThatIcebergEnumeratorMetricsExist();
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

      assertThatIcebergEnumeratorMetricsExist();
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

      assertThatIcebergEnumeratorMetricsExist();
    }
  }

  @Test
  public void testReadingFromBranch() throws Exception {
    String branch = "b1";
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(tableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    List<Record> batchBase =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(batchBase);

    // create branch
    tableResource
        .table()
        .manageSnapshots()
        .createBranch(branch, tableResource.table().currentSnapshot().snapshotId())
        .commit();

    // snapshot1 to branch
    List<Record> batch1 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
    dataAppender.appendToTable(branch, batch1);

    // snapshot2 to branch
    List<Record> batch2 =
        RandomGenericData.generate(tableResource.table().schema(), 2, randomSeed.incrementAndGet());
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
      TestHelpers.assertRecords(resultMain, branchExpectedRecords, tableResource.table().schema());

      // snapshot3 to branch
      List<Record> batch3 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(branch, batch3);

      List<Row> result3 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result3, batch3, tableResource.table().schema());

      // snapshot4 to branch
      List<Record> batch4 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(branch, batch4);

      List<Row> result4 = waitForResult(iter, 2);
      TestHelpers.assertRecords(result4, batch4, tableResource.table().schema());
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
      TestHelpers.assertRecords(resultMain, batchBase, tableResource.table().schema());

      List<Record> batchMain2 =
          RandomGenericData.generate(
              tableResource.table().schema(), 2, randomSeed.incrementAndGet());
      dataAppender.appendToTable(batchMain2);
      resultMain = waitForResult(iter, 2);
      TestHelpers.assertRecords(resultMain, batchMain2, tableResource.table().schema());
    }
  }

  @Test
  public void testValidation() {
    assertThatThrownBy(
            () ->
                IcebergSource.forRowData()
                    .tableLoader(tableResource.tableLoader())
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
                    .tableLoader(tableResource.tableLoader())
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
