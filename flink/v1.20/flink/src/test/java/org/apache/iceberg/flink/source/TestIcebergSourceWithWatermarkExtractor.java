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

import static org.apache.iceberg.flink.MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG;

import java.io.Serializable;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class TestIcebergSourceWithWatermarkExtractor implements Serializable {
  private static final int PARALLELISM = 4;
  private static final String SOURCE_NAME = "IcebergSource";
  private static final int RECORD_NUM_FOR_2_SPLITS = 200;
  private static final ConcurrentMap<Long, Integer> WINDOWS = Maps.newConcurrentMap();

  @TempDir protected Path temporaryFolder;

  private static final InMemoryReporter REPORTER = InMemoryReporter.createWithRetainedMetrics();

  @RegisterExtension
  public static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(PARALLELISM)
              .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
              .setConfiguration(REPORTER.addToConfiguration(DISABLE_CLASSLOADER_CHECK_CONFIG))
              .withHaLeadershipControl()
              .build());

  @RegisterExtension
  private static final HadoopTableExtension TABLE_EXTENSION =
      new HadoopTableExtension(TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.TS_SCHEMA);

  /**
   * This is an integration test for watermark handling and windowing. Integration testing the
   * following features:
   *
   * <ul>
   *   <li>- Ordering of the splits
   *   <li>- Emitting of watermarks
   *   <li>- Firing windows based on watermarks
   * </ul>
   *
   * <p>The test generates 4 splits
   *
   * <ul>
   *   <li>- Split 1 - Watermark 100 min
   *   <li>- Split 2, 3 - Watermark 0 min
   *   <li>- Split 4 - Watermark 6 min
   * </ul>
   *
   * <p>Creates a source with 5 minutes tumbling window with parallelism 1 (to prevent concurrency
   * issues).
   *
   * <p>Checks that windows are handled correctly based on the emitted watermarks, and splits are
   * read in the following order:
   *
   * <ul>
   *   <li>- Split 2, 3
   *   <li>- Split 4
   *   <li>- Split 1
   * </ul>
   *
   * <p>As a result the window aggregator emits the records based on in Split 2-3, and Split 4 data.
   *
   * <p>Add 2 more splits, so the task manager close the windows for the original 4 splits and emit
   * the appropriate aggregated records.
   */
  @Test
  public void testWindowing() throws Exception {
    GenericAppenderHelper dataAppender = appender();
    List<Record> expectedRecords = Lists.newArrayList();

    // Generate records with the following pattern:
    // - File 1 - Later records (Watermark 6000000)
    //    - Split 1 - 2 records (100, "file_1-recordTs_100"), (103, "file_1-recordTs_103")
    // - File 2 - First records (Watermark 0)
    //    - Split 1 - 100 records (0, "file_2-recordTs_0"), (1, "file_2-recordTs_1"),...
    //    - Split 2 - 100 records (0, "file_2-recordTs_0"), (1, "file_2-recordTs_1"),...
    // - File 3 - Parallel write for the first records (Watermark 360000)
    //    - Split 1 - 2 records (6, "file_3-recordTs_6"), (7, "file_3-recordTs_7")
    List<Record> batch =
        ImmutableList.of(
            generateRecord(100, "file_1-recordTs_100"),
            generateRecord(101, "file_1-recordTs_101"),
            generateRecord(103, "file_1-recordTs_103"));
    expectedRecords.addAll(batch);
    dataAppender.appendToTable(batch);

    batch = Lists.newArrayListWithCapacity(100);
    for (int i = 0; i < RECORD_NUM_FOR_2_SPLITS; ++i) {
      // Generate records where the timestamps are out of order, but still between 0-5 minutes
      batch.add(generateRecord(4 - i % 5, "file_2-recordTs_" + i));
    }
    expectedRecords.addAll(batch);
    dataAppender.appendToTable(batch);

    batch =
        ImmutableList.of(
            generateRecord(6, "file_3-recordTs_6"), generateRecord(7, "file_3-recordTs_7"));
    expectedRecords.addAll(batch);
    dataAppender.appendToTable(batch);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<RowData> stream =
        env.fromSource(
            source(),
            WatermarkStrategy.<RowData>noWatermarks()
                .withTimestampAssigner(new RowDataTimestampAssigner()),
            SOURCE_NAME,
            TypeInformation.of(RowData.class));

    stream
        .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
        .apply(
            new AllWindowFunction<RowData, RowData, TimeWindow>() {
              @Override
              public void apply(
                  TimeWindow window, Iterable<RowData> values, Collector<RowData> out) {
                // Emit RowData which contains the window start time, and the record count in
                // that window
                AtomicInteger count = new AtomicInteger(0);
                values.forEach(a -> count.incrementAndGet());
                out.collect(row(window.getStart(), count.get()));
                WINDOWS.put(window.getStart(), count.get());
              }
            });

    // Use static variable to collect the windows, since other solutions were flaky
    WINDOWS.clear();
    env.executeAsync("Iceberg Source Windowing Test");

    // Wait for the 2 first windows from File 2 and File 3
    Awaitility.await()
        .pollInterval(Duration.ofMillis(10))
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () ->
                WINDOWS.equals(
                    ImmutableMap.of(0L, RECORD_NUM_FOR_2_SPLITS, TimeUnit.MINUTES.toMillis(5), 2)));

    // Write data so the windows containing test data are closed
    dataAppender.appendToTable(
        dataAppender.writeFile(ImmutableList.of(generateRecord(1500, "last-record"))));

    // Wait for last test record window from File 1
    Awaitility.await()
        .pollInterval(Duration.ofMillis(10))
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () ->
                WINDOWS.equals(
                    ImmutableMap.of(
                        0L,
                        RECORD_NUM_FOR_2_SPLITS,
                        TimeUnit.MINUTES.toMillis(5),
                        2,
                        TimeUnit.MINUTES.toMillis(100),
                        3)));
  }

  /**
   * This is an integration test for watermark handling and throttling. Integration testing the
   * following:
   *
   * <ul>
   *   <li>- Emitting of watermarks
   *   <li>- Watermark alignment
   * </ul>
   *
   * <p>The test generates 3 splits
   *
   * <ul>
   *   <li>- Split 1 - Watermark 100 min
   *   <li>- Split 2, 3 - Watermark 0 min
   * </ul>
   *
   * The splits are read in the following order:
   *
   * <ul>
   *   <li>- Split 2, 3 (Task Manager 1, Task Manager 2)
   *   <li>- Split 1 (Task Manager 1 or ask Manager 2 depending on scheduling)
   * </ul>
   *
   * Reading split 1 will cause the watermark alignment to pause reading for the given task manager.
   *
   * <p>The status of the watermark alignment is checked by the alignment related metrics.
   *
   * <p>Adding new records with old timestamps to the table will enable the running reader to
   * continue reading the files, but the watermark alignment will still prevent the paused reader to
   * continue.
   *
   * <p>After adding some records with new timestamps the blocked reader is un-paused, and both ot
   * the readers continue reading.
   */
  @Test
  public void testThrottling(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
    GenericAppenderHelper dataAppender = appender();

    // Generate records in advance

    // File 1 - Later records (Watermark 6.000.000 - 100 min)
    //  - Split 1 - 2 records (100, "file_1-recordTs_100"), (103, "file_1-recordTs_103")
    List<Record> batch1 =
        ImmutableList.of(
            generateRecord(100, "file_1-recordTs_100"), generateRecord(103, "file_1-recordTs_103"));

    // File 2 - First records (Watermark 0 - 0 min)
    //  - Split 1 - 100 records (0, "file_2-recordTs_0"), (1, "file_2-recordTs_1"),...
    //  - Split 2 - 100 records (0, "file_2-recordTs_0"), (1, "file_2-recordTs_1"),...
    List<Record> batch2 = Lists.newArrayListWithCapacity(100);
    for (int i = 0; i < RECORD_NUM_FOR_2_SPLITS; ++i) {
      batch2.add(generateRecord(4 - i % 5, "file_2-recordTs_" + i));
    }

    // File 3 - Some records will be blocked (Watermark 900.000 - 15 min)
    List<Record> batch3 =
        ImmutableList.of(
            generateRecord(15, "file_3-recordTs_15"),
            generateRecord(16, "file_3-recordTs_16"),
            generateRecord(17, "file_3-recordTs_17"));

    // File 4 - Some records will be blocked (Watermark 900.000 - 15 min)
    List<Record> batch4 =
        ImmutableList.of(
            generateRecord(15, "file_4-recordTs_15"),
            generateRecord(16, "file_4-recordTs_16"),
            generateRecord(17, "file_4-recordTs_17"));

    // File 5 - Records which will remove the block (Watermark 5.400.000 - 90 min)
    List<Record> batch5 =
        ImmutableList.of(
            generateRecord(90, "file_5-recordTs_90"), generateRecord(91, "file_5-recordTs_91"));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    DataStream<RowData> stream =
        env.fromSource(
            source(),
            WatermarkStrategy.<RowData>noWatermarks()
                .withWatermarkAlignment("iceberg", Duration.ofMinutes(20), Duration.ofMillis(10)),
            SOURCE_NAME,
            TypeInformation.of(RowData.class));

    try (CloseableIterator<RowData> resultIterator = stream.collectAsync()) {
      JobClient jobClient = env.executeAsync("Iceberg Source Throttling Test");
      CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient.getJobID(), false);

      // Insert the first data into the table
      dataAppender.appendToTable(dataAppender.writeFile(batch1), dataAppender.writeFile(batch2));

      // Get the drift metric, wait for it to be created and reach the expected state
      // (100 min - 20 min - 0 min)
      // Also this validates that the WatermarkAlignment is working
      Awaitility.await()
          .pollInterval(Duration.ofMillis(10))
          .atMost(30, TimeUnit.SECONDS)
          .until(
              () ->
                  findAlignmentDriftMetric(jobClient.getJobID(), TimeUnit.MINUTES.toMillis(80))
                      .isPresent());
      Gauge<Long> drift =
          findAlignmentDriftMetric(jobClient.getJobID(), TimeUnit.MINUTES.toMillis(80)).get();

      // Add some old records with 2 splits, so even if the blocked gets one split, the other reader
      // one gets one as well
      dataAppender.appendToTable(dataAppender.writeFile(batch3), dataAppender.writeFile(batch4));

      // Get the drift metric, wait for it to be created and reach the expected state (100 min - 20
      // min - 15 min)
      Awaitility.await()
          .pollInterval(Duration.ofMillis(10))
          .atMost(30, TimeUnit.SECONDS)
          .until(() -> drift.getValue() == TimeUnit.MINUTES.toMillis(65));

      // Add some new records which should unblock the throttled reader
      dataAppender.appendToTable(batch5);

      // Wait for the new drift to decrease below the allowed drift to signal the normal state
      Awaitility.await()
          .pollInterval(Duration.ofMillis(10))
          .atMost(30, TimeUnit.SECONDS)
          .until(() -> drift.getValue() < TimeUnit.MINUTES.toMillis(20));
    }
  }

  protected IcebergSource<RowData> source() {
    return IcebergSource.<RowData>builder()
        .tableLoader(TABLE_EXTENSION.tableLoader())
        .watermarkColumn("ts")
        .project(TestFixtures.TS_SCHEMA)
        .splitSize(100L)
        .streaming(true)
        .monitorInterval(Duration.ofMillis(10))
        .streamingStartingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
  }

  protected Record generateRecord(int minutes, String str) {
    // Override the ts field to create a more realistic situation for event time alignment
    Record record = GenericRecord.create(TestFixtures.TS_SCHEMA);
    LocalDateTime ts =
        LocalDateTime.ofInstant(
            Instant.ofEpochMilli(Time.of(minutes, TimeUnit.MINUTES).toMilliseconds()),
            ZoneId.of("Z"));
    record.setField("ts", ts);
    record.setField("str", str);
    return record;
  }

  private Optional<Gauge<Long>> findAlignmentDriftMetric(JobID jobID, long withValue) {
    String metricsName = SOURCE_NAME + ".*" + MetricNames.WATERMARK_ALIGNMENT_DRIFT;
    return REPORTER.findMetrics(jobID, metricsName).values().stream()
        .map(m -> (Gauge<Long>) m)
        .filter(m -> m.getValue() == withValue)
        .findFirst();
  }

  private GenericAppenderHelper appender() {
    // We need to create multiple splits, so we need to generate parquet files with multiple offsets
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    hadoopConf.set("write.parquet.page-size-bytes", "64");
    hadoopConf.set("write.parquet.row-group-size-bytes", "64");
    return new GenericAppenderHelper(
        TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder, hadoopConf);
  }

  private static RowData row(long time, long count) {
    GenericRowData result = new GenericRowData(2);
    result.setField(0, time);
    result.setField(1, String.valueOf(count));
    return result;
  }

  private static class RowDataTimestampAssigner implements SerializableTimestampAssigner<RowData> {
    @Override
    public long extractTimestamp(RowData element, long recordTimestamp) {
      return element.getTimestamp(0, 0).getMillisecond();
    }
  }
}
