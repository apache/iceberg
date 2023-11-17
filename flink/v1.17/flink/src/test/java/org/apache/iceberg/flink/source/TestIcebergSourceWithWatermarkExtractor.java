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

import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.DEFAULT_COLLECT_DATA_TIMEOUT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceWithWatermarkExtractor implements Serializable {
  private static final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();
  private static final int PARALLELISM = 4;
  private static final String SOURCE_NAME = "IcebergSource";
  private static final int RECORD_NUM_FOR_2_SPLITS = 200;

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final MiniClusterWithClientResource miniClusterResource =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(PARALLELISM)
              .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
              .setConfiguration(reporter.addToConfiguration(new Configuration()))
              .withHaLeadershipControl()
              .build());

  @Rule
  public final HadoopTableResource sourceTableResource =
      new HadoopTableResource(
          TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.TS_SCHEMA);

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
    // - File 3 - Parallel write for the first records (Watermark 60000)
    //    - Split 1 - 2 records (1, "file_3-recordTs_1"), (3, "file_3-recordTs_3")
    List<Record> batch = ImmutableList.of(generateRecord(100, "100"), generateRecord(103, "103"));
    expectedRecords.addAll(batch);
    dataAppender.appendToTable(batch);

    batch = Lists.newArrayListWithCapacity(100);
    for (int i = 0; i < RECORD_NUM_FOR_2_SPLITS; ++i) {
      batch.add(generateRecord(i % 5, "file_2-recordTs_" + i));
    }
    expectedRecords.addAll(batch);
    dataAppender.appendToTable(batch);

    batch =
        ImmutableList.of(
            generateRecord(1, "file_3-recordTs_1"), generateRecord(3, "file_3-recordTs_3"));
    expectedRecords.addAll(batch);
    dataAppender.appendToTable(batch);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    DataStream<RowData> stream =
        env.fromSource(
            sourceBuilder()
                .streaming(true)
                .monitorInterval(Duration.ofMillis(10))
                .streamingStartingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
                .build(),
            WatermarkStrategy.<RowData>noWatermarks()
                .withTimestampAssigner(new RowDataTimestampAssigner()),
            SOURCE_NAME,
            TypeInformation.of(RowData.class));
    DataStream<RowData> windowed =
        stream
            .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
            .apply(
                new AllWindowFunction<RowData, RowData, TimeWindow>() {
                  @Override
                  public void apply(
                      TimeWindow window, Iterable<RowData> values, Collector<RowData> out) {
                    // Just print all the data to confirm everything has arrived
                    values.forEach(out::collect);
                  }
                });

    try (CloseableIterator<RowData> resultIterator = windowed.collectAsync()) {
      env.executeAsync("Iceberg Source Windowing Test");

      // Write data so the windows containing test data are closed
      dataAppender.appendToTable(ImmutableList.of(generateRecord(1500, "last-record")));
      dataAppender.appendToTable(ImmutableList.of(generateRecord(1500, "last-record")));
      dataAppender.appendToTable(ImmutableList.of(generateRecord(1500, "last-record")));

      assertRecords(resultIterator, expectedRecords);
    }
  }

  @Test
  public void testThrottling() throws Exception {
    GenericAppenderHelper dataAppender = appender();

    // Generate records with the following pattern:
    // - File 1 - Later records (Watermark 6000000)
    //    - Split 1 - 2 records (100, "file_1-recordTs_100"), (103, "file_1-recordTs_103")
    // - File 2 - First records (Watermark 0)
    //    - Split 1 - 100 records (0, "file_2-recordTs_0"), (1, "file_2-recordTs_1"),...
    //    - Split 2 - 100 records (0, "file_2-recordTs_0"), (1, "file_2-recordTs_1"),...
    List<Record> batch;
    batch =
        ImmutableList.of(
            generateRecord(100, "file_1-recordTs_100"), generateRecord(103, "file_1-recordTs_103"));
    dataAppender.appendToTable(batch);

    batch = Lists.newArrayListWithCapacity(100);
    for (int i = 0; i < RECORD_NUM_FOR_2_SPLITS; ++i) {
      batch.add(generateRecord(i % 5, "file_2-recordTs_" + i));
    }

    dataAppender.appendToTable(batch);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    DataStream<RowData> stream =
        env.fromSource(
            sourceBuilder()
                .streaming(true)
                .monitorInterval(Duration.ofMillis(10))
                .streamingStartingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
                .build(),
            WatermarkStrategy.<RowData>noWatermarks()
                .withWatermarkAlignment("iceberg", Duration.ofMinutes(20), Duration.ofMillis(10)),
            SOURCE_NAME,
            TypeInformation.of(RowData.class));

    try (CloseableIterator<RowData> resultIterator = stream.collectAsync()) {
      JobClient jobClient = env.executeAsync("Continuous Iceberg Source Failover Test");

      // Check that the read the non-blocked data
      // The first RECORD_NUM_FOR_2_SPLITS should be read
      // 1 or more from the runaway reader should be arrived depending on thread scheduling
      waitForRecords(resultIterator, RECORD_NUM_FOR_2_SPLITS + 1);

      // Get the drift metric, wait for it to be created and reach the expected state
      // (100 min - 20 min - 0 min)
      // Also this validates that the WatermarkAlignment is working
      Awaitility.await()
          .atMost(120, TimeUnit.SECONDS)
          .until(
              () ->
                  findAlignmentDriftMetric(jobClient.getJobID(), TimeUnit.MINUTES.toMillis(80))
                      .isPresent());
      Gauge<Long> drift =
          findAlignmentDriftMetric(jobClient.getJobID(), TimeUnit.MINUTES.toMillis(80)).get();

      // Add some old records with 2 splits, so even if the blocked gets one split, the other reader
      // one gets one as well
      batch =
          ImmutableList.of(
              generateRecord(15, "file_3-recordTs_15"),
              generateRecord(16, "file_3-recordTs_16"),
              generateRecord(17, "file_3-recordTs_17"));
      dataAppender.appendToTable(batch);
      batch =
          ImmutableList.of(
              generateRecord(15, "file_4-recordTs_15"),
              generateRecord(16, "file_4-recordTs_16"),
              generateRecord(17, "file_4-recordTs_17"));
      dataAppender.appendToTable(batch);
      // The records received will highly depend on scheduling
      // We minimally get 3 records from the non-blocked reader
      // We might get 1 record from the blocked reader (as part of the previous batch -
      // file1-record-ts)
      // We might get 3 records form the non-blocked reader if it gets both new splits
      waitForRecords(resultIterator, 3);

      // Get the drift metric, wait for it to be created and reach the expected state (100 min - 20
      // min - 15 min)
      Awaitility.await()
          .atMost(120, TimeUnit.SECONDS)
          .until(() -> drift.getValue() == TimeUnit.MINUTES.toMillis(65));

      // Add some new records which should unblock the throttled reader
      batch =
          ImmutableList.of(
              generateRecord(110, "file_5-recordTs_110"),
              generateRecord(111, "file_5-recordTs_111"));
      dataAppender.appendToTable(batch);
      // We should get all the records at this point
      waitForRecords(resultIterator, 6);

      // Wait for the new drift to decrease below the allowed drift to signal the normal state
      Awaitility.await()
          .atMost(120, TimeUnit.SECONDS)
          .until(() -> drift.getValue() < TimeUnit.MINUTES.toMillis(20));
    }
  }

  protected IcebergSource.Builder<RowData> sourceBuilder() {
    return IcebergSource.<RowData>builder()
        .tableLoader(sourceTableResource.tableLoader())
        .watermarkColumn("ts")
        .project(TestFixtures.TS_SCHEMA)
        .splitSize(100L);
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

  protected void assertRecords(CloseableIterator<RowData> iterator, List<Record> expectedRecords)
      throws Exception {

    Set<RowData> received = Sets.newHashSetWithExpectedSize(expectedRecords.size());

    assertThat(
            CompletableFuture.supplyAsync(
                () -> {
                  int count = 0;
                  while (count < expectedRecords.size() && iterator.hasNext()) {
                    received.add(iterator.next());
                    count++;
                  }

                  if (count < expectedRecords.size()) {
                    throw new IllegalStateException(
                        String.format("Fail to get %d records.", expectedRecords.size()));
                  }

                  return true;
                }))
        .succeedsWithin(DEFAULT_COLLECT_DATA_TIMEOUT);

    Set<RowData> expected =
        expectedRecords.stream()
            .map(e -> RowDataConverter.convert(TestFixtures.TS_SCHEMA, e))
            .collect(Collectors.toSet());
    Assert.assertEquals(expected, received);
  }

  protected void waitForRecords(CloseableIterator<RowData> iterator, int num) {
    assertThat(
            CompletableFuture.supplyAsync(
                () -> {
                  int count = 0;
                  while (count < num && iterator.hasNext()) {
                    iterator.next();
                    count++;
                  }

                  if (count < num) {
                    throw new IllegalStateException(String.format("Fail to get %d records.", num));
                  }

                  return true;
                }))
        .succeedsWithin(DEFAULT_COLLECT_DATA_TIMEOUT);
  }

  private Optional<Gauge<Long>> findAlignmentDriftMetric(JobID jobID, long withValue) {
    String metricsName = SOURCE_NAME + ".*" + MetricNames.WATERMARK_ALIGNMENT_DRIFT;
    return reporter.findMetrics(jobID, metricsName).values().stream()
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
        sourceTableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER, hadoopConf);
  }

  private static class RowDataTimestampAssigner implements SerializableTimestampAssigner<RowData> {
    @Override
    public long extractTimestamp(RowData element, long recordTimestamp) {
      return element.getTimestamp(0, 0).getMillisecond();
    }
  }
}
