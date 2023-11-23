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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceFailover {

  private static final int PARALLELISM = 4;

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final MiniClusterWithClientResource miniClusterResource =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(PARALLELISM)
              .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
              .withHaLeadershipControl()
              .build());

  @Rule
  public final HadoopTableResource sourceTableResource =
      new HadoopTableResource(
          TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE, schema());

  @Rule
  public final HadoopTableResource sinkTableResource =
      new HadoopTableResource(
          TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.SINK_TABLE, schema());

  protected IcebergSource.Builder<RowData> sourceBuilder() {
    Configuration config = new Configuration();
    config.setInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 128);
    return IcebergSource.forRowData()
        .tableLoader(sourceTableResource.tableLoader())
        .assignerFactory(new SimpleSplitAssignerFactory())
        .flinkConfig(config);
  }

  protected Schema schema() {
    return TestFixtures.SCHEMA;
  }

  protected List<Record> generateRecords(int numRecords, long seed) {
    return RandomGenericData.generate(schema(), numRecords, seed);
  }

  @Test
  public void testBoundedWithTaskManagerFailover() throws Exception {
    testBoundedIcebergSource(FailoverType.TM);
  }

  @Test
  public void testBoundedWithJobManagerFailover() throws Exception {
    testBoundedIcebergSource(FailoverType.JM);
  }

  private void testBoundedIcebergSource(FailoverType failoverType) throws Exception {
    List<Record> expectedRecords = Lists.newArrayList();
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(
            sourceTableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);
    for (int i = 0; i < 4; ++i) {
      List<Record> records = generateRecords(2, i);
      expectedRecords.addAll(records);
      dataAppender.appendToTable(records);
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

    DataStream<RowData> stream =
        env.fromSource(
            sourceBuilder().build(),
            WatermarkStrategy.noWatermarks(),
            "IcebergSource",
            TypeInformation.of(RowData.class));

    DataStream<RowData> streamFailingInTheMiddleOfReading =
        RecordCounterToFail.wrapWithFailureAfter(stream, expectedRecords.size() / 2);

    // CollectStreamSink from DataStream#executeAndCollect() doesn't guarantee
    // exactly-once behavior. When Iceberg sink, we can verify end-to-end
    // exactly-once. Here we mainly about source exactly-once behavior.
    FlinkSink.forRowData(streamFailingInTheMiddleOfReading)
        .table(sinkTableResource.table())
        .tableLoader(sinkTableResource.tableLoader())
        .append();

    JobClient jobClient = env.executeAsync("Bounded Iceberg Source Failover Test");
    JobID jobId = jobClient.getJobID();

    RecordCounterToFail.waitToFail();
    triggerFailover(
        failoverType,
        jobId,
        RecordCounterToFail::continueProcessing,
        miniClusterResource.getMiniCluster());

    SimpleDataUtil.assertTableRecords(
        sinkTableResource.table(), expectedRecords, Duration.ofSeconds(120));
  }

  @Test
  public void testContinuousWithTaskManagerFailover() throws Exception {
    testContinuousIcebergSource(FailoverType.TM);
  }

  @Test
  public void testContinuousWithJobManagerFailover() throws Exception {
    testContinuousIcebergSource(FailoverType.JM);
  }

  private void testContinuousIcebergSource(FailoverType failoverType) throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(
            sourceTableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);
    List<Record> expectedRecords = Lists.newArrayList();

    List<Record> batch = generateRecords(2, 0);
    expectedRecords.addAll(batch);
    dataAppender.appendToTable(batch);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.enableCheckpointing(10L);
    Configuration config = new Configuration();
    config.setInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 128);

    DataStream<RowData> stream =
        env.fromSource(
            sourceBuilder()
                .streaming(true)
                .monitorInterval(Duration.ofMillis(10))
                .streamingStartingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
                .build(),
            WatermarkStrategy.noWatermarks(),
            "IcebergSource",
            TypeInformation.of(RowData.class));

    // CollectStreamSink from DataStream#executeAndCollect() doesn't guarantee
    // exactly-once behavior. When Iceberg sink, we can verify end-to-end
    // exactly-once. Here we mainly about source exactly-once behavior.
    FlinkSink.forRowData(stream)
        .table(sinkTableResource.table())
        .tableLoader(sinkTableResource.tableLoader())
        .append();

    JobClient jobClient = env.executeAsync("Continuous Iceberg Source Failover Test");
    JobID jobId = jobClient.getJobID();

    for (int i = 1; i < 5; i++) {
      Thread.sleep(10);
      List<Record> records = generateRecords(2, i);
      expectedRecords.addAll(records);
      dataAppender.appendToTable(records);
      if (i == 2) {
        triggerFailover(failoverType, jobId, () -> {}, miniClusterResource.getMiniCluster());
      }
    }

    // wait longer for continuous source to reduce flakiness
    // because CI servers tend to be overloaded.
    SimpleDataUtil.assertTableRecords(
        sinkTableResource.table(), expectedRecords, Duration.ofSeconds(120));
  }

  // ------------------------------------------------------------------------
  // test utilities copied from Flink's FileSourceTextLinesITCase
  // ------------------------------------------------------------------------

  private enum FailoverType {
    NONE,
    TM,
    JM
  }

  private static void triggerFailover(
      FailoverType type, JobID jobId, Runnable afterFailAction, MiniCluster miniCluster)
      throws Exception {
    switch (type) {
      case NONE:
        afterFailAction.run();
        break;
      case TM:
        restartTaskManager(afterFailAction, miniCluster);
        break;
      case JM:
        triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
        break;
    }
  }

  private static void triggerJobManagerFailover(
      JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
    HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
    haLeadershipControl.revokeJobMasterLeadership(jobId).get();
    afterFailAction.run();
    haLeadershipControl.grantJobMasterLeadership(jobId).get();
  }

  private static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
      throws Exception {
    miniCluster.terminateTaskManager(0).get();
    afterFailAction.run();
    miniCluster.startTaskManager();
  }

  private static class RecordCounterToFail {

    private static AtomicInteger records;
    private static CompletableFuture<Void> fail;
    private static CompletableFuture<Void> continueProcessing;

    private static <T> DataStream<T> wrapWithFailureAfter(DataStream<T> stream, int failAfter) {

      records = new AtomicInteger();
      fail = new CompletableFuture<>();
      continueProcessing = new CompletableFuture<>();
      return stream.map(
          record -> {
            boolean reachedFailPoint = records.incrementAndGet() > failAfter;
            boolean notFailedYet = !fail.isDone();
            if (notFailedYet && reachedFailPoint) {
              fail.complete(null);
              continueProcessing.get();
            }
            return record;
          });
    }

    private static void waitToFail() throws ExecutionException, InterruptedException {
      fail.get();
    }

    private static void continueProcessing() {
      continueProcessing.complete(null);
    }
  }
}
