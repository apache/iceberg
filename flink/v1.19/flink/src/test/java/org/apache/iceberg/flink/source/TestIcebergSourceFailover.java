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

import static org.apache.iceberg.flink.SimpleDataUtil.tableRecords;
import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 120)
public class TestIcebergSourceFailover {

  // Parallelism higher than 1, but lower than the number of splits used by some of our tests
  // The goal is to allow some splits to remain in the enumerator when restoring the state
  private static final int PARALLELISM = 2;
  private static final int DO_NOT_FAIL = Integer.MAX_VALUE;
  protected static final MiniClusterResourceConfiguration MINI_CLUSTER_RESOURCE_CONFIG =
      new MiniClusterResourceConfiguration.Builder()
          .setNumberTaskManagers(1)
          .setNumberSlotsPerTaskManager(PARALLELISM)
          .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
          .withHaLeadershipControl()
          .build();

  @RegisterExtension
  public static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      new MiniClusterExtension(MINI_CLUSTER_RESOURCE_CONFIG);

  @TempDir protected Path temporaryFolder;

  @RegisterExtension
  protected static final HadoopTableExtension SOURCE_TABLE_EXTENSION =
      new HadoopTableExtension(DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  @RegisterExtension
  private static final HadoopTableExtension SINK_TABLE_EXTENSION =
      new HadoopTableExtension(DATABASE, TestFixtures.SINK_TABLE, TestFixtures.SCHEMA);

  protected IcebergSource.Builder<RowData> sourceBuilder() {
    Configuration config = new Configuration();
    return IcebergSource.forRowData()
        .tableLoader(SOURCE_TABLE_EXTENSION.tableLoader())
        .assignerFactory(new SimpleSplitAssignerFactory())
        // Prevent combining splits
        .set(
            FlinkReadOptions.SPLIT_FILE_OPEN_COST,
            Long.toString(TableProperties.SPLIT_SIZE_DEFAULT))
        .flinkConfig(config);
  }

  protected Schema schema() {
    return TestFixtures.SCHEMA;
  }

  protected List<Record> generateRecords(int numRecords, long seed) {
    return RandomGenericData.generate(schema(), numRecords, seed);
  }

  protected void assertRecords(Table table, List<Record> expectedRecords, Duration timeout)
      throws Exception {
    SimpleDataUtil.assertTableRecords(table, expectedRecords, timeout);
  }

  @Test
  public void testBoundedWithSavepoint(@InjectClusterClient ClusterClient<?> clusterClient)
      throws Exception {
    List<Record> expectedRecords = Lists.newArrayList();
    Table sinkTable = SINK_TABLE_EXTENSION.table();
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(
            SOURCE_TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);
    for (int i = 0; i < 4; ++i) {
      List<Record> records = generateRecords(2, i);
      expectedRecords.addAll(records);
      dataAppender.appendToTable(records);
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    createBoundedStreams(env, 2);

    JobClient jobClient = env.executeAsync("Bounded Iceberg Source Savepoint Test");
    JobID jobId = jobClient.getJobID();

    // Write something, but do not finish before checkpoint is created
    RecordCounterToWait.waitForCondition();
    CompletableFuture<String> savepoint =
        clusterClient.stopWithSavepoint(
            jobId, false, temporaryFolder.toString(), SavepointFormatType.CANONICAL);
    RecordCounterToWait.continueProcessing();

    // Wait for the job to stop with the savepoint
    String savepointPath = savepoint.get();

    // We expect that at least a few records has written
    assertThat(tableRecords(sinkTable)).hasSizeGreaterThan(0);

    // New env from the savepoint
    Configuration conf = new Configuration();
    conf.set(SavepointConfigOptions.SAVEPOINT_PATH, savepointPath);
    env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    createBoundedStreams(env, DO_NOT_FAIL);

    env.execute("Bounded Iceberg Source Savepoint Test");

    // We expect no duplications
    assertRecords(sinkTable, expectedRecords, Duration.ofSeconds(120));
  }

  @Test
  public void testBoundedWithTaskManagerFailover() throws Exception {
    runTestWithNewMiniCluster(
        miniCluster -> testBoundedIcebergSource(FailoverType.TM, miniCluster));
  }

  @Test
  public void testBoundedWithJobManagerFailover() throws Exception {
    runTestWithNewMiniCluster(
        miniCluster -> testBoundedIcebergSource(FailoverType.JM, miniCluster));
  }

  private void testBoundedIcebergSource(FailoverType failoverType, MiniCluster miniCluster)
      throws Exception {
    List<Record> expectedRecords = Lists.newArrayList();
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(
            SOURCE_TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);
    for (int i = 0; i < 4; ++i) {
      List<Record> records = generateRecords(2, i);
      expectedRecords.addAll(records);
      dataAppender.appendToTable(records);
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
    createBoundedStreams(env, 2);

    JobClient jobClient = env.executeAsync("Bounded Iceberg Source Failover Test");
    JobID jobId = jobClient.getJobID();

    RecordCounterToWait.waitForCondition();
    triggerFailover(failoverType, jobId, RecordCounterToWait::continueProcessing, miniCluster);

    assertRecords(SINK_TABLE_EXTENSION.table(), expectedRecords, Duration.ofSeconds(120));
  }

  @Test
  public void testContinuousWithTaskManagerFailover() throws Exception {
    runTestWithNewMiniCluster(
        miniCluster -> testContinuousIcebergSource(FailoverType.TM, miniCluster));
  }

  @Test
  public void testContinuousWithJobManagerFailover() throws Exception {
    runTestWithNewMiniCluster(
        miniCluster -> testContinuousIcebergSource(FailoverType.JM, miniCluster));
  }

  private void testContinuousIcebergSource(FailoverType failoverType, MiniCluster miniCluster)
      throws Exception {
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(
            SOURCE_TABLE_EXTENSION.table(), FileFormat.PARQUET, temporaryFolder);
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
        .table(SINK_TABLE_EXTENSION.table())
        .tableLoader(SINK_TABLE_EXTENSION.tableLoader())
        .append();

    JobClient jobClient = env.executeAsync("Continuous Iceberg Source Failover Test");
    JobID jobId = jobClient.getJobID();

    for (int i = 1; i < 5; i++) {
      Thread.sleep(10);
      List<Record> records = generateRecords(2, i);
      expectedRecords.addAll(records);
      dataAppender.appendToTable(records);
      if (i == 2) {
        triggerFailover(failoverType, jobId, () -> {}, miniCluster);
      }
    }

    // wait longer for continuous source to reduce flakiness
    // because CI servers tend to be overloaded.
    assertRecords(SINK_TABLE_EXTENSION.table(), expectedRecords, Duration.ofSeconds(120));
  }

  private void createBoundedStreams(StreamExecutionEnvironment env, int failAfter) {
    env.setParallelism(PARALLELISM);

    DataStream<RowData> stream =
        env.fromSource(
            sourceBuilder().build(),
            WatermarkStrategy.noWatermarks(),
            "IcebergSource",
            TypeInformation.of(RowData.class));

    DataStream<RowData> streamFailingInTheMiddleOfReading =
        RecordCounterToWait.wrapWithFailureAfter(stream, failAfter);

    // CollectStreamSink from DataStream#executeAndCollect() doesn't guarantee
    // exactly-once behavior. When Iceberg sink, we can verify end-to-end
    // exactly-once. Here we mainly about source exactly-once behavior.
    FlinkSink.forRowData(streamFailingInTheMiddleOfReading)
        .table(SINK_TABLE_EXTENSION.table())
        .tableLoader(SINK_TABLE_EXTENSION.tableLoader())
        .append();
  }

  // ------------------------------------------------------------------------
  // test utilities copied from Flink's FileSourceTextLinesITCase
  // ------------------------------------------------------------------------

  private static void runTestWithNewMiniCluster(ThrowingConsumer<MiniCluster, Exception> testMethod)
      throws Exception {
    MiniClusterWithClientResource miniCluster = null;
    try {
      miniCluster = new MiniClusterWithClientResource(MINI_CLUSTER_RESOURCE_CONFIG);
      miniCluster.before();
      testMethod.accept(miniCluster.getMiniCluster());
    } finally {
      if (miniCluster != null) {
        miniCluster.after();
      }
    }
  }

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

  private static class RecordCounterToWait {

    private static AtomicInteger records;
    private static CountDownLatch countDownLatch;
    private static CompletableFuture<Void> continueProcessing;

    private static <T> DataStream<T> wrapWithFailureAfter(DataStream<T> stream, int condition) {

      records = new AtomicInteger();
      continueProcessing = new CompletableFuture<>();
      countDownLatch = new CountDownLatch(stream.getParallelism());
      return stream.map(
          record -> {
            boolean reachedFailPoint = records.incrementAndGet() > condition;
            boolean notFailedYet = countDownLatch.getCount() != 0;
            if (notFailedYet && reachedFailPoint) {
              countDownLatch.countDown();
              continueProcessing.get();
            }
            return record;
          });
    }

    private static void waitForCondition() throws InterruptedException {
      countDownLatch.await();
    }

    private static void continueProcessing() {
      continueProcessing.complete(null);
    }
  }
}
