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
package org.apache.iceberg.flink.maintenance.operator;

import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.FAILED_TASK_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.LAST_RUN_DURATION_MS;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.SUCCEEDED_TASK_COUNTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 10)
class TestLockRemoverOperation extends OperatorTestBase {
  private static final String[] TASKS = new String[] {"task0", "task1", "task2"};
  private static final String OPERATOR_NAME = "TestCoordinator";
  private static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);

  @TempDir private File checkpointDir;
  private TableMaintenanceCoordinator tableMaintenanceCoordinator;
  private EventReceivingTasks receivingTasks;

  @BeforeEach
  public void before() {
    MetricsReporterFactoryForTests.reset();
    this.receivingTasks = EventReceivingTasks.createForRunningTasks();
    this.tableMaintenanceCoordinator = createCoordinator();
    try {
      tableMaintenanceCoordinator.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  void after() throws IOException {
    super.after();
    try {
      tableMaintenanceCoordinator.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testProcess() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TaskResult> source = new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    source
        .dataStream()
        .transform(
            DUMMY_TASK_NAME,
            TypeInformation.of(Void.class),
            new LockRemoverOperatorFactory(DUMMY_TABLE_NAME, Lists.newArrayList(TASKS)))
        .forceNonParallel();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      tableMaintenanceCoordinator.handleEventFromOperator(
          0, 0, new LockAcquiredEvent(false, DUMMY_TABLE_NAME));
      waitForCoordinatorToProcessActions(tableMaintenanceCoordinator);
      assertThat(tableMaintenanceCoordinator.lockHeldSet()).containsExactly(DUMMY_TABLE_NAME);

      // Start a successful trigger for task1 and assert the return value is correct
      processAndCheck(source, new TaskResult(0, 0L, true, Lists.newArrayList()));

      // Assert that the lock is removed
      assertThat(tableMaintenanceCoordinator.lockHeldSet()).doesNotContain(DUMMY_TABLE_NAME);
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testMetrics() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TaskResult> source = new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    source
        .dataStream()
        .transform(
            DUMMY_TASK_NAME,
            TypeInformation.of(Void.class),
            new LockRemoverOperatorFactory(DUMMY_TABLE_NAME, Lists.newArrayList(TASKS)))
        .forceNonParallel();

    JobClient jobClient = null;
    long time = System.currentTimeMillis();
    try {
      jobClient = env.executeAsync();
      // Start the 2 successful and one failed result trigger for task1, and 3 successful for task2
      processAndCheck(source, new TaskResult(0, time, true, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(1, 0L, true, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(1, 0L, true, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(0, time, false, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(0, time, true, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(1, 0L, true, Lists.newArrayList()));

      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          ImmutableList.of(
                              DUMMY_TASK_NAME,
                              DUMMY_TABLE_NAME,
                              TASKS[1],
                              "1",
                              SUCCEEDED_TASK_COUNTER))
                      .equals(3L));

      // Final check all the counters
      MetricsReporterFactoryForTests.assertCounters(
          new ImmutableMap.Builder<List<String>, Long>()
              .put(
                  ImmutableList.of(
                      DUMMY_TASK_NAME, DUMMY_TABLE_NAME, TASKS[0], "0", SUCCEEDED_TASK_COUNTER),
                  2L)
              .put(
                  ImmutableList.of(
                      DUMMY_TASK_NAME, DUMMY_TABLE_NAME, TASKS[0], "0", FAILED_TASK_COUNTER),
                  1L)
              .put(
                  ImmutableList.of(
                      DUMMY_TASK_NAME, DUMMY_TABLE_NAME, TASKS[1], "1", SUCCEEDED_TASK_COUNTER),
                  3L)
              .put(
                  ImmutableList.of(
                      DUMMY_TASK_NAME, DUMMY_TABLE_NAME, TASKS[1], "1", FAILED_TASK_COUNTER),
                  0L)
              .put(
                  ImmutableList.of(
                      DUMMY_TASK_NAME, DUMMY_TABLE_NAME, TASKS[2], "2", SUCCEEDED_TASK_COUNTER),
                  0L)
              .put(
                  ImmutableList.of(
                      DUMMY_TASK_NAME, DUMMY_TABLE_NAME, TASKS[2], "2", FAILED_TASK_COUNTER),
                  0L)
              .build());

      assertThat(
              MetricsReporterFactoryForTests.gauge(
                  ImmutableList.of(
                      DUMMY_TASK_NAME, DUMMY_TABLE_NAME, TASKS[0], "0", LAST_RUN_DURATION_MS)))
          .isPositive();
      assertThat(
              MetricsReporterFactoryForTests.gauge(
                  ImmutableList.of(
                      DUMMY_TASK_NAME, DUMMY_TABLE_NAME, TASKS[1], "1", LAST_RUN_DURATION_MS)))
          .isGreaterThan(time);
      assertThat(
              MetricsReporterFactoryForTests.gauge(
                  ImmutableList.of(
                      DUMMY_TASK_NAME, DUMMY_TABLE_NAME, TASKS[2], "2", LAST_RUN_DURATION_MS)))
          .isZero();
    } finally {
      closeJobClient(jobClient);
    }
  }

  /**
   * The test checks if the recovery watermark is only removed if the watermark has arrived from
   * both upstream sources.
   *
   * @throws Exception if any
   */
  @Test
  void testRecovery() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    setAllTasksReady(1, tableMaintenanceCoordinator, receivingTasks);
    ManualSource<TaskResult> source1 =
        new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    ManualSource<TaskResult> source2 =
        new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    source1
        .dataStream()
        .transform(
            DUMMY_TASK_NAME,
            TypeInformation.of(Void.class),
            new LockRemoverOperatorFactory(DUMMY_TABLE_NAME, Lists.newArrayList(TASKS)))
        .forceNonParallel();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      tableMaintenanceCoordinator.handleEventFromOperator(
          0, 0, new LockAcquiredEvent(true, DUMMY_TABLE_NAME));
      waitForCoordinatorToProcessActions(tableMaintenanceCoordinator);
      assertThat(tableMaintenanceCoordinator.recoverLockHeldSet())
          .containsExactly(DUMMY_TABLE_NAME);

      processAndCheck(source1, new TaskResult(0, 0L, true, Lists.newArrayList()));

      source1.sendRecord(new TaskResult(0, 1L, true, Lists.newArrayList()));
      // we receive the second result - this will not happen in real use cases, but with this we can
      // be sure that the previous watermark is processed
      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          ImmutableList.of(
                              DUMMY_TASK_NAME,
                              DUMMY_TABLE_NAME,
                              TASKS[0],
                              "0",
                              SUCCEEDED_TASK_COUNTER))
                      .equals(2L));

      // We did not remove the recovery lock, as no watermark received from the other source
      assertThat(tableMaintenanceCoordinator.recoverLockHeldSet())
          .containsExactly(DUMMY_TABLE_NAME);

      // Recovery arrives
      source1.sendWatermark(10L);
      source2.sendWatermark(10L);

      Awaitility.await()
          .until(
              () -> !tableMaintenanceCoordinator.recoverLockHeldSet().contains(DUMMY_TABLE_NAME));
    } finally {
      closeJobClient(jobClient);
    }
  }

  private void processAndCheck(ManualSource<TaskResult> source, TaskResult input) {
    processAndCheck(source, input, null);
  }

  private void processAndCheck(
      ManualSource<TaskResult> source, TaskResult input, String counterPrefix) {
    List<String> counterKey =
        ImmutableList.of(
            (counterPrefix != null ? counterPrefix : "") + DUMMY_TASK_NAME,
            DUMMY_TABLE_NAME,
            TASKS[input.taskIndex()],
            String.valueOf(input.taskIndex()),
            input.success() ? SUCCEEDED_TASK_COUNTER : FAILED_TASK_COUNTER);
    Long counterValue = MetricsReporterFactoryForTests.counter(counterKey);
    Long expected = counterValue != null ? counterValue + 1 : 1L;

    source.sendRecord(input);
    source.sendWatermark(input.startEpoch());

    Awaitility.await()
        .until(() -> expected.equals(MetricsReporterFactoryForTests.counter(counterKey)));
  }

  private static class SinkTest
      implements Sink<TaskResult>,
          SupportsCommitter<TaskResult>,
          SupportsPostCommitTopology<TaskResult> {
    @Override
    public SinkWriter<TaskResult> createWriter(WriterInitContext initContext) {
      return new CommittingSinkWriter<TaskResult, TaskResult>() {
        private final Collection<TaskResult> received = Lists.newArrayList();

        @Override
        public Collection<TaskResult> prepareCommit() {
          Collection<TaskResult> result = Lists.newArrayList(received);
          received.clear();
          return result;
        }

        @Override
        public void write(TaskResult taskResult, Context context) {
          received.add(taskResult);
        }

        @Override
        public void flush(boolean b) {
          // noop
        }

        @Override
        public void close() {
          // noop
        }
      };
    }

    @Override
    public Committer<TaskResult> createCommitter(CommitterInitContext committerInitContext) {
      return new Committer<>() {
        @Override
        public void commit(Collection<CommitRequest<TaskResult>> collection) {
          // noop
        }

        @Override
        public void close() {
          // noop
        }
      };
    }

    @Override
    public SimpleVersionedSerializer<TaskResult> getCommittableSerializer() {
      return new SimpleVersionedSerializer<>() {
        @Override
        public int getVersion() {
          return 0;
        }

        @Override
        public byte[] serialize(TaskResult taskResult) {
          return new byte[0];
        }

        @Override
        public TaskResult deserialize(int i, byte[] bytes) {
          return null;
        }
      };
    }

    @Override
    public void addPostCommitTopology(DataStream<CommittableMessage<TaskResult>> committables) {
      committables
          .flatMap(
              new FlatMapFunction<CommittableMessage<TaskResult>, TaskResult>() {
                @Override
                public void flatMap(
                    CommittableMessage<TaskResult> taskResultCommittableMessage,
                    Collector<TaskResult> collector) {
                  if (taskResultCommittableMessage instanceof CommittableWithLineage) {
                    collector.collect(
                        ((CommittableWithLineage<TaskResult>) taskResultCommittableMessage)
                            .getCommittable());
                  }
                }
              })
          .transform(
              DUMMY_TASK_NAME,
              TypeInformation.of(Void.class),
              new LockRemoverOperatorFactory(DUMMY_TABLE_NAME, Lists.newArrayList(TASKS[0])));
    }
  }

  private static TableMaintenanceCoordinator createCoordinator() {
    return new TableMaintenanceCoordinator(
        OPERATOR_NAME, new MockOperatorCoordinatorContext(TEST_OPERATOR_ID, 1));
  }

  private static void setAllTasksReady(
      int subtasks,
      TableMaintenanceCoordinator tableMaintenanceCoordinator,
      EventReceivingTasks receivingTasks) {
    for (int i = 0; i < subtasks; i++) {
      tableMaintenanceCoordinator.executionAttemptReady(
          i, 0, receivingTasks.createGatewayForSubtask(i, 0));
    }
  }

  private static void waitForCoordinatorToProcessActions(TableMaintenanceCoordinator coordinator) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    coordinator.callInCoordinatorThread(
        () -> {
          future.complete(null);
          return null;
        },
        "Coordinator fails to process action");

    try {
      future.get();
    } catch (InterruptedException e) {
      throw new AssertionError("test interrupted");
    } catch (ExecutionException e) {
      ExceptionUtils.rethrow(ExceptionUtils.stripExecutionException(e));
    }
  }
}
