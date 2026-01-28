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

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 10)
class TestLockRemoverOperation extends OperatorTestBase {
  private static final String[] TASKS = new String[] {"task0", "task1", "task2"};
  private static final String OPERATOR_NAME = "TestCoordinator";
  private static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);

  private TableMaintenanceCoordinator tableMaintenanceCoordinator;

  @BeforeEach
  public void before() {
    MetricsReporterFactoryForTests.reset();
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
    MockOperatorEventGateway mockGateway = new MockOperatorEventGateway();
    LockRemoverOperator operator =
        new LockRemoverOperator(null, mockGateway, DUMMY_TASK_NAME, Lists.newArrayList(TASKS[0]));
    try (OneInputStreamOperatorTestHarness<TaskResult, Void> testHarness =
        createHarness(operator)) {

      testHarness.processElement(
          new StreamRecord<>(new TaskResult(0, 0L, true, Lists.newArrayList())));
      assertThat(mockGateway.getEventsSent()).hasSize(0);

      testHarness.processWatermark(WATERMARK);
      assertThat(mockGateway.getEventsSent()).hasSize(1);
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

  private OneInputStreamOperatorTestHarness<TaskResult, Void> createHarness(
      LockRemoverOperator lockRemoverOperator) throws Exception {
    OneInputStreamOperatorTestHarness<TaskResult, Void> harness =
        new OneInputStreamOperatorTestHarness<>(lockRemoverOperator);
    harness.open();
    return harness;
  }

  private static TableMaintenanceCoordinator createCoordinator() {
    return new TableMaintenanceCoordinator(
        OPERATOR_NAME, new MockOperatorCoordinatorContext(TEST_OPERATOR_ID, 1));
  }
}
