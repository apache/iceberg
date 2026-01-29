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

import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.CONCURRENT_RUN_THROTTLED;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.NOTHING_TO_TRIGGER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.RATE_LIMITER_TRIGGERED;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.TRIGGERED;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestTriggerManagerOperator extends OperatorTestBase {
  private static final long DELAY = 10L;
  private static final String OPERATOR_NAME = "TestCoordinator";
  private static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);
  private static final String[] TASKS = new String[] {"task0", "task1"};
  private long processingTime = 0L;
  private String tableName;
  private TableMaintenanceCoordinator tableMaintenanceCoordinator;
  private LockReleasedEvent lockReleasedEvent;

  @BeforeEach
  void before() {
    super.before();
    Table table = createTable();
    this.tableName = table.name();
    lockReleasedEvent = new LockReleasedEvent(tableName, 1L);
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
  void testCommitCount() throws Exception {
    MockOperatorEventGateway mockGateway = new MockOperatorEventGateway();
    TriggerManagerOperator operator =
        createOperator(new TriggerEvaluator.Builder().commitCount(3).build(), mockGateway);
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(1).build(), 0);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(2).build(), 1);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(3).build(), 2);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(10).build(), 3);

      // No trigger in this case
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(1).build(), 3);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(1).build(), 3);

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(1).build(), 4);
    }
  }

  @Test
  void testDataFileCount() throws Exception {
    TriggerManagerOperator operator =
        createOperator(
            new TriggerEvaluator.Builder().dataFileCount(3).build(),
            new MockOperatorEventGateway());
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileCount(1).build(), 0);

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileCount(2).build(), 1);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileCount(3).build(), 2);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileCount(5).build(), 3);

      // No trigger in this case
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileCount(1).build(), 3);

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileCount(2).build(), 4);
    }
  }

  @Test
  void testDataFileSizeInBytes() throws Exception {
    TriggerManagerOperator operator =
        createOperator(
            new TriggerEvaluator.Builder().dataFileSizeInBytes(3).build(),
            new MockOperatorEventGateway());
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileSizeInBytes(1L).build(), 0);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileSizeInBytes(2L).build(), 1);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileSizeInBytes(5L).build(), 2);

      // No trigger in this case
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileSizeInBytes(1L).build(), 2);

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().dataFileSizeInBytes(2L).build(), 3);
    }
  }

  @Test
  void testPosDeleteFileCount() throws Exception {
    TriggerManagerOperator operator =
        createOperator(
            new TriggerEvaluator.Builder().posDeleteFileCount(3).build(),
            new MockOperatorEventGateway());
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteFileCount(1).build(), 0);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteFileCount(2).build(), 1);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteFileCount(3).build(), 2);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteFileCount(10).build(), 3);

      // No trigger in this case
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteFileCount(1).build(), 3);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteFileCount(1).build(), 3);

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteFileCount(1).build(), 4);
    }
  }

  @Test
  void testPosDeleteRecordCount() throws Exception {

    TriggerManagerOperator operator =
        createOperator(
            new TriggerEvaluator.Builder().posDeleteRecordCount(3).build(),
            new MockOperatorEventGateway());
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteRecordCount(1L).build(), 0);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteRecordCount(2L).build(), 1);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteRecordCount(5L).build(), 2);

      // No trigger in this case
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteRecordCount(1L).build(), 2);

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().posDeleteRecordCount(2L).build(), 3);
    }
  }

  @Test
  void testEqDeleteFileCount() throws Exception {
    TriggerManagerOperator operator =
        createOperator(
            new TriggerEvaluator.Builder().eqDeleteFileCount(3).build(),
            new MockOperatorEventGateway());
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteFileCount(1).build(), 0);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteFileCount(2).build(), 1);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteFileCount(3).build(), 2);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteFileCount(10).build(), 3);

      // No trigger in this case
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteFileCount(1).build(), 3);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteFileCount(1).build(), 3);

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteFileCount(1).build(), 4);
    }
  }

  @Test
  void testEqDeleteRecordCount() throws Exception {
    TriggerManagerOperator operator =
        createOperator(
            new TriggerEvaluator.Builder().eqDeleteRecordCount(3).build(),
            new MockOperatorEventGateway());
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteRecordCount(1L).build(), 0);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteRecordCount(2L).build(), 1);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteRecordCount(5L).build(), 2);

      // No trigger in this case
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteRecordCount(1L).build(), 2);

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().eqDeleteRecordCount(2L).build(), 3);
    }
  }

  @Test
  void testTimeout() throws Exception {
    TriggerManagerOperator operator =
        createOperator(
            new TriggerEvaluator.Builder().timeout(Duration.ofSeconds(1)).build(),
            new MockOperatorEventGateway());
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {
      TableChange event = TableChange.builder().dataFileCount(1).commitCount(1).build();

      // Wait for some time
      testHarness.processElement(event, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      // Wait for the timeout to expire
      long newTime = EVENT_TIME + Duration.ofSeconds(1).toMillis();
      testHarness.setProcessingTime(newTime);
      testHarness.processElement(event, newTime);
      assertThat(testHarness.extractOutputValues()).hasSize(1);

      // Remove the lock to allow the next trigger
      operator.handleLockReleaseResult(new LockReleasedEvent(tableName, newTime));

      // Send a new event
      testHarness.setProcessingTime(newTime + 1);
      testHarness.processElement(event, newTime);

      // No trigger yet
      assertThat(testHarness.extractOutputValues()).hasSize(1);

      // Send a new event
      newTime += Duration.ofSeconds(1).toMillis();
      testHarness.setProcessingTime(newTime);
      testHarness.processElement(event, newTime);

      // New trigger should arrive
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void testStateRestore() throws Exception {
    OperatorSubtaskState state;
    TriggerManagerOperator operator =
        createOperator(
            new TriggerEvaluator.Builder().commitCount(2).build(), new MockOperatorEventGateway());
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {
      testHarness.processElement(
          TableChange.builder().dataFileCount(1).commitCount(1).build(), EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      state = testHarness.snapshot(1, EVENT_TIME);
    }

    // Restore the state, write some more data, create a checkpoint, check the data which is written
    TriggerManagerOperator newOperator =
        createOperator(
            new TriggerEvaluator.Builder().commitCount(2).build(), new MockOperatorEventGateway());
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        new OneInputStreamOperatorTestHarness<>(newOperator)) {
      testHarness.initializeState(state);
      testHarness.open();

      // Mock a recovery trigger lock
      assertTriggers(
          testHarness.extractOutputValues(),
          Lists.newArrayList(Trigger.recovery(testHarness.getProcessingTime())));

      testHarness.processElement(TableChange.builder().commitCount(1).build(), EVENT_TIME_2);

      // Remove the lock to allow the next trigger
      newOperator.handleOperatorEvent(lockReleasedEvent);
      testHarness.setProcessingTime(EVENT_TIME_2);

      // At this point the output contains the recovery trigger and the real trigger
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void testMinFireDelay() throws Exception {
    TriggerManagerOperator operator =
        createOperator(
            tableName,
            new TriggerEvaluator.Builder().commitCount(2).build(),
            new MockOperatorEventGateway(),
            DELAY,
            1);
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {
      testHarness.open();

      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(2).build(), 1);
      long currentTime = testHarness.getProcessingTime();

      // No new fire yet
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(2).build(), 1);

      // Check that the trigger fired after the delay
      testHarness.setProcessingTime(currentTime + DELAY);
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void testLockCheckDelay() throws Exception {
    TriggerManagerOperator operator =
        createOperator(
            tableName,
            new TriggerEvaluator.Builder().commitCount(2).build(),
            new MockOperatorEventGateway(),
            1,
            DELAY);
    try (OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness =
        createHarness(operator)) {
      testHarness.open();

      // Create a lock to prevent execution, and check that there is no result
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(2).build(), 1, false);
      addEventAndCheckResult(
          operator, testHarness, TableChange.builder().commitCount(2).build(), 1);
      long currentTime = testHarness.getProcessingTime();

      // Remove the lock, and still no trigger
      operator.handleOperatorEvent(lockReleasedEvent);
      assertThat(testHarness.extractOutputValues()).hasSize(1);

      // Check that the trigger fired after the delay
      testHarness.setProcessingTime(currentTime + DELAY);
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void testTriggerMetrics() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TableChange> source =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));
    CollectingSink<Trigger> sink = new CollectingSink<>();

    TriggerManagerOperatorFactory triggerManagerOperatorFactory =
        new TriggerManagerOperatorFactory(
            tableName,
            Lists.newArrayList(TASKS),
            Lists.newArrayList(
                new TriggerEvaluator.Builder().commitCount(2).build(),
                new TriggerEvaluator.Builder().commitCount(4).build()),
            1L,
            1L);
    source
        .dataStream()
        .keyBy(unused -> true)
        .transform(
            DUMMY_TASK_NAME, TypeInformation.of(Trigger.class), triggerManagerOperatorFactory)
        .forceNonParallel()
        .sinkTo(sink);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // This one doesn't trigger - tests NOTHING_TO_TRIGGER
      source.sendRecord(TableChange.builder().commitCount(1).build());

      Awaitility.await()
          .until(
              () -> {
                Long notingCounter =
                    MetricsReporterFactoryForTests.counter(
                        ImmutableList.of(DUMMY_TASK_NAME, tableName, NOTHING_TO_TRIGGER));
                return notingCounter != null && notingCounter.equals(1L);
              });

      // Trigger one of the tasks - tests TRIGGERED
      source.sendRecord(TableChange.builder().commitCount(1).build());
      // Wait until we receive the trigger
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();
      assertThat(
              MetricsReporterFactoryForTests.counter(
                  ImmutableList.of(DUMMY_TASK_NAME, tableName, TASKS[0], "0", TRIGGERED)))
          .isEqualTo(1L);

      // manual unlock
      tableMaintenanceCoordinator.handleReleaseLock(
          new LockReleasedEvent(tableName, Long.MAX_VALUE));
      // Trigger both of the tasks - tests TRIGGERED
      source.sendRecord(TableChange.builder().commitCount(2).build());
      // Wait until we receive the trigger
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();

      // manual unlock
      tableMaintenanceCoordinator.handleReleaseLock(
          new LockReleasedEvent(tableName, Long.MAX_VALUE));
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();

      assertThat(
              MetricsReporterFactoryForTests.counter(
                  ImmutableList.of(DUMMY_TASK_NAME, tableName, TASKS[0], "0", TRIGGERED)))
          .isEqualTo(2L);
      assertThat(
              MetricsReporterFactoryForTests.counter(
                  ImmutableList.of(DUMMY_TASK_NAME, tableName, TASKS[1], "1", TRIGGERED)))
          .isEqualTo(1L);

      // Final check all the counters
      MetricsReporterFactoryForTests.assertCounters(
          new ImmutableMap.Builder<List<String>, Long>()
              .put(ImmutableList.of(DUMMY_TASK_NAME, tableName, RATE_LIMITER_TRIGGERED), -1L)
              .put(ImmutableList.of(DUMMY_TASK_NAME, tableName, CONCURRENT_RUN_THROTTLED), -1L)
              .put(ImmutableList.of(DUMMY_TASK_NAME, tableName, TASKS[0], "0", TRIGGERED), 2L)
              .put(ImmutableList.of(DUMMY_TASK_NAME, tableName, TASKS[1], "1", TRIGGERED), 1L)
              .put(ImmutableList.of(DUMMY_TASK_NAME, tableName, NOTHING_TO_TRIGGER), 1L)
              .build());
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testRateLimiterMetrics() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TableChange> source =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));
    CollectingSink<Trigger> sink = new CollectingSink<>();

    // High delay, so only triggered once
    TriggerManagerOperatorFactory manager = manager(1_000_000L, 1L);

    source
        .dataStream()
        .keyBy(unused -> true)
        .transform(DUMMY_TASK_NAME, TypeInformation.of(Trigger.class), manager)
        .forceNonParallel()
        .sinkTo(sink);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Start the first trigger
      source.sendRecord(TableChange.builder().commitCount(2).build());
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();

      // Remove the lock to allow the next trigger
      tableMaintenanceCoordinator.handleEventFromOperator(0, 0, lockReleasedEvent);

      // The second trigger will be blocked
      source.sendRecord(TableChange.builder().commitCount(2).build());
      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          ImmutableList.of(DUMMY_TASK_NAME, tableName, RATE_LIMITER_TRIGGERED))
                      .equals(1L));

      // Final check all the counters
      assertCounters(1L, 0L);
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testConcurrentRunMetrics() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TableChange> source =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));
    CollectingSink<Trigger> sink = new CollectingSink<>();

    // High delay, so only triggered once
    TriggerManagerOperatorFactory manager = manager(1L, 1_000_000L);

    source
        .dataStream()
        .keyBy(unused -> true)
        .transform(DUMMY_TASK_NAME, TypeInformation.of(Trigger.class), manager)
        .forceNonParallel()
        .sinkTo(sink);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Start the first trigger - notice that we do not remove the lock after the trigger
      source.sendRecord(TableChange.builder().commitCount(2).build());
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();

      // The second trigger will be blocked by the lock
      source.sendRecord(TableChange.builder().commitCount(2).build());
      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          ImmutableList.of(DUMMY_TASK_NAME, tableName, CONCURRENT_RUN_THROTTLED))
                      .equals(1L));

      // Final check all the counters
      assertCounters(0L, 1L);
    } finally {
      closeJobClient(jobClient);
    }
  }

  private void assertCounters(long rateLimiterTrigger, long concurrentRunTrigger) {
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(DUMMY_TASK_NAME, tableName, RATE_LIMITER_TRIGGERED),
                rateLimiterTrigger)
            .put(
                ImmutableList.of(DUMMY_TASK_NAME, tableName, CONCURRENT_RUN_THROTTLED),
                concurrentRunTrigger)
            .put(ImmutableList.of(DUMMY_TASK_NAME, tableName, TASKS[0], "0", TRIGGERED), 1L)
            .put(ImmutableList.of(DUMMY_TASK_NAME, tableName, NOTHING_TO_TRIGGER), 0L)
            .build());
  }

  private KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> harness(
      TriggerManager manager) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(manager), value -> true, Types.BOOLEAN);
  }

  private void addEventAndCheckResult(
      TriggerManagerOperator operator,
      OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness,
      TableChange event,
      int expectedSize)
      throws Exception {
    addEventAndCheckResult(operator, testHarness, event, expectedSize, true);
  }

  private void addEventAndCheckResult(
      TriggerManagerOperator operator,
      OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness,
      TableChange event,
      int expectedSize,
      boolean removeLock)
      throws Exception {
    ++processingTime;
    testHarness.setProcessingTime(processingTime);
    testHarness.processElement(event, processingTime);
    assertThat(testHarness.extractOutputValues()).hasSize(expectedSize);
    if (removeLock) {
      // Remove the lock to allow the next trigger
      operator.handleLockReleaseResult(new LockReleasedEvent(tableName, processingTime));
    }
  }

  private TriggerManagerOperatorFactory manager(long minFireDelayMs, long lockCheckDelayMs) {
    return new TriggerManagerOperatorFactory(
        tableName,
        Lists.newArrayList(TASKS[0]),
        Lists.newArrayList(new TriggerEvaluator.Builder().commitCount(2).build()),
        minFireDelayMs,
        lockCheckDelayMs);
  }

  private static void assertTriggers(List<Trigger> expected, List<Trigger> actual) {
    assertThat(actual).hasSize(expected.size());
    for (int i = 0; i < expected.size(); ++i) {
      Trigger expectedTrigger = expected.get(i);
      Trigger actualTrigger = actual.get(i);
      assertThat(actualTrigger.timestamp()).isEqualTo(expectedTrigger.timestamp());
      assertThat(actualTrigger.taskId()).isEqualTo(expectedTrigger.taskId());
      assertThat(actualTrigger.isRecovery()).isEqualTo(expectedTrigger.isRecovery());
    }
  }

  private static TableMaintenanceCoordinator createCoordinator() {
    return new TableMaintenanceCoordinator(
        OPERATOR_NAME, new MockOperatorCoordinatorContext(TEST_OPERATOR_ID, 2));
  }

  private TriggerManagerOperator createOperator(
      TriggerEvaluator evaluator, OperatorEventGateway mockGateway) {
    return createOperator(tableName, evaluator, mockGateway, 1, 1);
  }

  private TriggerManagerOperator createOperator(
      String lockId,
      TriggerEvaluator evaluator,
      OperatorEventGateway mockGateway,
      long minFireDelayMs,
      long lockCheckDelayMs) {
    TriggerManagerOperator operator =
        new TriggerManagerOperator(
            null,
            mockGateway,
            Lists.newArrayList(TASKS[0]),
            Lists.newArrayList(evaluator),
            minFireDelayMs,
            lockCheckDelayMs,
            lockId);
    return operator;
  }

  private OneInputStreamOperatorTestHarness<TableChange, Trigger> createHarness(
      TriggerManagerOperator triggerManagerOperator) throws Exception {
    OneInputStreamOperatorTestHarness<TableChange, Trigger> harness =
        new OneInputStreamOperatorTestHarness<>(triggerManagerOperator);
    harness.open();
    return harness;
  }
}
