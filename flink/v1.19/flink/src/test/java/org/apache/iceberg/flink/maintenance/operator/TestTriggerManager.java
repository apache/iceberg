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

import static org.apache.iceberg.flink.maintenance.operator.ConstantsForTests.DUMMY_NAME;
import static org.apache.iceberg.flink.maintenance.operator.ConstantsForTests.EVENT_TIME;
import static org.apache.iceberg.flink.maintenance.operator.ConstantsForTests.EVENT_TIME_2;
import static org.apache.iceberg.flink.maintenance.operator.MetricConstants.CONCURRENT_RUN_TRIGGERED;
import static org.apache.iceberg.flink.maintenance.operator.MetricConstants.GROUP_VALUE_DEFAULT;
import static org.apache.iceberg.flink.maintenance.operator.MetricConstants.NOTHING_TO_TRIGGER;
import static org.apache.iceberg.flink.maintenance.operator.MetricConstants.RATE_LIMITER_TRIGGERED;
import static org.apache.iceberg.flink.maintenance.operator.MetricConstants.TRIGGERED;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestTriggerManager extends OperatorTestBase {
  private static final long DELAY = 10L;
  private static final String NAME_1 = "name1";
  private static final String NAME_2 = "name2";
  private long processingTime = 0L;
  private TriggerLockFactory.Lock lock;
  private TriggerLockFactory.Lock recoveringLock;

  @BeforeEach
  void before() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    lock = new TagBasedLockFactory(sql.tableLoader(TABLE_NAME)).createLock();
    recoveringLock = new TagBasedLockFactory(sql.tableLoader(TABLE_NAME)).createRecoveryLock();
    MetricsReporterFactoryForTests.reset();
  }

  @Test
  void testCommitNumber() throws Exception {
    TriggerManager manager =
        manager(
            sql.tableLoader(TABLE_NAME), new TriggerEvaluator.Builder().commitNumber(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 1), 0);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 2), 1);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 3), 2);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 10), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 1), 3);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 1), 3);

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 1), 4);
    }
  }

  @Test
  void testFileNumber() throws Exception {
    TriggerManager manager =
        manager(sql.tableLoader(TABLE_NAME), new TriggerEvaluator.Builder().fileNumber(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(1, 0, 0, 0, 0), 0);

      addEventAndCheckResult(testHarness, new TableChange(1, 1, 0, 0, 0), 1);
      addEventAndCheckResult(testHarness, new TableChange(0, 3, 0, 0, 0), 2);
      addEventAndCheckResult(testHarness, new TableChange(5, 7, 0, 0, 0), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, new TableChange(1, 0, 0, 0, 0), 3);
      addEventAndCheckResult(testHarness, new TableChange(0, 1, 0, 0, 0), 3);

      addEventAndCheckResult(testHarness, new TableChange(1, 0, 0, 0, 0), 4);
    }
  }

  @Test
  void testFileSize() throws Exception {
    TriggerManager manager =
        manager(sql.tableLoader(TABLE_NAME), new TriggerEvaluator.Builder().fileSize(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 1, 0, 0), 0);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 1, 1, 0), 1);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 3, 0), 2);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 5, 7, 0), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 1, 0, 0), 3);
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 1, 0), 3);

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 1, 0, 0), 4);
    }
  }

  @Test
  void testDeleteFileNumber() throws Exception {
    TriggerManager manager =
        manager(
            sql.tableLoader(TABLE_NAME),
            new TriggerEvaluator.Builder().deleteFileNumber(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(3, 1, 0, 0, 0), 0);
      addEventAndCheckResult(testHarness, new TableChange(0, 2, 0, 0, 0), 1);
      addEventAndCheckResult(testHarness, new TableChange(0, 3, 0, 0, 0), 2);
      addEventAndCheckResult(testHarness, new TableChange(0, 10, 0, 0, 0), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, new TableChange(0, 1, 0, 0, 0), 3);
      addEventAndCheckResult(testHarness, new TableChange(0, 1, 0, 0, 0), 3);

      addEventAndCheckResult(testHarness, new TableChange(0, 1, 0, 0, 0), 4);
    }
  }

  @Test
  void testTimeout() throws Exception {
    TriggerManager manager =
        manager(
            sql.tableLoader(TABLE_NAME),
            new TriggerEvaluator.Builder().timeout(Duration.ofSeconds(1)).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      TableChange event = new TableChange(1, 0, 0, 0, 1);

      // Wait for one trigger
      testHarness.processElement(event, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      // Wait for a second trigger
      long newTime = EVENT_TIME + Duration.ofSeconds(1).toMillis();
      testHarness.setProcessingTime(newTime);
      testHarness.processElement(event, newTime);
      assertThat(testHarness.extractOutputValues()).hasSize(1);
    }
  }

  @Test
  void testStateRestore() throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TriggerManager manager = manager(tableLoader);
    OperatorSubtaskState state;
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      testHarness.processElement(new TableChange(1, 0, 0, 0, 1), EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      state = testHarness.snapshot(1, EVENT_TIME);
    }

    // Restore the state, write some more data, create a checkpoint, check the data which is written
    manager = manager(tableLoader);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.initializeState(state);
      testHarness.open();

      // Arrives the first real change
      testHarness.processElement(new TableChange(0, 0, 0, 0, 1), EVENT_TIME_2);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(Lists.newArrayList(Trigger.cleanUp(testHarness.getProcessingTime())));

      recoveringLock.unlock();
      testHarness.setProcessingTime(EVENT_TIME_2);
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void testMinFireDelay() throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TriggerManager manager = manager(tableLoader, DELAY, 1, false);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 2), 1);
      long currentTime = testHarness.getProcessingTime();

      // No new fire yet
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 2), 1);

      // Check that the trigger fired after the delay
      testHarness.setProcessingTime(currentTime + DELAY);
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void testLogCheckDelay() throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TriggerManager manager = manager(tableLoader, 1, DELAY, false);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 2), 1);

      // Create a lock to prevent execution, and check that there is no result
      assertThat(lock.tryLock()).isTrue();
      addEventAndCheckResult(testHarness, new TableChange(0, 0, 0, 0, 2), 1);
      long currentTime = testHarness.getProcessingTime();

      // Remove the lock, and still no trigger
      lock.unlock();
      assertThat(testHarness.extractOutputValues()).hasSize(1);

      // Check that the trigger fired after the delay
      testHarness.setProcessingTime(currentTime + DELAY);
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCleanup(boolean cleanup) throws Exception {
    // Simulate a lock remaining from a previous run
    assertThat(lock.tryLock()).isTrue();

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TriggerManager manager = manager(tableLoader, 1, 1, cleanup);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      assertThat(lock.isHeld()).isEqualTo(!cleanup);
    }
  }

  /**
   * Simulating recovery scenarios where there is a leftover table lock, and ongoing maintenance
   * task.
   *
   * @param tableLock left on the table on job recovery
   * @param runningTask is running and continues to run after job recovery
   */
  @ParameterizedTest
  @MethodSource("parametersForTestRecovery")
  void testRecovery(boolean tableLock, boolean runningTask) throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TriggerManager manager = manager(tableLoader);
    OperatorSubtaskState state;
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();
      state = testHarness.snapshot(1, EVENT_TIME);
    }

    if (tableLock) {
      assertThat(lock.tryLock()).isTrue();
    }

    manager = manager(tableLoader);
    List<Trigger> expected = Lists.newArrayListWithExpectedSize(3);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.initializeState(state);
      testHarness.open();

      ++processingTime;
      expected.add(Trigger.cleanUp(processingTime));
      testHarness.setProcessingTime(processingTime);
      testHarness.processElement(new TableChange(0, 0, 0, 0, 2), processingTime);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      // Nothing happens until the recovery is finished
      ++processingTime;
      testHarness.setProcessingTime(processingTime);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      if (runningTask) {
        lock.unlock();
      }

      // Still no results as the recovery is ongoing
      ++processingTime;
      testHarness.setProcessingTime(processingTime);
      testHarness.processElement(new TableChange(0, 0, 0, 0, 2), processingTime);
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);

      // All locks are removed when the recovery trigger is received by the lock cleaner
      lock.unlock();
      recoveringLock.unlock();

      // Emit only a single trigger
      ++processingTime;
      testHarness.setProcessingTime(processingTime);
      testHarness.processElement(new TableChange(0, 0, 0, 0, 2), processingTime);
      // Releasing lock will create a new snapshot, and we receive this in the trigger
      expected.add(
          Trigger.create(
              processingTime,
              (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable()),
              0));
      assertThat(testHarness.extractOutputValues()).isEqualTo(expected);
    }
  }

  @Test
  void testTriggerMetrics() throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TableChange> source =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));
    CollectingSink<Trigger> sink = new CollectingSink<>();

    TriggerManager manager =
        new TriggerManager(
            tableLoader,
            new TagBasedLockFactory(tableLoader),
            Lists.newArrayList(NAME_1, NAME_2),
            Lists.newArrayList(
                new TriggerEvaluator.Builder().commitNumber(2).build(),
                new TriggerEvaluator.Builder().commitNumber(4).build()),
            1L,
            1L,
            false);
    source
        .dataStream()
        .keyBy(unused -> true)
        .process(manager)
        .name(DUMMY_NAME)
        .forceNonParallel()
        .sinkTo(sink);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // This one doesn't trigger - tests NOTHING_TO_TRIGGER
      source.sendRecord(new TableChange(0, 0, 0L, 0L, 1));

      Awaitility.await()
          .until(
              () -> {
                Long notingCounter =
                    MetricsReporterFactoryForTests.counter(
                        DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + NOTHING_TO_TRIGGER);
                return notingCounter != null && notingCounter.equals(1L);
              });

      // Trigger one of the tasks - tests TRIGGERED
      source.sendRecord(new TableChange(0, 0, 0L, 0L, 1));
      // Wait until we receive the trigger
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();
      assertThat(
              MetricsReporterFactoryForTests.counter(DUMMY_NAME + "." + NAME_1 + "." + TRIGGERED))
          .isEqualTo(1L);
      lock.unlock();

      // Trigger both of the tasks - tests TRIGGERED
      source.sendRecord(new TableChange(0, 0, 0L, 0L, 2));
      // Wait until we receive the trigger
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();
      lock.unlock();
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();
      lock.unlock();
      assertThat(
              MetricsReporterFactoryForTests.counter(DUMMY_NAME + "." + NAME_1 + "." + TRIGGERED))
          .isEqualTo(2L);
      assertThat(
              MetricsReporterFactoryForTests.counter(DUMMY_NAME + "." + NAME_2 + "." + TRIGGERED))
          .isEqualTo(1L);

      // Final check all the counters
      MetricsReporterFactoryForTests.assertCounters(
          new ImmutableMap.Builder<String, Long>()
              .put(DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + RATE_LIMITER_TRIGGERED, -1L)
              .put(DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + CONCURRENT_RUN_TRIGGERED, -1L)
              .put(DUMMY_NAME + "." + NAME_1 + "." + TRIGGERED, 2L)
              .put(DUMMY_NAME + "." + NAME_2 + "." + TRIGGERED, 1L)
              .put(DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + NOTHING_TO_TRIGGER, 1L)
              .build());
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testRateLimiterMetrics() throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TableChange> source =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));
    CollectingSink<Trigger> sink = new CollectingSink<>();

    // High delay, so only triggered once
    TriggerManager manager = manager(tableLoader, 1_000_000L, 1L, false);
    source
        .dataStream()
        .keyBy(unused -> true)
        .process(manager)
        .name(DUMMY_NAME)
        .forceNonParallel()
        .sinkTo(sink);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Start the first trigger
      source.sendRecord(new TableChange(0, 0, 0L, 0L, 2));
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();
      lock.unlock();

      // The second trigger will be blocked
      source.sendRecord(new TableChange(0, 0, 0L, 0L, 2));
      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + RATE_LIMITER_TRIGGERED)
                      .equals(1L));

      // Final check all the counters
      assertCounters(1L, 0L);
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testConcurrentRunMetrics() throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TableChange> source =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));
    CollectingSink<Trigger> sink = new CollectingSink<>();

    // High delay, so only triggered once
    TriggerManager manager = manager(tableLoader, 1L, 1_000_000L, false);
    source
        .dataStream()
        .keyBy(unused -> true)
        .process(manager)
        .name(DUMMY_NAME)
        .forceNonParallel()
        .sinkTo(sink);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Start the first trigger - notice that we do not remove the lock after the trigger
      source.sendRecord(new TableChange(0, 0, 0L, 0L, 2));
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();

      // The second trigger will be blocked by the lock
      source.sendRecord(new TableChange(0, 0, 0L, 0L, 2));
      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + CONCURRENT_RUN_TRIGGERED)
                      .equals(1L));

      // Final check all the counters
      assertCounters(0L, 1L);
    } finally {
      closeJobClient(jobClient);
    }
  }

  private static Stream<Arguments> parametersForTestRecovery() {
    return Stream.of(
        Arguments.of(true, false),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false));
  }

  private void assertCounters(long rateLimiterTrigger, long concurrentRunTrigger) {
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<String, Long>()
            .put(
                DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + RATE_LIMITER_TRIGGERED,
                rateLimiterTrigger)
            .put(
                DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + CONCURRENT_RUN_TRIGGERED,
                concurrentRunTrigger)
            .put(DUMMY_NAME + "." + NAME_1 + "." + TRIGGERED, 1L)
            .put(DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + NOTHING_TO_TRIGGER, 0L)
            .build());
  }

  private KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> harness(
      TriggerManager manager) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new KeyedProcessOperator<>(manager), value -> true, Types.BOOLEAN);
  }

  private void addEventAndCheckResult(
      OneInputStreamOperatorTestHarness<TableChange, Trigger> testHarness,
      TableChange event,
      int expectedSize)
      throws Exception {
    ++processingTime;
    testHarness.setProcessingTime(processingTime);
    testHarness.processElement(event, processingTime);
    assertThat(testHarness.extractOutputValues()).hasSize(expectedSize);
    lock.unlock();
  }

  private static TriggerManager manager(
      TableLoader tableLoader,
      TriggerEvaluator evaluator,
      long minFireDelayMs,
      long lockCheckDelayMs,
      boolean clearLocks) {
    return new TriggerManager(
        tableLoader,
        new TagBasedLockFactory(tableLoader),
        Lists.newArrayList(NAME_1),
        Lists.newArrayList(evaluator),
        minFireDelayMs,
        lockCheckDelayMs,
        clearLocks);
  }

  private static TriggerEvaluator triggerEvaluatorWith2Commits() {
    return new TriggerEvaluator.Builder().commitNumber(2).build();
  }

  private static TriggerManager manager(TableLoader tableLoader, TriggerEvaluator evaluator) {
    return manager(tableLoader, evaluator, 1, 1, false);
  }

  private static TriggerManager manager(TableLoader tableLoader) {
    return manager(tableLoader, triggerEvaluatorWith2Commits());
  }

  private static TriggerManager manager(
      TableLoader tableLoader, long minFireDelayMs, long lockCheckDelayMs, boolean clearLocks) {
    return manager(
        tableLoader, triggerEvaluatorWith2Commits(), minFireDelayMs, lockCheckDelayMs, clearLocks);
  }
}
