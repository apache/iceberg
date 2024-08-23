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
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.CONCURRENT_RUN_THROTTLED;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.GROUP_VALUE_DEFAULT;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.NOTHING_TO_TRIGGER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.RATE_LIMITER_TRIGGERED;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.TRIGGERED;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestTriggerManager extends OperatorTestBase {
  private static final long DELAY = 10L;
  private static final String NAME_1 = "name1";
  private static final String NAME_2 = "name2";
  private long processingTime = 0L;
  private TriggerLockFactory lockFactory;
  private TriggerLockFactory.Lock lock;
  private TriggerLockFactory.Lock recoveringLock;

  @BeforeEach
  void before() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    this.lockFactory = lockFactory();
    lockFactory.open();
    this.lock = lockFactory.createLock();
    this.recoveringLock = lockFactory.createRecoveryLock();
    lock.unlock();
    recoveringLock.unlock();
    MetricsReporterFactoryForTests.reset();
  }

  @AfterEach
  void after() throws IOException {
    lockFactory.close();
  }

  @Test
  void testCommitCount() throws Exception {
    TriggerManager manager =
        manager(sql.tableLoader(TABLE_NAME), new TriggerEvaluator.Builder().commitCount(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(1).build(), 0);
      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(2).build(), 1);
      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(3).build(), 2);
      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(10).build(), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(1).build(), 3);
      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(1).build(), 3);

      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(1).build(), 4);
    }
  }

  @Test
  void testDataFileCount() throws Exception {
    TriggerManager manager =
        manager(
            sql.tableLoader(TABLE_NAME), new TriggerEvaluator.Builder().dataFileCount(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, TableChange.builder().dataFileCount(1).build(), 0);

      addEventAndCheckResult(testHarness, TableChange.builder().dataFileCount(2).build(), 1);
      addEventAndCheckResult(testHarness, TableChange.builder().dataFileCount(3).build(), 2);
      addEventAndCheckResult(testHarness, TableChange.builder().dataFileCount(5).build(), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, TableChange.builder().dataFileCount(1).build(), 3);

      addEventAndCheckResult(testHarness, TableChange.builder().dataFileCount(2).build(), 4);
    }
  }

  @Test
  void testDataFileSizeInBytes() throws Exception {
    TriggerManager manager =
        manager(
            sql.tableLoader(TABLE_NAME),
            new TriggerEvaluator.Builder().dataFileSizeInBytes(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, TableChange.builder().dataFileSizeInBytes(1L).build(), 0);
      addEventAndCheckResult(testHarness, TableChange.builder().dataFileSizeInBytes(2L).build(), 1);
      addEventAndCheckResult(testHarness, TableChange.builder().dataFileSizeInBytes(5L).build(), 2);

      // No trigger in this case
      addEventAndCheckResult(testHarness, TableChange.builder().dataFileSizeInBytes(1L).build(), 2);

      addEventAndCheckResult(testHarness, TableChange.builder().dataFileSizeInBytes(2L).build(), 3);
    }
  }

  @Test
  void testPosDeleteFileCount() throws Exception {
    TriggerManager manager =
        manager(
            sql.tableLoader(TABLE_NAME),
            new TriggerEvaluator.Builder().posDeleteFileCount(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, TableChange.builder().posDeleteFileCount(1).build(), 0);
      addEventAndCheckResult(testHarness, TableChange.builder().posDeleteFileCount(2).build(), 1);
      addEventAndCheckResult(testHarness, TableChange.builder().posDeleteFileCount(3).build(), 2);
      addEventAndCheckResult(testHarness, TableChange.builder().posDeleteFileCount(10).build(), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, TableChange.builder().posDeleteFileCount(1).build(), 3);
      addEventAndCheckResult(testHarness, TableChange.builder().posDeleteFileCount(1).build(), 3);

      addEventAndCheckResult(testHarness, TableChange.builder().posDeleteFileCount(1).build(), 4);
    }
  }

  @Test
  void testPosDeleteRecordCount() throws Exception {
    TriggerManager manager =
        manager(
            sql.tableLoader(TABLE_NAME),
            new TriggerEvaluator.Builder().posDeleteRecordCount(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(
          testHarness, TableChange.builder().posDeleteRecordCount(1L).build(), 0);
      addEventAndCheckResult(
          testHarness, TableChange.builder().posDeleteRecordCount(2L).build(), 1);
      addEventAndCheckResult(
          testHarness, TableChange.builder().posDeleteRecordCount(5L).build(), 2);

      // No trigger in this case
      addEventAndCheckResult(
          testHarness, TableChange.builder().posDeleteRecordCount(1L).build(), 2);

      addEventAndCheckResult(
          testHarness, TableChange.builder().posDeleteRecordCount(2L).build(), 3);
    }
  }

  @Test
  void testEqDeleteFileCount() throws Exception {
    TriggerManager manager =
        manager(
            sql.tableLoader(TABLE_NAME),
            new TriggerEvaluator.Builder().eqDeleteFileCount(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteFileCount(1).build(), 0);
      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteFileCount(2).build(), 1);
      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteFileCount(3).build(), 2);
      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteFileCount(10).build(), 3);

      // No trigger in this case
      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteFileCount(1).build(), 3);
      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteFileCount(1).build(), 3);

      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteFileCount(1).build(), 4);
    }
  }

  @Test
  void testEqDeleteRecordCount() throws Exception {
    TriggerManager manager =
        manager(
            sql.tableLoader(TABLE_NAME),
            new TriggerEvaluator.Builder().eqDeleteRecordCount(3).build());
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteRecordCount(1L).build(), 0);
      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteRecordCount(2L).build(), 1);
      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteRecordCount(5L).build(), 2);

      // No trigger in this case
      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteRecordCount(1L).build(), 2);

      addEventAndCheckResult(testHarness, TableChange.builder().eqDeleteRecordCount(2L).build(), 3);
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
      lock.unlock();

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
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TriggerManager manager = manager(tableLoader);
    OperatorSubtaskState state;
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      testHarness.processElement(
          TableChange.builder().dataFileCount(1).commitCount(1).build(), EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      state = testHarness.snapshot(1, EVENT_TIME);
    }

    // Restore the state, write some more data, create a checkpoint, check the data which is written
    manager = manager(tableLoader);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.initializeState(state);
      testHarness.open();

      // Arrives the first real change which triggers the recovery process
      testHarness.processElement(TableChange.builder().commitCount(1).build(), EVENT_TIME_2);
      assertTriggers(
          testHarness.extractOutputValues(),
          Lists.newArrayList(Trigger.recovery(testHarness.getProcessingTime())));

      // Remove the lock to allow the next trigger
      recoveringLock.unlock();
      testHarness.setProcessingTime(EVENT_TIME_2);
      // At this point the output contains the recovery trigger and the real trigger
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void testMinFireDelay() throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TriggerManager manager = manager(tableLoader, DELAY, 1);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(2).build(), 1);
      long currentTime = testHarness.getProcessingTime();

      // No new fire yet
      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(2).build(), 1);

      // Check that the trigger fired after the delay
      testHarness.setProcessingTime(currentTime + DELAY);
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void testLockCheckDelay() throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TriggerManager manager = manager(tableLoader, 1, DELAY);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();

      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(2).build(), 1);

      // Create a lock to prevent execution, and check that there is no result
      assertThat(lock.tryLock()).isTrue();
      addEventAndCheckResult(testHarness, TableChange.builder().commitCount(2).build(), 1);
      long currentTime = testHarness.getProcessingTime();

      // Remove the lock, and still no trigger
      lock.unlock();
      assertThat(testHarness.extractOutputValues()).hasSize(1);

      // Check that the trigger fired after the delay
      testHarness.setProcessingTime(currentTime + DELAY);
      assertThat(testHarness.extractOutputValues()).hasSize(2);
    }
  }

  /**
   * Simulating recovery scenarios where there is a leftover table lock, and ongoing maintenance
   * task.
   *
   * @param locked if a lock exists on the table on job recovery
   * @param runningTask is running and continues to run after job recovery
   */
  @ParameterizedTest
  @MethodSource("parametersForTestRecovery")
  void testRecovery(boolean locked, boolean runningTask) throws Exception {
    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    TriggerManager manager = manager(tableLoader);
    OperatorSubtaskState state;
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.open();
      state = testHarness.snapshot(1, EVENT_TIME);
    }

    if (locked) {
      assertThat(lock.tryLock()).isTrue();
    }

    manager = manager(tableLoader);
    List<Trigger> expected = Lists.newArrayListWithExpectedSize(3);
    try (KeyedOneInputStreamOperatorTestHarness<Boolean, TableChange, Trigger> testHarness =
        harness(manager)) {
      testHarness.initializeState(state);
      testHarness.open();

      ++processingTime;
      expected.add(Trigger.recovery(processingTime));
      testHarness.setProcessingTime(processingTime);
      testHarness.processElement(TableChange.builder().commitCount(2).build(), processingTime);
      assertTriggers(testHarness.extractOutputValues(), expected);

      // Nothing happens until the recovery is finished
      ++processingTime;
      testHarness.setProcessingTime(processingTime);
      assertTriggers(testHarness.extractOutputValues(), expected);

      if (runningTask) {
        // Simulate the action of the recovered maintenance task lock removal when it finishes
        lock.unlock();
      }

      // Still no results as the recovery is ongoing
      ++processingTime;
      testHarness.setProcessingTime(processingTime);
      testHarness.processElement(TableChange.builder().commitCount(2).build(), processingTime);
      assertTriggers(testHarness.extractOutputValues(), expected);

      // Simulate the action of removing lock and recoveryLock by downstream lock cleaner when it
      // received recovery trigger
      lock.unlock();
      recoveringLock.unlock();

      // Emit only a single trigger
      ++processingTime;
      testHarness.setProcessingTime(processingTime);
      // Releasing lock will create a new snapshot, and we receive this in the trigger
      expected.add(
          Trigger.create(
              processingTime,
              (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable()),
              0));
      assertTriggers(testHarness.extractOutputValues(), expected);
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
            lockFactory,
            Lists.newArrayList(NAME_1, NAME_2),
            Lists.newArrayList(
                new TriggerEvaluator.Builder().commitCount(2).build(),
                new TriggerEvaluator.Builder().commitCount(4).build()),
            1L,
            1L);
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
      source.sendRecord(TableChange.builder().commitCount(1).build());

      Awaitility.await()
          .until(
              () -> {
                Long notingCounter =
                    MetricsReporterFactoryForTests.counter(
                        DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + NOTHING_TO_TRIGGER);
                return notingCounter != null && notingCounter.equals(1L);
              });

      // Trigger one of the tasks - tests TRIGGERED
      source.sendRecord(TableChange.builder().commitCount(1).build());
      // Wait until we receive the trigger
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();
      assertThat(
              MetricsReporterFactoryForTests.counter(DUMMY_NAME + "." + NAME_1 + "." + TRIGGERED))
          .isEqualTo(1L);
      lock.unlock();

      // Trigger both of the tasks - tests TRIGGERED
      source.sendRecord(TableChange.builder().commitCount(2).build());
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
              .put(DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + CONCURRENT_RUN_THROTTLED, -1L)
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
    TriggerManager manager = manager(tableLoader, 1_000_000L, 1L);
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
      source.sendRecord(TableChange.builder().commitCount(2).build());
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();

      // Remove the lock to allow the next trigger
      lock.unlock();

      // The second trigger will be blocked
      source.sendRecord(TableChange.builder().commitCount(2).build());
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
    TriggerManager manager = manager(tableLoader, 1L, 1_000_000L);
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
      source.sendRecord(TableChange.builder().commitCount(2).build());
      assertThat(sink.poll(Duration.ofSeconds(5))).isNotNull();

      // The second trigger will be blocked by the lock
      source.sendRecord(TableChange.builder().commitCount(2).build());
      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + CONCURRENT_RUN_THROTTLED)
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
                DUMMY_NAME + "." + GROUP_VALUE_DEFAULT + "." + CONCURRENT_RUN_THROTTLED,
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
    // Remove the lock to allow the next trigger
    lock.unlock();
  }

  private TriggerManager manager(TableLoader tableLoader, TriggerEvaluator evaluator) {
    return new TriggerManager(
        tableLoader, lockFactory, Lists.newArrayList(NAME_1), Lists.newArrayList(evaluator), 1, 1);
  }

  private TriggerManager manager(
      TableLoader tableLoader, long minFireDelayMs, long lockCheckDelayMs) {
    return new TriggerManager(
        tableLoader,
        lockFactory,
        Lists.newArrayList(NAME_1),
        Lists.newArrayList(new TriggerEvaluator.Builder().commitCount(2).build()),
        minFireDelayMs,
        lockCheckDelayMs);
  }

  private TriggerManager manager(TableLoader tableLoader) {
    return manager(tableLoader, new TriggerEvaluator.Builder().commitCount(2).build());
  }

  private static void assertTriggers(List<Trigger> expected, List<Trigger> actual) {
    assertThat(actual).hasSize(expected.size());
    for (int i = 0; i < expected.size(); ++i) {
      Trigger expectedTrigger = expected.get(i);
      Trigger actualTrigger = actual.get(i);
      assertThat(actualTrigger.timestamp()).isEqualTo(expectedTrigger.timestamp());
      assertThat(actualTrigger.taskId()).isEqualTo(expectedTrigger.taskId());
      assertThat(actualTrigger.isRecovery()).isEqualTo(expectedTrigger.isRecovery());
      if (expectedTrigger.table() == null) {
        assertThat(actualTrigger.table()).isNull();
      } else {
        Iterator<Snapshot> expectedSnapshots = expectedTrigger.table().snapshots().iterator();
        Iterator<Snapshot> actualSnapshots = actualTrigger.table().snapshots().iterator();
        while (expectedSnapshots.hasNext()) {
          assertThat(actualSnapshots.hasNext()).isTrue();
          assertThat(expectedSnapshots.next().snapshotId())
              .isEqualTo(actualSnapshots.next().snapshotId());
        }
      }
    }
  }
}
