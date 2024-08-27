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
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.FAILED_TASK_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.SUCCEEDED_TASK_COUNTER;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 10)
class TestLockRemover extends OperatorTestBase {
  private static final String TASK_0 = "task0";
  private static final String TASK_1 = "task1";
  private static final TriggerLockFactory.Lock LOCK = new TestingLock();
  private static final TriggerLockFactory.Lock RECOVERY_LOCK = new TestingLock();

  @BeforeEach
  void before() {
    MetricsReporterFactoryForTests.reset();
  }

  @Test
  void testProcess() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TaskResult> source = new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    DataStream<MaintenanceResult> stream =
        source
            .dataStream()
            .transform(
                DUMMY_NAME,
                TypeInformation.of(MaintenanceResult.class),
                new LockRemover(new TestingLockFactory(), Lists.newArrayList(TASK_0, TASK_1)))
            .setParallelism(1);

    try (CloseableIterator<MaintenanceResult> result = stream.executeAndCollect()) {
      LOCK.tryLock();
      assertThat(LOCK.isHeld()).isTrue();

      // Start a successful trigger for task1 and assert the return value is correct
      processAndCheck(source, result, new TaskResult(0, 0L, true, Lists.newArrayList()));

      // Assert that the lock is removed
      assertThat(LOCK.isHeld()).isFalse();
    }
  }

  @Test
  void testMetrics() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TaskResult> source = new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    DataStream<MaintenanceResult> stream =
        source
            .dataStream()
            .transform(
                DUMMY_NAME,
                TypeInformation.of(MaintenanceResult.class),
                new LockRemover(new TestingLockFactory(), Lists.newArrayList(TASK_0, TASK_1)))
            .setParallelism(1);

    try (CloseableIterator<MaintenanceResult> result = stream.executeAndCollect()) {
      // Start the 2 successful and one failed result trigger for task1, and 3 successful for task2
      processAndCheck(source, result, new TaskResult(0, 0L, true, Lists.newArrayList()));
      processAndCheck(source, result, new TaskResult(1, 1L, true, Lists.newArrayList()));
      processAndCheck(source, result, new TaskResult(1, 2L, true, Lists.newArrayList()));
      processAndCheck(source, result, new TaskResult(0, 3L, false, Lists.newArrayList()));
      processAndCheck(source, result, new TaskResult(0, 4L, true, Lists.newArrayList()));
      processAndCheck(source, result, new TaskResult(1, 5L, true, Lists.newArrayList()));

      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          DUMMY_NAME + "." + TASK_1 + "." + SUCCEEDED_TASK_COUNTER)
                      .equals(3L));

      // Final check all the counters
      MetricsReporterFactoryForTests.assertCounters(
          new ImmutableMap.Builder<String, Long>()
              .put(DUMMY_NAME + "." + TASK_0 + "." + SUCCEEDED_TASK_COUNTER, 2L)
              .put(DUMMY_NAME + "." + TASK_0 + "." + FAILED_TASK_COUNTER, 1L)
              .put(DUMMY_NAME + "." + TASK_1 + "." + SUCCEEDED_TASK_COUNTER, 3L)
              .put(DUMMY_NAME + "." + TASK_1 + "." + FAILED_TASK_COUNTER, 0L)
              .build());
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
    ManualSource<TaskResult> source1 =
        new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    ManualSource<TaskResult> source2 =
        new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    DataStream<MaintenanceResult> stream =
        source1
            .dataStream()
            .union(source2.dataStream())
            .transform(
                DUMMY_NAME,
                TypeInformation.of(MaintenanceResult.class),
                new LockRemover(new TestingLockFactory(), Lists.newArrayList(TASK_0)))
            .setParallelism(1);

    try (CloseableIterator<MaintenanceResult> result = stream.executeAndCollect()) {
      RECOVERY_LOCK.tryLock();
      assertThat(RECOVERY_LOCK.isHeld()).isTrue();

      processAndCheck(source1, result, new TaskResult(0, 0L, true, Lists.newArrayList()));

      source1.sendRecord(new TaskResult(0, 1L, true, Lists.newArrayList()));
      // we receive the second result - this will not happen in real use cases, but with this we can
      // be sure that the previous watermark is processed
      result.next();

      // We did not remove the recovery lock, as no watermark received from the other source
      assertThat(RECOVERY_LOCK.isHeld()).isTrue();

      // Recovery arrives
      source1.sendWatermark(10L);
      source2.sendWatermark(10L);

      Awaitility.await().until(() -> !RECOVERY_LOCK.isHeld());
    }
  }

  private void processAndCheck(
      ManualSource<TaskResult> source,
      CloseableIterator<MaintenanceResult> iter,
      TaskResult expected) {
    source.sendRecord(expected);
    source.sendWatermark(expected.startEpoch());

    MaintenanceResult result = iter.next();
    assertThat(result.startEpoch()).isEqualTo(expected.startEpoch());
    assertThat(result.taskIndex()).isEqualTo(expected.taskIndex());
    assertThat(result.length()).isLessThan(System.currentTimeMillis() - expected.startEpoch());
    assertThat(result.success()).isEqualTo(expected.success());
    assertThat(result.exceptions()).hasSize(expected.exceptions().size());
  }

  private static class TestingLockFactory implements TriggerLockFactory {
    @Override
    public void open() {
      // Do nothing
    }

    @Override
    public Lock createLock() {
      return LOCK;
    }

    @Override
    public Lock createRecoveryLock() {
      return RECOVERY_LOCK;
    }

    @Override
    public void close() {
      // Do nothing
    }
  }

  private static class TestingLock implements TriggerLockFactory.Lock {
    private boolean locked = false;

    @Override
    public boolean tryLock() {
      if (isHeld()) {
        return false;
      } else {
        locked = true;
        return true;
      }
    }

    @Override
    public boolean isHeld() {
      return locked;
    }

    @Override
    public void unlock() {
      locked = false;
    }
  }
}
