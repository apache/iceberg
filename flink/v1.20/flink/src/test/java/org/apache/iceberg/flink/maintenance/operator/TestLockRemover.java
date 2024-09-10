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

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 10)
class TestLockRemover extends OperatorTestBase {
  private static final String[] TASKS = new String[] {"task0", "task1"};
  private static final TriggerLockFactory.Lock LOCK = new TestingLock();
  private static final TriggerLockFactory.Lock RECOVERY_LOCK = new TestingLock();

  @TempDir private File checkpointDir;

  @BeforeEach
  void before() {
    MetricsReporterFactoryForTests.reset();
  }

  @Test
  void testProcess() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ManualSource<TaskResult> source = new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    source
        .dataStream()
        .transform(
            DUMMY_NAME,
            TypeInformation.of(Void.class),
            new LockRemover(new TestingLockFactory(), Lists.newArrayList(TASKS)))
        .setParallelism(1);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      LOCK.tryLock();
      assertThat(LOCK.isHeld()).isTrue();

      // Start a successful trigger for task1 and assert the return value is correct
      processAndCheck(source, new TaskResult(0, 0L, true, Lists.newArrayList()));

      // Assert that the lock is removed
      assertThat(LOCK.isHeld()).isFalse();
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testInSink() throws Exception {
    String sinkName = "TestSink";
    Configuration config = new Configuration();
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + checkpointDir.getPath());
    config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(10));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    ManualSource<TaskResult> source = new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    source.dataStream().global().sinkTo(new SinkTest()).name(sinkName).setParallelism(1);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      LOCK.tryLock();
      assertThat(LOCK.isHeld()).isTrue();

      // Start a successful trigger for task1 and assert the return value is correct
      processAndCheck(source, new TaskResult(0, 0L, true, Lists.newArrayList()), sinkName + ": ");

      // Assert that the lock is removed
      assertThat(LOCK.isHeld()).isFalse();
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
            DUMMY_NAME,
            TypeInformation.of(Void.class),
            new LockRemover(new TestingLockFactory(), Lists.newArrayList(TASKS)))
        .setParallelism(1);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();
      // Start the 2 successful and one failed result trigger for task1, and 3 successful for task2
      processAndCheck(source, new TaskResult(0, 0L, true, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(1, 1L, true, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(1, 2L, true, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(0, 3L, false, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(0, 4L, true, Lists.newArrayList()));
      processAndCheck(source, new TaskResult(1, 5L, true, Lists.newArrayList()));

      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          DUMMY_NAME + "." + TASKS[1] + "." + SUCCEEDED_TASK_COUNTER)
                      .equals(3L));

      // Final check all the counters
      MetricsReporterFactoryForTests.assertCounters(
          new ImmutableMap.Builder<String, Long>()
              .put(DUMMY_NAME + "." + TASKS[0] + "." + SUCCEEDED_TASK_COUNTER, 2L)
              .put(DUMMY_NAME + "." + TASKS[0] + "." + FAILED_TASK_COUNTER, 1L)
              .put(DUMMY_NAME + "." + TASKS[1] + "." + SUCCEEDED_TASK_COUNTER, 3L)
              .put(DUMMY_NAME + "." + TASKS[1] + "." + FAILED_TASK_COUNTER, 0L)
              .build());
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
    ManualSource<TaskResult> source1 =
        new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    ManualSource<TaskResult> source2 =
        new ManualSource<>(env, TypeInformation.of(TaskResult.class));
    source1
        .dataStream()
        .union(source2.dataStream())
        .transform(
            DUMMY_NAME,
            TypeInformation.of(Void.class),
            new LockRemover(new TestingLockFactory(), Lists.newArrayList(TASKS[0])))
        .setParallelism(1);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();
      RECOVERY_LOCK.tryLock();
      assertThat(RECOVERY_LOCK.isHeld()).isTrue();

      processAndCheck(source1, new TaskResult(0, 0L, true, Lists.newArrayList()));

      source1.sendRecord(new TaskResult(0, 1L, true, Lists.newArrayList()));
      // we receive the second result - this will not happen in real use cases, but with this we can
      // be sure that the previous watermark is processed
      Awaitility.await()
          .until(
              () ->
                  MetricsReporterFactoryForTests.counter(
                          DUMMY_NAME + "." + TASKS[0] + "." + SUCCEEDED_TASK_COUNTER)
                      .equals(2L));

      // We did not remove the recovery lock, as no watermark received from the other source
      assertThat(RECOVERY_LOCK.isHeld()).isTrue();

      // Recovery arrives
      source1.sendWatermark(10L);
      source2.sendWatermark(10L);

      Awaitility.await().until(() -> !RECOVERY_LOCK.isHeld());
    } finally {
      closeJobClient(jobClient);
    }
  }

  private void processAndCheck(ManualSource<TaskResult> source, TaskResult input) {
    processAndCheck(source, input, null);
  }

  private void processAndCheck(
      ManualSource<TaskResult> source, TaskResult input, String counterPrefix) {
    source.sendRecord(input);
    source.sendWatermark(input.startEpoch());

    String counterName =
        (counterPrefix != null ? counterPrefix : "")
            .concat(
                input.success()
                    ? DUMMY_NAME + "." + TASKS[input.taskIndex()] + "." + SUCCEEDED_TASK_COUNTER
                    : DUMMY_NAME + "." + TASKS[input.taskIndex()] + "." + FAILED_TASK_COUNTER);
    Long counterValue = MetricsReporterFactoryForTests.counter(counterName);
    Long expected = counterValue != null ? counterValue + 1 : 1L;

    Awaitility.await()
        .until(() -> expected.equals(MetricsReporterFactoryForTests.counter(counterName)));
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

  private static class SinkTest
      implements Sink<TaskResult>,
          SupportsCommitter<TaskResult>,
          SupportsPostCommitTopology<TaskResult> {
    @Override
    public SinkWriter<TaskResult> createWriter(InitContext initContext) {
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
              DUMMY_NAME,
              TypeInformation.of(Void.class),
              new LockRemover(new TestingLockFactory(), Lists.newArrayList(TASKS[0])));
    }
  }
}
