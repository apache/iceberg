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
package org.apache.iceberg.flink.maintenance.api;

import static org.apache.iceberg.flink.SimpleDataUtil.createRowData;
import static org.apache.iceberg.flink.maintenance.api.TableMaintenance.LOCK_REMOVER_OPERATOR_NAME;
import static org.apache.iceberg.flink.maintenance.api.TableMaintenance.SOURCE_OPERATOR_NAME_PREFIX;
import static org.apache.iceberg.flink.maintenance.api.TableMaintenance.TRIGGER_MANAGER_OPERATOR_NAME;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.CONCURRENT_RUN_THROTTLED;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.FAILED_TASK_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.NOTHING_TO_TRIGGER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.RATE_LIMITER_TRIGGERED;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.SUCCEEDED_TASK_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.TRIGGERED;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.ManualSource;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestTableMaintenance extends OperatorTestBase {
  private static final String[] TASKS =
      new String[] {
        MaintenanceTaskBuilderForTest.class.getSimpleName() + " [0]",
        MaintenanceTaskBuilderForTest.class.getSimpleName() + " [1]"
      };
  private static final TableChange DUMMY_CHANGE = TableChange.builder().commitCount(1).build();
  private static final List<Trigger> PROCESSED =
      Collections.synchronizedList(Lists.newArrayListWithCapacity(1));

  private StreamExecutionEnvironment env;
  private Table table;

  @TempDir private File checkpointDir;

  @BeforeEach
  public void beforeEach() throws IOException {
    Configuration config = new Configuration();
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + checkpointDir.getPath());
    this.env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    this.table = createTable();
    insert(table, 1, "a");

    PROCESSED.clear();
    MaintenanceTaskBuilderForTest.counter = 0;
  }

  @Test
  void testForChangeStream() throws Exception {
    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.forChangeStream(schedulerSource.dataStream(), tableLoader(), LOCK_FACTORY)
            .rateLimit(Duration.ofMillis(2))
            .lockCheckDelay(Duration.ofSeconds(3))
            .add(
                new MaintenanceTaskBuilderForTest(true)
                    .scheduleOnCommitCount(1)
                    .scheduleOnDataFileCount(2)
                    .scheduleOnDataFileSize(3L)
                    .scheduleOnEqDeleteFileCount(4)
                    .scheduleOnEqDeleteRecordCount(5L)
                    .scheduleOnPosDeleteFileCount(6)
                    .scheduleOnPosDeleteRecordCount(7L)
                    .scheduleOnInterval(Duration.ofHours(1)));

    sendEvents(schedulerSource, streamBuilder, ImmutableList.of(Tuple2.of(DUMMY_CHANGE, 1)));
  }

  @Test
  void testForTable() throws Exception {
    TableLoader tableLoader = tableLoader();

    env.enableCheckpointing(10);

    TableMaintenance.forTable(env, tableLoader, LOCK_FACTORY)
        .rateLimit(Duration.ofMillis(2))
        .maxReadBack(2)
        .add(new MaintenanceTaskBuilderForTest(true).scheduleOnCommitCount(2))
        .append();

    // Creating a stream for inserting data into the table concurrently
    ManualSource<RowData> insertSource =
        new ManualSource<>(env, InternalTypeInfo.of(FlinkSchemaUtil.convert(table.schema())));
    FlinkSink.forRowData(insertSource.dataStream())
        .tableLoader(tableLoader)
        .uidPrefix(UID_SUFFIX + "-iceberg-sink")
        .append();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      insertSource.sendRecord(createRowData(2, "b"));

      Awaitility.await().until(() -> PROCESSED.size() == 1);
    } finally {
      closeJobClient(jobClient);
    }
  }

  @Test
  void testLocking() throws Exception {
    TriggerLockFactory.Lock lock = LOCK_FACTORY.createLock();

    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.forChangeStream(schedulerSource.dataStream(), tableLoader(), LOCK_FACTORY)
            .rateLimit(Duration.ofMillis(2))
            .add(new MaintenanceTaskBuilderForTest(true).scheduleOnCommitCount(1));

    assertThat(lock.isHeld()).isFalse();
    sendEvents(schedulerSource, streamBuilder, ImmutableList.of(Tuple2.of(DUMMY_CHANGE, 1)));

    assertThat(lock.isHeld()).isFalse();
  }

  @Test
  void testMetrics() throws Exception {
    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.forChangeStream(schedulerSource.dataStream(), tableLoader(), LOCK_FACTORY)
            .rateLimit(Duration.ofMillis(2))
            .lockCheckDelay(Duration.ofMillis(2))
            .add(new MaintenanceTaskBuilderForTest(true).scheduleOnCommitCount(1))
            .add(new MaintenanceTaskBuilderForTest(false).scheduleOnCommitCount(2));

    sendEvents(
        schedulerSource,
        streamBuilder,
        ImmutableList.of(Tuple2.of(DUMMY_CHANGE, 1), Tuple2.of(DUMMY_CHANGE, 2)));

    Awaitility.await()
        .until(
            () ->
                MetricsReporterFactoryForTests.counter(
                        ImmutableList.of(
                            LOCK_REMOVER_OPERATOR_NAME,
                            table.name(),
                            TASKS[0],
                            "0",
                            SUCCEEDED_TASK_COUNTER))
                    .equals(2L));

    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    LOCK_REMOVER_OPERATOR_NAME,
                    table.name(),
                    TASKS[0],
                    "0",
                    SUCCEEDED_TASK_COUNTER),
                2L)
            .put(
                ImmutableList.of(
                    LOCK_REMOVER_OPERATOR_NAME, table.name(), TASKS[0], "0", FAILED_TASK_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    TRIGGER_MANAGER_OPERATOR_NAME, table.name(), TASKS[0], "0", TRIGGERED),
                2L)
            .put(
                ImmutableList.of(
                    LOCK_REMOVER_OPERATOR_NAME,
                    table.name(),
                    TASKS[1],
                    "1",
                    SUCCEEDED_TASK_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    LOCK_REMOVER_OPERATOR_NAME, table.name(), TASKS[1], "1", FAILED_TASK_COUNTER),
                1L)
            .put(
                ImmutableList.of(
                    TRIGGER_MANAGER_OPERATOR_NAME, table.name(), TASKS[1], "1", TRIGGERED),
                1L)
            .put(
                ImmutableList.of(TRIGGER_MANAGER_OPERATOR_NAME, table.name(), NOTHING_TO_TRIGGER),
                -1L)
            .put(
                ImmutableList.of(
                    TRIGGER_MANAGER_OPERATOR_NAME, table.name(), CONCURRENT_RUN_THROTTLED),
                -1L)
            .put(
                ImmutableList.of(
                    TRIGGER_MANAGER_OPERATOR_NAME, table.name(), RATE_LIMITER_TRIGGERED),
                -1L)
            .build());
  }

  @Test
  void testUidAndSlotSharingGroup() throws IOException {
    TableMaintenance.forChangeStream(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).dataStream(),
            tableLoader(),
            LOCK_FACTORY)
        .uidSuffix(UID_SUFFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(
            new MaintenanceTaskBuilderForTest(true)
                .scheduleOnCommitCount(1)
                .uidSuffix(UID_SUFFIX)
                .slotSharingGroup(SLOT_SHARING_GROUP))
        .append();

    checkUidsAreSet(env, UID_SUFFIX);
    checkSlotSharingGroupsAreSet(env, SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupUnset() throws IOException {
    TableMaintenance.forChangeStream(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).dataStream(),
            tableLoader(),
            LOCK_FACTORY)
        .add(new MaintenanceTaskBuilderForTest(true).scheduleOnCommitCount(1))
        .append();

    checkUidsAreSet(env, null);
    checkSlotSharingGroupsAreSet(env, null);
  }

  @Test
  void testUidAndSlotSharingGroupInherit() throws IOException {
    TableMaintenance.forChangeStream(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).dataStream(),
            tableLoader(),
            LOCK_FACTORY)
        .uidSuffix(UID_SUFFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(new MaintenanceTaskBuilderForTest(true).scheduleOnCommitCount(1))
        .append();

    checkUidsAreSet(env, UID_SUFFIX);
    checkSlotSharingGroupsAreSet(env, SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupOverWrite() throws IOException {
    String anotherUid = "Another-UID";
    String anotherSlotSharingGroup = "Another-SlotSharingGroup";
    TableMaintenance.forChangeStream(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).dataStream(),
            tableLoader(),
            LOCK_FACTORY)
        .uidSuffix(UID_SUFFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(
            new MaintenanceTaskBuilderForTest(true)
                .scheduleOnCommitCount(1)
                .uidSuffix(anotherUid)
                .slotSharingGroup(anotherSlotSharingGroup))
        .append();

    // Choose an operator from the scheduler part of the graph
    Transformation<?> schedulerTransformation =
        env.getTransformations().stream()
            .filter(t -> t.getName().equals("Trigger manager"))
            .findFirst()
            .orElseThrow();
    assertThat(schedulerTransformation.getUid()).contains(UID_SUFFIX);
    assertThat(schedulerTransformation.getSlotSharingGroup()).isPresent();
    assertThat(schedulerTransformation.getSlotSharingGroup().get().getName())
        .isEqualTo(SLOT_SHARING_GROUP);

    // Choose an operator from the maintenance task part of the graph
    Transformation<?> scheduledTransformation =
        env.getTransformations().stream()
            .filter(
                t -> t.getName().startsWith(MaintenanceTaskBuilderForTest.class.getSimpleName()))
            .findFirst()
            .orElseThrow();
    assertThat(scheduledTransformation.getUid()).contains(anotherUid);
    assertThat(scheduledTransformation.getSlotSharingGroup()).isPresent();
    assertThat(scheduledTransformation.getSlotSharingGroup().get().getName())
        .isEqualTo(anotherSlotSharingGroup);
  }

  @Test
  void testUidAndSlotSharingGroupForMonitorSource() throws IOException {
    TableMaintenance.forTable(env, tableLoader(), LOCK_FACTORY)
        .uidSuffix(UID_SUFFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(
            new MaintenanceTaskBuilderForTest(true)
                .scheduleOnCommitCount(1)
                .uidSuffix(UID_SUFFIX)
                .slotSharingGroup(SLOT_SHARING_GROUP))
        .append();

    Transformation<?> source = monitorSource();
    assertThat(source).isNotNull();
    assertThat(source.getUid()).contains(UID_SUFFIX);
    assertThat(source.getSlotSharingGroup()).isPresent();
    assertThat(source.getSlotSharingGroup().get().getName()).isEqualTo(SLOT_SHARING_GROUP);

    checkUidsAreSet(env, UID_SUFFIX);
    checkSlotSharingGroupsAreSet(env, SLOT_SHARING_GROUP);
  }

  /**
   * Sends the events though the {@link ManualSource} provided, and waits until the given number of
   * records are processed.
   *
   * @param schedulerSource used for sending the events
   * @param streamBuilder used for generating the job
   * @param eventsAndResultNumbers the pair of the event and the expected processed records
   * @throws Exception if any
   */
  private void sendEvents(
      ManualSource<TableChange> schedulerSource,
      TableMaintenance.Builder streamBuilder,
      List<Tuple2<TableChange, Integer>> eventsAndResultNumbers)
      throws Exception {
    streamBuilder.append();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      eventsAndResultNumbers.forEach(
          eventsAndResultNumber -> {
            int expectedSize = PROCESSED.size() + eventsAndResultNumber.f1;
            schedulerSource.sendRecord(eventsAndResultNumber.f0);
            Awaitility.await()
                .until(
                    () -> PROCESSED.size() == expectedSize && !LOCK_FACTORY.createLock().isHeld());
          });
    } finally {
      closeJobClient(jobClient);
    }
  }

  /**
   * Finds the {@link org.apache.iceberg.flink.maintenance.operator.MonitorSource} for testing
   * purposes by parsing the transformation tree.
   *
   * @return The monitor source if we found it
   */
  private Transformation<?> monitorSource() {
    assertThat(env.getTransformations()).isNotEmpty();
    assertThat(env.getTransformations().get(0).getInputs()).isNotEmpty();
    assertThat(env.getTransformations().get(0).getInputs().get(0).getInputs()).isNotEmpty();

    Transformation<?> result =
        env.getTransformations().get(0).getInputs().get(0).getInputs().get(0);

    // Some checks to make sure this is the transformation we are looking for
    assertThat(result).isInstanceOf(SourceTransformation.class);
    assertThat(result.getName()).startsWith(SOURCE_OPERATOR_NAME_PREFIX);

    return result;
  }

  private static class MaintenanceTaskBuilderForTest
      extends MaintenanceTaskBuilder<MaintenanceTaskBuilderForTest> {
    private final boolean success;
    private final int id;
    private static int counter = 0;

    MaintenanceTaskBuilderForTest(boolean success) {
      this.success = success;
      this.id = counter;
      ++counter;
    }

    @Override
    DataStream<TaskResult> append(DataStream<Trigger> trigger) {
      String name = TASKS[id];
      return trigger
          .map(new DummyMaintenanceTask(success))
          .name(name)
          .uid(uidSuffix() + "-test-mapper-" + name + "-" + id)
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
    }
  }

  private static class DummyMaintenanceTask
      implements MapFunction<Trigger, TaskResult>, ResultTypeQueryable<TaskResult>, Serializable {
    private final boolean success;

    private DummyMaintenanceTask(boolean success) {
      this.success = success;
    }

    @Override
    public TaskResult map(Trigger trigger) {
      // Ensure that the lock is held when processing
      assertThat(LOCK_FACTORY.createLock().isHeld()).isTrue();
      PROCESSED.add(trigger);

      return new TaskResult(
          trigger.taskId(),
          trigger.timestamp(),
          success,
          success ? Collections.emptyList() : Lists.newArrayList(new Exception("Testing error")));
    }

    @Override
    public TypeInformation<TaskResult> getProducedType() {
      return TypeInformation.of(TaskResult.class);
    }
  }
}
