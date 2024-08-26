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
package org.apache.iceberg.flink.maintenance.stream;

import static org.apache.iceberg.flink.SimpleDataUtil.createRowData;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.CONCURRENT_RUN_THROTTLED;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.FAILED_TASK_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.GROUP_VALUE_DEFAULT;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.NOTHING_TO_TRIGGER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.RATE_LIMITER_TRIGGERED;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.SUCCEEDED_TASK_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.TRIGGERED;
import static org.apache.iceberg.flink.maintenance.stream.TableMaintenance.LOCK_REMOVER_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.stream.TableMaintenance.SOURCE_NAME;
import static org.apache.iceberg.flink.maintenance.stream.TableMaintenance.TRIGGER_MANAGER_TASK_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
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
import org.apache.iceberg.flink.maintenance.operator.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.Trigger;
import org.apache.iceberg.flink.maintenance.operator.TriggerLockFactory;
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

  @TempDir private File checkpointDir;

  @BeforeEach
  public void beforeEach() {
    Configuration config = new Configuration();
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + checkpointDir.getPath());
    env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    PROCESSED.clear();
    MaintenanceTaskBuilderForTest.counter = 0;
  }

  @Test
  void testFromStream() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.builder(schedulerSource.dataStream(), tableLoader, LOCK_FACTORY)
            .rateLimit(Duration.ofMillis(2))
            .concurrentCheckDelay(Duration.ofSeconds(3))
            .add(
                new MaintenanceTaskBuilderForTest(true)
                    .scheduleOnCommitCount(1)
                    .scheduleOnDataFileCount(2)
                    .scheduleOnDataFileSize(3L)
                    .scheduleOnEqDeleteFileCount(4)
                    .scheduleOnEqDeleteRecordCount(5L)
                    .schedulerOnPosDeleteFileCount(6)
                    .schedulerOnPosDeleteRecordCount(7L)
                    .scheduleOnTime(Duration.ofHours(1)));

    sendEvents(schedulerSource, streamBuilder, ImmutableList.of(Tuple2.of(DUMMY_CHANGE, 1)));
  }

  @Test
  void testFromEnv() throws Exception {
    sql.exec(
        "CREATE TABLE %s (id int, data varchar)"
            + "WITH ('flink.max-continuous-empty-commits'='100000')",
        TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();
    Table table = tableLoader.loadTable();

    env.enableCheckpointing(10);

    TableMaintenance.builder(env, tableLoader, LOCK_FACTORY)
        .rateLimit(Duration.ofMillis(2))
        .maxReadBack(2)
        .add(new MaintenanceTaskBuilderForTest(true).scheduleOnCommitCount(2))
        .append();

    // Creating a stream for inserting data into the table concurrently
    ManualSource<RowData> insertSource =
        new ManualSource<>(env, InternalTypeInfo.of(FlinkSchemaUtil.convert(table.schema())));
    FlinkSink.forRowData(insertSource.dataStream())
        .tableLoader(tableLoader)
        .uidPrefix(UID_PREFIX + "-iceberg-sink")
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
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TriggerLockFactory.Lock lock = LOCK_FACTORY.createLock();

    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.builder(schedulerSource.dataStream(), tableLoader, LOCK_FACTORY)
            .rateLimit(Duration.ofMillis(2))
            .add(new MaintenanceTaskBuilderForTest(true).scheduleOnCommitCount(1));

    assertThat(lock.isHeld()).isFalse();
    sendEvents(schedulerSource, streamBuilder, ImmutableList.of(Tuple2.of(DUMMY_CHANGE, 1)));

    assertThat(lock.isHeld()).isFalse();
  }

  @Test
  void testMetrics() throws Exception {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    ManualSource<TableChange> schedulerSource =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.Builder streamBuilder =
        TableMaintenance.builder(schedulerSource.dataStream(), tableLoader, LOCK_FACTORY)
            .rateLimit(Duration.ofMillis(2))
            .concurrentCheckDelay(Duration.ofMillis(2))
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
                        LOCK_REMOVER_TASK_NAME + "." + TASKS[0] + "." + SUCCEEDED_TASK_COUNTER)
                    .equals(2L));

    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<String, Long>()
            .put(LOCK_REMOVER_TASK_NAME + "." + TASKS[0] + "." + SUCCEEDED_TASK_COUNTER, 2L)
            .put(LOCK_REMOVER_TASK_NAME + "." + TASKS[0] + "." + FAILED_TASK_COUNTER, 0L)
            .put(TRIGGER_MANAGER_TASK_NAME + "." + TASKS[0] + "." + TRIGGERED, 2L)
            .put(LOCK_REMOVER_TASK_NAME + "." + TASKS[1] + "." + SUCCEEDED_TASK_COUNTER, 0L)
            .put(LOCK_REMOVER_TASK_NAME + "." + TASKS[1] + "." + FAILED_TASK_COUNTER, 1L)
            .put(TRIGGER_MANAGER_TASK_NAME + "." + TASKS[1] + "." + TRIGGERED, 1L)
            .put(
                TRIGGER_MANAGER_TASK_NAME + "." + GROUP_VALUE_DEFAULT + "." + NOTHING_TO_TRIGGER,
                -1L)
            .put(
                TRIGGER_MANAGER_TASK_NAME
                    + "."
                    + GROUP_VALUE_DEFAULT
                    + "."
                    + CONCURRENT_RUN_THROTTLED,
                -1L)
            .put(
                TRIGGER_MANAGER_TASK_NAME
                    + "."
                    + GROUP_VALUE_DEFAULT
                    + "."
                    + RATE_LIMITER_TRIGGERED,
                -1L)
            .build());
  }

  @Test
  void testUidAndSlotSharingGroup() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TableMaintenance.builder(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).dataStream(),
            tableLoader,
            LOCK_FACTORY)
        .uidPrefix(UID_PREFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(
            new MaintenanceTaskBuilderForTest(true)
                .scheduleOnCommitCount(1)
                .uidPrefix(UID_PREFIX)
                .slotSharingGroup(SLOT_SHARING_GROUP))
        .append();

    checkUidsAreSet(env, UID_PREFIX);
    checkSlotSharingGroupsAreSet(env, SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupUnset() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TableMaintenance.builder(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).dataStream(),
            tableLoader,
            LOCK_FACTORY)
        .add(new MaintenanceTaskBuilderForTest(true).scheduleOnCommitCount(1))
        .append();

    checkUidsAreSet(env, null);
    checkSlotSharingGroupsAreSet(env, null);
  }

  @Test
  void testUidAndSlotSharingGroupInherit() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TableMaintenance.builder(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).dataStream(),
            tableLoader,
            LOCK_FACTORY)
        .uidPrefix(UID_PREFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(new MaintenanceTaskBuilderForTest(true).scheduleOnCommitCount(1))
        .append();

    checkUidsAreSet(env, UID_PREFIX);
    checkSlotSharingGroupsAreSet(env, SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupOverWrite() {
    String anotherUid = "Another-UID";
    String anotherSlotSharingGroup = "Another-SlotSharingGroup";
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TableMaintenance.builder(
            new ManualSource<>(env, TypeInformation.of(TableChange.class)).dataStream(),
            tableLoader,
            LOCK_FACTORY)
        .uidPrefix(UID_PREFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(
            new MaintenanceTaskBuilderForTest(true)
                .scheduleOnCommitCount(1)
                .uidPrefix(anotherUid)
                .slotSharingGroup(anotherSlotSharingGroup))
        .append();

    // Something from the scheduler
    Transformation<?> schedulerTransformation =
        env.getTransformations().stream()
            .filter(t -> t.getName().equals("Trigger manager"))
            .findFirst()
            .orElseThrow();
    assertThat(schedulerTransformation.getUid()).contains(UID_PREFIX);
    assertThat(schedulerTransformation.getSlotSharingGroup()).isPresent();
    assertThat(schedulerTransformation.getSlotSharingGroup().get().getName())
        .isEqualTo(SLOT_SHARING_GROUP);

    // Something from the scheduled stream
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
  void testUidAndSlotSharingGroupForMonitor() {
    sql.exec("CREATE TABLE %s (id int, data varchar)", TABLE_NAME);
    sql.exec("INSERT INTO %s VALUES (1, 'a')", TABLE_NAME);

    TableLoader tableLoader = sql.tableLoader(TABLE_NAME);
    tableLoader.open();

    TableMaintenance.builder(env, tableLoader, LOCK_FACTORY)
        .uidPrefix(UID_PREFIX)
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .add(
            new MaintenanceTaskBuilderForTest(true)
                .scheduleOnCommitCount(1)
                .uidPrefix(UID_PREFIX)
                .slotSharingGroup(SLOT_SHARING_GROUP))
        .append();

    Transformation<?> source = monitorSource();
    assertThat(source).isNotNull();
    assertThat(source.getUid()).contains(UID_PREFIX);
    assertThat(source.getSlotSharingGroup()).isPresent();
    assertThat(source.getSlotSharingGroup().get().getName()).isEqualTo(SLOT_SHARING_GROUP);

    checkUidsAreSet(env, UID_PREFIX);
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
    DataStream<TaskResult> buildInternal(DataStream<Trigger> trigger) {
      String name = TASKS[id];
      return trigger
          .map(new DummyMaintenanceTask(success))
          .name(name)
          .uid(uidPrefix() + "-test-mapper-" + name)
          .slotSharingGroup(slotSharingGroup())
          .forceNonParallel();
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
    assertThat(result.getName()).isEqualTo(SOURCE_NAME);

    return result;
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
