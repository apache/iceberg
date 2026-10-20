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

import static org.apache.iceberg.flink.maintenance.api.TableMaintenance.LOCK_REMOVER_OPERATOR_NAME;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.SUCCEEDED_TASK_COUNTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.operator.ManualSource;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/**
 * ConvertEqualityDeletes shares the Flink-maintained maintenance lock with another task. The lock
 * is released only when the unioned watermark reaches the lock remover, so a watermark from the
 * other task has to pass through an idle converter.
 */
class TestConvertEqualityDeletesSharedLock extends OperatorTestBase {
  private static final String PROBE_TASK_NAME = "SharedLockProbe";

  @Test
  void releasesSharedLockAfterEachCycle() throws Exception {
    Table table = createTable(3, FileFormat.PARQUET);
    String tableName = table.name();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    ManualSource<TableChange> changes =
        new ManualSource<>(env, TypeInformation.of(TableChange.class));

    TableMaintenance.forChangeStream(changes.dataStream(), tableLoader())
        .uidSuffix("ConvertEqualityDeletesSharedLock")
        .rateLimit(Duration.ofMillis(50))
        .lockCheckDelay(Duration.ofMillis(50))
        .add(new CompletingTaskBuilder().scheduleOnCommitCount(1))
        .add(
            ConvertEqualityDeletes.builder()
                .scheduleOnCommitCount(1)
                .stagingBranch(SnapshotRef.MAIN_BRANCH)
                .targetBranch(SnapshotRef.MAIN_BRANCH)
                .equalityFieldColumns(ImmutableList.of("id"))
                .parallelism(1))
        .append();

    List<String> probeSucceeded =
        ImmutableList.of(
            LOCK_REMOVER_OPERATOR_NAME,
            tableName,
            PROBE_TASK_NAME + " [0]",
            "0",
            SUCCEEDED_TASK_COUNTER);
    List<String> converterSucceeded =
        ImmutableList.of(
            LOCK_REMOVER_OPERATOR_NAME,
            tableName,
            "ConvertEqualityDeletes [1]",
            "1",
            SUCCEEDED_TASK_COUNTER);

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // The probe runs first, while the converter has no plan. Its watermark must leave the
      // converter or the shared lock stays held and the converter never starts.
      changes.sendRecord(TableChange.builder().commitCount(1).build());
      awaitSucceeded(probeSucceeded, 1L);
      awaitSucceeded(converterSucceeded, 1L);

      changes.sendRecord(TableChange.builder().commitCount(1).build());
      awaitSucceeded(probeSucceeded, 2L);
      awaitSucceeded(converterSucceeded, 2L);
    } finally {
      closeJobClient(jobClient);
    }
  }

  private static void awaitSucceeded(List<String> counterKey, long expected) {
    Awaitility.await()
        .atMost(Duration.ofMinutes(3))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () ->
                assertThat(MetricsReporterFactoryForTests.counter(counterKey)).isEqualTo(expected));
  }

  private static class CompletingTaskBuilder extends MaintenanceTaskBuilder<CompletingTaskBuilder> {

    @Override
    String maintenanceTaskName() {
      return PROBE_TASK_NAME;
    }

    @Override
    DataStream<TaskResult> append(DataStream<Trigger> trigger) {
      return setSlotSharingGroup(
          trigger
              .map(new CompletingTask())
              .name(maintenanceTaskName())
              .uid(uidSuffix() + "-shared-lock-probe")
              .forceNonParallel());
    }
  }

  private static class CompletingTask
      implements MapFunction<Trigger, TaskResult>, ResultTypeQueryable<TaskResult>, Serializable {

    @Override
    public TaskResult map(Trigger trigger) {
      return new TaskResult(trigger.taskId(), trigger.timestamp(), true, Collections.emptyList());
    }

    @Override
    public TypeInformation<TaskResult> getProducedType() {
      return TypeInformation.of(TaskResult.class);
    }
  }
}
