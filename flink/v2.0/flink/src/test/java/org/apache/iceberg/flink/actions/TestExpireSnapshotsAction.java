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
package org.apache.iceberg.flink.actions;

import static org.apache.iceberg.flink.SimpleDataUtil.createRecord;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.DELETE_FILE_FAILED_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.DELETE_FILE_SUCCEEDED_COUNTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ExpireSnapshotsActionResult;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.MaintenanceTaskTestBase;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestExpireSnapshotsAction extends MaintenanceTaskTestBase {
  private Table table;
  private static final String TASK_NAME = "ExpireSnapshots";
  private static final String DELETE_FILES_OPERATOR_NAME = "Delete file";

  @BeforeEach
  void before() {
    MetricsReporterFactoryForTests.reset();
    this.table = createTable();
    tableLoader().open();
  }

  @AfterEach
  void after() throws IOException {
    tableLoader().close();
  }

  @Test
  void testExpireSnapshots() throws Exception {
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");

    Set<Snapshot> snapshots = Sets.newHashSet(table.snapshots());
    assertThat(snapshots).hasSize(4);

    long triggerTime = System.currentTimeMillis();
    ExpireSnapshots.Builder builder =
        ExpireSnapshots.builder()
            .deleteBatchSize(3)
            .maxSnapshotAge(Duration.ZERO)
            .planningWorkerPoolSize(1)
            .retainLast(1);

    TaskResult result =
        new TableMaintenanceAction(
                StreamExecutionEnvironment.getExecutionEnvironment(),
                tableLoader(),
                builder,
                triggerTime)
            .collect();

    assertThat(result.success()).isTrue();
    assertThat(result.startEpoch()).isEqualTo(triggerTime);
    assertThat(result.exceptions()).isEmpty();

    ExpireSnapshotsActionResult actionResult = (ExpireSnapshotsActionResult) result.actionResult();
    assertThat(actionResult.deletedFiles()).isEqualTo(3);

    table.refresh();
    assertThat(Sets.newHashSet(table.snapshots())).hasSize(1);
    // Check that the table data not changed
    SimpleDataUtil.assertTableRecords(
        table,
        ImmutableList.of(
            createRecord(1, "a"),
            createRecord(2, "b"),
            createRecord(3, "c"),
            createRecord(4, "d")));
  }

  @Test
  void testMetrics() throws Exception {
    insert(table, 1, "a");
    insert(table, 2, "b");

    long triggerTime = System.currentTimeMillis();
    ExpireSnapshots.Builder builder =
        ExpireSnapshots.builder()
            .deleteBatchSize(1)
            .maxSnapshotAge(Duration.ZERO)
            .planningWorkerPoolSize(1)
            .retainLast(1)
            .parallelism(1);

    TaskResult result =
        new TableMaintenanceAction(
                StreamExecutionEnvironment.getExecutionEnvironment(),
                tableLoader(),
                builder,
                triggerTime)
            .collect();

    assertThat(result.success()).isTrue();
    assertThat(result.startEpoch()).isEqualTo(triggerTime);
    assertThat(result.exceptions()).isEmpty();

    ExpireSnapshotsActionResult actionResult = (ExpireSnapshotsActionResult) result.actionResult();
    assertThat(actionResult.deletedFiles()).isEqualTo(1);

    // Check the metrics
    Awaitility.await()
        .untilAsserted(
            () ->
                MetricsReporterFactoryForTests.assertCounters(
                    new ImmutableMap.Builder<List<String>, Long>()
                        .put(
                            ImmutableList.of(
                                DELETE_FILES_OPERATOR_NAME + "[0]",
                                table.name(),
                                TASK_NAME,
                                "0",
                                DELETE_FILE_FAILED_COUNTER),
                            0L)
                        .put(
                            ImmutableList.of(
                                DELETE_FILES_OPERATOR_NAME + "[0]",
                                table.name(),
                                TASK_NAME,
                                "0",
                                DELETE_FILE_SUCCEEDED_COUNTER),
                            1L)
                        .build()));
  }
}
