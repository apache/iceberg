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
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ADDED_DATA_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ADDED_DATA_FILE_SIZE_METRIC;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ERROR_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.REMOVED_DATA_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.REMOVED_DATA_FILE_SIZE_METRIC;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.api.MaintenanceTaskTestBase;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenanceAction;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestRewriteDataFilesActionV2 extends MaintenanceTaskTestBase {

  private static final String TASK_NAME = "RewriteDataFiles";
  private static final String PLANNER_TASK_NAME = "RDF Planner";
  private static final String REWRITE_TASK_NAME = "Rewrite";
  private static final String COMMIT_TASK_NAME = "Rewrite commit";

  @Test
  void testRewriteUnPartitioned() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");

    assertFileNum(table, 4, 0);

    long triggerTime = System.currentTimeMillis();

    RewriteDataFiles.Builder builder =
        RewriteDataFiles.builder()
            .parallelism(2)
            .deleteFileThreshold(10)
            .targetFileSizeBytes(1_000_000L)
            .maxFileGroupSizeBytes(10_000_000L)
            .maxFileSizeBytes(2_000_000L)
            .minFileSizeBytes(500_000L)
            .minInputFiles(2)
            .partialProgressEnabled(true)
            .partialProgressMaxCommits(1)
            .maxRewriteBytes(100_000L)
            .rewriteAll(false)
            .collectResults(true);

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
    RewriteDataFilesActionResult actionResult =
        (RewriteDataFilesActionResult) result.actionResult();
    assertThat(actionResult.addedDataFiles()).hasSize(1);
    assertThat(actionResult.deletedDataFiles()).hasSize(4);

    assertFileNum(table, 1, 0);

    SimpleDataUtil.assertTableRecords(
        table,
        ImmutableList.of(
            createRecord(1, "a"),
            createRecord(2, "b"),
            createRecord(3, "c"),
            createRecord(4, "d")));
  }

  @Test
  void testRewriteUnPartitionedWithFilter() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");

    assertFileNum(table, 4, 0);

    long triggerTime = System.currentTimeMillis();

    RewriteDataFiles.Builder builder =
        RewriteDataFiles.builder()
            .parallelism(2)
            .deleteFileThreshold(10)
            .targetFileSizeBytes(1_000_000L)
            .maxFileGroupSizeBytes(10_000_000L)
            .maxFileSizeBytes(2_000_000L)
            .minFileSizeBytes(500_000L)
            .minInputFiles(2)
            .filter(Expressions.in("id", 1, 2))
            .partialProgressEnabled(true)
            .partialProgressMaxCommits(1)
            .maxRewriteBytes(100_000L)
            .rewriteAll(false)
            .collectResults(true);

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

    RewriteDataFilesActionResult actionResult =
        (RewriteDataFilesActionResult) result.actionResult();
    assertThat(actionResult.addedDataFiles()).hasSize(1);
    assertThat(actionResult.deletedDataFiles()).hasSize(2);

    assertFileNum(table, 3, 0);

    SimpleDataUtil.assertTableRecords(
        table,
        ImmutableList.of(
            createRecord(1, "a"),
            createRecord(2, "b"),
            createRecord(3, "c"),
            createRecord(4, "d")));
  }

  @Test
  void testRewritePartitioned() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");

    assertFileNum(table, 4, 0);

    long triggerTime = System.currentTimeMillis();

    RewriteDataFiles.Builder builder =
        RewriteDataFiles.builder().rewriteAll(true).collectResults(true);

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

    RewriteDataFilesActionResult actionResult =
        (RewriteDataFilesActionResult) result.actionResult();
    assertThat(actionResult.addedDataFiles()).hasSize(2);
    assertThat(actionResult.deletedDataFiles()).hasSize(4);

    assertFileNum(table, 2, 0);

    SimpleDataUtil.assertTableRecords(
        table,
        ImmutableList.of(
            createRecord(1, "p1"),
            createRecord(2, "p1"),
            createRecord(3, "p2"),
            createRecord(4, "p2")));
  }

  @Test
  void testMetrics() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    assertFileNum(table, 2, 0);

    long triggerTime = System.currentTimeMillis();
    RewriteDataFiles.Builder builder =
        RewriteDataFiles.builder().parallelism(1).rewriteAll(true).collectResults(true);

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

    RewriteDataFilesActionResult actionResult =
        (RewriteDataFilesActionResult) result.actionResult();
    assertThat(actionResult.addedDataFiles()).hasSize(1);
    assertThat(actionResult.deletedDataFiles()).hasSize(2);

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]", table.name(), TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    REWRITE_TASK_NAME + "[0]", table.name(), TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]", table.name(), TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    table.name(),
                    TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_NUM_METRIC),
                1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    table.name(),
                    TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_SIZE_METRIC),
                -1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    table.name(),
                    TASK_NAME,
                    "0",
                    REMOVED_DATA_FILE_NUM_METRIC),
                2L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    table.name(),
                    TASK_NAME,
                    "0",
                    REMOVED_DATA_FILE_SIZE_METRIC),
                -1L)
            .build());
  }

  @Test
  void testV2Table() throws Exception {
    Table table = createTableWithDelete();
    update(table, 1, null, "a", "b");
    update(table, 1, "b", "c");

    assertFileNum(table, 2, 3);
    SimpleDataUtil.assertTableRecords(table, ImmutableList.of(createRecord(1, "c")));

    long triggerTime = System.currentTimeMillis();
    RewriteDataFiles.Builder builder =
        RewriteDataFiles.builder().parallelism(1).rewriteAll(true).collectResults(true);

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

    // After #11131 we don't remove the delete files
    assertFileNum(table, 1, 1);

    SimpleDataUtil.assertTableRecords(table, ImmutableList.of(createRecord(1, "c")));

    RewriteDataFilesActionResult actionResult =
        (RewriteDataFilesActionResult) result.actionResult();
    assertThat(actionResult.addedDataFiles()).hasSize(1);
    assertThat(actionResult.deletedDataFiles()).hasSize(2);

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]", table.name(), TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    REWRITE_TASK_NAME + "[0]", table.name(), TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]", table.name(), TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    table.name(),
                    TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_NUM_METRIC),
                1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    table.name(),
                    TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_SIZE_METRIC),
                -1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    table.name(),
                    TASK_NAME,
                    "0",
                    REMOVED_DATA_FILE_NUM_METRIC),
                2L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    table.name(),
                    TASK_NAME,
                    "0",
                    REMOVED_DATA_FILE_SIZE_METRIC),
                -1L)
            .build());
  }

  private static void assertFileNum(
      Table table, int expectedDataFileNum, int expectedDeleteFileNum) {
    table.refresh();
    assertThat(
            table.currentSnapshot().dataManifests(table.io()).stream()
                .flatMap(
                    m ->
                        StreamSupport.stream(
                            ManifestFiles.read(m, table.io(), table.specs()).spliterator(), false))
                .count())
        .isEqualTo(expectedDataFileNum);
    assertThat(
            table.currentSnapshot().deleteManifests(table.io()).stream()
                .flatMap(
                    m ->
                        StreamSupport.stream(
                            ManifestFiles.readDeleteManifest(m, table.io(), table.specs())
                                .spliterator(),
                            false))
                .count())
        .isEqualTo(expectedDeleteFileNum);
  }
}
