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

import static org.apache.iceberg.flink.SimpleDataUtil.createRecord;
import static org.apache.iceberg.flink.maintenance.api.RewriteDataFiles.COMMIT_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.RewriteDataFiles.PLANNER_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.RewriteDataFiles.REWRITE_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ADDED_DATA_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ADDED_DATA_FILE_SIZE_METRIC;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ERROR_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.REMOVED_DATA_FILE_NUM_METRIC;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.REMOVED_DATA_FILE_SIZE_METRIC;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestRewriteDataFiles extends MaintenanceTaskTestBase {
  @Test
  void testRewriteUnpartitioned() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");

    assertFileNum(table, 4, 0);

    appendRewriteDataFiles(
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
            .rewriteAll(false));

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

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
  void testRewritePartitioned() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");

    assertFileNum(table, 4, 0);

    appendRewriteDataFiles();

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

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
  void testPlannerFailure() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    assertFileNum(table, 2, 0);

    appendRewriteDataFiles();

    runAndWaitForFailure(infra.env(), infra.source(), infra.sink());

    // Check the metrics. The first task should be successful, but the second one should fail. This
    // should be represented in the counters.
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                1L)
            .put(
                ImmutableList.of(
                    REWRITE_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_NUM_METRIC),
                1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_SIZE_METRIC),
                -1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    REMOVED_DATA_FILE_NUM_METRIC),
                2L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    REMOVED_DATA_FILE_SIZE_METRIC),
                -1L)
            .build());
  }

  @Test
  void testUidAndSlotSharingGroup() {
    createTable();

    RewriteDataFiles.builder()
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .uidSuffix(UID_SUFFIX)
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            "OTHER",
            "OTHER",
            1)
        .sinkTo(infra.sink());

    checkUidsAreSet(infra.env(), UID_SUFFIX);
    checkSlotSharingGroupsAreSet(infra.env(), SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupUnset() {
    createTable();

    RewriteDataFiles.builder()
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            UID_SUFFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    checkUidsAreSet(infra.env(), null);
    checkSlotSharingGroupsAreSet(infra.env(), null);
  }

  @Test
  void testMetrics() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    assertFileNum(table, 2, 0);

    appendRewriteDataFiles();

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    REWRITE_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_NUM_METRIC),
                1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_SIZE_METRIC),
                -1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    REMOVED_DATA_FILE_NUM_METRIC),
                2L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
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

    appendRewriteDataFiles();

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    assertFileNum(table, 1, 1);

    SimpleDataUtil.assertTableRecords(table, ImmutableList.of(createRecord(1, "c")));

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    REWRITE_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_NUM_METRIC),
                1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    ADDED_DATA_FILE_SIZE_METRIC),
                -1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    REMOVED_DATA_FILE_NUM_METRIC),
                2L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]",
                    DUMMY_TABLE_NAME,
                    DUMMY_TASK_NAME,
                    "0",
                    REMOVED_DATA_FILE_SIZE_METRIC),
                -1L)
            .build());
  }

  @Test
  void testRewriteWithFilter() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");

    assertFileNum(table, 4, 0);

    appendRewriteDataFiles(
        RewriteDataFiles.builder()
            .parallelism(2)
            .deleteFileThreshold(10)
            .targetFileSizeBytes(1_000_000L)
            .maxFileGroupSizeBytes(10_000_000L)
            .maxFileSizeBytes(2_000_000L)
            .minFileSizeBytes(500_000L)
            .minInputFiles(2)
            // Only rewrite data files where id is 1 or 2 for testing rewrite
            .filter(Expressions.in("id", 1, 2))
            .partialProgressEnabled(true)
            .partialProgressMaxCommits(1)
            .maxRewriteBytes(100_000L)
            .rewriteAll(false));

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    // There is four files, only id is 1 and 2 will be rewritten. so expect 3 files.
    assertFileNum(table, 3, 0);

    SimpleDataUtil.assertTableRecords(
        table,
        ImmutableList.of(
            createRecord(1, "a"),
            createRecord(2, "b"),
            createRecord(3, "c"),
            createRecord(4, "d")));
  }

  private void appendRewriteDataFiles() {
    appendRewriteDataFiles(RewriteDataFiles.builder().rewriteAll(true));
  }

  private void appendRewriteDataFiles(RewriteDataFiles.Builder builder) {
    builder
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            UID_SUFFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());
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
