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
import static org.apache.iceberg.flink.maintenance.api.RewriteManifestFiles.COMMIT_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.RewriteManifestFiles.MANIFEST_SCAN_NAME;
import static org.apache.iceberg.flink.maintenance.api.RewriteManifestFiles.PLANNER_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.RewriteManifestFiles.READER_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.RewriteManifestFiles.SPEC_CHANGE_NAME;
import static org.apache.iceberg.flink.maintenance.api.RewriteManifestFiles.WRITE_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ERROR_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.INCOMPATIBLE_SPEC_CHANGE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestRewriteManifests extends MaintenanceTaskTestBase {

  @BeforeEach
  void before() {
    MetricsReporterFactoryForTests.reset();
  }

  @Test
  void testRewriteManifestsUnPartitioned() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(4);

    RewriteManifestFiles.builder()
        .parallelism(1)
        .planningWorkerPoolSize(2)
        .uidSuffix(UID_SUFFIX)
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            "OTHER",
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    // Check rewrite manifest success
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    // Check that the table data not changed
    SimpleDataUtil.assertTableRecords(
        table,
        ImmutableList.of(
            createRecord(1, "a"),
            createRecord(2, "b"),
            createRecord(3, "c"),
            createRecord(4, "d")));

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    MANIFEST_SCAN_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    READER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    SPEC_CHANGE_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    SPEC_CHANGE_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    INCOMPATIBLE_SPEC_CHANGE),
                0L)
            .put(
                ImmutableList.of(
                    WRITE_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .build());
  }

  @Test
  void testRewriteManifestsPartitioned() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(4);

    RewriteManifestFiles.builder()
        .parallelism(1)
        .planningWorkerPoolSize(2)
        .uidSuffix(UID_SUFFIX)
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            "OTHER",
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink());

    table.refresh();
    // Check rewrite manifest success
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    // Check that the table data not changed
    SimpleDataUtil.assertTableRecords(
        table,
        ImmutableList.of(
            createRecord(1, "p1"),
            createRecord(2, "p1"),
            createRecord(3, "p2"),
            createRecord(4, "p2")));

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    MANIFEST_SCAN_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    READER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    SPEC_CHANGE_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    SPEC_CHANGE_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    INCOMPATIBLE_SPEC_CHANGE),
                0L)
            .put(
                ImmutableList.of(
                    WRITE_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .build());
  }

  @Test
  void testFailure() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(4);

    RewriteManifestFiles.builder()
        .parallelism(1)
        .planningWorkerPoolSize(2)
        .uidSuffix(UID_SUFFIX)
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            "OTHER",
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    // Mock error
    Long parentId = table.currentSnapshot().parentId();
    for (ManifestFile manifestFile : table.snapshot(parentId).allManifests(table.io())) {
      table.io().deleteFile(manifestFile.path());
    }

    runAndWaitForResult(
        infra.env(),
        infra.source(),
        infra.sink(),
        false /* generateFailure */,
        () -> true,
        false /* resultSuccess*/);

    table.refresh();
    // Check rewrite manifest success
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(4);

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    MANIFEST_SCAN_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    READER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                1L)
            .put(
                ImmutableList.of(
                    COMMIT_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    SPEC_CHANGE_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    SPEC_CHANGE_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    INCOMPATIBLE_SPEC_CHANGE),
                0L)
            .put(
                ImmutableList.of(
                    WRITE_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .build());
  }
}
