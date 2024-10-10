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
import static org.apache.iceberg.flink.maintenance.api.ExpireSnapshots.DELETE_FILES_OPERATOR_NAME;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.DELETE_FILE_FAILED_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.DELETE_FILE_SUCCEEDED_COUNTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Set;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestExpireSnapshots extends MaintenanceTaskTestBase {
  private Table table;

  @BeforeEach
  void before() {
    MetricsReporterFactoryForTests.reset();
    this.table = createTable();
    tableLoader().open();
  }

  @Test
  void testExpireSnapshots() throws Exception {
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");

    Set<Snapshot> snapshots = Sets.newHashSet(table.snapshots());
    assertThat(snapshots).hasSize(4);

    ExpireSnapshots.builder()
        .parallelism(1)
        .planningWorkerPoolSize(2)
        .deleteBatchSize(3)
        .maxSnapshotAge(Duration.ZERO)
        .retainLast(1)
        .uidSuffix(UID_SUFFIX)
        .append(
            infra.triggerStream(),
            0,
            DUMMY_NAME,
            tableLoader(),
            "OTHER",
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink(), () -> checkDeleteFinished(3L));

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
  void testFailure() throws Exception {
    insert(table, 1, "a");
    insert(table, 2, "b");

    ExpireSnapshots.builder()
        .append(
            infra.triggerStream(),
            0,
            DUMMY_NAME,
            tableLoader(),
            UID_SUFFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    JobClient jobClient = null;
    try {
      jobClient = infra.env().executeAsync();

      // Do a single task run
      long time = System.currentTimeMillis();
      infra.source().sendRecord(Trigger.create(time, 1), time);

      // First successful run (ensure that the operators are loaded/opened etc.)
      assertThat(infra.sink().poll(Duration.ofSeconds(5)).success()).isTrue();

      // Drop the table, so it will cause an exception
      dropTable();

      // Failed run
      infra.source().sendRecord(Trigger.create(time + 1, 1), time + 1);

      assertThat(infra.sink().poll(Duration.ofSeconds(5)).success()).isFalse();
    } finally {
      closeJobClient(jobClient);
    }

    // Check the metrics. There are no expired snapshots or data files because ExpireSnapshots has
    // no max age of number of snapshots set, so no files are removed.
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<String, Long>()
            .put(
                DELETE_FILES_OPERATOR_NAME + "[0]." + DUMMY_NAME + "." + DELETE_FILE_FAILED_COUNTER,
                0L)
            .put(
                DELETE_FILES_OPERATOR_NAME
                    + "[0]."
                    + DUMMY_NAME
                    + "."
                    + DELETE_FILE_SUCCEEDED_COUNTER,
                0L)
            .build());
  }

  @Test
  void testUidAndSlotSharingGroup() {
    ExpireSnapshots.builder()
        .slotSharingGroup(SLOT_SHARING_GROUP)
        .uidSuffix(UID_SUFFIX)
        .append(
            infra.triggerStream(),
            0,
            DUMMY_NAME,
            tableLoader(),
            UID_SUFFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    checkUidsAreSet(infra.env(), UID_SUFFIX);
    checkSlotSharingGroupsAreSet(infra.env(), SLOT_SHARING_GROUP);
  }

  @Test
  void testUidAndSlotSharingGroupUnset() {
    ExpireSnapshots.builder()
        .append(
            infra.triggerStream(),
            0,
            DUMMY_NAME,
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
    insert(table, 1, "a");
    insert(table, 2, "b");

    ExpireSnapshots.builder()
        .maxSnapshotAge(Duration.ZERO)
        .retainLast(1)
        .parallelism(1)
        .append(
            infra.triggerStream(),
            0,
            DUMMY_NAME,
            tableLoader(),
            UID_SUFFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());

    runAndWaitForSuccess(infra.env(), infra.source(), infra.sink(), () -> checkDeleteFinished(1L));

    // Check the metrics
    Awaitility.await()
        .untilAsserted(
            () ->
                MetricsReporterFactoryForTests.assertCounters(
                    new ImmutableMap.Builder<String, Long>()
                        .put(
                            DELETE_FILES_OPERATOR_NAME
                                + "[0]."
                                + DUMMY_NAME
                                + "."
                                + DELETE_FILE_FAILED_COUNTER,
                            0L)
                        .put(
                            DELETE_FILES_OPERATOR_NAME
                                + "[0]."
                                + DUMMY_NAME
                                + "."
                                + DELETE_FILE_SUCCEEDED_COUNTER,
                            1L)
                        .build()));
  }

  private static boolean checkDeleteFinished(Long expectedDeleteNum) {
    return expectedDeleteNum.equals(
        MetricsReporterFactoryForTests.counter(
            DELETE_FILES_OPERATOR_NAME
                + "[0]."
                + DUMMY_NAME
                + "."
                + DELETE_FILE_SUCCEEDED_COUNTER));
  }
}
