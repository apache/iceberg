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

import static org.apache.iceberg.actions.SizeBasedFileRewritePlanner.MIN_INPUT_FILES;
import static org.apache.iceberg.flink.maintenance.operator.RewriteUtil.newDataFiles;
import static org.apache.iceberg.flink.maintenance.operator.RewriteUtil.planDataFileRewrite;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TestDataFileRewritePlanner extends OperatorTestBase {
  @Test
  void testFailsOnV3Table() throws Exception {
    Table table = createTable("3");
    Set<DataFile> expected = Sets.newHashSetWithExpectedSize(3);
    insert(table, 1, "a");
    expected.addAll(newDataFiles(table));

    assertThatThrownBy(() -> planDataFileRewrite(tableLoader()))
        .hasMessageContaining(
            "Flink does not support compaction on row lineage enabled tables (V3+)")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testUnpartitioned() throws Exception {
    Set<DataFile> expected = Sets.newHashSetWithExpectedSize(3);
    Table table = createTable();
    insert(table, 1, "a");
    expected.addAll(newDataFiles(table));
    insert(table, 2, "b");
    expected.addAll(newDataFiles(table));
    insert(table, 3, "c");
    expected.addAll(newDataFiles(table));

    List<DataFileRewritePlanner.PlannedGroup> actual = planDataFileRewrite(tableLoader());

    assertThat(actual).hasSize(1);
    assertRewriteFileGroup(actual.get(0), table, expected);
  }

  @Test
  void testPartitioned() throws Exception {
    Set<DataFile> expectedP1 = Sets.newHashSetWithExpectedSize(2);
    Set<DataFile> expectedP2 = Sets.newHashSetWithExpectedSize(2);
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    expectedP1.addAll(newDataFiles(table));
    insertPartitioned(table, 2, "p1");
    expectedP1.addAll(newDataFiles(table));

    insertPartitioned(table, 3, "p2");
    expectedP2.addAll(newDataFiles(table));
    insertPartitioned(table, 4, "p2");
    expectedP2.addAll(newDataFiles(table));

    // This should not participate in compaction, as there is no more files in the partition
    insertPartitioned(table, 5, "p3");

    List<DataFileRewritePlanner.PlannedGroup> actual = planDataFileRewrite(tableLoader());

    assertThat(actual).hasSize(2);
    if (actual.get(0).group().info().partition().get(0, String.class).equals("p1")) {
      assertRewriteFileGroup(actual.get(0), table, expectedP1);
      assertRewriteFileGroup(actual.get(1), table, expectedP2);
    } else {
      assertRewriteFileGroup(actual.get(0), table, expectedP2);
      assertRewriteFileGroup(actual.get(1), table, expectedP1);
    }
  }

  @Test
  void testError() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    try (OneInputStreamOperatorTestHarness<Trigger, DataFileRewritePlanner.PlannedGroup>
        testHarness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new DataFileRewritePlanner(
                    OperatorTestBase.DUMMY_TABLE_NAME,
                    OperatorTestBase.DUMMY_TABLE_NAME,
                    0,
                    tableLoader(),
                    11,
                    1L,
                    ImmutableMap.of(MIN_INPUT_FILES, "2"),
                    Expressions.alwaysTrue()))) {
      testHarness.open();

      // Cause an exception
      dropTable();

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      trigger(testHarness);
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(
              testHarness
                  .getSideOutput(TaskResultAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("Table does not exist: ");
    }
  }

  @Test
  void testV2Table() throws Exception {
    Table table = createTableWithDelete();
    update(table, 1, null, "a", "b");
    update(table, 1, "b", "c");

    List<DataFileRewritePlanner.PlannedGroup> actual = planDataFileRewrite(tableLoader());

    assertThat(actual).hasSize(1);
    List<FileScanTask> tasks = actual.get(0).group().fileScanTasks();
    assertThat(tasks).hasSize(2);
    // Find the task with the deletes
    FileScanTask withDelete = tasks.get(0).deletes().isEmpty() ? tasks.get(1) : tasks.get(0);
    assertThat(withDelete.deletes()).hasSize(2);
    assertThat(withDelete.deletes().stream().map(ContentFile::content).collect(Collectors.toList()))
        .containsExactlyInAnyOrder(FileContent.POSITION_DELETES, FileContent.EQUALITY_DELETES);
  }

  @Test
  void testMaxRewriteBytes() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");

    // First run with high maxRewriteBytes
    List<DataFileRewritePlanner.PlannedGroup> planWithNoMaxRewriteBytes =
        planDataFileRewrite(tableLoader());
    assertThat(planWithNoMaxRewriteBytes).hasSize(2);

    // Second run with low maxRewriteBytes, the 2nd group should be removed from the plan
    long maxRewriteBytes =
        planWithNoMaxRewriteBytes.get(0).group().fileScanTasks().get(0).sizeBytes()
            + planWithNoMaxRewriteBytes.get(1).group().fileScanTasks().get(0).sizeBytes()
            + 1;
    try (OneInputStreamOperatorTestHarness<Trigger, DataFileRewritePlanner.PlannedGroup>
        testHarness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new DataFileRewritePlanner(
                    OperatorTestBase.DUMMY_TABLE_NAME,
                    OperatorTestBase.DUMMY_TABLE_NAME,
                    0,
                    tableLoader(),
                    11,
                    maxRewriteBytes,
                    ImmutableMap.of(MIN_INPUT_FILES, "2"),
                    Expressions.alwaysTrue()))) {
      testHarness.open();

      OperatorTestBase.trigger(testHarness);

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      // Only a single group is planned
      assertThat(testHarness.extractOutputValues()).hasSize(1);
    }
  }

  void assertRewriteFileGroup(
      DataFileRewritePlanner.PlannedGroup plannedGroup, Table table, Set<DataFile> files) {
    assertThat(plannedGroup.table().currentSnapshot().snapshotId())
        .isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(plannedGroup.groupsPerCommit()).isEqualTo(1);
    assertThat(
            plannedGroup.group().fileScanTasks().stream()
                .map(s -> s.file().location())
                .collect(Collectors.toSet()))
        .containsExactlyInAnyOrderElementsOf(
            files.stream().map(ContentFile::location).collect(Collectors.toList()));
  }
}
