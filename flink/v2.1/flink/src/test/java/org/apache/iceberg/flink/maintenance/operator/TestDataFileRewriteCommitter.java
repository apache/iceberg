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

import static org.apache.iceberg.flink.maintenance.operator.RewriteUtil.executeRewrite;
import static org.apache.iceberg.flink.maintenance.operator.RewriteUtil.planDataFileRewrite;
import static org.apache.iceberg.metrics.CommitMetricsResult.TOTAL_DATA_FILES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TestDataFileRewriteCommitter extends OperatorTestBase {
  @Test
  void testUnpartitioned() throws Exception {
    Table table = createTable();
    insert(table, 1, "p1");
    insert(table, 2, "p2");
    insert(table, 3, "p3");

    List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
    assertThat(planned).hasSize(1);
    List<DataFileRewriteRunner.ExecutedGroup> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(1);

    try (OneInputStreamOperatorTestHarness<DataFileRewriteRunner.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement(rewritten.get(0), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    assertDataFiles(
        table, rewritten.get(0).group().addedFiles(), rewritten.get(0).group().rewrittenFiles(), 1);
  }

  @Test
  void testPartitioned() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");

    List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
    assertThat(planned).hasSize(2);
    List<DataFileRewriteRunner.ExecutedGroup> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(2);
    assertThat(rewritten.get(0).groupsPerCommit()).isEqualTo(1);
    assertThat(rewritten.get(1).groupsPerCommit()).isEqualTo(1);
    ensureDifferentGroups(rewritten);

    try (OneInputStreamOperatorTestHarness<DataFileRewriteRunner.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement(rewritten.get(0), EVENT_TIME);
      assertDataFiles(
          table,
          rewritten.get(0).group().addedFiles(),
          rewritten.get(0).group().rewrittenFiles(),
          3);

      testHarness.processElement(rewritten.get(1), EVENT_TIME);
      assertDataFiles(
          table,
          rewritten.get(1).group().addedFiles(),
          rewritten.get(1).group().rewrittenFiles(),
          2);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void testNewTable() throws Exception {
    Table table = createTable();
    List<DataFileRewriteRunner.ExecutedGroup> rewritten;

    try (OneInputStreamOperatorTestHarness<DataFileRewriteRunner.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      insert(table, 1, "p1");
      insert(table, 2, "p2");
      insert(table, 3, "p3");

      List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
      assertThat(planned).hasSize(1);
      rewritten = executeRewrite(planned);
      assertThat(rewritten).hasSize(1);

      testHarness.processElement(rewritten.get(0), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    assertDataFiles(
        table, rewritten.get(0).group().addedFiles(), rewritten.get(0).group().rewrittenFiles(), 1);
  }

  @Test
  void testBatchSize() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");
    insertPartitioned(table, 5, "p3");
    insertPartitioned(table, 6, "p3");

    List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
    assertThat(planned).hasSize(3);
    List<DataFileRewriteRunner.ExecutedGroup> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(3);
    ensureDifferentGroups(rewritten);

    try (OneInputStreamOperatorTestHarness<DataFileRewriteRunner.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement(setBatchSizeToTwo(rewritten.get(0)), EVENT_TIME);
      assertNoChange(table);
      testHarness.processElement(setBatchSizeToTwo(rewritten.get(1)), EVENT_TIME);

      Set<DataFile> added = Sets.newHashSet(rewritten.get(0).group().addedFiles());
      added.addAll(rewritten.get(1).group().addedFiles());
      Set<DataFile> removed = Sets.newHashSet(rewritten.get(0).group().rewrittenFiles());
      removed.addAll(rewritten.get(1).group().rewrittenFiles());
      assertDataFiles(table, added, removed, 4);

      testHarness.processElement(setBatchSizeToTwo(rewritten.get(2)), EVENT_TIME);
      assertNoChange(table);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    // This should be committed on close
    assertDataFiles(
        table, rewritten.get(2).group().addedFiles(), rewritten.get(2).group().rewrittenFiles(), 3);
  }

  @Test
  void testError() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");
    insertPartitioned(table, 5, "p3");
    insertPartitioned(table, 6, "p3");
    insertPartitioned(table, 7, "p4");
    insertPartitioned(table, 8, "p4");

    List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
    assertThat(planned).hasSize(4);
    List<DataFileRewriteRunner.ExecutedGroup> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(4);

    try (OneInputStreamOperatorTestHarness<DataFileRewriteRunner.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement(setBatchSizeToTwo(rewritten.get(0)), EVENT_TIME);
      assertNoChange(table);
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();

      DataFileRewriteRunner.ExecutedGroup group = spy(setBatchSizeToTwo(rewritten.get(1)));
      when(group.group()).thenThrow(new RuntimeException("Testing error"));
      testHarness.processElement(group, EVENT_TIME);

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(
              testHarness
                  .getSideOutput(TaskResultAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("Testing error");
    }
  }

  private OneInputStreamOperatorTestHarness<DataFileRewriteRunner.ExecutedGroup, Trigger> harness()
      throws Exception {
    return new OneInputStreamOperatorTestHarness<>(
        new DataFileRewriteCommitter(
            OperatorTestBase.DUMMY_TABLE_NAME,
            OperatorTestBase.DUMMY_TABLE_NAME,
            0,
            tableLoader()));
  }

  private static DataFileRewriteRunner.ExecutedGroup setBatchSizeToTwo(
      DataFileRewriteRunner.ExecutedGroup from) {
    return new DataFileRewriteRunner.ExecutedGroup(from.snapshotId(), 2, from.group());
  }

  // Ensure that the groups are different, so the tests are not accidentally passing
  private static void ensureDifferentGroups(List<DataFileRewriteRunner.ExecutedGroup> rewritten) {
    List<String> resultFiles =
        rewritten.stream()
            .flatMap(task -> task.group().addedFiles().stream().map(ContentFile::location))
            .collect(Collectors.toList());
    assertThat(resultFiles).hasSize(Set.copyOf(resultFiles).size());
  }

  /**
   * Assert that the number of the data files in the table is as expected. Additionally, tests that
   * the last commit contains the expected added and removed files.
   *
   * @param table the table to check
   * @param expectedAdded the expected added data files
   * @param expectedRemoved the expected removed data files
   * @param expectedCurrent the expected current data files count
   */
  private static void assertDataFiles(
      Table table,
      Set<DataFile> expectedAdded,
      Set<DataFile> expectedRemoved,
      long expectedCurrent) {
    table.refresh();

    assertThat(table.currentSnapshot().summary().get(TOTAL_DATA_FILES))
        .isEqualTo(String.valueOf(expectedCurrent));
    Set<DataFile> actualAdded = Sets.newHashSet(table.currentSnapshot().addedDataFiles(table.io()));
    Set<DataFile> actualRemoved =
        Sets.newHashSet(table.currentSnapshot().removedDataFiles(table.io()));
    assertThat(actualAdded.stream().map(DataFile::location).collect(Collectors.toSet()))
        .isEqualTo(expectedAdded.stream().map(DataFile::location).collect(Collectors.toSet()));
    assertThat(actualRemoved.stream().map(DataFile::location).collect(Collectors.toSet()))
        .isEqualTo(expectedRemoved.stream().map(DataFile::location).collect(Collectors.toSet()));
  }

  private static void assertNoChange(Table table) {
    long original = table.currentSnapshot().snapshotId();
    table.refresh();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(original);
  }
}
