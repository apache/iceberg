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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDataFileRewriteCommitter extends OperatorTestBase {
  @Test
  void testUnpartitioned() throws Exception {
    Table table = createTable();
    insert(table, 1, "p1");
    insert(table, 2, "p2");
    insert(table, 3, "p3");

    List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
    assertThat(planned).hasSize(1);
    List<DataFileRewriteExecutor.ExecutedGroup> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(1);

    try (OneInputStreamOperatorTestHarness<DataFileRewriteExecutor.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement(rewritten.get(0), EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    assertDataFiles(
        table, rewritten.get(0).group().addedFiles(), rewritten.get(0).group().rewrittenFiles());
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
    List<DataFileRewriteExecutor.ExecutedGroup> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(2);
    assertThat(rewritten.get(0).groupsPerCommit()).isEqualTo(1);
    assertThat(rewritten.get(1).groupsPerCommit()).isEqualTo(1);

    try (OneInputStreamOperatorTestHarness<DataFileRewriteExecutor.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement(rewritten.get(0), EVENT_TIME);
      // This should be committed synchronously
      assertDataFiles(
          table, rewritten.get(0).group().addedFiles(), rewritten.get(0).group().rewrittenFiles());

      testHarness.processElement(rewritten.get(1), EVENT_TIME);
      // This should be committed synchronously
      assertDataFiles(
          table, rewritten.get(1).group().addedFiles(), rewritten.get(1).group().rewrittenFiles());

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
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
    List<DataFileRewriteExecutor.ExecutedGroup> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(3);

    try (OneInputStreamOperatorTestHarness<DataFileRewriteExecutor.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement(updateBatchSize(rewritten.get(0)), EVENT_TIME);
      assertNoChange(table);
      testHarness.processElement(updateBatchSize(rewritten.get(1)), EVENT_TIME);
      // This should be committed synchronously
      Set<DataFile> added = Sets.newHashSet(rewritten.get(0).group().addedFiles());
      added.addAll(rewritten.get(1).group().addedFiles());
      Set<DataFile> removed = Sets.newHashSet(rewritten.get(0).group().rewrittenFiles());
      removed.addAll(rewritten.get(1).group().rewrittenFiles());
      assertDataFiles(table, added, removed);

      testHarness.processElement(updateBatchSize(rewritten.get(2)), EVENT_TIME);
      assertNoChange(table);

      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    // This should be committed on close
    assertDataFiles(
        table, rewritten.get(2).group().addedFiles(), rewritten.get(2).group().rewrittenFiles());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStateRestore(boolean withException) throws Exception {
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
    List<DataFileRewriteExecutor.ExecutedGroup> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(4);

    OperatorSubtaskState state = null;
    try (OneInputStreamOperatorTestHarness<DataFileRewriteExecutor.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement(updateBatchSize(rewritten.get(0)), EVENT_TIME);
      assertNoChange(table);

      state = testHarness.snapshot(1, System.currentTimeMillis());
      if (withException) {
        throw new RuntimeException("Testing exception");
      }
    } catch (Exception e) {
      // do nothing
    }

    if (withException) {
      // Check that the previous commit was successful
      assertDataFiles(
          table, rewritten.get(0).group().addedFiles(), rewritten.get(0).group().rewrittenFiles());
      // Revert the last commit to simulate that the commit at DataFileRewriteCommitter.close was
      // not successful
      table.manageSnapshots().rollbackTo(table.currentSnapshot().parentId()).commit();
      table.refresh();
      assertThat(table.currentSnapshot().removedDataFiles(table.io())).isEmpty();
    }

    try (OneInputStreamOperatorTestHarness<DataFileRewriteExecutor.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement(updateBatchSize(rewritten.get(1)), EVENT_TIME);

      assertDataFiles(
          table, rewritten.get(0).group().addedFiles(), rewritten.get(0).group().rewrittenFiles());
      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertNoChange(table);

      testHarness.processElement(updateBatchSize(rewritten.get(2)), EVENT_TIME);

      // This should be committed synchronously
      Set<DataFile> added = Sets.newHashSet(rewritten.get(1).group().addedFiles());
      added.addAll(rewritten.get(2).group().addedFiles());
      Set<DataFile> removed = Sets.newHashSet(rewritten.get(1).group().rewrittenFiles());
      removed.addAll(rewritten.get(2).group().rewrittenFiles());
      assertDataFiles(table, added, removed);

      testHarness.processElement(updateBatchSize(rewritten.get(3)), EVENT_TIME);
      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }

    // This should be committed on close
    assertDataFiles(
        table, rewritten.get(3).group().addedFiles(), rewritten.get(3).group().rewrittenFiles());
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
    List<DataFileRewriteExecutor.ExecutedGroup> rewritten = executeRewrite(planned);
    assertThat(rewritten).hasSize(4);

    OperatorSubtaskState state = null;
    try (OneInputStreamOperatorTestHarness<DataFileRewriteExecutor.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.open();

      testHarness.processElement(updateBatchSize(rewritten.get(0)), EVENT_TIME);
      assertNoChange(table);

      state = testHarness.snapshot(1, System.currentTimeMillis());
    } catch (Exception e) {
      // do nothing
    }

    try (OneInputStreamOperatorTestHarness<DataFileRewriteExecutor.ExecutedGroup, Trigger>
        testHarness = harness()) {
      testHarness.initializeState(state);
      testHarness.open();

      // Cause an exception
      dropTable();

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      testHarness.processElement(rewritten.get(1), EVENT_TIME);
      testHarness.processWatermark(EVENT_TIME);
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(
              testHarness
                  .getSideOutput(TaskResultAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("From 1 commits only 0 were unsuccessful");
    }
  }

  private OneInputStreamOperatorTestHarness<DataFileRewriteExecutor.ExecutedGroup, Trigger>
      harness() throws Exception {
    return new OneInputStreamOperatorTestHarness<>(
        new DataFileRewriteCommitter(
            OperatorTestBase.DUMMY_TABLE_NAME,
            OperatorTestBase.DUMMY_TABLE_NAME,
            0,
            tableLoader()));
  }

  private static DataFileRewriteExecutor.ExecutedGroup updateBatchSize(
      DataFileRewriteExecutor.ExecutedGroup from) {
    return new DataFileRewriteExecutor.ExecutedGroup(from.snapshotId(), 2, from.group());
  }

  private static void assertDataFiles(
      Table actual, Set<DataFile> expectedAdded, Set<DataFile> expectedRemoved) {
    actual.refresh();

    Set<DataFile> actualAdded =
        Sets.newHashSet(actual.currentSnapshot().addedDataFiles(actual.io()));
    Set<DataFile> actualRemoved =
        Sets.newHashSet(actual.currentSnapshot().removedDataFiles(actual.io()));
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
