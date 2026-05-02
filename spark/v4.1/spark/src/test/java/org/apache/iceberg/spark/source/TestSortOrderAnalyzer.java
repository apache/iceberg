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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestSortOrderAnalyzer {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();

  // Represents the grouping key type for a single-column string partition
  private static final Types.StructType KEY_TYPE =
      Types.StructType.of(Types.NestedField.required(3, "partition", Types.StringType.get()));

  @Test
  void testUnsortedTableReturnsFalse() {
    Table table = mockTable(SortOrder.unsorted());
    List<ScanTaskGroup<?>> groups = ImmutableList.of(taskGroupWithKey(SORT_ORDER.orderId(), "P1"));

    assertThat(SortOrderAnalyzer.canReportOrdering(table, groups, KEY_TYPE)).isFalse();
  }

  @Test
  void testNullTaskGroupsReturnsFalse() {
    Table table = mockTable(SORT_ORDER);

    assertThat(SortOrderAnalyzer.canReportOrdering(table, null, KEY_TYPE)).isFalse();
  }

  @Test
  void testEmptyTaskGroupsReturnsFalse() {
    Table table = mockTable(SORT_ORDER);

    assertThat(SortOrderAnalyzer.canReportOrdering(table, ImmutableList.of(), KEY_TYPE)).isFalse();
  }

  @Test
  void testFileWithMismatchedSortOrderIdReturnsFalse() {
    Table table = mockTable(SORT_ORDER);
    // File was written with sort order ID 999, which doesn't match the table's current order
    ScanTaskGroup<?> group = taskGroupWithKey(999, "P1");

    assertThat(SortOrderAnalyzer.canReportOrdering(table, ImmutableList.of(group), KEY_TYPE))
        .isFalse();
  }

  @Test
  void testFileWithNullSortOrderIdReturnsFalse() {
    Table table = mockTable(SORT_ORDER);
    // File has no sort order recorded (null) — written before sort order was set
    ScanTaskGroup<?> group = taskGroupWithNullSortOrderId("P1");

    assertThat(SortOrderAnalyzer.canReportOrdering(table, ImmutableList.of(group), KEY_TYPE))
        .isFalse();
  }

  @Test
  void testOnlyOneFileWithWrongSortOrderInMultiFileGroupReturnsFalse() {
    Table table = mockTable(SORT_ORDER);
    // Two files: one correct, one from old sort order — the group must fail
    FileScanTask goodTask = fileTask(SORT_ORDER.orderId());
    FileScanTask badTask = fileTask(999);
    ScanTaskGroup<?> group = taskGroupWithTasks("P1", ImmutableList.of(goodTask, badTask));

    assertThat(SortOrderAnalyzer.canReportOrdering(table, ImmutableList.of(group), KEY_TYPE))
        .isFalse();
  }

  @Test
  void testDuplicatePartitionKeyReturnsFalse() {
    Table table = mockTable(SORT_ORDER);
    // Two task groups that share the same partition key value — Spark coalesces them
    // without merge-sorting, destroying the ordering guarantee.
    ScanTaskGroup<?> group1 = taskGroupWithKey(SORT_ORDER.orderId(), "same-partition");
    ScanTaskGroup<?> group2 = taskGroupWithKey(SORT_ORDER.orderId(), "same-partition");

    assertThat(
            SortOrderAnalyzer.canReportOrdering(table, ImmutableList.of(group1, group2), KEY_TYPE))
        .isFalse();
  }

  @Test
  void testUniquePartitionKeysReturnsTrue() {
    Table table = mockTable(SORT_ORDER);
    ScanTaskGroup<?> group1 = taskGroupWithKey(SORT_ORDER.orderId(), "partition-A");
    ScanTaskGroup<?> group2 = taskGroupWithKey(SORT_ORDER.orderId(), "partition-B");

    assertThat(
            SortOrderAnalyzer.canReportOrdering(table, ImmutableList.of(group1, group2), KEY_TYPE))
        .isTrue();
  }

  @Test
  void testUnpartitionedTableSkipsUniquenessCheck() {
    Table table = mockTable(SORT_ORDER);
    // Both groups have null grouping key — acceptable for unpartitioned tables
    ScanTaskGroup<?> group1 = taskGroupWithNullKey(SORT_ORDER.orderId());
    ScanTaskGroup<?> group2 = taskGroupWithNullKey(SORT_ORDER.orderId());

    // Empty struct type signals unpartitioned; uniqueness check is skipped
    assertThat(
            SortOrderAnalyzer.canReportOrdering(
                table, ImmutableList.of(group1, group2), Types.StructType.of()))
        .isTrue();
  }

  @Test
  void testSingleGroupSingleFileReturnsTrue() {
    Table table = mockTable(SORT_ORDER);
    ScanTaskGroup<?> group = taskGroupWithKey(SORT_ORDER.orderId(), "P1");

    assertThat(SortOrderAnalyzer.canReportOrdering(table, ImmutableList.of(group), KEY_TYPE))
        .isTrue();
  }

  @Test
  void testSingleGroupMultipleFilesAllMatchingReturnsTrue() {
    Table table = mockTable(SORT_ORDER);
    FileScanTask task1 = fileTask(SORT_ORDER.orderId());
    FileScanTask task2 = fileTask(SORT_ORDER.orderId());
    ScanTaskGroup<?> group = taskGroupWithTasks("P1", ImmutableList.of(task1, task2));

    assertThat(SortOrderAnalyzer.canReportOrdering(table, ImmutableList.of(group), KEY_TYPE))
        .isTrue();
  }

  @Test
  void testMultipleGroupsAllValidReturnsTrue() {
    Table table = mockTable(SORT_ORDER);
    ScanTaskGroup<?> group1 = taskGroupWithKey(SORT_ORDER.orderId(), "P1");
    ScanTaskGroup<?> group2 = taskGroupWithKey(SORT_ORDER.orderId(), "P2");
    ScanTaskGroup<?> group3 = taskGroupWithKey(SORT_ORDER.orderId(), "P3");

    assertThat(
            SortOrderAnalyzer.canReportOrdering(
                table, ImmutableList.of(group1, group2, group3), KEY_TYPE))
        .isTrue();
  }

  private static Table mockTable(SortOrder sortOrder) {
    Table table = mock(Table.class);
    when(table.sortOrder()).thenReturn(sortOrder);
    when(table.name()).thenReturn("test_table");
    return table;
  }

  private static ScanTaskGroup<?> taskGroupWithKey(int sortOrderId, String partitionValue) {
    return taskGroupWithTasks(partitionValue, ImmutableList.of(fileTask(sortOrderId)));
  }

  private static ScanTaskGroup<?> taskGroupWithNullSortOrderId(String partitionValue) {
    FileScanTask task = mock(FileScanTask.class);
    DataFile file = mock(DataFile.class);
    when(file.sortOrderId()).thenReturn(null);
    when(task.file()).thenReturn(file);

    ScanTaskGroup<?> group = mock(ScanTaskGroup.class);
    doReturn(ImmutableList.of(task)).when(group).tasks();
    when(group.groupingKey()).thenReturn(makeKey(partitionValue));
    return group;
  }

  private static ScanTaskGroup<?> taskGroupWithNullKey(int sortOrderId) {
    ScanTaskGroup<?> group = mock(ScanTaskGroup.class);
    doReturn(ImmutableList.of(fileTask(sortOrderId))).when(group).tasks();
    when(group.groupingKey()).thenReturn(null);
    return group;
  }

  private static ScanTaskGroup<?> taskGroupWithTasks(
      String partitionValue, List<FileScanTask> tasks) {
    ScanTaskGroup<?> group = mock(ScanTaskGroup.class);
    doReturn(tasks).when(group).tasks();
    when(group.groupingKey()).thenReturn(makeKey(partitionValue));
    return group;
  }

  private static FileScanTask fileTask(int sortOrderId) {
    FileScanTask task = mock(FileScanTask.class);
    DataFile file = mock(DataFile.class);
    when(file.sortOrderId()).thenReturn(sortOrderId);
    when(task.file()).thenReturn(file);
    return task;
  }

  private static StructLike makeKey(String value) {
    GenericRecord record = GenericRecord.create(KEY_TYPE);
    record.setField("partition", value);
    return record;
  }
}
