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

import java.util.Collections;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestSortOrderAnalyzer {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "uuid", Types.UUIDType.get()),
          Types.NestedField.required(4, "score", Types.DoubleType.get()),
          Types.NestedField.required(
              5,
              "nested",
              Types.StructType.of(Types.NestedField.required(6, "key", Types.IntegerType.get()))));

  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();
  private static final int SORT_ORDER_ID = 1;
  private static final int MAX_FILES = 3;

  @Test
  void unsortedTableReportsNothing() {
    assertThat(prefixLength(SortOrder.unsorted(), SCHEMA)).isZero();
  }

  @Test
  void allProjectedIdentityFieldsReported() {
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").desc("data").build();
    assertThat(prefixLength(sortOrder, SCHEMA)).isEqualTo(2);
  }

  @Test
  void nestedKeyReportsNothing() {
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").asc("nested.key").build();
    assertThat(prefixLength(sortOrder, SCHEMA)).isZero();
  }

  @Test
  void uuidKeyReportsNothing() {
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("uuid").build();
    assertThat(prefixLength(sortOrder, SCHEMA)).isZero();
  }

  @Test
  void uuidKeyAfterReportableFieldReportsNothing() {
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").asc("uuid").build();
    assertThat(prefixLength(sortOrder, SCHEMA)).isZero();
  }

  @Test
  void floatingPointKeyInLastPositionReported() {
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").asc("score").build();
    assertThat(prefixLength(sortOrder, SCHEMA)).isEqualTo(2);
  }

  @Test
  void floatingPointKeyNotLastReportsNothing() {
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("score").asc("id").build();
    assertThat(prefixLength(sortOrder, SCHEMA)).isZero();
  }

  @Test
  void unprojectedFieldEndsPrefix() {
    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).asc("id").asc("data").asc("score").build();
    Schema projection = SCHEMA.select("id", "score");
    assertThat(prefixLength(sortOrder, projection)).isEqualTo(1);
  }

  @Test
  void unprojectedFirstFieldReportsNothing() {
    Schema projection = SCHEMA.select("data");
    assertThat(prefixLength(SORT_ORDER, projection)).isZero();
  }

  @Test
  void nonIdentityFieldEndsPrefix() {
    SortOrder sortOrder =
        SortOrder.builderFor(SCHEMA)
            .asc("id")
            .asc(Expressions.bucket("data", 4))
            .asc("score")
            .build();
    assertThat(prefixLength(sortOrder, SCHEMA)).isEqualTo(1);
  }

  @Test
  void nonIdentityFirstFieldReportsNothing() {
    SortOrder sortOrder =
        SortOrder.builderFor(SCHEMA).asc(Expressions.truncate("data", 2)).asc("id").build();
    assertThat(prefixLength(sortOrder, SCHEMA)).isZero();
  }

  @Test
  void noTasksCannotMerge() {
    assertThat(canMerge()).isFalse();
  }

  @Test
  void matchingFilesCanMerge() {
    assertThat(canMerge(fileTask(SORT_ORDER_ID), fileTask(SORT_ORDER_ID))).isTrue();
  }

  @Test
  void fileWithOtherSortOrderCannotMerge() {
    assertThat(canMerge(fileTask(SORT_ORDER_ID), fileTask(SORT_ORDER_ID + 1))).isFalse();
  }

  @Test
  void fileWithoutSortOrderCannotMerge() {
    assertThat(canMerge(fileTask(null))).isFalse();
  }

  @Test
  void positionDeletesCanMerge() {
    FileScanTask task = fileTask(SORT_ORDER_ID, deleteFile(FileContent.POSITION_DELETES));
    assertThat(canMerge(task, fileTask(SORT_ORDER_ID))).isTrue();
  }

  @Test
  void equalityDeletesCannotMerge() {
    FileScanTask task = fileTask(SORT_ORDER_ID, deleteFile(FileContent.EQUALITY_DELETES));
    assertThat(canMerge(fileTask(SORT_ORDER_ID), task)).isFalse();
  }

  @Test
  void nonFileScanTaskCannotMerge() {
    assertThat(canMerge(mock(ChangelogScanTask.class))).isFalse();
  }

  @Test
  void groupAtFileLimitIsWithinLimit() {
    ScanTaskGroup<?> group = taskGroup(MAX_FILES);
    assertThat(SortOrderAnalyzer.withinFileLimit(ImmutableList.of(group), MAX_FILES)).isTrue();
  }

  @Test
  void groupAboveFileLimitIsNotWithinLimit() {
    ScanTaskGroup<?> group1 = taskGroup(1);
    ScanTaskGroup<?> group2 = taskGroup(MAX_FILES + 1);
    assertThat(SortOrderAnalyzer.withinFileLimit(ImmutableList.of(group1, group2), MAX_FILES))
        .isFalse();
  }

  private static int prefixLength(SortOrder sortOrder, Schema projection) {
    Table table = mock(Table.class);
    when(table.sortOrder()).thenReturn(sortOrder);
    when(table.schema()).thenReturn(SCHEMA);
    when(table.name()).thenReturn("test_table");
    return SortOrderAnalyzer.reportablePrefixLength(table, projection);
  }

  private static boolean canMerge(ScanTask... tasks) {
    return SortOrderAnalyzer.canMerge(ImmutableList.copyOf(tasks), SORT_ORDER_ID);
  }

  private static ScanTaskGroup<?> taskGroup(int numFiles) {
    ScanTaskGroup<?> group = mock(ScanTaskGroup.class);
    doReturn(Collections.nCopies(numFiles, fileTask(SORT_ORDER_ID))).when(group).tasks();
    return group;
  }

  private static FileScanTask fileTask(Integer sortOrderId, DeleteFile... deletes) {
    DataFile file = mock(DataFile.class);
    when(file.sortOrderId()).thenReturn(sortOrderId);
    FileScanTask task = mock(FileScanTask.class);
    when(task.file()).thenReturn(file);
    when(task.deletes()).thenReturn(ImmutableList.copyOf(deletes));
    return task;
  }

  private static DeleteFile deleteFile(FileContent content) {
    DeleteFile deleteFile = mock(DeleteFile.class);
    when(deleteFile.content()).thenReturn(content);
    return deleteFile;
  }
}
