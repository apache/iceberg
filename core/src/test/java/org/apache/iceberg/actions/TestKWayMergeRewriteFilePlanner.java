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
package org.apache.iceberg.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestKWayMergeRewriteFilePlanner {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();

  @TempDir private File tableDir;
  private TestTables.TestTable table;

  @BeforeEach
  void setupTable() {
    this.table = TestTables.create(tableDir, "test", SCHEMA, SPEC, SORT_ORDER, 2);
  }

  @AfterEach
  void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  void testValidOptions() {
    KWayMergeRewriteFilePlanner planner = new KWayMergeRewriteFilePlanner(table, null, null, false);

    assertThat(planner.validOptions())
        .as("Planner must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES,
                BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES,
                BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES,
                BinPackRewriteFilePlanner.MIN_INPUT_FILES,
                BinPackRewriteFilePlanner.REWRITE_ALL,
                BinPackRewriteFilePlanner.MAX_FILE_GROUP_SIZE_BYTES,
                BinPackRewriteFilePlanner.DELETE_FILE_THRESHOLD,
                BinPackRewriteFilePlanner.DELETE_RATIO_THRESHOLD,
                RewriteDataFiles.REWRITE_JOB_ORDER,
                BinPackRewriteFilePlanner.MAX_FILES_TO_REWRITE));
  }

  @Test
  void testRejectsUnsortedTable() {
    File unsortedDir = new File(tableDir.getParentFile(), "unsorted");
    unsortedDir.mkdirs();
    TestTables.TestTable unsortedTable =
        TestTables.create(unsortedDir, "unsorted", SCHEMA, SPEC, SortOrder.unsorted(), 2);

    KWayMergeRewriteFilePlanner planner =
        new KWayMergeRewriteFilePlanner(unsortedTable, null, null, false);
    planner.init(ImmutableMap.of(BinPackRewriteFilePlanner.REWRITE_ALL, "true"));

    List<FileScanTask> tasks = ImmutableList.of(new MockFileScanTask(100L));

    assertThatThrownBy(() -> planner.planFileGroups(tasks))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("K-way merge requires a table sort order");
  }

  @Test
  void testSortsFilesByLowerBounds() {
    KWayMergeRewriteFilePlanner planner = new KWayMergeRewriteFilePlanner(table, null, null, false);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.REWRITE_ALL, "true",
            BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1"));

    FileScanTask taskC = mockTaskWithLowerBound(300, 100L);
    FileScanTask taskA = mockTaskWithLowerBound(100, 100L);
    FileScanTask taskB = mockTaskWithLowerBound(200, 100L);

    List<FileScanTask> tasks = ImmutableList.of(taskC, taskA, taskB);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).containsExactly(taskA, taskB, taskC);
  }

  @Test
  void testNullLowerBoundsSortedLast() {
    KWayMergeRewriteFilePlanner planner = new KWayMergeRewriteFilePlanner(table, null, null, false);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.REWRITE_ALL, "true",
            BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1"));

    FileScanTask taskWithBound = mockTaskWithLowerBound(100, 100L);
    FileScanTask taskNoBound = mockTaskWithoutLowerBound(100L);

    List<FileScanTask> tasks = ImmutableList.of(taskNoBound, taskWithBound);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).containsExactly(taskWithBound, taskNoBound);
  }

  @Test
  void testSortOrderPreservedAcrossGroups() {
    KWayMergeRewriteFilePlanner planner = new KWayMergeRewriteFilePlanner(table, null, null, false);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.REWRITE_ALL, "true",
            BinPackRewriteFilePlanner.MIN_INPUT_FILES, "1",
            BinPackRewriteFilePlanner.MAX_FILE_GROUP_SIZE_BYTES, "200"));

    FileScanTask taskA = mockTaskWithLowerBound(100, 100L);
    FileScanTask taskB = mockTaskWithLowerBound(200, 100L);
    FileScanTask taskC = mockTaskWithLowerBound(300, 100L);

    List<FileScanTask> tasks = ImmutableList.of(taskC, taskA, taskB);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    List<List<FileScanTask>> groupList = ImmutableList.copyOf(groups);
    assertThat(groupList).hasSize(2);

    // First group should contain files with lower bounds (sorted: A=100, B=200)
    // Second group should contain the remaining file (C=300)
    List<FileScanTask> allTasks =
        ImmutableList.<FileScanTask>builder()
            .addAll(groupList.get(0))
            .addAll(groupList.get(1))
            .build();
    assertThat(allTasks).containsExactly(taskA, taskB, taskC);
  }

  @Test
  void testInheritsSizeFiltering() {
    KWayMergeRewriteFilePlanner planner = new KWayMergeRewriteFilePlanner(table, null, null, false);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, "250",
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES, "500",
            BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES, "750"));

    FileScanTask tooSmall = mockTaskWithLowerBound(300, 100L);
    FileScanTask optimal = mockTaskWithLowerBound(200, 450L);
    FileScanTask tooBig = mockTaskWithLowerBound(100, 1000L);

    List<FileScanTask> tasks = ImmutableList.of(tooSmall, optimal, tooBig);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group)
        .as("Should filter to only too-small and too-big, sorted by lower bound")
        .containsExactly(tooBig, tooSmall);
  }

  private FileScanTask mockTaskWithLowerBound(int lowerBoundValue, long fileSize) {
    Map<Integer, ByteBuffer> lowerBounds =
        ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), lowerBoundValue));
    Metrics metrics = new Metrics(1L, null, null, null, null, lowerBounds, null);

    DataFile file =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
            .withFileSizeInBytes(fileSize)
            .withRecordCount(1)
            .withMetrics(metrics)
            .withSortOrder(SORT_ORDER)
            .build();

    return new MockFileScanTask(file);
  }

  private FileScanTask mockTaskWithoutLowerBound(long fileSize) {
    DataFile file =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
            .withFileSizeInBytes(fileSize)
            .withRecordCount(1)
            .withSortOrder(SORT_ORDER)
            .build();

    return new MockFileScanTask(file);
  }
}
