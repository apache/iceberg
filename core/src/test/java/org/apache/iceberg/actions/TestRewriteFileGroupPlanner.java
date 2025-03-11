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

import static org.apache.iceberg.actions.RewriteDataFiles.REWRITE_JOB_ORDER;
import static org.apache.iceberg.actions.RewriteFileGroupPlanner.MAX_FILE_SIZE_DEFAULT_RATIO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestRewriteFileGroupPlanner {
  private static final Map<String, String> REWRITE_ALL =
      ImmutableMap.of(RewriteFileGroupPlanner.REWRITE_ALL, "true");

  private static final DataFile FILE_1 = newDataFile("data_bucket=0", 10);
  private static final DataFile FILE_2 = newDataFile("data_bucket=0", 10);
  private static final DataFile FILE_3 = newDataFile("data_bucket=0", 10);
  private static final DataFile FILE_4 = newDataFile("data_bucket=1", 11);
  private static final DataFile FILE_5 = newDataFile("data_bucket=1", 11);
  private static final DataFile FILE_6 = newDataFile("data_bucket=2", 50);

  private static final Map<RewriteJobOrder, List<StructLike>> EXPECTED =
      ImmutableMap.of(
          RewriteJobOrder.FILES_DESC,
          ImmutableList.of(FILE_1.partition(), FILE_4.partition(), FILE_6.partition()),
          RewriteJobOrder.FILES_ASC,
          ImmutableList.of(FILE_6.partition(), FILE_4.partition(), FILE_1.partition()),
          RewriteJobOrder.BYTES_DESC,
          ImmutableList.of(FILE_6.partition(), FILE_1.partition(), FILE_4.partition()),
          RewriteJobOrder.BYTES_ASC,
          ImmutableList.of(FILE_4.partition(), FILE_1.partition(), FILE_6.partition()));

  @TempDir private File tableDir = null;
  private TestTables.TestTable table = null;

  @BeforeEach
  public void setupTable() throws Exception {
    this.table = TestTables.create(tableDir, "test", TestBase.SCHEMA, TestBase.SPEC, 3);
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  void testPartitionedTable() {
    addFiles();
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    planner.init(ImmutableMap.of(RewriteFileGroupPlanner.REWRITE_ALL, "true"));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(groups).hasSize(3);
    assertThat(groups.stream().mapToInt(RewriteGroupBase::inputFileNum).sum()).isEqualTo(6);
  }

  @Test
  void testUnpartitionedTable() {
    table.updateSpec().removeField("data_bucket").commit();
    table.refresh();
    table
        .newAppend()
        .appendFile(newDataFile("", 10))
        .appendFile(newDataFile("", 20))
        .appendFile(newDataFile("", 30))
        .commit();
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    planner.init(
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_INPUT_FILES,
            "1",
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES,
            "30"));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    assertThat(plan.totalGroupCount()).isEqualTo(1);
    assertThat(plan.groups().iterator().next().inputFileNum()).isEqualTo(2);
  }

  @Test
  void testEmptyTable() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    planner.init(REWRITE_ALL);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();
    assertThat(plan.totalGroupCount()).isZero();
  }

  @ParameterizedTest
  @EnumSource(
      value = RewriteJobOrder.class,
      names = {"FILES_DESC", "FILES_ASC", "BYTES_DESC", "BYTES_ASC"})
  void testJobOrder(RewriteJobOrder order) {
    addFiles();
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    planner.init(
        ImmutableMap.of(
            RewriteFileGroupPlanner.REWRITE_ALL, "true", REWRITE_JOB_ORDER, order.name()));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(groups.stream().map(group -> group.info().partition()).collect(Collectors.toList()))
        .isEqualTo(EXPECTED.get(order));
    assertThat(plan.totalGroupCount()).isEqualTo(3);
    EXPECTED.get(order).forEach(s -> assertThat(plan.groupsInPartition(s)).isEqualTo(1));
  }

  @Test
  void testMaxGroupSize() {
    addFiles();
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    planner.init(
        ImmutableMap.of(
            RewriteFileGroupPlanner.REWRITE_ALL,
            "true",
            RewriteFileGroupPlanner.MAX_FILE_GROUP_SIZE_BYTES,
            "10"));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    assertThat(plan.totalGroupCount()).isEqualTo(6);
    assertThat(plan.groupsInPartition(FILE_1.partition())).isEqualTo(3);
    assertThat(plan.groupsInPartition(FILE_4.partition())).isEqualTo(2);
    assertThat(plan.groupsInPartition(FILE_6.partition())).isEqualTo(1);
  }

  @Test
  void testFilter() {
    addFiles();
    RewriteFileGroupPlanner planner =
        new RewriteFileGroupPlanner(
            table,
            Expressions.or(
                Expressions.equal(Expressions.bucket("data", 16), 0),
                Expressions.equal(Expressions.bucket("data", 16), 2)));
    planner.init(REWRITE_ALL);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(plan.totalGroupCount()).isEqualTo(2);
    assertThat(groups).hasSize(2);
    assertThat(groups.stream().mapToLong(RewriteFileGroup::inputFileNum).sum()).isEqualTo(4);
  }

  @Test
  void testMaxOutputFileSize() {
    addFiles();
    int targetFileSize = 10;
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    planner.init(
        ImmutableMap.of(
            RewriteFileGroupPlanner.REWRITE_ALL,
            "true",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES,
            String.valueOf(targetFileSize)));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    assertThat(plan.groups().iterator().next().maxOutputFileSize())
        .isGreaterThan(targetFileSize)
        .isLessThan((long) (targetFileSize * MAX_FILE_SIZE_DEFAULT_RATIO));
  }

  @Test
  void testExpectedOutputFiles() {
    addFiles();
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    planner.init(
        ImmutableMap.of(
            RewriteFileGroupPlanner.REWRITE_ALL,
            "true",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES,
            "21"));

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    List<RewriteFileGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(groups).hasSize(3);
    for (RewriteFileGroup group : groups) {
      if (FILE_1.partition().equals(group.info().partition())) {
        assertThat(group.expectedOutputFiles()).isEqualTo(2);
      } else if (FILE_4.partition().equals(group.info().partition())) {
        assertThat(group.expectedOutputFiles()).isEqualTo(1);
      } else if (FILE_6.partition().equals(group.info().partition())) {
        assertThat(group.expectedOutputFiles()).isEqualTo(3);
      } else {
        throw new IllegalStateException("Unexpected partition: " + group.info().partition());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testOutputSpec(boolean specific) {
    addFiles();
    int oldSpecId = table.spec().specId();
    table.updateSpec().removeField("data_bucket").commit();
    table.newAppend().appendFile(newDataFile("", 10)).commit();
    table.refresh();
    int newSpecId = table.spec().specId();

    Map<String, String> options = Maps.newHashMap(REWRITE_ALL);
    if (specific) {
      options.put(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(oldSpecId));
    }

    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    planner.init(options);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan = planner.plan();

    assertThat(plan.groups().iterator().next().outputSpecId())
        .isEqualTo(specific ? oldSpecId : newSpecId);
  }

  @Test
  void testValidOptions() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);

    assertThat(planner.validOptions())
        .as("Planner must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES,
                RewriteFileGroupPlanner.MIN_INPUT_FILES,
                RewriteFileGroupPlanner.REWRITE_ALL,
                RewriteFileGroupPlanner.MAX_FILE_GROUP_SIZE_BYTES,
                RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD,
                RewriteFileGroupPlanner.DELETE_RATIO_THRESHOLD,
                RewriteDataFiles.REWRITE_JOB_ORDER));
  }

  @Test
  void testInvalidOption() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);

    Map<String, String> invalidRewriteJobOrderOptions =
        ImmutableMap.of(RewriteDataFiles.REWRITE_JOB_ORDER, "foo");
    assertThatThrownBy(() -> planner.init(invalidRewriteJobOrderOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid rewrite job order name: foo");

    Map<String, String> invalidOutputSpecIdOptions =
        ImmutableMap.of(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(1234));
    assertThatThrownBy(() -> planner.init(invalidOutputSpecIdOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot use output spec id 1234 because the table does not contain a reference to this spec-id.");

    Map<String, String> invalidDeleteFileThresholdOptions =
        ImmutableMap.of(RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, "-1");
    assertThatThrownBy(() -> planner.init(invalidDeleteFileThresholdOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'delete-file-threshold' is set to -1 but must be >= 0");

    Map<String, String> negativeDeleteRatioThresholdOptions =
        ImmutableMap.of(RewriteFileGroupPlanner.DELETE_RATIO_THRESHOLD, "-1");
    assertThatThrownBy(() -> planner.init(negativeDeleteRatioThresholdOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'delete-ratio-threshold' is set to -1.0 but must be > 0");

    Map<String, String> invalidDeleteRatioThresholdOptions =
        ImmutableMap.of(RewriteFileGroupPlanner.DELETE_RATIO_THRESHOLD, "127");
    assertThatThrownBy(() -> planner.init(invalidDeleteRatioThresholdOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'delete-ratio-threshold' is set to 127.0 but must be <= 1");
  }

  @Test
  void testSizeFiltering() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    FileScanTask tooSmallTask = new MockFileScanTask(100L);
    FileScanTask optimal = new MockFileScanTask(450);
    FileScanTask tooBigTask = new MockFileScanTask(1000L);
    List<FileScanTask> tasks = ImmutableList.of(tooSmallTask, optimal, tooBigTask);

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "250",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, "500",
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, "750");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 2 files").hasSize(2);
    assertThat(group).containsExactlyInAnyOrder(tooSmallTask, tooBigTask);
  }

  @Test
  void testGroupWithEnoughFiles() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    List<FileScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(100L),
            new MockFileScanTask(100L),
            new MockFileScanTask(100L),
            new MockFileScanTask(100L));

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_INPUT_FILES, "3",
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "150",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, "1000",
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, "5000");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 4 files").hasSize(4);
  }

  @Test
  void testGroupWithEnoughData() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    List<FileScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(100L), new MockFileScanTask(100L), new MockFileScanTask(100L));

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "200",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, "250",
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, "500");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 3 files").hasSize(3);
  }

  @Test
  void testFileSizeAboveTarget() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    List<FileScanTask> tasks = ImmutableList.of(new MockFileScanTask(2000L));

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "200",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, "250",
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, "500");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite big file").hasSize(1);
  }

  @Test
  void testDeleteFileThreshold() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    FileScanTask tooManyDeletesTask = MockFileScanTask.mockTaskWithDeletes(1000L, 3);
    FileScanTask optimalTask = MockFileScanTask.mockTaskWithDeletes(1001L, 1);
    List<FileScanTask> tasks = ImmutableList.of(tooManyDeletesTask, optimalTask);

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "0",
            RewriteFileGroupPlanner.DELETE_FILE_THRESHOLD, "2");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 1 file").hasSize(1);
    assertThat(group).containsExactly(tooManyDeletesTask);
  }

  @Test
  void testDeleteRatioThreshold() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    FileScanTask tooManyDeletesTask =
        MockFileScanTask.mockTaskWithFileScopedDeleteRecords(1000L, 100, 1, 20);
    FileScanTask optimalTask =
        MockFileScanTask.mockTaskWithFileScopedDeleteRecords(1001L, 100, 1, 19);
    List<FileScanTask> tasks = ImmutableList.of(tooManyDeletesTask, optimalTask);

    Map<String, String> options =
        ImmutableMap.of(
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, "0",
            RewriteFileGroupPlanner.DELETE_RATIO_THRESHOLD, "0.2");
    planner.init(options);

    Iterable<List<FileScanTask>> groups = planner.planFileGroups(tasks);
    assertThat(groups).as("Must have 1 group").hasSize(1);
    List<FileScanTask> group = Iterables.getOnlyElement(groups);
    assertThat(group).as("Must rewrite 1 file").hasSize(1);
    assertThat(group).containsExactly(tooManyDeletesTask);
  }

  private void addFiles() {
    table
        .newAppend()
        .appendFile(FILE_1)
        .appendFile(FILE_2)
        .appendFile(FILE_3)
        .appendFile(FILE_4)
        .appendFile(FILE_5)
        .appendFile(FILE_6)
        .commit();
  }

  private static DataFile newDataFile(String partitionPath, long fileSize) {
    return DataFiles.builder(TestBase.SPEC)
        .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(fileSize)
        .withPartitionPath(partitionPath)
        .withRecordCount(1)
        .build();
  }
}
