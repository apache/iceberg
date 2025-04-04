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

import static org.apache.iceberg.actions.BinPackRewritePositionDeletePlanner.MAX_FILE_SIZE_DEFAULT_RATIO;
import static org.apache.iceberg.actions.RewritePositionDeleteFiles.REWRITE_JOB_ORDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupInfo;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestBinPackRewritePositionDeletePlanner {
  private static final Map<String, String> REWRITE_ALL =
      ImmutableMap.of(BinPackRewritePositionDeletePlanner.REWRITE_ALL, "true");

  private static final DataFile FILE_1 = newDataFile("data_bucket=0");
  private static final DataFile FILE_2 = newDataFile("data_bucket=1");
  private static final DataFile FILE_3 = newDataFile("data_bucket=2");
  private static final Map<RewriteJobOrder, List<StructLike>> EXPECTED =
      ImmutableMap.of(
          RewriteJobOrder.FILES_DESC,
          ImmutableList.of(FILE_1.partition(), FILE_2.partition(), FILE_3.partition()),
          RewriteJobOrder.FILES_ASC,
          ImmutableList.of(FILE_3.partition(), FILE_2.partition(), FILE_1.partition()),
          RewriteJobOrder.BYTES_DESC,
          ImmutableList.of(FILE_3.partition(), FILE_1.partition(), FILE_2.partition()),
          RewriteJobOrder.BYTES_ASC,
          ImmutableList.of(FILE_2.partition(), FILE_1.partition(), FILE_3.partition()));

  @TempDir private File tableDir = null;
  private TestTables.TestTable table = null;

  @BeforeEach
  public void setupTable() throws Exception {
    this.table = TestTables.create(tableDir, "test", TestBase.SCHEMA, TestBase.SPEC, 2);
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  void testPartitionedTable() {
    addFiles();
    BinPackRewritePositionDeletePlanner planner = new BinPackRewritePositionDeletePlanner(table);
    planner.init(ImmutableMap.of(BinPackRewritePositionDeletePlanner.REWRITE_ALL, "true"));

    FileRewritePlan<FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
        plan = planner.plan();

    List<RewritePositionDeletesGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(groups).hasSize(3);
    assertThat(groups.stream().mapToInt(RewriteGroupBase::inputFileNum).sum()).isEqualTo(6);
  }

  @Test
  void testUnpartitionedTable() {
    table.updateSpec().removeField("data_bucket").commit();
    table.refresh();
    table
        .newRowDelta()
        .addRows(newDataFile(""))
        .addDeletes(newDeleteFile(10))
        .addDeletes(newDeleteFile(20))
        .addDeletes(newDeleteFile(30))
        .commit();

    BinPackRewritePositionDeletePlanner planner = new BinPackRewritePositionDeletePlanner(table);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_INPUT_FILES,
            "1",
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES,
            "30"));

    FileRewritePlan<FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
        plan = planner.plan();

    assertThat(plan.totalGroupCount()).isEqualTo(1);
    assertThat(plan.groups().iterator().next().inputFileNum()).isEqualTo(2);
  }

  @Test
  void testEmptyTable() {
    BinPackRewritePositionDeletePlanner planner = new BinPackRewritePositionDeletePlanner(table);
    planner.init(REWRITE_ALL);

    FileRewritePlan<FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
        plan = planner.plan();

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();
    assertThat(plan.totalGroupCount()).isZero();
  }

  @ParameterizedTest
  @EnumSource(
      value = RewriteJobOrder.class,
      names = {"FILES_DESC", "FILES_ASC", "BYTES_DESC", "BYTES_ASC"})
  void testJobOrder(RewriteJobOrder order) {
    addFiles();
    BinPackRewritePositionDeletePlanner planner = new BinPackRewritePositionDeletePlanner(table);
    planner.init(
        ImmutableMap.of(
            BinPackRewriteFilePlanner.REWRITE_ALL, "true", REWRITE_JOB_ORDER, order.name()));

    FileRewritePlan<FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
        plan = planner.plan();

    List<RewritePositionDeletesGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(
            groups.stream()
                .map(
                    group ->
                        new PartitionData(TestBase.SPEC.partitionType())
                            .copyFor(group.info().partition()))
                .collect(Collectors.toList()))
        .isEqualTo(EXPECTED.get(order));
    assertThat(plan.totalGroupCount()).isEqualTo(3);
    EXPECTED.get(order).forEach(s -> assertThat(plan.groupsInPartition(s)).isEqualTo(1));
  }

  @Test
  void testMaxGroupSize() {
    addFiles();
    BinPackRewritePositionDeletePlanner planner = new BinPackRewritePositionDeletePlanner(table);
    planner.init(
        ImmutableMap.of(
            BinPackRewritePositionDeletePlanner.REWRITE_ALL,
            "true",
            BinPackRewritePositionDeletePlanner.MAX_FILE_GROUP_SIZE_BYTES,
            "10"));

    FileRewritePlan<FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
        plan = planner.plan();

    assertThat(plan.totalGroupCount()).isEqualTo(6);
    assertThat(plan.groupsInPartition(FILE_1.partition())).isEqualTo(3);
    assertThat(plan.groupsInPartition(FILE_2.partition())).isEqualTo(2);
    assertThat(plan.groupsInPartition(FILE_3.partition())).isEqualTo(1);
  }

  @Test
  void testFilter() {
    addFiles();
    BinPackRewritePositionDeletePlanner planner =
        new BinPackRewritePositionDeletePlanner(
            table,
            Expressions.or(
                Expressions.equal(Expressions.bucket("data", 16), 0),
                Expressions.equal(Expressions.bucket("data", 16), 2)),
            false);
    planner.init(REWRITE_ALL);

    FileRewritePlan<FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
        plan = planner.plan();

    List<RewritePositionDeletesGroup> groups = Lists.newArrayList(plan.groups().iterator());
    assertThat(plan.totalGroupCount()).isEqualTo(2);
    assertThat(groups).hasSize(2);
    assertThat(groups.stream().mapToLong(RewritePositionDeletesGroup::inputFileNum).sum())
        .isEqualTo(4);
  }

  @Test
  void testMaxOutputFileSize() {
    addFiles();
    int targetFileSize = 10;
    BinPackRewritePositionDeletePlanner planner = new BinPackRewritePositionDeletePlanner(table);
    planner.init(
        ImmutableMap.of(
            BinPackRewritePositionDeletePlanner.REWRITE_ALL,
            "true",
            BinPackRewritePositionDeletePlanner.TARGET_FILE_SIZE_BYTES,
            String.valueOf(targetFileSize)));

    FileRewritePlan<FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
        plan = planner.plan();

    assertThat(plan.groups().iterator().next().maxOutputFileSize())
        .isGreaterThan(targetFileSize)
        .isLessThan((long) (targetFileSize * MAX_FILE_SIZE_DEFAULT_RATIO));
  }

  @Test
  void testValidOptions() {
    BinPackRewritePositionDeletePlanner planner = new BinPackRewritePositionDeletePlanner(table);

    assertThat(planner.validOptions())
        .as("Planner must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                BinPackRewritePositionDeletePlanner.TARGET_FILE_SIZE_BYTES,
                BinPackRewritePositionDeletePlanner.MIN_FILE_SIZE_BYTES,
                BinPackRewritePositionDeletePlanner.MAX_FILE_SIZE_BYTES,
                BinPackRewritePositionDeletePlanner.MIN_INPUT_FILES,
                BinPackRewritePositionDeletePlanner.REWRITE_ALL,
                BinPackRewritePositionDeletePlanner.MAX_FILE_GROUP_SIZE_BYTES,
                RewriteDataFiles.REWRITE_JOB_ORDER));
  }

  @Test
  void testInvalidOption() {
    BinPackRewritePositionDeletePlanner planner = new BinPackRewritePositionDeletePlanner(table);

    Map<String, String> invalidRewriteJobOrderOptions =
        ImmutableMap.of(RewritePositionDeleteFiles.REWRITE_JOB_ORDER, "foo");
    assertThatThrownBy(() -> planner.init(invalidRewriteJobOrderOptions))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid rewrite job order name: foo");
  }

  private void addFiles() {
    table
        .newRowDelta()
        .addRows(FILE_1)
        .addDeletes(newDeleteFile(FILE_1.partition(), 10))
        .addDeletes(newDeleteFile(FILE_1.partition(), 10))
        .addDeletes(newDeleteFile(FILE_1.partition(), 10))
        .addRows(FILE_2)
        .addDeletes(newDeleteFile(FILE_2.partition(), 11))
        .addDeletes(newDeleteFile(FILE_2.partition(), 11))
        .addRows(FILE_3)
        .addDeletes(newDeleteFile(FILE_3.partition(), 50))
        .commit();
  }

  private static DataFile newDataFile(String partitionPath) {
    return DataFiles.builder(TestBase.SPEC)
        .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath(partitionPath)
        .withRecordCount(1)
        .build();
  }

  private static DeleteFile newDeleteFile(long fileSize) {
    return newDeleteFile(
        new PartitionData(PartitionSpec.unpartitioned().partitionType()), fileSize);
  }

  private static DeleteFile newDeleteFile(StructLike partition, long fileSize) {
    return FileMetadata.deleteFileBuilder(TestBase.SPEC)
        .ofPositionDeletes()
        .withPath("/path/to/delete-" + UUID.randomUUID() + ".parquet")
        .withFileSizeInBytes(fileSize)
        .withPartition(partition)
        .withRecordCount(1)
        .build();
  }
}
