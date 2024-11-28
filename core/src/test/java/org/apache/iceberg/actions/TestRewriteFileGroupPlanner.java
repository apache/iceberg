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
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> result =
        planner.plan();
    List<RewriteFileGroup> groups = result.groups().collect(Collectors.toList());
    assertThat(groups.stream().map(group -> group.info().partition()).collect(Collectors.toList()))
        .isEqualTo(EXPECTED.get(order));
    assertThat(result.totalGroupCount()).isEqualTo(3);
    EXPECTED.get(order).forEach(s -> assertThat(result.groupsInPartition(s)).isEqualTo(1));
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
    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> result =
        planner.plan();
    assertThat(result.totalGroupCount()).isEqualTo(1);
    assertThat(result.groups().iterator().next().numInputFiles()).isEqualTo(2);
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
    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> result =
        planner.plan();
    assertThat(result.totalGroupCount()).isEqualTo(6);
    assertThat(result.groupsInPartition(FILE_1.partition())).isEqualTo(3);
    assertThat(result.groupsInPartition(FILE_4.partition())).isEqualTo(2);
    assertThat(result.groupsInPartition(FILE_6.partition())).isEqualTo(1);
  }

  @Test
  void testEmptyTable() {
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);

    planner.init(REWRITE_ALL);

    FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> result =
        planner.plan();

    assertThat(table.currentSnapshot()).as("Table must be empty").isNull();
    assertThat(result.totalGroupCount()).isZero();
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
    FileRewritePlan<RewriteDataFiles.FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan =
        planner.plan();
    List<RewriteFileGroup> groups = plan.groups().collect(Collectors.toList());

    assertThat(plan.totalGroupCount()).isEqualTo(2);
    assertThat(groups).hasSize(2);
    assertThat(groups.stream().mapToLong(FileRewriteGroup::numInputFiles).sum()).isEqualTo(4);
  }

  @Test
  void testWriteMaxFileSize() {
    int targetFileSize = 10;
    addFiles();

    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);
    planner.init(
        ImmutableMap.of(
            RewriteFileGroupPlanner.REWRITE_ALL,
            "true",
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES,
            String.valueOf(targetFileSize)));
    FileRewritePlan<RewriteDataFiles.FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan =
        planner.plan();
    assertThat(plan.writeMaxFileSize())
        .isGreaterThan(targetFileSize)
        .isLessThan((long) (targetFileSize * MAX_FILE_SIZE_DEFAULT_RATIO));
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

    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);

    Map<String, String> options = Maps.newHashMap(REWRITE_ALL);
    if (specific) {
      options.put(RewriteDataFiles.OUTPUT_SPEC_ID, String.valueOf(oldSpecId));
    }

    planner.init(options);

    FileRewritePlan<RewriteDataFiles.FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan =
        planner.plan();
    assertThat(plan.outputSpecId()).isEqualTo(specific ? oldSpecId : newSpecId);
  }

  @Test
  public void testInvalidOption() {
    addFiles();

    assertThatThrownBy(
            () -> {
              RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);

              planner.init(ImmutableMap.of(RewriteDataFiles.REWRITE_JOB_ORDER, "foo"));
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid rewrite job order name: foo");

    assertThatThrownBy(
            () -> {
              RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(table);

              planner.init(
                  ImmutableMap.of(
                      RewriteFileGroupPlanner.REWRITE_ALL,
                      "true",
                      RewriteDataFiles.OUTPUT_SPEC_ID,
                      String.valueOf(1234)));
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot use output spec id 1234 because the table does not contain a reference to this spec-id.");
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
