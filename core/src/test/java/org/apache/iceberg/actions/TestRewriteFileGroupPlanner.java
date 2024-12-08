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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestRewriteFileGroupPlanner {
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
  void testGroups(RewriteJobOrder order) {
    table
        .newAppend()
        .appendFile(FILE_1)
        .appendFile(FILE_2)
        .appendFile(FILE_3)
        .appendFile(FILE_4)
        .appendFile(FILE_5)
        .appendFile(FILE_6)
        .commit();
    RewriteFileGroupPlanner planner = new RewriteFileGroupPlanner(new DummyRewriter(false), order);
    RewriteFileGroupPlanner.RewritePlanResult result =
        planner.plan(table, Expressions.alwaysTrue(), table.currentSnapshot().snapshotId(), false);
    List<RewriteFileGroup> groups = result.groups().collect(Collectors.toList());
    assertThat(groups.stream().map(group -> group.info().partition()).collect(Collectors.toList()))
        .isEqualTo(EXPECTED.get(order));
    assertThat(result.totalGroupCount()).isEqualTo(3);
    EXPECTED.get(order).forEach(s -> assertThat(result.groupsInPartition(s)).isEqualTo(1));
  }

  @Test
  void testContext() {
    table
        .newAppend()
        .appendFile(FILE_1)
        .appendFile(FILE_2)
        .appendFile(FILE_3)
        .appendFile(FILE_4)
        .appendFile(FILE_5)
        .appendFile(FILE_6)
        .commit();
    RewriteFileGroupPlanner planner =
        new RewriteFileGroupPlanner(new DummyRewriter(true), RewriteJobOrder.FILES_DESC);
    RewriteFileGroupPlanner.RewritePlanResult result =
        planner.plan(table, Expressions.alwaysTrue(), table.currentSnapshot().snapshotId(), false);
    assertThat(result.totalGroupCount()).isEqualTo(6);
    assertThat(result.groupsInPartition(FILE_1.partition())).isEqualTo(3);
    assertThat(result.groupsInPartition(FILE_4.partition())).isEqualTo(2);
    assertThat(result.groupsInPartition(FILE_6.partition())).isEqualTo(1);
  }

  private static class DummyRewriter implements FileRewriter<FileScanTask, DataFile> {
    private final boolean split;

    private DummyRewriter(boolean split) {
      this.split = split;
    }

    @Override
    public Set<String> validOptions() {
      return Set.of();
    }

    @Override
    public void init(Map<String, String> options) {}

    @Override
    public Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> tasks) {
      List<FileScanTask> taskList = Lists.newArrayList(tasks);
      return split
          ? taskList.stream().map(ImmutableList::of).collect(Collectors.toList())
          : ImmutableList.of(taskList);
    }

    @Override
    public Set<DataFile> rewrite(List<FileScanTask> group) {
      return Set.of();
    }
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
