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
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class TestSizeBasedFileRewritePlanner {
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
  void testInputSplitSizeLowerBound() {
    FileScanTask task1 = new MockFileScanTask(mockDataFile());
    FileScanTask task2 = new MockFileScanTask(mockDataFile());
    FileScanTask task3 = new MockFileScanTask(mockDataFile());
    FileScanTask task4 = new MockFileScanTask(mockDataFile());
    List<FileScanTask> tasks = ImmutableList.of(task1, task2, task3, task4);

    TestingPlanner planner = new TestingPlanner(table);

    long minFileSize = 256L * 1024 * 1024;
    long targetFileSize = 512L * 1024 * 1024;
    long maxFileSize = 768L * 1024 * 1024;

    Map<String, String> options =
        ImmutableMap.of(
            BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES, String.valueOf(minFileSize),
            BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSize),
            BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES, String.valueOf(maxFileSize));
    planner.init(options);

    // the total task size is 580 MB and the target file size is 512 MB
    // the remainder must be written into a separate file as it exceeds 10%
    List<List<FileScanTask>> groups = Lists.newArrayList(planner.planFileGroups(tasks).iterator());

    assertThat(groups).hasSize(1);

    List<FileScanTask> group = groups.get(0);
    // the split size must be >= targetFileSize and < maxFileSize
    long splitSize = group.stream().mapToLong(FileScanTask::sizeBytes).sum();
    assertThat(splitSize).isGreaterThanOrEqualTo(targetFileSize).isLessThan(maxFileSize);
  }

  @Test
  void testValidOptions() {
    TestingPlanner planner = new TestingPlanner(table);

    assertThat(planner.validOptions())
        .as("Planner must report all supported options")
        .isEqualTo(
            ImmutableSet.of(
                BinPackRewriteFilePlanner.TARGET_FILE_SIZE_BYTES,
                BinPackRewriteFilePlanner.MIN_FILE_SIZE_BYTES,
                BinPackRewriteFilePlanner.MAX_FILE_SIZE_BYTES,
                BinPackRewriteFilePlanner.MIN_INPUT_FILES,
                BinPackRewriteFilePlanner.REWRITE_ALL,
                BinPackRewriteFilePlanner.MAX_FILE_GROUP_SIZE_BYTES));
  }

  @Test
  void testInvalidOption() {
    TestingPlanner planner = new TestingPlanner(table);

    Map<String, String> invalidTargetSizeOptions =
        ImmutableMap.of(SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, "0");
    assertThatThrownBy(() -> planner.init(invalidTargetSizeOptions))
        .hasMessageContaining("'target-file-size-bytes' is set to 0 but must be > 0");

    Map<String, String> invalidMinSizeOptions =
        ImmutableMap.of(SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES, "-1");
    assertThatThrownBy(() -> planner.init(invalidMinSizeOptions))
        .hasMessageContaining("'min-file-size-bytes' is set to -1 but must be >= 0");

    Map<String, String> invalidTargetMinSizeOptions =
        ImmutableMap.of(
            SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, "3",
            SizeBasedFileRewritePlanner.MIN_FILE_SIZE_BYTES, "5");
    assertThatThrownBy(() -> planner.init(invalidTargetMinSizeOptions))
        .hasMessageContaining("'target-file-size-bytes' (3) must be > 'min-file-size-bytes' (5)")
        .hasMessageContaining("all new files will be smaller than the min threshold");

    Map<String, String> invalidTargetMaxSizeOptions =
        ImmutableMap.of(
            SizeBasedFileRewritePlanner.TARGET_FILE_SIZE_BYTES, "5",
            SizeBasedFileRewritePlanner.MAX_FILE_SIZE_BYTES, "3");
    assertThatThrownBy(() -> planner.init(invalidTargetMaxSizeOptions))
        .hasMessageContaining("'target-file-size-bytes' (5) must be < 'max-file-size-bytes' (3)")
        .hasMessageContaining("all new files will be larger than the max threshold");

    Map<String, String> invalidMinInputFilesOptions =
        ImmutableMap.of(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "0");
    assertThatThrownBy(() -> planner.init(invalidMinInputFilesOptions))
        .hasMessageContaining("'min-input-files' is set to 0 but must be > 0");

    Map<String, String> invalidMaxFileGroupSizeOptions =
        ImmutableMap.of(SizeBasedFileRewritePlanner.MAX_FILE_GROUP_SIZE_BYTES, "0");
    assertThatThrownBy(() -> planner.init(invalidMaxFileGroupSizeOptions))
        .hasMessageContaining("'max-file-group-size-bytes' is set to 0 but must be > 0");
  }

  private static class TestingPlanner
      extends SizeBasedFileRewritePlanner<
          RewriteDataFiles.FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {
    protected TestingPlanner(Table table) {
      super(table);
    }

    @Override
    protected long defaultTargetFileSize() {
      return TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;
    }

    @Override
    protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
      return tasks;
    }

    @Override
    protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
      return groups;
    }

    @Override
    public FileRewritePlan<RewriteDataFiles.FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup>
        plan() {
      throw new UnsupportedOperationException("Not supported");
    }
  }

  private DataFile mockDataFile() {
    DataFile file = Mockito.mock(DataFile.class);
    when(file.partition()).thenReturn(Mockito.mock(StructLike.class));
    when(file.fileSizeInBytes()).thenReturn(145L * 1024 * 1024);
    return file;
  }
}
