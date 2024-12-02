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
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
  void testSplitSizeLowerBound() {
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
            RewriteFileGroupPlanner.MIN_FILE_SIZE_BYTES, String.valueOf(minFileSize),
            RewriteFileGroupPlanner.TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSize),
            RewriteFileGroupPlanner.MAX_FILE_SIZE_BYTES, String.valueOf(maxFileSize));
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

  private static class TestingPlanner
      extends SizeBasedFileRewritePlanner<
          RewriteDataFiles.FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {
    protected TestingPlanner(Table table) {
      super(table);
    }

    @Override
    protected long defaultTargetFileSize() {
      return 0;
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
