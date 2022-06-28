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

package org.apache.iceberg.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MergeableScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.SplittableScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestTableScanUtil {

  private List<FileScanTask> tasksWithDataAndDeleteSizes(List<Pair<Long, Long[]>> sizePairs) {
    return sizePairs.stream().map(sizePair -> {
      DataFile dataFile = dataFileWithSize(sizePair.first());
      DeleteFile[] deleteFiles = deleteFilesWithSizes(
          Arrays.stream(sizePair.second()).mapToLong(Long::longValue).toArray());
      return new MockFileScanTask(dataFile, deleteFiles);
    }).collect(Collectors.toList());
  }

  private DataFile dataFileWithSize(long size) {
    DataFile mockFile = Mockito.mock(DataFile.class);
    Mockito.when(mockFile.fileSizeInBytes()).thenReturn(size);
    return mockFile;
  }

  private DeleteFile[] deleteFilesWithSizes(long... sizes) {
    return Arrays.stream(sizes).mapToObj(size -> {
      DeleteFile mockDeleteFile = Mockito.mock(DeleteFile.class);
      Mockito.when(mockDeleteFile.fileSizeInBytes()).thenReturn(size);
      return mockDeleteFile;
    }).toArray(DeleteFile[]::new);
  }

  @Test
  public void testPlanTaskWithDeleteFiles() {
    List<FileScanTask> testFiles = tasksWithDataAndDeleteSizes(
        Arrays.asList(
            Pair.of(150L, new Long[] {50L, 100L}),
            Pair.of(50L, new Long[] {1L, 50L}),
            Pair.of(50L, new Long[] {100L}),
            Pair.of(1L, new Long[] {1L, 1L}),
            Pair.of(75L, new Long[] {75L})
        ));

    List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(
        TableScanUtil.planTasks(CloseableIterable.withNoopClose(testFiles), 300L, 3, 50L)
    );

    List<CombinedScanTask> expectedCombinedTasks = Arrays.asList(
        new BaseCombinedScanTask(Collections.singletonList(testFiles.get(0))),
        new BaseCombinedScanTask(Arrays.asList(testFiles.get(1), testFiles.get(2))),
        new BaseCombinedScanTask(Arrays.asList(testFiles.get(3), testFiles.get(4)))
    );

    Assert.assertEquals("Should plan 3 Combined tasks since there is delete files to be considered",
        3, combinedScanTasks.size());
    for (int i = 0; i < expectedCombinedTasks.size(); ++i) {
      Assert.assertEquals("Scan tasks detail in combined task check failed",
          expectedCombinedTasks.get(i).files(), combinedScanTasks.get(i).files());
    }
  }

  @Test
  public void testTaskGroupPlanning() {
    List<ParentTask> tasks = ImmutableList.of(
        new ChildTask1(64),
        new ChildTask1(32),
        new ChildTask3(64),
        new ChildTask3(32),
        new ChildTask2(128),
        new ChildTask3(32),
        new ChildTask3(32)
    );

    CloseableIterable<ScanTaskGroup<ParentTask>> taskGroups = TableScanUtil.planTaskGroups(
        CloseableIterable.withNoopClose(tasks), 128, 10, 4);

    Assert.assertEquals("Must have 3 task groups", 3, Iterables.size(taskGroups));
  }

  @Test
  public void testTaskMerging() {
    List<ParentTask> tasks = ImmutableList.of(
      new ChildTask1(64),
      new ChildTask1(64),
      new ChildTask2(128),
      new ChildTask3(32),
      new ChildTask3(32)
    );
    List<ParentTask> mergedTasks = TableScanUtil.mergeTasks(tasks);
    Assert.assertEquals("Appropriate tasks should be merged", 3, mergedTasks.size());
  }

  private interface ParentTask extends ScanTask {
  }

  private static class ChildTask1 implements ParentTask, SplittableScanTask<ChildTask1>, MergeableScanTask<ChildTask1> {
    private final long sizeBytes;

    ChildTask1(long sizeBytes) {
      this.sizeBytes = sizeBytes;
    }

    @Override
    public Iterable<ChildTask1> split(long targetSplitSize) {
      return ImmutableList.of(new ChildTask1(sizeBytes / 2), new ChildTask1(sizeBytes / 2));
    }

    @Override
    public boolean canMerge(ScanTask other) {
      return other instanceof ChildTask1;
    }

    @Override
    public ChildTask1 merge(ScanTask other) {
      ChildTask1 that = (ChildTask1) other;
      return new ChildTask1(sizeBytes + that.sizeBytes);
    }

    @Override
    public long sizeBytes() {
      return sizeBytes;
    }
  }

  private static class ChildTask2 implements ParentTask, SplittableScanTask<ChildTask2> {
    private final long sizeBytes;

    ChildTask2(long sizeBytes) {
      this.sizeBytes = sizeBytes;
    }

    @Override
    public Iterable<ChildTask2> split(long targetSplitSize) {
      return ImmutableList.of(new ChildTask2(sizeBytes / 2), new ChildTask2(sizeBytes / 2));
    }

    @Override
    public long sizeBytes() {
      return sizeBytes;
    }
  }

  private static class ChildTask3 implements ParentTask, MergeableScanTask<ChildTask3> {
    private final long sizeBytes;

    ChildTask3(long sizeBytes) {
      this.sizeBytes = sizeBytes;
    }

    @Override
    public boolean canMerge(ScanTask other) {
      return other instanceof ChildTask3;
    }

    @Override
    public ChildTask3 merge(ScanTask other) {
      ChildTask3 that = (ChildTask3) other;
      return new ChildTask3(sizeBytes + that.sizeBytes);
    }

    @Override
    public long sizeBytes() {
      return sizeBytes;
    }
  }
}
