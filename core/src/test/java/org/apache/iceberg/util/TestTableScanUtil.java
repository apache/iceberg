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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MergeableScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SplittableScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestTableScanUtil {

  private List<FileScanTask> tasksWithDataAndDeleteSizes(List<Pair<Long, Long[]>> sizePairs) {
    return sizePairs.stream()
        .map(
            sizePair -> {
              DataFile dataFile = dataFileWithSize(sizePair.first());
              DeleteFile[] deleteFiles =
                  deleteFilesWithSizes(
                      Arrays.stream(sizePair.second()).mapToLong(Long::longValue).toArray());
              return new MockFileScanTask(dataFile, deleteFiles);
            })
        .collect(Collectors.toList());
  }

  private DataFile dataFileWithSize(long size) {
    DataFile mockFile = Mockito.mock(DataFile.class);
    Mockito.when(mockFile.fileSizeInBytes()).thenReturn(size);
    return mockFile;
  }

  private DeleteFile[] deleteFilesWithSizes(long... sizes) {
    return Arrays.stream(sizes)
        .mapToObj(
            size -> {
              DeleteFile mockDeleteFile = Mockito.mock(DeleteFile.class);
              Mockito.when(mockDeleteFile.fileSizeInBytes()).thenReturn(size);
              return mockDeleteFile;
            })
        .toArray(DeleteFile[]::new);
  }

  @Test
  public void testPlanTaskWithDeleteFiles() {
    List<FileScanTask> testFiles =
        tasksWithDataAndDeleteSizes(
            Arrays.asList(
                Pair.of(150L, new Long[] {50L, 100L}),
                Pair.of(50L, new Long[] {1L, 50L}),
                Pair.of(50L, new Long[] {100L}),
                Pair.of(1L, new Long[] {1L, 1L}),
                Pair.of(75L, new Long[] {75L})));

    List<CombinedScanTask> combinedScanTasks =
        Lists.newArrayList(
            TableScanUtil.planTasks(CloseableIterable.withNoopClose(testFiles), 300L, 3, 50L));

    List<CombinedScanTask> expectedCombinedTasks =
        Arrays.asList(
            new BaseCombinedScanTask(Collections.singletonList(testFiles.get(0))),
            new BaseCombinedScanTask(Arrays.asList(testFiles.get(1), testFiles.get(2))),
            new BaseCombinedScanTask(Arrays.asList(testFiles.get(3), testFiles.get(4))));

    assertThat(combinedScanTasks)
        .as("Should plan 3 Combined tasks since there is delete files to be considered")
        .hasSize(3);
    for (int i = 0; i < expectedCombinedTasks.size(); ++i) {
      assertThat(combinedScanTasks.get(i).files())
          .as("Scan tasks detail in combined task check failed")
          .isEqualTo(expectedCombinedTasks.get(i).files());
    }
  }

  @Test
  public void testTaskGroupPlanning() {
    List<ParentTask> tasks =
        ImmutableList.of(
            new ChildTask1(64),
            new ChildTask1(32),
            new ChildTask3(64),
            new ChildTask3(32),
            new ChildTask2(128),
            new ChildTask3(32),
            new ChildTask3(32));

    CloseableIterable<ScanTaskGroup<ParentTask>> taskGroups =
        TableScanUtil.planTaskGroups(CloseableIterable.withNoopClose(tasks), 128, 10, 4);
    assertThat(taskGroups).as("Must have 3 task groups").hasSize(3);
  }

  @Test
  public void testTaskGroupPlanningCorruptedOffset() {
    DataFile dataFile =
        DataFiles.builder(TableTestBase.SPEC)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .withSplitOffsets(
                ImmutableList.of(2L, 12L)) // the last offset is beyond the end of the file
            .build();

    ResidualEvaluator residualEvaluator =
        ResidualEvaluator.of(TableTestBase.SPEC, Expressions.equal("id", 1), false);

    BaseFileScanTask baseFileScanTask =
        new BaseFileScanTask(
            dataFile,
            null,
            SchemaParser.toJson(TableTestBase.SCHEMA),
            PartitionSpecParser.toJson(TableTestBase.SPEC),
            residualEvaluator);

    List<BaseFileScanTask> baseFileScanTasks = ImmutableList.of(baseFileScanTask);

    int taskCount = 0;
    for (ScanTaskGroup<BaseFileScanTask> task :
        TableScanUtil.planTaskGroups(CloseableIterable.withNoopClose(baseFileScanTasks), 1, 1, 0)) {
      for (FileScanTask fileScanTask : task.tasks()) {
        DataFile taskDataFile = fileScanTask.file();
        Assertions.assertThat(taskDataFile.splitOffsets()).isNull();
        taskCount++;
      }
    }

    // 10 tasks since the split offsets are ignored and there are 1 byte splits for a 10 byte file
    Assertions.assertThat(taskCount).isEqualTo(10);
  }

  @Test
  public void testTaskMerging() {
    List<ParentTask> tasks =
        ImmutableList.of(
            new ChildTask1(64),
            new ChildTask1(64),
            new ChildTask2(128),
            new ChildTask3(32),
            new ChildTask3(32));
    List<ParentTask> mergedTasks = TableScanUtil.mergeTasks(tasks);
    assertThat(mergedTasks).as("Appropriate tasks should be merged").hasSize(3);
  }

  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
          Types.NestedField.optional(2, "c2", Types.StringType.get()),
          Types.NestedField.optional(3, "c3", Types.StringType.get()),
          Types.NestedField.optional(4, "c4", Types.StringType.get()));

  private static final PartitionSpec SPEC1 =
      PartitionSpec.builderFor(TEST_SCHEMA).identity("c1").identity("c2").build();

  private static final PartitionSpec SPEC2 =
      PartitionSpec.builderFor(TEST_SCHEMA).identity("c1").identity("c3").identity("c2").build();

  private static final StructLike PARTITION1 = new TestStructLike(100, "a");
  private static final StructLike PARTITION2 = new TestStructLike(200, "b");

  @Test
  public void testTaskGroupPlanningByPartition() {
    // When all files belong to the same partition, we should combine them together as long as the
    // total file size is <= split size
    List<PartitionScanTask> tasks =
        ImmutableList.of(
            taskWithPartition(SPEC1, PARTITION1, 64),
            taskWithPartition(SPEC1, PARTITION1, 128),
            taskWithPartition(SPEC1, PARTITION1, 64),
            taskWithPartition(SPEC1, PARTITION1, 128));

    int count = 0;
    for (ScanTaskGroup<PartitionScanTask> task :
        TableScanUtil.planTaskGroups(tasks, 512, 10, 4, SPEC1.partitionType())) {
      assertThat(task.filesCount()).isEqualTo(4);
      assertThat(task.sizeBytes()).isEqualTo(64 + 128 + 64 + 128);
      count += 1;
    }
    assertThat(count).isOne();

    // We have 2 files from partition 1 and 2 files from partition 2, so they should be combined
    // separately
    tasks =
        ImmutableList.of(
            taskWithPartition(SPEC1, PARTITION1, 64),
            taskWithPartition(SPEC1, PARTITION1, 128),
            taskWithPartition(SPEC1, PARTITION2, 64),
            taskWithPartition(SPEC1, PARTITION2, 128));

    count = 0;
    for (ScanTaskGroup<PartitionScanTask> task :
        TableScanUtil.planTaskGroups(tasks, 512, 10, 4, SPEC1.partitionType())) {
      assertThat(task.filesCount()).isEqualTo(2);
      assertThat(task.sizeBytes()).isEqualTo(64 + 128);
      count += 1;
    }
    assertThat(count).isEqualTo(2);

    // Similar to the case above, but now files have different partition specs
    tasks =
        ImmutableList.of(
            taskWithPartition(SPEC1, PARTITION1, 64),
            taskWithPartition(SPEC2, PARTITION1, 128),
            taskWithPartition(SPEC1, PARTITION2, 64),
            taskWithPartition(SPEC2, PARTITION2, 128));

    count = 0;
    for (ScanTaskGroup<PartitionScanTask> task :
        TableScanUtil.planTaskGroups(tasks, 512, 10, 4, SPEC1.partitionType())) {
      assertThat(task.filesCount()).isEqualTo(2);
      assertThat(task.sizeBytes()).isEqualTo(64 + 128);
      count += 1;
    }
    assertThat(count).isEqualTo(2);

    // Combining within partitions should also respect split size. In this case, the split size
    // is equal to the file size, so each partition will have 2 tasks.
    tasks =
        ImmutableList.of(
            taskWithPartition(SPEC1, PARTITION1, 128),
            taskWithPartition(SPEC2, PARTITION1, 128),
            taskWithPartition(SPEC1, PARTITION2, 128),
            taskWithPartition(SPEC2, PARTITION2, 128));

    count = 0;
    for (ScanTaskGroup<PartitionScanTask> task :
        TableScanUtil.planTaskGroups(tasks, 128, 10, 4, SPEC1.partitionType())) {
      assertThat(task.filesCount()).isOne();
      assertThat(task.sizeBytes()).isEqualTo(128);
      count += 1;
    }
    assertThat(count).isEqualTo(4);

    // The following should throw exception since `SPEC2` is not an intersection of partition specs
    // across all tasks.
    List<PartitionScanTask> tasks2 =
        ImmutableList.of(
            taskWithPartition(SPEC1, PARTITION1, 128), taskWithPartition(SPEC2, PARTITION2, 128));

    Assertions.assertThatThrownBy(
            () -> TableScanUtil.planTaskGroups(tasks2, 128, 10, 4, SPEC2.partitionType()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot find field");
  }

  @Test
  public void testAdaptiveSplitSize() {
    long scanSize = 500L * 1024 * 1024 * 1024; // 500 GB
    int parallelism = 500;
    long smallDefaultSplitSize = 128 * 1024 * 1024; // 128 MB
    long largeDefaultSplitSize = 2L * 1024 * 1024 * 1024; // 2 GB

    long adjusted1 = TableScanUtil.adjustSplitSize(scanSize, parallelism, smallDefaultSplitSize);
    assertThat(adjusted1).isEqualTo(smallDefaultSplitSize);

    long adjusted2 = TableScanUtil.adjustSplitSize(scanSize, parallelism, largeDefaultSplitSize);
    assertThat(adjusted2).isEqualTo(scanSize / parallelism);
  }

  private PartitionScanTask taskWithPartition(
      PartitionSpec spec, StructLike partition, long sizeBytes) {
    PartitionScanTask task = Mockito.mock(PartitionScanTask.class);
    Mockito.when(task.spec()).thenReturn(spec);
    Mockito.when(task.partition()).thenReturn(partition);
    Mockito.when(task.sizeBytes()).thenReturn(sizeBytes);
    Mockito.when(task.filesCount()).thenReturn(1);
    return task;
  }

  private static class TestStructLike implements StructLike {
    private final Object[] values;

    TestStructLike(Object... values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("set is not supported");
    }
  }

  private interface ParentTask extends ScanTask {}

  private static class ChildTask1
      implements ParentTask, SplittableScanTask<ChildTask1>, MergeableScanTask<ChildTask1> {
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
