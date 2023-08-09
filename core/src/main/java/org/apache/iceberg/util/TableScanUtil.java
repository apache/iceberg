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

import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MergeableScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.SplittableScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.math.LongMath;
import org.apache.iceberg.types.Types;

public class TableScanUtil {

  private static final long MIN_SPLIT_SIZE = 16 * 1024 * 1024; // 16 MB

  private TableScanUtil() {}

  public static boolean hasDeletes(CombinedScanTask task) {
    return task.files().stream().anyMatch(TableScanUtil::hasDeletes);
  }

  /**
   * This is temporarily introduced since we plan to support pos-delete vectorized read first, then
   * get to the equality-delete support. We will remove this method once both are supported.
   */
  public static boolean hasEqDeletes(CombinedScanTask task) {
    return task.files().stream()
        .anyMatch(
            t ->
                t.deletes().stream()
                    .anyMatch(
                        deleteFile -> deleteFile.content().equals(FileContent.EQUALITY_DELETES)));
  }

  public static boolean hasDeletes(FileScanTask task) {
    return !task.deletes().isEmpty();
  }

  public static CloseableIterable<FileScanTask> splitFiles(
      CloseableIterable<FileScanTask> tasks, long splitSize) {
    Preconditions.checkArgument(splitSize > 0, "Split size must be > 0: %s", splitSize);

    Iterable<FileScanTask> splitTasks =
        FluentIterable.from(tasks).transformAndConcat(input -> input.split(splitSize));
    // Capture manifests which can be closed after scan planning
    return CloseableIterable.combine(splitTasks, tasks);
  }

  public static CloseableIterable<CombinedScanTask> planTasks(
      CloseableIterable<FileScanTask> splitFiles, long splitSize, int lookback, long openFileCost) {

    validatePlanningArguments(splitSize, lookback, openFileCost);

    // Check the size of delete file as well to avoid unbalanced bin-packing
    Function<FileScanTask, Long> weightFunc =
        file ->
            Math.max(
                file.length()
                    + file.deletes().stream().mapToLong(ContentFile::fileSizeInBytes).sum(),
                (1 + file.deletes().size()) * openFileCost);

    return CloseableIterable.transform(
        CloseableIterable.combine(
            new BinPacking.PackingIterable<>(splitFiles, splitSize, lookback, weightFunc, true),
            splitFiles),
        BaseCombinedScanTask::new);
  }

  public static <T extends ScanTask> List<ScanTaskGroup<T>> planTaskGroups(
      List<T> tasks, long splitSize, int lookback, long openFileCost) {
    return Lists.newArrayList(
        planTaskGroups(CloseableIterable.withNoopClose(tasks), splitSize, lookback, openFileCost));
  }

  @SuppressWarnings("unchecked")
  public static <T extends ScanTask> CloseableIterable<ScanTaskGroup<T>> planTaskGroups(
      CloseableIterable<T> tasks, long splitSize, int lookback, long openFileCost) {

    validatePlanningArguments(splitSize, lookback, openFileCost);

    // capture manifests which can be closed after scan planning
    CloseableIterable<T> splitTasks =
        CloseableIterable.combine(
            FluentIterable.from(tasks)
                .transformAndConcat(
                    task -> {
                      if (task instanceof SplittableScanTask<?>) {
                        return ((SplittableScanTask<? extends T>) task).split(splitSize);
                      } else {
                        return ImmutableList.of(task);
                      }
                    }),
            tasks);

    Function<T, Long> weightFunc =
        task -> Math.max(task.sizeBytes(), task.filesCount() * openFileCost);

    return CloseableIterable.transform(
        CloseableIterable.combine(
            new BinPacking.PackingIterable<>(splitTasks, splitSize, lookback, weightFunc, true),
            splitTasks),
        combinedTasks -> new BaseScanTaskGroup<>(mergeTasks(combinedTasks)));
  }

  @SuppressWarnings("unchecked")
  public static <T extends PartitionScanTask> List<ScanTaskGroup<T>> planTaskGroups(
      List<T> tasks,
      long splitSize,
      int lookback,
      long openFileCost,
      Types.StructType groupingKeyType) {

    validatePlanningArguments(splitSize, lookback, openFileCost);

    Function<T, Long> weightFunc =
        task -> Math.max(task.sizeBytes(), task.filesCount() * openFileCost);

    Map<Integer, StructProjection> groupingKeyProjectionsBySpec = Maps.newHashMap();

    // group tasks by grouping keys derived from their partition tuples
    StructLikeMap<List<T>> tasksByGroupingKey = StructLikeMap.create(groupingKeyType);

    for (T task : tasks) {
      PartitionSpec spec = task.spec();
      StructLike partition = task.partition();
      StructProjection groupingKeyProjection =
          groupingKeyProjectionsBySpec.computeIfAbsent(
              spec.specId(),
              specId -> StructProjection.create(spec.partitionType(), groupingKeyType));
      List<T> groupingKeyTasks =
          tasksByGroupingKey.computeIfAbsent(
              projectGroupingKey(groupingKeyProjection, groupingKeyType, partition),
              groupingKey -> Lists.newArrayList());
      if (task instanceof SplittableScanTask<?>) {
        ((SplittableScanTask<? extends T>) task).split(splitSize).forEach(groupingKeyTasks::add);
      } else {
        groupingKeyTasks.add(task);
      }
    }

    List<ScanTaskGroup<T>> taskGroups = Lists.newArrayList();

    for (Map.Entry<StructLike, List<T>> entry : tasksByGroupingKey.entrySet()) {
      StructLike groupingKey = entry.getKey();
      List<T> groupingKeyTasks = entry.getValue();
      Iterables.addAll(
          taskGroups,
          toTaskGroupIterable(groupingKey, groupingKeyTasks, splitSize, lookback, weightFunc));
    }

    return taskGroups;
  }

  private static StructLike projectGroupingKey(
      StructProjection groupingKeyProjection,
      Types.StructType groupingKeyType,
      StructLike partition) {

    PartitionData groupingKey = new PartitionData(groupingKeyType);

    groupingKeyProjection.wrap(partition);

    for (int pos = 0; pos < groupingKeyProjection.size(); pos++) {
      Class<?> javaClass = groupingKey.getType(pos).typeId().javaClass();
      groupingKey.set(pos, groupingKeyProjection.get(pos, javaClass));
    }

    return groupingKey;
  }

  private static <T extends ScanTask> Iterable<ScanTaskGroup<T>> toTaskGroupIterable(
      StructLike groupingKey,
      Iterable<T> tasks,
      long splitSize,
      int lookback,
      Function<T, Long> weightFunc) {

    return Iterables.transform(
        new BinPacking.PackingIterable<>(tasks, splitSize, lookback, weightFunc, true),
        combinedTasks -> new BaseScanTaskGroup<>(groupingKey, mergeTasks(combinedTasks)));
  }

  @SuppressWarnings("unchecked")
  public static <T extends ScanTask> List<T> mergeTasks(List<T> tasks) {
    List<T> mergedTasks = Lists.newArrayList();

    T lastTask = null;

    for (T task : tasks) {
      if (lastTask != null) {
        if (lastTask instanceof MergeableScanTask<?>) {
          MergeableScanTask<? extends T> mergeableLastTask =
              (MergeableScanTask<? extends T>) lastTask;
          if (mergeableLastTask.canMerge(task)) {
            lastTask = mergeableLastTask.merge(task);
          } else {
            mergedTasks.add(lastTask);
            lastTask = task;
          }
        } else {
          mergedTasks.add(lastTask);
          lastTask = task;
        }
      } else {
        lastTask = task;
      }
    }

    if (lastTask != null) {
      mergedTasks.add(lastTask);
    }

    return mergedTasks;
  }

  public static long adjustSplitSize(long scanSize, int parallelism, long splitSize) {
    // use the configured split size if it produces at least one split per slot
    // otherwise, adjust the split size to target parallelism with a reasonable minimum
    // increasing the split size may cause expensive spills and is not done automatically
    long splitCount = LongMath.divide(scanSize, splitSize, RoundingMode.CEILING);
    long adjustedSplitSize = Math.max(scanSize / parallelism, Math.min(MIN_SPLIT_SIZE, splitSize));
    return splitCount < parallelism ? adjustedSplitSize : splitSize;
  }

  private static void validatePlanningArguments(long splitSize, int lookback, long openFileCost) {
    Preconditions.checkArgument(splitSize > 0, "Split size must be > 0: %s", splitSize);
    Preconditions.checkArgument(lookback > 0, "Split planning lookback must be > 0: %s", lookback);
    Preconditions.checkArgument(openFileCost >= 0, "File open cost must be >= 0: %s", openFileCost);
  }
}
