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

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
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
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableScanUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TableScanUtil.class);
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

  /**
   * Produces {@link CombinedScanTask combined tasks} from an iterable of {@link FileScanTask file
   * tasks}, using an adaptive target split size that targets a minimum number of tasks
   * (parallelism).
   *
   * @param files incoming iterable of file tasks
   * @param parallelism target minimum number of tasks
   * @param splitSize target split size
   * @param lookback bin packing lookback
   * @param openFileCost minimum file cost
   * @return an iterable of combined tasks
   */
  public static CloseableIterable<CombinedScanTask> planTasksAdaptive(
      CloseableIterable<FileScanTask> files,
      int parallelism,
      long splitSize,
      int lookback,
      long openFileCost) {

    validatePlanningArguments(splitSize, lookback, openFileCost);

    Function<FileScanTask, Long> weightFunc =
        file ->
            Math.max(
                file.length()
                    + file.deletes().stream().mapToLong(ContentFile::fileSizeInBytes).sum(),
                (1 + file.deletes().size()) * openFileCost);

    return new AdaptiveSplitPlanningIterable<>(
        files,
        parallelism,
        splitSize,
        lookback,
        weightFunc,
        TableScanUtil::splitFiles,
        BaseCombinedScanTask::new);
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

    return planTasksInternal(
        splitFiles, splitSize, lookback, weightFunc, BaseCombinedScanTask::new);
  }

  public static <T extends ScanTask> List<ScanTaskGroup<T>> planTaskGroups(
      List<T> tasks, long splitSize, int lookback, long openFileCost) {
    return Lists.newArrayList(
        planTaskGroups(CloseableIterable.withNoopClose(tasks), splitSize, lookback, openFileCost));
  }

  @SuppressWarnings("unchecked")
  private static <T extends ScanTask>
      BiFunction<CloseableIterable<T>, Long, CloseableIterable<T>> splitFunc() {
    return (tasks, splitSize) ->
        // capture manifests which can be closed after scan planning
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
  }

  private static <T extends ScanTask> Function<T, Long> weightFunc(long openFileCost) {
    return task -> Math.max(task.sizeBytes(), task.filesCount() * openFileCost);
  }

  public static <T extends ScanTask> CloseableIterable<ScanTaskGroup<T>> planTaskGroupsAdaptive(
      CloseableIterable<T> tasks,
      int parallelism,
      long splitSize,
      int lookback,
      long openFileCost) {

    validatePlanningArguments(splitSize, lookback, openFileCost);

    return new AdaptiveSplitPlanningIterable<>(
        tasks,
        parallelism,
        splitSize,
        lookback,
        weightFunc(openFileCost),
        splitFunc(),
        combinedTasks -> new BaseScanTaskGroup<>(mergeTasks(combinedTasks)));
  }

  public static <T extends ScanTask> CloseableIterable<ScanTaskGroup<T>> planTaskGroups(
      CloseableIterable<T> tasks, long splitSize, int lookback, long openFileCost) {

    validatePlanningArguments(splitSize, lookback, openFileCost);

    // capture manifests which can be closed after scan planning
    CloseableIterable<T> splitTasks = TableScanUtil.<T>splitFunc().apply(tasks, splitSize);

    return planTasksInternal(
        splitTasks,
        splitSize,
        lookback,
        weightFunc(openFileCost),
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

  private static void validatePlanningArguments(long splitSize, int lookback, long openFileCost) {
    Preconditions.checkArgument(splitSize > 0, "Split size must be > 0: %s", splitSize);
    Preconditions.checkArgument(lookback > 0, "Split planning lookback must be > 0: %s", lookback);
    Preconditions.checkArgument(openFileCost >= 0, "File open cost must be >= 0: %s", openFileCost);
  }

  private static <T extends ScanTask, G extends ScanTaskGroup<T>>
      CloseableIterable<G> planTasksInternal(
          CloseableIterable<T> splitFiles,
          long splitSize,
          int lookback,
          Function<T, Long> weightFunc,
          Function<List<T>, G> groupFunc) {

    return CloseableIterable.transform(
        CloseableIterable.combine(
            new BinPacking.PackingIterable<>(splitFiles, splitSize, lookback, weightFunc, true),
            splitFiles),
        groupFunc);
  }

  private static class AdaptiveSplitPlanningIterable<T extends ScanTask, G extends ScanTaskGroup<T>>
      extends CloseableGroup implements CloseableIterable<G> {
    private final CloseableIterable<T> files;
    private final int parallelism;
    private final long splitSize;
    private final int lookback;
    private final Function<T, Long> weightFunc;
    private final BiFunction<CloseableIterable<T>, Long, CloseableIterable<T>> splitFunc;
    private final Function<List<T>, G> groupFunc;

    private Long targetSize = null;

    private AdaptiveSplitPlanningIterable(
        CloseableIterable<T> files,
        int parallelism,
        long splitSize,
        int lookback,
        Function<T, Long> weightFunc,
        BiFunction<CloseableIterable<T>, Long, CloseableIterable<T>> splitFunc,
        Function<List<T>, G> groupFunc) {
      this.files = files;
      this.parallelism = parallelism;
      this.splitSize = splitSize;
      this.lookback = lookback;
      this.weightFunc = weightFunc;
      this.splitFunc = splitFunc;
      this.groupFunc = groupFunc;
    }

    @Override
    public CloseableIterator<G> iterator() {
      if (targetSize != null) {
        // target size is already known so plan with the static target size
        CloseableIterable<T> splitTasks = splitFunc.apply(files, targetSize);
        CloseableIterator<G> iter =
            planTasksInternal(splitTasks, targetSize, lookback, weightFunc, groupFunc).iterator();
        addCloseable(iter);
        return iter;
      }

      boolean shouldClose = true;
      CloseableIterator<T> tasksIter = files.iterator();
      try {
        // load tasks until the iterator is exhausted or until the total weight is enough to get the
        // parallelism at the split size passed in.
        Deque<T> readAheadTasks = Lists.newLinkedList();
        long readToSize = parallelism * splitSize;
        long totalSize = 0L;

        while (tasksIter.hasNext()) {
          T task = tasksIter.next();
          readAheadTasks.addLast(task);
          totalSize += weightFunc.apply(task);

          if (totalSize > readToSize) {
            break;
          }
        }

        // if total size was reached, then the requested split size is used. otherwise, the iterator
        // was exhausted and the split size will be adjusted to target parallelism with a reasonable
        // minimum.
        this.targetSize = Math.max(MIN_SPLIT_SIZE, Math.min(totalSize / parallelism, splitSize));

        CloseableIterable<T> tasksToReplay = CloseableIterable.withNoopClose(readAheadTasks);
        CloseableIterable<T> allTasks;
        if (!tasksIter.hasNext()) {
          allTasks = tasksToReplay;

        } else {
          CloseableIterable<T> remainingTasks =
              new CloseableIterable<T>() {
                @Override
                public CloseableIterator<T> iterator() {
                  return tasksIter;
                }

                @Override
                public void close() throws IOException {
                  tasksIter.close();
                }
              };
          allTasks = CloseableIterable.concat(tasksToReplay, remainingTasks);
        }

        CloseableIterable<T> splitTasks = splitFunc.apply(allTasks, targetSize);
        CloseableIterator<G> iter =
            planTasksInternal(splitTasks, targetSize, lookback, weightFunc, groupFunc).iterator();

        // after this point, closing the open iterator is the responsibility of the caller
        addCloseable(iter);
        shouldClose = false;

        return iter;

      } finally {
        if (shouldClose) {
          try {
            tasksIter.close();
          } catch (IOException e) {
            LOG.warn("Failed to close file tasks iterator", e);
          }
        }
      }
    }
  }
}
