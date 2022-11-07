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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MergeableScanTask;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.SplittableScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.types.Types;

public class TableScanUtil {

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
    Preconditions.checkArgument(splitSize > 0, "Invalid split size (negative or 0): %s", splitSize);

    Iterable<FileScanTask> splitTasks =
        FluentIterable.from(tasks).transformAndConcat(input -> input.split(splitSize));
    // Capture manifests which can be closed after scan planning
    return CloseableIterable.combine(splitTasks, tasks);
  }

  public static CloseableIterable<CombinedScanTask> planTasks(
      CloseableIterable<FileScanTask> splitFiles, long splitSize, int lookback, long openFileCost) {
    Preconditions.checkArgument(splitSize > 0, "Invalid split size (negative or 0): %s", splitSize);
    Preconditions.checkArgument(
        lookback > 0, "Invalid split planning lookback (negative or 0): %s", lookback);
    Preconditions.checkArgument(
        openFileCost >= 0, "Invalid file open cost (negative): %s", openFileCost);

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

  @SuppressWarnings("unchecked")
  public static <T extends ScanTask> CloseableIterable<ScanTaskGroup<T>> planTaskGroups(
      CloseableIterable<T> tasks, long splitSize, int lookback, long openFileCost) {

    Preconditions.checkArgument(splitSize > 0, "Invalid split size (negative or 0): %s", splitSize);
    Preconditions.checkArgument(
        lookback > 0, "Invalid split planning lookback (negative or 0): %s", lookback);
    Preconditions.checkArgument(
        openFileCost >= 0, "Invalid file open cost (negative): %s", openFileCost);

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
      Types.StructType projectedPartitionType) {

    Preconditions.checkArgument(splitSize > 0, "Invalid split size (negative or 0): %s", splitSize);
    Preconditions.checkArgument(
        lookback > 0, "Invalid split planning lookback (negative or 0): %s", lookback);
    Preconditions.checkArgument(
        openFileCost >= 0, "Invalid file open cost (negative): %s", openFileCost);

    List<T> splitTasks = Lists.newArrayList();
    for (T task : tasks) {
      if (task instanceof SplittableScanTask<?>) {
        ((SplittableScanTask<? extends T>) task).split(splitSize).forEach(splitTasks::add);
      } else {
        splitTasks.add(task);
      }
    }

    Function<T, Long> weightFunc =
        task -> Math.max(task.sizeBytes(), task.filesCount() * openFileCost);

    StructLikeWrapper wrapper = StructLikeWrapper.forType(projectedPartitionType);
    Map<Integer, StructProjection> projectionsBySpec = Maps.newHashMap();

    // Group tasks by their partition values
    ListMultimap<StructLikeWrapper, T> groupedTasks =
        Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);

    for (T task : splitTasks) {
      PartitionSpec spec = task.spec();
      projectionsBySpec.computeIfAbsent(
          spec.specId(),
          specId -> StructProjection.create(spec.partitionType(), projectedPartitionType));
      StructProjection projectedStruct = projectionsBySpec.get(spec.specId());
      StructLikeWrapper wrapperCopy =
          wrapper.copyFor(projectedStruct.copy().wrap(task.partition()));
      groupedTasks.put(wrapperCopy, task);
    }

    // Now apply task combining within each partition
    List<List<BaseScanTaskGroup<T>>> nestedTaskGroups =
        groupedTasks.asMap().values().stream()
            .map(
                t -> {
                  List<BaseScanTaskGroup<T>> taskGroups = Lists.newArrayList();
                  new BinPacking.PackingIterable<>(
                          CloseableIterable.withNoopClose(t), splitSize, lookback, weightFunc, true)
                      .forEach(ts -> taskGroups.add(new BaseScanTaskGroup<>(mergeTasks(ts))));
                  return taskGroups;
                })
            .collect(Collectors.toList());

    return nestedTaskGroups.stream().flatMap(Collection::stream).collect(Collectors.toList());
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
}
