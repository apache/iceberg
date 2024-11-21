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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Groups specified files in the {@link Table} by {@link RewriteFileGroup}s. These will be grouped
 * by partitions.
 */
public class RewriteFileGroupPlanner
    extends SizeBasedFileRewritePlanner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {
  /**
   * The minimum number of deletes that needs to be associated with a data file for it to be
   * considered for rewriting. If a data file has this number of deletes or more, it will be
   * rewritten regardless of its file size determined by {@link #MIN_FILE_SIZE_BYTES} and {@link
   * #MAX_FILE_SIZE_BYTES}. If a file group contains a file that satisfies this condition, the file
   * group will be rewritten regardless of the number of files in the file group determined by
   * {@link #MIN_INPUT_FILES}.
   *
   * <p>Defaults to Integer.MAX_VALUE, which means this feature is not enabled by default.
   */
  public static final String DELETE_FILE_THRESHOLD = "delete-file-threshold";

  public static final int DELETE_FILE_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

  private static final Logger LOG = LoggerFactory.getLogger(RewriteFileGroupPlanner.class);

  private final Expression filter;
  private final long snapshotId;
  private final boolean caseSensitive;

  private int deleteFileThreshold;
  private RewriteJobOrder rewriteJobOrder;

  public RewriteFileGroupPlanner(
      Table table, Expression filter, long snapshotId, boolean caseSensitive) {
    super(table);
    this.filter = filter;
    this.snapshotId = snapshotId;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(DELETE_FILE_THRESHOLD)
        .add(RewriteDataFiles.REWRITE_JOB_ORDER)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.deleteFileThreshold = deleteFileThreshold(options);
    this.rewriteJobOrder =
        RewriteJobOrder.fromName(
            PropertyUtil.propertyAsString(
                options,
                RewriteDataFiles.REWRITE_JOB_ORDER,
                RewriteDataFiles.REWRITE_JOB_ORDER_DEFAULT));
  }

  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(tasks, task -> wronglySized(task) || tooManyDeletes(task));
  }

  @Override
  protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    return Iterables.filter(groups, this::shouldRewrite);
  }

  @Override
  protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }

  /**
   * Generates the plan for the current table.
   *
   * @return the generated plan which could be executed during the compaction
   */
  @Override
  public FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan() {
    StructLikeMap<List<List<FileScanTask>>> plan = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext();
    Stream<RewriteFileGroup> groups =
        plan.entrySet().stream()
            .filter(e -> !e.getValue().isEmpty())
            .flatMap(
                e -> {
                  StructLike partition = e.getKey();
                  List<List<FileScanTask>> scanGroups = e.getValue();
                  return scanGroups.stream()
                      .map(
                          tasks -> {
                            long inputSize = inputSize(tasks);
                            return newRewriteGroup(
                                ctx,
                                partition,
                                tasks,
                                splitSize(inputSize),
                                numOutputFiles(inputSize));
                          });
                })
            .sorted(RewriteFileGroup.comparator(rewriteJobOrder));
    Map<StructLike, Integer> groupsInPartition = plan.transformValues(List::size);
    int totalGroupCount = groupsInPartition.values().stream().reduce(Integer::sum).orElse(0);
    return new FileRewritePlan<>(
        groups, totalGroupCount, groupsInPartition, writeMaxFileSize(), outputSpecId());
  }

  @VisibleForTesting
  CloseableIterable<FileScanTask> tasks() {
    return table()
        .newScan()
        .useSnapshot(snapshotId)
        .caseSensitive(caseSensitive)
        .filter(filter)
        .ignoreResiduals()
        .planFiles();
  }

  private int deleteFileThreshold(Map<String, String> options) {
    int value =
        PropertyUtil.propertyAsInt(options, DELETE_FILE_THRESHOLD, DELETE_FILE_THRESHOLD_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", DELETE_FILE_THRESHOLD, value);
    return value;
  }

  private boolean tooManyDeletes(FileScanTask task) {
    return task.deletes() != null && task.deletes().size() >= deleteFileThreshold;
  }

  private boolean shouldRewrite(List<FileScanTask> group) {
    return enoughInputFiles(group)
        || enoughContent(group)
        || tooMuchContent(group)
        || anyTaskHasTooManyDeletes(group);
  }

  private boolean anyTaskHasTooManyDeletes(List<FileScanTask> group) {
    return group.stream().anyMatch(this::tooManyDeletes);
  }

  private StructLikeMap<List<List<FileScanTask>>> planFileGroups() {
    CloseableIterable<FileScanTask> fileScanTasks = tasks();

    try {
      Types.StructType partitionType = table().spec().partitionType();
      StructLikeMap<List<FileScanTask>> filesByPartition =
          groupByPartition(table(), partitionType, fileScanTasks);
      return filesByPartition.transformValues(tasks -> ImmutableList.copyOf(planFileGroups(tasks)));
    } finally {
      try {
        fileScanTasks.close();
      } catch (IOException io) {
        LOG.error("Cannot properly close file iterable while planning for rewrite", io);
      }
    }
  }

  private StructLikeMap<List<FileScanTask>> groupByPartition(
      Table table, Types.StructType partitionType, Iterable<FileScanTask> tasks) {
    StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
    StructLike emptyStruct = GenericRecord.create(partitionType);

    for (FileScanTask task : tasks) {
      // If a task uses an incompatible partition spec the data inside could contain values
      // which belong to multiple partitions in the current spec. Treating all such files as
      // un-partitioned and grouping them together helps to minimize new files made.
      StructLike taskPartition =
          task.file().specId() == table.spec().specId() ? task.file().partition() : emptyStruct;

      filesByPartition.computeIfAbsent(taskPartition, unused -> Lists.newArrayList()).add(task);
    }

    return filesByPartition;
  }

  private RewriteFileGroup newRewriteGroup(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> tasks,
      long splitSize,
      int numOutputSize) {
    FileGroupInfo info =
        ImmutableRewriteDataFiles.FileGroupInfo.builder()
            .globalIndex(ctx.currentGlobalIndex())
            .partitionIndex(ctx.currentPartitionIndex(partition))
            .partition(partition)
            .build();
    return new RewriteFileGroup(info, Lists.newArrayList(tasks), splitSize, numOutputSize);
  }

  private static class RewriteExecutionContext {
    private final Map<StructLike, Integer> partitionIndexMap;
    private final AtomicInteger groupIndex;

    private RewriteExecutionContext() {
      this.partitionIndexMap = Maps.newConcurrentMap();
      this.groupIndex = new AtomicInteger(1);
    }

    private int currentGlobalIndex() {
      return groupIndex.getAndIncrement();
    }

    private int currentPartitionIndex(StructLike partition) {
      return partitionIndexMap.merge(partition, 1, Integer::sum);
    }
  }
}
