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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.PositionDeletesTable;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupInfo;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Groups specified files in the {@link Table} by {@link RewriteFileGroup}s. These will be grouped
 * by partitions.
 */
public class RewritePositionDeletesGroupPlanner
    extends SizeBasedFileRewritePlanner<
        FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup> {
  private static final Logger LOG =
      LoggerFactory.getLogger(RewritePositionDeletesGroupPlanner.class);

  private final Expression filter;
  private final boolean caseSensitive;
  private RewriteJobOrder rewriteJobOrder;

  public RewritePositionDeletesGroupPlanner(Table table) {
    this(table, Expressions.alwaysTrue(), false);
  }

  public RewritePositionDeletesGroupPlanner(Table table, Expression filter, boolean caseSensitive) {
    super(table);
    this.caseSensitive = caseSensitive;
    this.filter = filter;
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(RewriteDataFiles.REWRITE_JOB_ORDER)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.rewriteJobOrder =
        RewriteJobOrder.fromName(
            PropertyUtil.propertyAsString(
                options,
                RewritePositionDeleteFiles.REWRITE_JOB_ORDER,
                RewritePositionDeleteFiles.REWRITE_JOB_ORDER_DEFAULT));
  }

  /**
   * Generates the plan for the current table.
   *
   * @return the generated plan which could be executed during the compaction
   */
  @Override
  public RewritePositionDeletePlan plan() {
    StructLikeMap<List<List<PositionDeletesScanTask>>> plan = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext();
    Stream<RewritePositionDeletesGroup> groups =
        plan.entrySet().stream()
            .filter(e -> !e.getValue().isEmpty())
            .flatMap(
                e -> {
                  StructLike partition = e.getKey();
                  List<List<PositionDeletesScanTask>> scanGroups = e.getValue();
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
            .sorted(FileRewriteGroup.taskComparator(rewriteJobOrder));
    Map<StructLike, Integer> groupsInPartition = plan.transformValues(List::size);
    int totalGroupCount = groupsInPartition.values().stream().reduce(Integer::sum).orElse(0);
    return new RewritePositionDeletePlan(
        groups, totalGroupCount, groupsInPartition, writeMaxFileSize());
  }

  private StructLikeMap<List<List<PositionDeletesScanTask>>> planFileGroups() {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table(), MetadataTableType.POSITION_DELETES);
    CloseableIterable<PositionDeletesScanTask> fileTasks = planFiles(deletesTable);

    try {
      Types.StructType partitionType = Partitioning.partitionType(deletesTable);
      StructLikeMap<List<PositionDeletesScanTask>> fileTasksByPartition =
          groupByPartition(partitionType, fileTasks);
      return fileTasksByPartition.transformValues(
          tasks -> ImmutableList.copyOf(planFileGroups(tasks)));
    } finally {
      try {
        fileTasks.close();
      } catch (IOException io) {
        LOG.error("Cannot properly close file iterable while planning for rewrite", io);
      }
    }
  }

  @Override
  protected Iterable<PositionDeletesScanTask> filterFiles(Iterable<PositionDeletesScanTask> tasks) {
    return Iterables.filter(tasks, this::wronglySized);
  }

  @Override
  protected Iterable<List<PositionDeletesScanTask>> filterFileGroups(
      List<List<PositionDeletesScanTask>> groups) {
    return Iterables.filter(groups, this::shouldRewrite);
  }

  private boolean shouldRewrite(List<PositionDeletesScanTask> group) {
    return enoughInputFiles(group) || enoughContent(group) || tooMuchContent(group);
  }

  @Override
  protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.DELETE_TARGET_FILE_SIZE_BYTES,
        TableProperties.DELETE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }

  private CloseableIterable<PositionDeletesScanTask> planFiles(Table deletesTable) {
    PositionDeletesTable.PositionDeletesBatchScan scan =
        (PositionDeletesTable.PositionDeletesBatchScan) deletesTable.newBatchScan();
    return CloseableIterable.transform(
        scan.baseTableFilter(filter).caseSensitive(caseSensitive).ignoreResiduals().planFiles(),
        PositionDeletesScanTask.class::cast);
  }

  private StructLikeMap<List<PositionDeletesScanTask>> groupByPartition(
      Types.StructType partitionType, Iterable<PositionDeletesScanTask> tasks) {
    StructLikeMap<List<PositionDeletesScanTask>> filesByPartition =
        StructLikeMap.create(partitionType);

    for (PositionDeletesScanTask task : tasks) {
      StructLike coerced = coercePartition(task, partitionType);

      List<PositionDeletesScanTask> partitionTasks = filesByPartition.get(coerced);
      if (partitionTasks == null) {
        partitionTasks = Lists.newArrayList();
      }
      partitionTasks.add(task);
      filesByPartition.put(coerced, partitionTasks);
    }

    return filesByPartition;
  }

  private RewritePositionDeletesGroup newRewriteGroup(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<PositionDeletesScanTask> tasks,
      long splitSize,
      int numOutputSize) {
    ImmutableRewritePositionDeleteFiles.FileGroupInfo info =
        ImmutableRewritePositionDeleteFiles.FileGroupInfo.builder()
            .globalIndex(ctx.currentGlobalIndex())
            .partitionIndex(ctx.currentPartitionIndex(partition))
            .partition(partition)
            .build();
    return new RewritePositionDeletesGroup(
        info, Lists.newArrayList(tasks), splitSize, numOutputSize);
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

  private StructLike coercePartition(PositionDeletesScanTask task, Types.StructType partitionType) {
    return PartitionUtil.coercePartition(partitionType, task.spec(), task.partition());
  }
}
