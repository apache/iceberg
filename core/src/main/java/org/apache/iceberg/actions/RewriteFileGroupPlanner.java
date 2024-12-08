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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks the files in the table, and using the {@link FileRewriter} plans the groups for
 * compaction.
 */
public class RewriteFileGroupPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteFileGroupPlanner.class);

  private final FileRewriter<FileScanTask, DataFile> rewriter;
  private final RewriteJobOrder rewriteJobOrder;

  public RewriteFileGroupPlanner(
      FileRewriter<FileScanTask, DataFile> rewriter, RewriteJobOrder rewriteJobOrder) {
    this.rewriter = rewriter;
    this.rewriteJobOrder = rewriteJobOrder;
  }

  public RewritePlanResult plan(
      Table table, Expression filter, long startingSnapshotId, boolean caseSensitive) {
    StructLikeMap<List<List<FileScanTask>>> plan =
        planFileGroups(table, filter, startingSnapshotId, caseSensitive);
    RewriteExecutionContext ctx = new RewriteExecutionContext();
    Stream<RewriteFileGroup> groups =
        plan.entrySet().stream()
            .filter(e -> !e.getValue().isEmpty())
            .flatMap(
                e -> {
                  StructLike partition = e.getKey();
                  List<List<FileScanTask>> scanGroups = e.getValue();
                  return scanGroups.stream().map(tasks -> newRewriteGroup(ctx, partition, tasks));
                })
            .sorted(RewriteFileGroup.comparator(rewriteJobOrder));
    Map<StructLike, Integer> groupsInPartition = plan.transformValues(List::size);
    int totalGroupCount = groupsInPartition.values().stream().reduce(Integer::sum).orElse(0);
    return new RewritePlanResult(groups, totalGroupCount, groupsInPartition);
  }

  private StructLikeMap<List<List<FileScanTask>>> planFileGroups(
      Table table, Expression filter, long startingSnapshotId, boolean caseSensitive) {
    CloseableIterable<FileScanTask> fileScanTasks =
        table
            .newScan()
            .useSnapshot(startingSnapshotId)
            .caseSensitive(caseSensitive)
            .filter(filter)
            .ignoreResiduals()
            .planFiles();

    try {
      Types.StructType partitionType = table.spec().partitionType();
      StructLikeMap<List<FileScanTask>> filesByPartition =
          groupByPartition(table, partitionType, fileScanTasks);
      return filesByPartition.transformValues(
          tasks -> ImmutableList.copyOf(rewriter.planFileGroups(tasks)));
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
      RewriteExecutionContext ctx, StructLike partition, List<FileScanTask> tasks) {
    RewriteDataFiles.FileGroupInfo info =
        ImmutableRewriteDataFiles.FileGroupInfo.builder()
            .globalIndex(ctx.currentGlobalIndex())
            .partitionIndex(ctx.currentPartitionIndex(partition))
            .partition(partition)
            .build();
    return new RewriteFileGroup(info, Lists.newArrayList(tasks));
  }

  public static class RewritePlanResult {
    private final Stream<RewriteFileGroup> groups;
    private final int totalGroupCount;
    private final Map<StructLike, Integer> groupsInPartition;

    private RewritePlanResult(
        Stream<RewriteFileGroup> groups,
        int totalGroupCount,
        Map<StructLike, Integer> groupsInPartition) {
      this.groups = groups;
      this.totalGroupCount = totalGroupCount;
      this.groupsInPartition = groupsInPartition;
    }

    public Stream<RewriteFileGroup> groups() {
      return groups;
    }

    public int groupsInPartition(StructLike partition) {
      return groupsInPartition.get(partition);
    }

    public int totalGroupCount() {
      return totalGroupCount;
    }
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
