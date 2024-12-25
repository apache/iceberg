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
import java.io.Serializable;
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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
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
    RewriteExecutionContext ctx = new RewriteExecutionContext(plan);
    Stream<RewriteFileGroup> groups =
        plan.entrySet().stream()
            .filter(e -> e.getValue().size() != 0)
            .flatMap(
                e -> {
                  StructLike partition = e.getKey();
                  List<List<FileScanTask>> scanGroups = e.getValue();
                  return scanGroups.stream().map(tasks -> newRewriteGroup(ctx, partition, tasks));
                })
            .sorted(RewriteFileGroup.comparator(rewriteJobOrder));
    return new RewritePlanResult(ctx, groups);
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
      return fileGroupsByPartition(filesByPartition);
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

      List<FileScanTask> files = filesByPartition.get(taskPartition);
      if (files == null) {
        files = Lists.newArrayList();
      }

      files.add(task);
      filesByPartition.put(taskPartition, files);
    }
    return filesByPartition;
  }

  private StructLikeMap<List<List<FileScanTask>>> fileGroupsByPartition(
      StructLikeMap<List<FileScanTask>> filesByPartition) {
    return filesByPartition.transformValues(this::planFileGroups);
  }

  private List<List<FileScanTask>> planFileGroups(List<FileScanTask> tasks) {
    return ImmutableList.copyOf(rewriter.planFileGroups(tasks));
  }

  private RewriteFileGroup newRewriteGroup(
      RewriteExecutionContext ctx, StructLike partition, List<FileScanTask> tasks) {
    int globalIndex = ctx.currentGlobalIndex();
    int partitionIndex = ctx.currentPartitionIndex(partition);
    RewriteDataFiles.FileGroupInfo info =
        ImmutableRewriteDataFiles.FileGroupInfo.builder()
            .globalIndex(globalIndex)
            .partitionIndex(partitionIndex)
            .partition(partition)
            .build();
    return new RewriteFileGroup(info, Lists.newArrayList(tasks));
  }

  public static class RewriteExecutionContext implements Serializable {
    private final StructLikeMap<Integer> numGroupsByPartition;
    private final int totalGroupCount;
    private final Map<StructLike, Integer> partitionIndexMap;
    private final AtomicInteger groupIndex;

    @VisibleForTesting
    RewriteExecutionContext(StructLikeMap<List<List<FileScanTask>>> fileGroupsByPartition) {
      this.numGroupsByPartition = fileGroupsByPartition.transformValues(List::size);
      this.totalGroupCount = numGroupsByPartition.values().stream().reduce(Integer::sum).orElse(0);
      this.partitionIndexMap = Maps.newConcurrentMap();
      this.groupIndex = new AtomicInteger(1);
    }

    public int currentGlobalIndex() {
      return groupIndex.getAndIncrement();
    }

    public int currentPartitionIndex(StructLike partition) {
      return partitionIndexMap.merge(partition, 1, Integer::sum);
    }

    public int groupsInPartition(StructLike partition) {
      return numGroupsByPartition.get(partition);
    }

    public int totalGroupCount() {
      return totalGroupCount;
    }
  }

  public static class RewritePlanResult {
    private RewriteExecutionContext context;
    private Stream<RewriteFileGroup> fileGroups;

    public RewritePlanResult(RewriteExecutionContext context, Stream<RewriteFileGroup> fileGroups) {
      this.context = context;
      this.fileGroups = fileGroups;
    }

    public RewriteExecutionContext context() {
      return context;
    }

    public Stream<RewriteFileGroup> fileGroups() {
      return fileGroups;
    }
  }
}
