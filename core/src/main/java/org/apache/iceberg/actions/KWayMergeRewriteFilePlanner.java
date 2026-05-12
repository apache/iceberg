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
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A planner for K-way merge rewriting that selects files based on size thresholds and orders them
 * by sort-key lower bounds before bin-packing into groups.
 *
 * <p>File selection uses the same size-based criteria as other strategies: files outside the
 * desired size range (too small or too large) are candidates for rewriting. This ensures K-way
 * merge compacts fragmented files within a partition without shuffling, preserving the existing
 * sort order.
 *
 * <p>Files are sorted by their lower bounds on the first sort field before bin-packing so that
 * adjacent files in key space are packed into the same group, minimizing cross-group overlap.
 */
public class KWayMergeRewriteFilePlanner
    extends SizeBasedFileRewritePlanner<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> {

  private static final Logger LOG = LoggerFactory.getLogger(KWayMergeRewriteFilePlanner.class);

  private final Expression filter;
  private final Long snapshotId;
  private final boolean caseSensitive;

  private RewriteJobOrder rewriteJobOrder;

  public KWayMergeRewriteFilePlanner(Table table) {
    this(table, Expressions.alwaysTrue(), null, false);
  }

  public KWayMergeRewriteFilePlanner(Table table, Expression filter) {
    this(
        table,
        filter,
        table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null,
        false);
  }

  public KWayMergeRewriteFilePlanner(
      Table table, Expression filter, Long snapshotId, boolean caseSensitive) {
    super(table);
    this.filter = filter;
    this.snapshotId = snapshotId;
    this.caseSensitive = caseSensitive;
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
                RewriteDataFiles.REWRITE_JOB_ORDER,
                RewriteDataFiles.REWRITE_JOB_ORDER_DEFAULT));
  }

  @Override
  protected long defaultTargetFileSize() {
    return PropertyUtil.propertyAsLong(
        table().properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
  }

  @Override
  protected Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
    return Iterables.filter(tasks, this::outsideDesiredFileSizeRange);
  }

  @Override
  protected Iterable<List<FileScanTask>> filterFileGroups(List<List<FileScanTask>> groups) {
    return Iterables.filter(
        groups, group -> enoughInputFiles(group) || enoughContent(group) || tooMuchContent(group));
  }

  @Override
  protected Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> tasks) {
    SortOrder sortOrder = table().sortOrder();
    Preconditions.checkArgument(
        !sortOrder.isUnsorted(),
        "K-way merge requires a table sort order, but table %s is unsorted. "
            + "Use the SORT strategy to sort the files first.",
        table().name());

    int sortFieldId = sortOrder.fields().get(0).sourceId();
    Types.NestedField sortField = table().schema().findField(sortFieldId);
    Type.PrimitiveType sortFieldType = sortField.type().asPrimitiveType();
    Comparator<Object> valueComparator = Comparators.forType(sortFieldType);

    List<FileScanTask> taskList = Lists.newArrayList(tasks);

    taskList.sort(
        Comparator.comparing(
            (FileScanTask task) -> {
              Map<Integer, ByteBuffer> lowerBounds = task.file().lowerBounds();
              if (lowerBounds == null || !lowerBounds.containsKey(sortFieldId)) {
                return null;
              }
              return Conversions.fromByteBuffer(sortFieldType, lowerBounds.get(sortFieldId));
            },
            Comparator.nullsLast(valueComparator)));

    return super.planFileGroups(taskList);
  }

  @Override
  public FileRewritePlan<FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup> plan() {
    StructLikeMap<List<List<FileScanTask>>> groupsByPartition = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext();
    List<RewriteFileGroup> selectedFileGroups = Lists.newArrayList();

    groupsByPartition.entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .forEach(
            entry -> {
              StructLike partition = entry.getKey();
              entry
                  .getValue()
                  .forEach(
                      fileScanTasks -> {
                        long inputSize = inputSize(fileScanTasks);
                        selectedFileGroups.add(
                            newRewriteGroup(
                                ctx,
                                partition,
                                fileScanTasks,
                                inputSplitSize(inputSize),
                                expectedOutputFiles(inputSize)));
                      });
            });

    Map<StructLike, Integer> groupsInPartition = groupsByPartition.transformValues(List::size);
    int totalGroupCount = groupsInPartition.values().stream().reduce(Integer::sum).orElse(0);
    return new FileRewritePlan<>(
        CloseableIterable.of(
            selectedFileGroups.stream()
                .sorted(RewriteFileGroup.comparator(rewriteJobOrder))
                .collect(Collectors.toList())),
        totalGroupCount,
        groupsInPartition);
  }

  private StructLikeMap<List<List<FileScanTask>>> planFileGroups() {
    TableScan scan =
        table()
            .newScan()
            .filter(filter)
            .caseSensitive(caseSensitive)
            .ignoreResiduals()
            .includeColumnStats();

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();

    try {
      Types.StructType partitionType = table().spec().partitionType();
      StructLikeMap<List<FileScanTask>> filesByPartition =
          groupByPartition(partitionType, fileScanTasks);
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
      Types.StructType partitionType, Iterable<FileScanTask> tasks) {
    StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
    StructLike emptyStruct = GenericRecord.create(partitionType);

    for (FileScanTask task : tasks) {
      StructLike taskPartition =
          task.file().specId() == table().spec().specId() ? task.file().partition() : emptyStruct;
      filesByPartition.computeIfAbsent(taskPartition, unused -> Lists.newArrayList()).add(task);
    }

    return filesByPartition;
  }

  private RewriteFileGroup newRewriteGroup(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> tasks,
      long inputSplitSize,
      int expectedOutputFiles) {
    FileGroupInfo info =
        ImmutableRewriteDataFiles.FileGroupInfo.builder()
            .globalIndex(ctx.currentGlobalIndex())
            .partitionIndex(ctx.currentPartitionIndex(partition))
            .partition(partition)
            .build();
    return new RewriteFileGroup(
        info,
        Lists.newArrayList(tasks),
        outputSpecId(),
        writeMaxFileSize(),
        inputSplitSize,
        expectedOutputFiles);
  }
}
