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

package org.apache.iceberg.flink.sink.compact;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.CommonControllerMessage;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.CompactionUnit;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.EndCheckpoint;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.EndCompaction;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SmallFileUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;

public class CompactFileGenerator extends AbstractStreamOperator<CommonControllerMessage>
    implements OneInputStreamOperator<EndCheckpoint, CommonControllerMessage>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(CompactFileGenerator.class);

  private long startingSnapshotId;
  private final TableLoader tableLoader;
  private transient Table table;
  private boolean caseSensitive;
  private PartitionSpec spec;
  private Expression filter;
  private long targetSizeInBytes;
  private int splitLookback;
  private long splitOpenFileCost;

  public CompactFileGenerator(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();

    this.spec = table.spec();
    this.filter = Expressions.alwaysTrue();
    this.caseSensitive = false;

    long splitSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_SIZE,
        TableProperties.SPLIT_SIZE_DEFAULT);
    long targetFileSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
    long targetMergeFileSize = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.WRITE_FLINK_COMPACT_TARGET_FILE_SIZE_BYTES,
        targetFileSize);
    this.targetSizeInBytes = Math.min(splitSize, targetMergeFileSize);
    this.splitLookback = PropertyUtil.propertyAsInt(
        table.properties(),
        TableProperties.SPLIT_LOOKBACK,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);
    this.splitOpenFileCost = PropertyUtil.propertyAsLong(
        table.properties(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
  }

  @Override
  public void processElement(StreamRecord<EndCheckpoint> element) throws Exception {
    EndCheckpoint endCheckpoint = element.getValue();
    LOG.info("Received an EndCheckpoint {}, begin to compute CompactionUnit", endCheckpoint.getCheckpointId());
    if (SmallFileUtil.shouldMergeSmallFiles(table)) {
      table.refresh();
      AtomicInteger index = new AtomicInteger();
      List<CompactionUnit> tasks = getCombinedScanTasks()
          .stream()
          .map(combineTask -> new CompactionUnit(combineTask, index.getAndIncrement()))
          .collect(Collectors.toList());

      if (!tasks.isEmpty()) {
        tasks.forEach(task -> {
          emit(task);
          LOG.info(
              "Emit CompactionUnit(id: {}, files: {}, total: {}, checkpoint: {})",
              task.getUnitId(),
              Joiner.on(", ").join(task.getCombinedScanTask().files().stream()
                  .map(t -> t.file().content() + "=>" + t.file().fileSizeInBytes() / 1024 / 1024 + "M")
                  .collect(Collectors.toList())),
              task.getCombinedScanTask().files().size(),
              endCheckpoint.getCheckpointId());
        });
        LOG.info("Summary: (checkpoint: {}, total CompactionUnit: {})", endCheckpoint.getCheckpointId(), tasks.size());

        // broadcast emit checkpoint barrier
        emit(new EndCompaction(endCheckpoint.getCheckpointId(), startingSnapshotId));
      } else {
        LOG.warn("CombinedScanTasks is empty, no files need to be compacted");
      }
    }
  }

  private List<CombinedScanTask> getCombinedScanTasks() {
    CloseableIterable<FileScanTask> fileScanTasks = null;
    if (table.currentSnapshot() == null) {
      return emptyList();
    }

    long startingSnapshot = table.currentSnapshot().snapshotId();
    try {
      fileScanTasks = table.newScan()
          .useSnapshot(startingSnapshot)
          .caseSensitive(caseSensitive)
          .ignoreResiduals()
          .filter(filter)
          .planFiles();
    } finally {
      try {
        if (fileScanTasks != null) {
          fileScanTasks.close();
        }
      } catch (IOException ioe) {
        LOG.warn("Failed to close task iterable", ioe);
      }
    }

    Map<StructLikeWrapper, Collection<FileScanTask>> groupedTasks = groupTasksByPartition(fileScanTasks.iterator());
    Map<StructLikeWrapper, Collection<FileScanTask>> filteredGroupedTasks = groupedTasks.entrySet().stream()
        .filter(kv -> kv.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Nothing to rewrite if there's only one DataFile in each partition.
    if (filteredGroupedTasks.isEmpty()) {
      return emptyList();
    }
    // Split and combine tasks under each partition
    List<CombinedScanTask> combinedScanTasks = filteredGroupedTasks.values().stream()
        .map(scanTasks -> {
          CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
              CloseableIterable.withNoopClose(scanTasks), targetSizeInBytes);
          return TableScanUtil.planTasks(splitTasks, targetSizeInBytes, splitLookback, splitOpenFileCost);
        })
        .flatMap(Streams::stream)
        .filter(task -> task.files().size() > 1 || isPartialFileScan(task))
        .collect(Collectors.toList());

    this.startingSnapshotId = startingSnapshot;
    return combinedScanTasks;
  }

  private Map<StructLikeWrapper, Collection<FileScanTask>> groupTasksByPartition(
      CloseableIterator<FileScanTask> tasksIter) {
    ListMultimap<StructLikeWrapper, FileScanTask> tasksGroupedByPartition = Multimaps.newListMultimap(
        Maps.newHashMap(), Lists::newArrayList);
    try (CloseableIterator<FileScanTask> iterator = tasksIter) {
      iterator.forEachRemaining(task -> {
        StructLikeWrapper structLike = StructLikeWrapper.forType(spec.partitionType()).set(task.file().partition());
        tasksGroupedByPartition.put(structLike, task);
      });
    } catch (IOException e) {
      LOG.warn("Failed to close task iterator", e);
    }
    return tasksGroupedByPartition.asMap();
  }

  //  Determine whether the file is part of the file, for MR: fix data loss in compact action (#2196)
  private boolean isPartialFileScan(CombinedScanTask task) {
    if (task.files().size() == 1) {
      FileScanTask fileScanTask = task.files().iterator().next();
      return fileScanTask.file().fileSizeInBytes() != fileScanTask.length();
    } else {
      return false;
    }
  }

  /**
   * It is notified that no more data will arrive on the input.
   */
  @Override
  public void endInput() throws Exception {
  }

  @Override
  public void dispose() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

  private void emit(CommonControllerMessage result) {
    output.collect(new StreamRecord<>(result));
  }
}
