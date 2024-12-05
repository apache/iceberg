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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager.CommitService;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits the rewrite changes using {@link RewriteDataFilesCommitManager}. The input is a {@link
 * DataFileRewriteExecutor.ExecutedGroup}. Only {@link Watermark} is emitted which is chained to
 * {@link TaskResultAggregator} input 1.
 */
@Internal
public class DataFileRewriteCommitter extends AbstractStreamOperator<Trigger>
    implements OneInputStreamOperator<DataFileRewriteExecutor.ExecutedGroup, Trigger> {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileRewriteCommitter.class);

  private final String tableName;
  private final String taskName;
  private final int taskIndex;
  private final TableLoader tableLoader;

  private transient Table table;
  private transient Long startingSnapshotId;
  private transient Set<RewriteFileGroup> inProgress;
  private transient ListState<Long> startingSnapshotIdState;
  private transient ListState<RewriteFileGroup> inProgressState;
  private transient CommitService commitService;
  private transient int processed;
  private transient Counter errorCounter;
  private transient Counter addedDataFileNumCounter;
  private transient Counter addedDataFileSizeCounter;
  private transient Counter removedDataFileNumCounter;
  private transient Counter removedDataFileSizeCounter;

  public DataFileRewriteCommitter(
      String tableName, String taskName, int taskIndex, TableLoader tableLoader) {
    Preconditions.checkNotNull(tableName, "Table name should no be null");
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");

    this.tableName = tableName;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    tableLoader.open();
    this.table = tableLoader.loadTable();

    MetricGroup taskMetricGroup =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), tableName, taskName, taskIndex);
    this.errorCounter = taskMetricGroup.counter(TableMaintenanceMetrics.ERROR_COUNTER);
    this.addedDataFileNumCounter =
        taskMetricGroup.counter(TableMaintenanceMetrics.ADDED_DATA_FILE_NUM_METRIC);
    this.addedDataFileSizeCounter =
        taskMetricGroup.counter(TableMaintenanceMetrics.ADDED_DATA_FILE_SIZE_METRIC);
    this.removedDataFileNumCounter =
        taskMetricGroup.counter(TableMaintenanceMetrics.REMOVED_DATA_FILE_NUM_METRIC);
    this.removedDataFileSizeCounter =
        taskMetricGroup.counter(TableMaintenanceMetrics.REMOVED_DATA_FILE_SIZE_METRIC);

    this.startingSnapshotIdState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "data-file-updater-snapshot-id", BasicTypeInfo.LONG_TYPE_INFO));
    this.inProgressState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "data-file-updater-in-progress", TypeInformation.of(RewriteFileGroup.class)));

    this.startingSnapshotId = null;
    Iterable<Long> snapshotIdIterable = startingSnapshotIdState.get();
    if (snapshotIdIterable != null) {
      Iterator<Long> snapshotIdIterator = snapshotIdIterable.iterator();
      if (snapshotIdIterator.hasNext()) {
        this.startingSnapshotId = snapshotIdIterator.next();
      }
    }

    // Has to be concurrent since it is accessed by the CommitService from another thread
    this.inProgress = Sets.newConcurrentHashSet();
    Iterable<RewriteFileGroup> inProgressIterable = inProgressState.get();
    if (inProgressIterable != null) {
      for (RewriteFileGroup group : inProgressIterable) {
        inProgress.add(group);
      }
    }

    commitInProgress(System.currentTimeMillis());

    this.processed = 0;
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);

    startingSnapshotIdState.clear();
    if (startingSnapshotId != null) {
      startingSnapshotIdState.add(startingSnapshotId);
    }

    inProgressState.clear();
    for (RewriteFileGroup group : inProgress) {
      inProgressState.add(group);
    }
  }

  @Override
  public void processElement(StreamRecord<DataFileRewriteExecutor.ExecutedGroup> streamRecord) {
    DataFileRewriteExecutor.ExecutedGroup executedGroup = streamRecord.getValue();
    try {
      if (commitService == null) {
        this.startingSnapshotId = executedGroup.snapshotId();
        this.commitService =
            createCommitService(streamRecord.getTimestamp(), executedGroup.groupsPerCommit());
      }

      commitService.offer(executedGroup.group());
      inProgress.add(executedGroup.group());
      ++processed;
    } catch (Exception e) {
      LOG.warn(
          LogUtil.MESSAGE_PREFIX + "Exception processing {}",
          tableName,
          taskName,
          taskIndex,
          streamRecord.getTimestamp(),
          executedGroup,
          e);
      output.collect(TaskResultAggregator.ERROR_STREAM, new StreamRecord<>(e));
      errorCounter.inc();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    try {
      if (commitService != null) {
        commitService.close();
        if (processed != commitService.results().size()) {
          throw new RuntimeException(
              String.format(
                  Locale.ROOT,
                  LogUtil.MESSAGE_FORMAT_PREFIX + "From %d commits only %d were unsuccessful",
                  tableName,
                  taskName,
                  taskIndex,
                  mark.getTimestamp(),
                  processed,
                  commitService.results().size()));
        }
      }

      LOG.info(
          LogUtil.MESSAGE_PREFIX + "Successfully completed data file compaction",
          tableName,
          taskName,
          taskIndex,
          mark.getTimestamp());
    } catch (Exception e) {
      LOG.warn(
          LogUtil.MESSAGE_PREFIX + "Exception closing commit service",
          tableName,
          taskName,
          taskIndex,
          mark.getTimestamp(),
          e);
      output.collect(TaskResultAggregator.ERROR_STREAM, new StreamRecord<>(e));
      errorCounter.inc();
    }

    // Cleanup
    this.commitService = null;
    this.startingSnapshotId = null;
    this.processed = 0;
    inProgress.clear();

    super.processWatermark(mark);
  }

  @Override
  public void close() throws IOException {
    if (commitService != null) {
      commitService.close();
    }
  }

  private CommitService createCommitService(long timestamp, int groupsPerCommit) {
    FlinkRewriteDataFilesCommitManager commitManager =
        new FlinkRewriteDataFilesCommitManager(table, startingSnapshotId, timestamp);
    CommitService service = commitManager.service(groupsPerCommit);
    service.start();
    return service;
  }

  private void commitInProgress(long timestamp) {
    if (!inProgress.isEmpty()) {
      CommitService service = null;
      try {
        service = createCommitService(timestamp, inProgress.size());
        inProgress.forEach(service::offer);
      } catch (Exception e) {
        LOG.info(
            LogUtil.MESSAGE_PREFIX + "Failed committing pending groups {}",
            tableName,
            taskName,
            taskIndex,
            timestamp,
            inProgress,
            e);
      } finally {
        inProgress.clear();
        if (service != null) {
          try {
            service.close();
          } catch (Exception e) {
            LOG.warn(
                LogUtil.MESSAGE_PREFIX + "Failed close pending groups committer",
                tableName,
                taskName,
                taskIndex,
                timestamp,
                e);
          }
        }
      }
    }
  }

  private class FlinkRewriteDataFilesCommitManager extends RewriteDataFilesCommitManager {
    private final long timestamp;

    FlinkRewriteDataFilesCommitManager(Table table, long snapshotId, long timestamp) {
      super(table, snapshotId);
      this.timestamp = timestamp;
    }

    @Override
    public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
      super.commitFileGroups(fileGroups);
      LOG.debug(
          LogUtil.MESSAGE_PREFIX + "Committed {}",
          tableName,
          taskName,
          taskIndex,
          timestamp,
          fileGroups);
      updateMetrics(fileGroups);
      inProgress.removeAll(fileGroups);
    }

    private void updateMetrics(Set<RewriteFileGroup> fileGroups) {
      for (RewriteFileGroup fileGroup : fileGroups) {
        for (DataFile added : fileGroup.addedFiles()) {
          addedDataFileNumCounter.inc();
          addedDataFileSizeCounter.inc(added.fileSizeInBytes());
        }

        for (DataFile rewritten : fileGroup.rewrittenFiles()) {
          removedDataFileNumCounter.inc();
          removedDataFileSizeCounter.inc(rewritten.fileSizeInBytes());
        }
      }
    }
  }
}
