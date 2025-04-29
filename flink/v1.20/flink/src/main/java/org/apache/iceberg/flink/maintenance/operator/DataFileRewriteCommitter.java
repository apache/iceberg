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
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commits the rewrite changes using {@link RewriteDataFilesCommitManager}. The input is a {@link
 * DataFileRewriteRunner.ExecutedGroup}. Only {@link Watermark} is emitted which is chained to
 * {@link TaskResultAggregator} input 1.
 */
@Internal
public class DataFileRewriteCommitter extends AbstractStreamOperator<Trigger>
    implements OneInputStreamOperator<DataFileRewriteRunner.ExecutedGroup, Trigger> {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileRewriteCommitter.class);

  private final String tableName;
  private final String taskName;
  private final int taskIndex;
  private final TableLoader tableLoader;

  private transient Table table;
  private transient CommitService commitService;
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
  public void open() throws Exception {
    super.open();

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
  }

  @Override
  public void processElement(StreamRecord<DataFileRewriteRunner.ExecutedGroup> streamRecord) {
    DataFileRewriteRunner.ExecutedGroup executedGroup = streamRecord.getValue();
    try {
      if (commitService == null) {
        // Refresh the table to get the latest snapshot for the committer
        table.refresh();

        FlinkRewriteDataFilesCommitManager commitManager =
            new FlinkRewriteDataFilesCommitManager(
                table, executedGroup.snapshotId(), streamRecord.getTimestamp());
        this.commitService = commitManager.service(executedGroup.groupsPerCommit());
        commitService.start();
      }

      commitService.offer(executedGroup.group());
    } catch (Exception e) {
      LOG.warn(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Exception processing {}",
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
      }

      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Successfully completed data file compaction",
          tableName,
          taskName,
          taskIndex,
          mark.getTimestamp());
    } catch (Exception e) {
      LOG.warn(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Exception closing commit service",
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

    super.processWatermark(mark);
  }

  @Override
  public void close() throws IOException {
    if (commitService != null) {
      commitService.close();
    }
  }

  private class FlinkRewriteDataFilesCommitManager extends RewriteDataFilesCommitManager {
    private final long timestamp;

    FlinkRewriteDataFilesCommitManager(Table table, long startingSnapshotId, long timestamp) {
      super(table, startingSnapshotId);
      this.timestamp = timestamp;
    }

    @Override
    public void commitFileGroups(Set<RewriteFileGroup> fileGroups) {
      super.commitFileGroups(fileGroups);
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Committed {}",
          tableName,
          taskName,
          taskIndex,
          timestamp,
          fileGroups);
      updateMetrics(fileGroups);
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
