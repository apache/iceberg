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

import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Delete the files using the {@link FileIO} which implements {@link SupportsBulkOperations}. */
@Internal
public class DeleteFilesProcessor extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<String, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteFilesProcessor.class);

  private final String taskIndex;
  private final String taskName;
  private final SupportsBulkOperations io;
  private final String tableName;
  private final Set<String> filesToDelete = Sets.newHashSet();
  private final int batchSize;

  private transient Counter failedCounter;
  private transient Counter succeededCounter;

  public DeleteFilesProcessor(int taskIndex, String taskName, Table table, int batchSize) {
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    Preconditions.checkNotNull(table, "Table should no be null");

    FileIO fileIO = table.io();
    Preconditions.checkArgument(
        fileIO instanceof SupportsBulkOperations,
        "%s doesn't support bulk delete",
        fileIO.getClass().getSimpleName());

    this.taskIndex = String.valueOf(taskIndex);
    this.taskName = taskName;
    this.io = (SupportsBulkOperations) fileIO;
    this.tableName = table.name();
    this.batchSize = batchSize;
  }

  @Override
  public void open() throws Exception {
    this.failedCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(TableMaintenanceMetrics.GROUP_KEY)
            .addGroup(TableMaintenanceMetrics.TABLE_NAME_KEY, tableName)
            .addGroup(TableMaintenanceMetrics.TASK_NAME_KEY, taskName)
            .addGroup(TableMaintenanceMetrics.TASK_INDEX_KEY, taskIndex)
            .counter(TableMaintenanceMetrics.DELETE_FILE_FAILED_COUNTER);
    this.succeededCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(TableMaintenanceMetrics.GROUP_KEY)
            .addGroup(TableMaintenanceMetrics.TABLE_NAME_KEY, tableName)
            .addGroup(TableMaintenanceMetrics.TASK_NAME_KEY, taskName)
            .addGroup(TableMaintenanceMetrics.TASK_INDEX_KEY, taskIndex)
            .counter(TableMaintenanceMetrics.DELETE_FILE_SUCCEEDED_COUNTER);
  }

  @Override
  public void processElement(StreamRecord<String> element) throws Exception {
    if (element.isRecord()) {
      filesToDelete.add(element.getValue());
    }

    if (filesToDelete.size() >= batchSize) {
      deleteFiles();
    }
  }

  @Override
  public void processWatermark(Watermark mark) {
    deleteFiles();
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) {
    deleteFiles();
  }

  private void deleteFiles() {
    try {
      io.deleteFiles(filesToDelete);
      LOG.info(
          "Deleted {} files from table {} using bulk deletes", filesToDelete.size(), tableName);
      succeededCounter.inc(filesToDelete.size());
      filesToDelete.clear();
    } catch (BulkDeletionFailureException e) {
      int deletedFilesCount = filesToDelete.size() - e.numberFailedObjects();
      LOG.warn(
          "Deleted only {} of {} files from table {} using bulk deletes",
          deletedFilesCount,
          filesToDelete.size(),
          tableName,
          e);
      succeededCounter.inc(deletedFilesCount);
      failedCounter.inc(e.numberFailedObjects());
    }
  }
}
