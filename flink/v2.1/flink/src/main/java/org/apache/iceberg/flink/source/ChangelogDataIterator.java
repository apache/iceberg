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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Iterator for reading changelog data from Iceberg tables.
 *
 * <p>This iterator processes ChangelogScanTask and produces RowData with appropriate RowKind based
 * on the changelog operation type:
 *
 * <ul>
 *   <li>INSERT: RowKind.INSERT (+I)
 *   <li>DELETE: RowKind.DELETE (-D)
 *   <li>UPDATE_BEFORE: RowKind.UPDATE_BEFORE (-U)
 *   <li>UPDATE_AFTER: RowKind.UPDATE_AFTER (+U)
 * </ul>
 *
 * @param <T> the output data type
 */
@Internal
public class ChangelogDataIterator<T> implements CloseableIterator<T> {

  private final ChangelogScanTaskReader<T> taskReader;
  private final List<ChangelogScanTask> tasks;

  private Iterator<ChangelogScanTask> taskIterator;
  private CloseableIterator<T> currentIterator;
  private ChangelogScanTask currentTask;
  private int taskOffset;
  private long recordOffset;

  public ChangelogDataIterator(
      ChangelogScanTaskReader<T> taskReader, List<ChangelogScanTask> tasks) {
    this.taskReader = taskReader;
    this.tasks = tasks;
    this.taskIterator = tasks.iterator();
    this.currentIterator = CloseableIterator.empty();
    this.taskOffset = -1;
    this.recordOffset = 0L;
  }

  /**
   * Seek to a specific position in the changelog tasks.
   *
   * @param startingTaskOffset the task offset to start from
   * @param startingRecordOffset the record offset within the task
   */
  public void seek(int startingTaskOffset, long startingRecordOffset) {
    Preconditions.checkState(
        taskOffset == -1, "Seek should be called before any other iterator actions");

    Preconditions.checkArgument(
        startingTaskOffset < tasks.size(),
        "Invalid starting task offset %s for changelog tasks with %s tasks",
        startingTaskOffset,
        tasks.size());

    // Skip to the target task
    for (int i = 0; i < startingTaskOffset; i++) {
      taskIterator.next();
    }

    updateCurrentIterator();

    // Skip records within the task
    for (long i = 0; i < startingRecordOffset; i++) {
      if (currentTaskHasNext() && hasNext()) {
        next();
      } else {
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Invalid starting record offset %d for task %d from changelog tasks",
                startingRecordOffset,
                startingTaskOffset));
      }
    }

    taskOffset = startingTaskOffset;
    recordOffset = startingRecordOffset;
  }

  @Override
  public boolean hasNext() {
    updateCurrentIterator();
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    updateCurrentIterator();
    recordOffset++;
    return currentIterator.next();
  }

  public boolean currentTaskHasNext() {
    return currentIterator.hasNext();
  }

  private void updateCurrentIterator() {
    try {
      while (!currentIterator.hasNext() && taskIterator.hasNext()) {
        currentIterator.close();
        currentTask = taskIterator.next();
        currentIterator = openTaskIterator(currentTask);
        taskOffset++;
        recordOffset = 0L;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private CloseableIterator<T> openTaskIterator(ChangelogScanTask task) {
    ChangelogOperation operation = task.operation();
    ContentScanTask<DataFile> contentTask = toContentScanTask(task);

    return taskReader.open(contentTask, operation, task.changeOrdinal(), task.commitSnapshotId());
  }

  @SuppressWarnings("unchecked")
  private ContentScanTask<DataFile> toContentScanTask(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return (ContentScanTask<DataFile>) task;
    } else if (task instanceof DeletedDataFileScanTask) {
      return (ContentScanTask<DataFile>) task;
    } else {
      throw new UnsupportedOperationException(
          "Unknown changelog scan task type: " + task.getClass().getName());
    }
  }

  @Override
  public void close() throws IOException {
    currentIterator.close();
    taskIterator = null;
  }

  public int taskOffset() {
    return taskOffset;
  }

  public long recordOffset() {
    return recordOffset;
  }

  /**
   * Reader interface for reading ChangelogScanTask data.
   *
   * @param <T> the output data type
   */
  public interface ChangelogScanTaskReader<T> {
    /**
     * Open an iterator for the given content scan task with changelog metadata.
     *
     * @param task the content scan task
     * @param operation the changelog operation type
     * @param changeOrdinal the change ordinal
     * @param commitSnapshotId the commit snapshot ID
     * @return an iterator over the task data
     */
    CloseableIterator<T> open(
        ContentScanTask<DataFile> task,
        ChangelogOperation operation,
        int changeOrdinal,
        long commitSnapshotId);
  }
}
