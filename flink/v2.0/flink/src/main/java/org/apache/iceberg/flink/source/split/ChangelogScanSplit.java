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
package org.apache.iceberg.flink.source.split;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * A source split for CDC (Change Data Capture) streaming reads that wraps a list of {@link
 * ChangelogScanTask}.
 *
 * <p>This split supports reading changelog operations including INSERT, UPDATE_BEFORE,
 * UPDATE_AFTER, and DELETE.
 */
@Internal
public class ChangelogScanSplit implements SourceSplit, Serializable {
  private static final long serialVersionUID = 1L;

  private final List<ChangelogScanTask> tasks;
  private int taskOffset;
  private long recordOffset;

  public ChangelogScanSplit(List<ChangelogScanTask> tasks) {
    this(tasks, 0, 0L);
  }

  public ChangelogScanSplit(List<ChangelogScanTask> tasks, int taskOffset, long recordOffset) {
    this.tasks = tasks;
    this.taskOffset = taskOffset;
    this.recordOffset = recordOffset;
  }

  public List<ChangelogScanTask> tasks() {
    return tasks;
  }

  public int taskOffset() {
    return taskOffset;
  }

  public long recordOffset() {
    return recordOffset;
  }

  public void updatePosition(int newTaskOffset, long newRecordOffset) {
    this.taskOffset = newTaskOffset;
    this.recordOffset = newRecordOffset;
  }

  @Override
  public String splitId() {
    StringBuilder sb = new StringBuilder();
    sb.append("ChangelogScanSplit[tasks=").append(tasks.size());
    if (!tasks.isEmpty()) {
      ChangelogScanTask firstTask = tasks.get(0);
      sb.append(", firstOp=").append(firstTask.operation());
      sb.append(", snapshotId=").append(firstTask.commitSnapshotId());
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tasks", tasks.size())
        .add("taskOffset", taskOffset)
        .add("recordOffset", recordOffset)
        .add("operations", getOperationsSummary())
        .toString();
  }

  private String getOperationsSummary() {
    int inserts = 0;
    int deletes = 0;
    for (ChangelogScanTask task : tasks) {
      if (task.operation() == ChangelogOperation.INSERT) {
        inserts++;
      } else if (task.operation() == ChangelogOperation.DELETE) {
        deletes++;
      }
    }
    return String.format(Locale.ROOT, "inserts=%d, deletes=%d", inserts, deletes);
  }
}
