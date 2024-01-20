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
package org.apache.iceberg;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.TableScanUtil;

public class BaseCombinedScanTask implements CombinedScanTask {
  private final FileScanTask[] tasks;
  private String[] preferredLocations = new String[0];
  private transient volatile List<FileScanTask> taskList = null;

  public BaseCombinedScanTask(FileScanTask... tasks) {
    Preconditions.checkNotNull(tasks, "tasks cannot be null");
    this.tasks = tasks;
  }

  public BaseCombinedScanTask(List<FileScanTask> tasks) {
    Preconditions.checkNotNull(tasks, "tasks cannot be null");
    this.tasks = TableScanUtil.mergeTasks(tasks).toArray(new FileScanTask[0]);
  }

  @Override
  public Collection<FileScanTask> files() {
    if (taskList == null) {
      this.taskList = ImmutableList.copyOf(tasks);
    }

    return taskList;
  }

  @Override
  public long sizeBytes() {
    long sizeBytes = 0L;
    for (FileScanTask task : tasks) {
      sizeBytes += task.sizeBytes();
    }
    return sizeBytes;
  }

  @Override
  public long estimatedRowsCount() {
    long estimatedRowsCount = 0L;
    for (FileScanTask task : tasks) {
      estimatedRowsCount += task.estimatedRowsCount();
    }
    return estimatedRowsCount;
  }

  @Override
  public int filesCount() {
    int filesCount = 0;
    for (FileScanTask task : tasks) {
      filesCount += task.filesCount();
    }
    return filesCount;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("tasks", Joiner.on(", ").join(tasks)).toString();
  }

  @Override
  public String[] setIfNeededAndGetPreferredLocations(
      Supplier<String[]> preferredLocationsEvaluator) {
    if (this.preferredLocations.length == 0) {
      this.preferredLocations = preferredLocationsEvaluator.get();
    }
    return this.preferredLocations;
  }
}
