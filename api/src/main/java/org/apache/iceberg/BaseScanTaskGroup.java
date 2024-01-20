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
import java.util.Collections;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class BaseScanTaskGroup<T extends ScanTask> implements ScanTaskGroup<T> {
  private final StructLike groupingKey;
  private final Object[] tasks;
  private transient volatile Collection<T> taskCollection;

  private volatile String[] preferredLocations = new String[0];

  public BaseScanTaskGroup(StructLike groupingKey, Collection<T> tasks) {
    Preconditions.checkNotNull(tasks, "tasks cannot be null");
    this.groupingKey = groupingKey;
    this.tasks = tasks.toArray();
    this.taskCollection = Collections.unmodifiableCollection(tasks);
  }

  public BaseScanTaskGroup(Collection<T> tasks) {
    this(EmptyStructLike.get(), tasks);
  }

  @Override
  public StructLike groupingKey() {
    return groupingKey;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<T> tasks() {
    if (taskCollection == null) {
      synchronized (this) {
        if (taskCollection == null) {
          ImmutableList.Builder<T> listBuilder =
              ImmutableList.builderWithExpectedSize(tasks.length);
          for (Object task : tasks) {
            listBuilder.add((T) task);
          }
          this.taskCollection = listBuilder.build();
        }
      }
    }

    return taskCollection;
  }

  @Override
  public long sizeBytes() {
    long sizeBytes = 0L;
    for (Object task : tasks) {
      sizeBytes += ((ScanTask) task).sizeBytes();
    }
    return sizeBytes;
  }

  @Override
  public long estimatedRowsCount() {
    long estimatedRowsCount = 0L;
    for (Object task : tasks) {
      estimatedRowsCount += ((ScanTask) task).estimatedRowsCount();
    }
    return estimatedRowsCount;
  }

  @Override
  public int filesCount() {
    int filesCount = 0;
    for (Object task : tasks) {
      filesCount += ((ScanTask) task).filesCount();
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
