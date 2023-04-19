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
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class BaseScanTaskGroup<T extends ScanTask> implements ScanTaskGroup<T> {
  private final StructLike groupingKey;
  private final Object[] tasks;
  private transient volatile List<T> taskList;

  public BaseScanTaskGroup(StructLike groupingKey, Collection<T> tasks) {
    Preconditions.checkNotNull(tasks, "tasks cannot be null");
    this.groupingKey = groupingKey;
    this.tasks = tasks.toArray();
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
    if (taskList == null) {
      synchronized (this) {
        if (taskList == null) {
          ImmutableList.Builder<T> listBuilder =
              ImmutableList.builderWithExpectedSize(tasks.length);
          for (Object task : tasks) {
            listBuilder.add((T) task);
          }
          taskList = listBuilder.build();
        }
      }
    }

    return taskList;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("tasks", Joiner.on(", ").join(tasks)).toString();
  }
}
