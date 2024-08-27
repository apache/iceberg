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

import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/** The result of a single Maintenance Task. */
@Internal
public class TaskResult {
  private final int taskIndex;
  private final long startEpoch;
  private final boolean success;
  private final List<Exception> exceptions;

  public TaskResult(int taskIndex, long startEpoch, boolean success, List<Exception> exceptions) {
    this.taskIndex = taskIndex;
    this.startEpoch = startEpoch;
    this.success = success;
    this.exceptions = exceptions;
  }

  public int taskIndex() {
    return taskIndex;
  }

  public long startEpoch() {
    return startEpoch;
  }

  public boolean success() {
    return success;
  }

  public List<Exception> exceptions() {
    return exceptions;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("taskIndex", taskIndex)
        .add("startEpoch", startEpoch)
        .add("success", success)
        .add("exceptions", exceptions)
        .toString();
  }
}
