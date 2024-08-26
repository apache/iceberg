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
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/** The output of the Maintenance Flow. */
public class MaintenanceResult {
  private final long startEpoch;
  private final int taskId;
  private final long length;
  private final boolean success;
  private final List<Exception> exceptions;

  public MaintenanceResult(
      long startEpoch, int taskId, long length, boolean success, List<Exception> exceptions) {
    this.startEpoch = startEpoch;
    this.taskId = taskId;
    this.length = length;
    this.success = success;
    this.exceptions = exceptions;
  }

  public long startEpoch() {
    return startEpoch;
  }

  public int taskId() {
    return taskId;
  }

  public long length() {
    return length;
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
        .add("startEpoch", startEpoch)
        .add("taskId", taskId)
        .add("length", length)
        .add("success", success)
        .add("exceptions", exceptions)
        .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    MaintenanceResult that = (MaintenanceResult) other;
    return this.startEpoch == that.startEpoch
        && this.taskId == that.taskId
        && this.length == that.length
        && this.success == that.success
        && this.exceptions.size() == that.exceptions.size();
  }

  @Override
  public int hashCode() {
    return Objects.hash(startEpoch, taskId, length, success, exceptions.size());
  }
}
