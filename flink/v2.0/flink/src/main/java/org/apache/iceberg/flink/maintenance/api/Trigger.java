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
package org.apache.iceberg.flink.maintenance.api;

import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.actions.ActionResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class Trigger {
  private final long timestamp;
  private final Integer taskId;
  private final boolean isRecovery;
  private final @Nullable ActionResult actionResult;

  private Trigger(
      long timestamp, Integer taskId, boolean isRecovery, @Nullable ActionResult actionResult) {
    this.timestamp = timestamp;
    this.taskId = taskId;
    this.isRecovery = isRecovery;
    this.actionResult = actionResult;
  }

  @Internal
  public static Trigger create(long timestamp, int taskId) {
    return new Trigger(timestamp, taskId, false, null);
  }

  @Internal
  public static Trigger create(ActionResult actionResult) {
    return new Trigger(-1, null, false, actionResult);
  }

  @Internal
  public static Trigger recovery(long timestamp) {
    return new Trigger(timestamp, null, true, null);
  }

  public long timestamp() {
    return timestamp;
  }

  public Integer taskId() {
    return taskId;
  }

  public boolean isRecovery() {
    return isRecovery;
  }

  public ActionResult actionResult() {
    return actionResult;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("timestamp", timestamp)
        .add("taskId", taskId)
        .add("isRecovery", isRecovery)
        .toString();
  }
}
