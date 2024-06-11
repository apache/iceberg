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

import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

@Internal
class Trigger {
  private final long timestamp;
  private final SerializableTable table;
  private final Integer taskId;
  private final boolean isCleanUp;

  private Trigger(long timestamp, SerializableTable table, Integer taskId, boolean isCleanUp) {
    this.timestamp = timestamp;
    this.table = table;
    this.taskId = taskId;
    this.isCleanUp = isCleanUp;
  }

  static Trigger create(long timestamp, SerializableTable table, int taskId) {
    return new Trigger(timestamp, table, taskId, false);
  }

  static Trigger cleanUp(long timestamp) {
    return new Trigger(timestamp, null, null, true);
  }

  long timestamp() {
    return timestamp;
  }

  SerializableTable table() {
    return table;
  }

  boolean isCleanUp() {
    return isCleanUp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Trigger other = (Trigger) o;
    return timestamp == other.timestamp
        && Objects.equals(table, other.table)
        && Objects.equals(taskId, other.taskId)
        && isCleanUp == other.isCleanUp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, isCleanUp, taskId, table);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("timestamp", timestamp)
        .add("table", table == null ? null : table.name())
        .add("taskId", taskId)
        .add("isCleanUp", isCleanUp)
        .toString();
  }
}
