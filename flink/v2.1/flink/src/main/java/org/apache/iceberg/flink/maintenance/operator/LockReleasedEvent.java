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

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Event sent from TriggerManagerOperatorCoordinator to TriggerManager operator to notify that a
 * lock has been released (watermark arrived).
 */
public class LockReleasedEvent implements OperatorEvent {

  private final String lockId;
  private final long timestamp;
  private final boolean isWatermark;

  public LockReleasedEvent(String lockId, long timestamp, boolean isWatermark) {
    this.lockId = lockId;
    this.timestamp = timestamp;
    this.isWatermark = isWatermark;
  }

  public long timestamp() {
    return timestamp;
  }

  public String lockId() {
    return lockId;
  }

  public boolean isWatermark() {
    return isWatermark;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("lockId", lockId)
        .add("timestamp", timestamp)
        .add("isWatermark", isWatermark)
        .toString();
  }
}
