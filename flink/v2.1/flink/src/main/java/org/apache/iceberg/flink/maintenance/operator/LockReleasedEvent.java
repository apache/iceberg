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

/**
 * Event sent from TriggerManagerOperatorCoordinator to TriggerManager operator to notify that a
 * lock has been released (watermark arrived).
 */
public class LockReleasedEvent implements OperatorEvent {

  private static final long serialVersionUID = 1L;

  private final boolean isRecover;

  private final String lockId;

  public LockReleasedEvent(boolean isRecover, String lockId) {
    this.isRecover = isRecover;
    this.lockId = lockId;
  }

  public boolean isRecover() {
    return isRecover;
  }

  public String lockId() {
    return lockId;
  }

  @Override
  public String toString() {
    return "LockReleasedEvent{isRecover=" + isRecover + ", lockId='" + lockId + "'}";
  }
}
