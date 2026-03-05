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
package org.apache.iceberg.hive;

import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class LockInfo {
  private long lockId;
  private LockState lockState;

  public LockInfo() {
    this.lockId = -1;
    this.lockState = null;
  }

  public LockInfo(long lockId, LockState lockState) {
    this.lockId = lockId;
    this.lockState = lockState;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("lockId", lockId)
        .add("lockState", lockState)
        .toString();
  }

  public long getLockId() {
    return lockId;
  }

  public void setLockId(long lockId) {
    this.lockId = lockId;
  }

  public LockState getLockState() {
    return lockState;
  }

  public void setLockState(LockState lockState) {
    this.lockState = lockState;
  }
}
