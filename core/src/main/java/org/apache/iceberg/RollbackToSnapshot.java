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

import com.google.common.base.Preconditions;
import org.apache.iceberg.exceptions.ValidationException;

class RollbackToSnapshot implements Rollback {
  private final TableOperations ops;
  private TableMetadata base = null;
  private Long targetSnapshotId = null;

  RollbackToSnapshot(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current(); // do not retry
  }

  @Override
  public Rollback toSnapshotId(long snapshotId) {
    Preconditions.checkArgument(base.snapshot(snapshotId) != null,
        "Cannot roll back to unknown snapshot id: %s", snapshotId);

    this.targetSnapshotId = snapshotId;

    return this;
  }

  @Override
  public Rollback toSnapshotAtTime(long timestampMillis) {
    long snapshotId = 0;
    long snapshotTimestamp = 0;
    // find the latest snapshot by timestamp older than timestampMillis
    for (Snapshot snapshot : base.snapshots()) {
      if (snapshot.timestampMillis() < timestampMillis &&
          snapshot.timestampMillis() > snapshotTimestamp) {
        snapshotId = snapshot.snapshotId();
        snapshotTimestamp = snapshot.timestampMillis();
      }
    }

    Preconditions.checkArgument(base.snapshot(snapshotId) != null,
        "Cannot roll back, no valid snapshot older than: %s", timestampMillis);

    this.targetSnapshotId = snapshotId;

    return this;
  }

  @Override
  public Snapshot apply() {
    ValidationException.check(targetSnapshotId != null,
        "Cannot roll back to unknown version: call toSnapshotId or toSnapshotAtTime");
    return base.snapshot(targetSnapshotId);
  }

  @Override
  public void commit() {
    // rollback does not refresh or retry. it only operates on the state of the table when rollback
    // was called to create the transaction.
    ops.commit(base, base.rollbackTo(apply()));
  }
}
