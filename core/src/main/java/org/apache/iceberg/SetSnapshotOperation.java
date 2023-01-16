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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.util.List;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;

/**
 * Sets the current snapshot directly or by rolling back.
 *
 * <p>This update is not exposed though the Table API. Instead, it is a package-private part of the
 * Transaction API intended for use in {@link ManageSnapshots}.
 */
class SetSnapshotOperation implements PendingUpdate<Snapshot> {

  private final TableOperations ops;
  private TableMetadata base;
  private Long targetSnapshotId = null;
  private boolean isRollback = false;

  SetSnapshotOperation(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  public SetSnapshotOperation setCurrentSnapshot(long snapshotId) {
    ValidationException.check(
        base.snapshot(snapshotId) != null,
        "Cannot roll back to unknown snapshot id: %s",
        snapshotId);

    this.targetSnapshotId = snapshotId;

    return this;
  }

  public SetSnapshotOperation rollbackToTime(long timestampMillis) {
    // find the latest snapshot by timestamp older than timestampMillis
    Snapshot snapshot = findLatestAncestorOlderThan(base, timestampMillis);
    Preconditions.checkArgument(
        snapshot != null, "Cannot roll back, no valid snapshot older than: %s", timestampMillis);

    this.targetSnapshotId = snapshot.snapshotId();
    this.isRollback = true;

    return this;
  }

  public SetSnapshotOperation rollbackTo(long snapshotId) {
    TableMetadata current = base;
    ValidationException.check(
        current.snapshot(snapshotId) != null,
        "Cannot roll back to unknown snapshot id: %s",
        snapshotId);
    ValidationException.check(
        isCurrentAncestor(current, snapshotId),
        "Cannot roll back to snapshot, not an ancestor of the current state: %s",
        snapshotId);
    return setCurrentSnapshot(snapshotId);
  }

  @Override
  public Snapshot apply() {
    this.base = ops.refresh();

    if (targetSnapshotId == null) {
      // if no target snapshot was configured then NOOP by returning current state
      return base.currentSnapshot();
    }

    ValidationException.check(
        !isRollback || isCurrentAncestor(base, targetSnapshotId),
        "Cannot roll back to %s: not an ancestor of the current table state",
        targetSnapshotId);

    return base.snapshot(targetSnapshotId);
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            taskOps -> {
              Snapshot snapshot = apply();
              TableMetadata updated =
                  TableMetadata.buildFrom(base)
                      .setBranchSnapshot(snapshot.snapshotId(), SnapshotRef.MAIN_BRANCH)
                      .build();

              // Do commit this operation even if the metadata has not changed, as we need to
              // advance the hasLastOpCommited for the transaction's commit to work properly.
              // (Without any other operations in the transaction, the commitTransaction() call
              // will be a no-op anyway)

              // if the table UUID is missing, add it here. the UUID will be re-created each time
              // this operation retries
              // to ensure that if a concurrent operation assigns the UUID, this operation will not
              // fail.
              taskOps.commit(base, updated.withUUID());
            });
  }

  /**
   * Return the latest snapshot whose timestamp is before the provided timestamp.
   *
   * @param meta {@link TableMetadata} for a table
   * @param timestampMillis lookup snapshots before this timestamp
   * @return the ID of the snapshot that was current at the given timestamp, or null
   */
  private static Snapshot findLatestAncestorOlderThan(TableMetadata meta, long timestampMillis) {
    long snapshotTimestamp = 0;
    Snapshot result = null;
    for (Long snapshotId : currentAncestors(meta)) {
      Snapshot snapshot = meta.snapshot(snapshotId);
      if (snapshot.timestampMillis() < timestampMillis
          && snapshot.timestampMillis() > snapshotTimestamp) {
        result = snapshot;
        snapshotTimestamp = snapshot.timestampMillis();
      }
    }
    return result;
  }

  private static List<Long> currentAncestors(TableMetadata meta) {
    return SnapshotUtil.ancestorIds(meta.currentSnapshot(), meta::snapshot);
  }

  private static boolean isCurrentAncestor(TableMetadata meta, long snapshotId) {
    return currentAncestors(meta).contains(snapshotId);
  }
}
