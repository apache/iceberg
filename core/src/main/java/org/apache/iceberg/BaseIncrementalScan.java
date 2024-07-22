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

import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SnapshotUtil;

abstract class BaseIncrementalScan<
        ThisT extends Scan, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends BaseScan<ThisT, T, G> implements IncrementalScan<ThisT, T, G> {

  protected BaseIncrementalScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  protected abstract CloseableIterable<T> doPlanFiles(
      Long fromSnapshotIdExclusive, long toSnapshotIdInclusive);

  @Override
  public ThisT fromSnapshotInclusive(String ref) {
    SnapshotRef snapshotRef = table().refs().get(ref);
    Preconditions.checkArgument(snapshotRef != null, "Cannot find ref: %s", ref);
    Preconditions.checkArgument(snapshotRef.isTag(), "Ref %s is not a tag", ref);
    return fromSnapshotInclusive(snapshotRef.snapshotId());
  }

  @Override
  public ThisT fromSnapshotInclusive(long fromSnapshotId) {
    Preconditions.checkArgument(
        table().snapshot(fromSnapshotId) != null,
        "Cannot find the starting snapshot: %s",
        fromSnapshotId);
    TableScanContext newContext = context().fromSnapshotIdInclusive(fromSnapshotId);
    return newRefinedScan(table(), schema(), newContext);
  }

  @Override
  public ThisT fromSnapshotExclusive(String ref) {
    SnapshotRef snapshotRef = table().refs().get(ref);
    Preconditions.checkArgument(snapshotRef != null, "Cannot find ref: %s", ref);
    Preconditions.checkArgument(snapshotRef.isTag(), "Ref %s is not a tag", ref);
    return fromSnapshotExclusive(snapshotRef.snapshotId());
  }

  @Override
  public ThisT fromSnapshotExclusive(long fromSnapshotId) {
    // for exclusive behavior, table().snapshot(fromSnapshotId) check can't be applied
    // as fromSnapshotId could be matched to a parent snapshot that is already expired
    TableScanContext newContext = context().fromSnapshotIdExclusive(fromSnapshotId);
    return newRefinedScan(table(), schema(), newContext);
  }

  @Override
  public ThisT toSnapshot(long toSnapshotId) {
    Preconditions.checkArgument(
        table().snapshot(toSnapshotId) != null, "Cannot find the end snapshot: %s", toSnapshotId);
    TableScanContext newContext = context().toSnapshotId(toSnapshotId);
    return newRefinedScan(table(), schema(), newContext);
  }

  @Override
  public ThisT toSnapshot(String ref) {
    SnapshotRef snapshotRef = table().refs().get(ref);
    Preconditions.checkArgument(snapshotRef != null, "Cannot find ref: %s", ref);
    Preconditions.checkArgument(snapshotRef.isTag(), "Ref %s is not a tag", ref);
    return toSnapshot(snapshotRef.snapshotId());
  }

  @Override
  public ThisT useBranch(String branch) {
    SnapshotRef snapshotRef = table().refs().get(branch);
    Preconditions.checkArgument(snapshotRef != null, "Cannot find ref: %s", branch);
    Preconditions.checkArgument(snapshotRef.isBranch(), "Ref %s is not a branch", branch);
    return newRefinedScan(table(), schema(), context().useBranch(branch));
  }

  @Override
  public CloseableIterable<T> planFiles() {
    if (scanCurrentLineage() && table().currentSnapshot() == null) {
      // If the table is empty (no current snapshot) and both from and to snapshots aren't set,
      // simply return an empty iterable. In this case, the listener notification is also skipped.
      return CloseableIterable.empty();
    }

    long toSnapshotIdInclusive = toSnapshotIdInclusive();
    Long fromSnapshotIdExclusive = fromSnapshotIdExclusive(toSnapshotIdInclusive);

    if (fromSnapshotIdExclusive != null) {
      Listeners.notifyAll(
          new IncrementalScanEvent(
              table().name(),
              fromSnapshotIdExclusive,
              toSnapshotIdInclusive,
              filter(),
              schema(),
              false /* from snapshot ID inclusive */));
    } else {
      Listeners.notifyAll(
          new IncrementalScanEvent(
              table().name(),
              SnapshotUtil.oldestAncestorOf(table(), toSnapshotIdInclusive).snapshotId(),
              toSnapshotIdInclusive,
              filter(),
              schema(),
              true /* from snapshot ID inclusive */));
    }

    return doPlanFiles(fromSnapshotIdExclusive, toSnapshotIdInclusive);
  }

  private boolean scanCurrentLineage() {
    return context().fromSnapshotId() == null && context().toSnapshotId() == null;
  }

  private long toSnapshotIdInclusive() {
    if (context().toSnapshotId() != null) {
      if (context().branch() != null) {
        Snapshot currentSnapshot = table().snapshot(context().branch());
        Preconditions.checkArgument(
            SnapshotUtil.isAncestorOf(
                table(), currentSnapshot.snapshotId(), context().toSnapshotId()),
            "End snapshot is not a valid snapshot on the current branch: %s",
            context().branch());
      }

      return context().toSnapshotId();
    } else {
      Snapshot currentSnapshot;
      if (context().branch() != null) {
        currentSnapshot = table().snapshot(context().branch());
      } else {
        currentSnapshot = table().currentSnapshot();
      }

      Preconditions.checkArgument(
          currentSnapshot != null, "End snapshot is not set and table has no current snapshot");
      return currentSnapshot.snapshotId();
    }
  }

  private Long fromSnapshotIdExclusive(long toSnapshotIdInclusive) {
    Long fromSnapshotId = context().fromSnapshotId();
    boolean fromSnapshotInclusive = context().fromSnapshotInclusive();

    if (fromSnapshotId == null) {
      return null;
    } else {
      if (fromSnapshotInclusive) {
        // validate fromSnapshotId is an ancestor of toSnapshotIdInclusive
        Preconditions.checkArgument(
            SnapshotUtil.isAncestorOf(table(), toSnapshotIdInclusive, fromSnapshotId),
            "Starting snapshot (inclusive) %s is not an ancestor of end snapshot %s",
            fromSnapshotId,
            toSnapshotIdInclusive);
        // for inclusive behavior fromSnapshotIdExclusive is set to the parent snapshot ID,
        // which can be null
        return table().snapshot(fromSnapshotId).parentId();

      } else {
        // validate there is an ancestor of toSnapshotIdInclusive where parent is fromSnapshotId
        Preconditions.checkArgument(
            SnapshotUtil.isParentAncestorOf(table(), toSnapshotIdInclusive, fromSnapshotId),
            "Starting snapshot (exclusive) %s is not a parent ancestor of end snapshot %s",
            fromSnapshotId,
            toSnapshotIdInclusive);
        return fromSnapshotId;
      }
    }
  }
}
