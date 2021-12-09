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

package org.apache.iceberg.util;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class SnapshotUtil {
  private SnapshotUtil() {
  }

  /**
   * Returns whether ancestorSnapshotId is an ancestor of snapshotId.
   */
  public static boolean isAncestorOf(Table table, long snapshotId, long ancestorSnapshotId) {
    for (Snapshot snapshot : ancestorsOf(snapshotId, table::snapshot)) {
      if (snapshot.snapshotId() == ancestorSnapshotId) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns whether ancestorSnapshotId is an ancestor of the table's current state.
   */
  public static boolean isAncestorOf(Table table, long ancestorSnapshotId) {
    return isAncestorOf(table, table.currentSnapshot().snapshotId(), ancestorSnapshotId);
  }

  /**
   * Returns an iterable that traverses the table's snapshots from the current to the last known ancestor.
   *
   * @param table a Table
   * @return an iterable from the table's current snapshot to its last known ancestor
   */
  public static Iterable<Snapshot> currentAncestors(Table table) {
    return ancestorsOf(table.currentSnapshot(), table::snapshot);
  }

  /**
   * Return the snapshot IDs for the ancestors of the current table state.
   * <p>
   * Ancestor IDs are ordered by commit time, descending. The first ID is the current snapshot, followed by its parent,
   * and so on.
   *
   * @param table a {@link Table}
   * @return a set of snapshot IDs of the known ancestor snapshots, including the current ID
   */
  public static List<Long> currentAncestorIds(Table table) {
    return ancestorIds(table.currentSnapshot(), table::snapshot);
  }

  /**
   * Traverses the history of the table's current snapshot and finds the oldest Snapshot.
   * @return null if there is no current snapshot in the table, else the oldest Snapshot.
   */
  public static Snapshot oldestAncestor(Table table) {
    Snapshot lastSnapshot = null;
    for (Snapshot snapshot : currentAncestors(table)) {
      lastSnapshot = snapshot;
    }

    return lastSnapshot;
  }

  public static Iterable<Snapshot> ancestorsOf(long snapshotId, Function<Long, Snapshot> lookup) {
    Snapshot start = lookup.apply(snapshotId);
    Preconditions.checkArgument(start != null, "Cannot find snapshot: %s", snapshotId);
    return ancestorsOf(start, lookup);
  }

  /**
   * Traverses the history of the table's current snapshot and:
   * 1. returns null, if no snapshot exists or target timestamp is more recent than the current snapshot.
   * 2. else return the first snapshot which satisfies {@literal >=} targetTimestamp.
   * <p>
   * Given the snapshots (with timestamp): [S1 (10), S2 (11), S3 (12), S4 (14)]
   * <p>
   * firstSnapshotAfterTimestamp(table, x {@literal <=} 10) = S1
   * firstSnapshotAfterTimestamp(table, 11) = S2
   * firstSnapshotAfterTimestamp(table, 13) = S4
   * firstSnapshotAfterTimestamp(table, 14) = S4
   * firstSnapshotAfterTimestamp(table, x {@literal >} 14) = null
   * <p>
   * where x is the target timestamp in milliseconds and Si is the snapshot
   *
   * @param table a table
   * @param targetTimestampMillis a timestamp in milliseconds
   * @return the first snapshot which satisfies {@literal >=} targetTimestamp, or null if the current snapshot is
   * more recent than the target timestamp
   */
  public static Snapshot firstSnapshotAfterTimestamp(Table table, Long targetTimestampMillis) {
    Snapshot currentSnapshot = table.currentSnapshot();
    // Return null if no snapshot exists or target timestamp is more recent than the current snapshot
    if (currentSnapshot == null || currentSnapshot.timestampMillis() < targetTimestampMillis) {
      return null;
    }

    // Return the oldest snapshot which satisfies >= targetTimestamp
    Snapshot lastSnapshot = null;
    for (Snapshot snapshot : currentAncestors(table)) {
      if (snapshot.timestampMillis() < targetTimestampMillis) {
        return lastSnapshot;
      }
      lastSnapshot = snapshot;
    }

    // Return the oldest snapshot if the target timestamp is less than the oldest snapshot of the table
    return lastSnapshot;
  }

  /**
   * Returns list of snapshot ids in the range - (fromSnapshotId, toSnapshotId]
   * <p>
   * This method assumes that fromSnapshotId is an ancestor of toSnapshotId.
   */
  public static List<Long> snapshotIdsBetween(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Long> snapshotIds = Lists.newArrayList(ancestorIds(table.snapshot(toSnapshotId),
        snapshotId -> snapshotId != fromSnapshotId ? table.snapshot(snapshotId) : null));
    return snapshotIds;
  }

  public static Iterable<Long> ancestorIdsBetween(long latestSnapshotId, Long oldestSnapshotId,
                                                  Function<Long, Snapshot> lookup) {
    return toIds(ancestorsBetween(latestSnapshotId, oldestSnapshotId, lookup));
  }

  public static Iterable<Snapshot> ancestorsBetween(long latestSnapshotId, Long oldestSnapshotId,
                                                    Function<Long, Snapshot> lookup) {
    if (oldestSnapshotId != null) {
      if (latestSnapshotId == oldestSnapshotId) {
        return ImmutableList.of();
      }

      return ancestorsOf(latestSnapshotId,
          snapshotId -> !oldestSnapshotId.equals(snapshotId) ? lookup.apply(snapshotId) : null);
    } else {
      return ancestorsOf(latestSnapshotId, lookup);
    }
  }

  private static Iterable<Snapshot> ancestorsOf(Snapshot snapshot, Function<Long, Snapshot> lookup) {
    if (snapshot != null) {
      return () -> new Iterator<Snapshot>() {
        private Snapshot next = snapshot;
        private boolean consumed = false; // include the snapshot in its history

        @Override
        public boolean hasNext() {
          if (!consumed) {
            return true;
          }

          Long parentId = next.parentId();
          if (parentId == null) {
            return false;
          }

          this.next = lookup.apply(parentId);
          if (next != null) {
            this.consumed = false;
            return true;
          }

          return false;
        }

        @Override
        public Snapshot next() {
          if (hasNext()) {
            this.consumed = true;
            return next;
          }

          throw new NoSuchElementException();
        }
      };

    } else {
      return ImmutableList.of();
    }
  }

  public static List<Long> ancestorIds(Snapshot snapshot, Function<Long, Snapshot> lookup) {
    return Lists.newArrayList(toIds(ancestorsOf(snapshot, lookup)));
  }

  private static Iterable<Long> toIds(Iterable<Snapshot> snapshots) {
    return Iterables.transform(snapshots, Snapshot::snapshotId);
  }

  public static List<DataFile> newFiles(Long baseSnapshotId, long latestSnapshotId, Function<Long, Snapshot> lookup) {
    List<DataFile> newFiles = Lists.newArrayList();
    Snapshot lastSnapshot = null;
    for (Snapshot currentSnapshot : ancestorsOf(latestSnapshotId, lookup)) {
      lastSnapshot = currentSnapshot;
      if (Objects.equals(currentSnapshot.snapshotId(), baseSnapshotId)) {
        return newFiles;
      }

      Iterables.addAll(newFiles, currentSnapshot.addedFiles());
    }

    ValidationException.check(Objects.equals(lastSnapshot.parentId(), baseSnapshotId),
        "Cannot determine history between read snapshot %s and the last known ancestor %s",
        baseSnapshotId, lastSnapshot.snapshotId());

    return newFiles;
  }

  /**
   * Traverses the history of the table's current snapshot and finds the snapshot with the given snapshot id as its
   * parent.
   * @return the snapshot for which the given snapshot is the parent
   * @throws IllegalArgumentException when the given snapshotId is not found in the table
   * @throws IllegalStateException when the given snapshotId is not an ancestor of the current table state
   */
  public static Snapshot snapshotAfter(Table table, long snapshotId) {
    Preconditions.checkArgument(table.snapshot(snapshotId) != null, "Cannot find parent snapshot: %s", snapshotId);
    for (Snapshot current : currentAncestors(table)) {
      if (current.parentId() == snapshotId) {
        return current;
      }
    }

    throw new IllegalStateException(
        String.format("Cannot find snapshot after %s: not an ancestor of table's current snapshot", snapshotId));
  }

  /**
   * Returns the ID of the most recent snapshot for the table as of the timestamp.
   *
   * @param table a {@link Table}
   * @param timestampMillis the timestamp in millis since the Unix epoch
   * @return the snapshot ID
   * @throws IllegalArgumentException when no snapshot is found in the table
   * older than the timestamp
   */
  public static long snapshotIdAsOfTime(Table table, long timestampMillis) {
    Long snapshotId = null;
    for (HistoryEntry logEntry : table.history()) {
      if (logEntry.timestampMillis() <= timestampMillis) {
        snapshotId = logEntry.snapshotId();
      }
    }

    Preconditions.checkArgument(snapshotId != null,
        "Cannot find a snapshot older than %s", DateTimeUtil.formatTimestampMillis(timestampMillis));
    return snapshotId;
  }

  /**
   * Returns the schema of the table for the specified snapshot.
   *
   * @param table a {@link Table}
   * @param snapshotId the ID of the snapshot
   * @return the schema
   */
  public static Schema schemaFor(Table table, long snapshotId) {
    Snapshot snapshot = table.snapshot(snapshotId);
    Preconditions.checkArgument(snapshot != null, "Cannot find snapshot with ID %s", snapshotId);
    Integer schemaId = snapshot.schemaId();

    // schemaId could be null, if snapshot was created before Iceberg added schema id to snapshot
    if (schemaId != null) {
      Schema schema = table.schemas().get(schemaId);
      Preconditions.checkState(schema != null,
          "Cannot find schema with schema id %s", schemaId);
      return schema;
    }

    // TODO: recover the schema by reading previous metadata files
    return table.schema();
  }
}
