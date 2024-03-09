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
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class SnapshotUtil {
  private SnapshotUtil() {}

  /** Returns whether ancestorSnapshotId is an ancestor of snapshotId. */
  public static boolean isAncestorOf(Table table, long snapshotId, long ancestorSnapshotId) {
    for (Snapshot snapshot : ancestorsOf(snapshotId, table::snapshot)) {
      if (snapshot.snapshotId() == ancestorSnapshotId) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns whether ancestorSnapshotId is an ancestor of snapshotId using the given lookup
   * function.
   */
  public static boolean isAncestorOf(
      long snapshotId, long ancestorSnapshotId, Function<Long, Snapshot> lookup) {
    for (Snapshot snapshot : ancestorsOf(snapshotId, lookup)) {
      if (snapshot.snapshotId() == ancestorSnapshotId) {
        return true;
      }
    }
    return false;
  }

  /** Returns whether ancestorSnapshotId is an ancestor of the table's current state. */
  public static boolean isAncestorOf(Table table, long ancestorSnapshotId) {
    return isAncestorOf(table, table.currentSnapshot().snapshotId(), ancestorSnapshotId);
  }

  /** Returns whether some ancestor of snapshotId has parentId matches ancestorParentSnapshotId */
  public static boolean isParentAncestorOf(
      Table table, long snapshotId, long ancestorParentSnapshotId) {
    for (Snapshot snapshot : ancestorsOf(snapshotId, table::snapshot)) {
      if (snapshot.parentId() != null && snapshot.parentId() == ancestorParentSnapshotId) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns an iterable that traverses the table's snapshots from the current to the last known
   * ancestor.
   *
   * @param table a Table
   * @return an iterable from the table's current snapshot to its last known ancestor
   */
  public static Iterable<Snapshot> currentAncestors(Table table) {
    return ancestorsOf(table.currentSnapshot(), table::snapshot);
  }

  /**
   * Return the snapshot IDs for the ancestors of the current table state.
   *
   * <p>Ancestor IDs are ordered by commit time, descending. The first ID is the current snapshot,
   * followed by its parent, and so on.
   *
   * @param table a {@link Table}
   * @return a set of snapshot IDs of the known ancestor snapshots, including the current ID
   */
  public static List<Long> currentAncestorIds(Table table) {
    return ancestorIds(table.currentSnapshot(), table::snapshot);
  }

  /**
   * Traverses the history of the table's current snapshot and finds the oldest Snapshot.
   *
   * @return null if there is no current snapshot in the table, else the oldest Snapshot.
   */
  public static Snapshot oldestAncestor(Table table) {
    Snapshot lastSnapshot = null;
    for (Snapshot snapshot : currentAncestors(table)) {
      lastSnapshot = snapshot;
    }

    return lastSnapshot;
  }

  public static Snapshot oldestAncestorOf(Table table, long snapshotId) {
    return oldestAncestorOf(snapshotId, table::snapshot);
  }

  /**
   * Traverses the history and finds the oldest ancestor of the specified snapshot.
   *
   * <p>Oldest ancestor is defined as the ancestor snapshot whose parent is null or has been
   * expired. If the specified snapshot has no parent or parent has been expired, the specified
   * snapshot itself is returned.
   *
   * @param snapshotId the ID of the snapshot to find the oldest ancestor
   * @param lookup lookup function from snapshot ID to snapshot
   * @return null if there is no current snapshot in the table, else the oldest Snapshot.
   */
  public static Snapshot oldestAncestorOf(long snapshotId, Function<Long, Snapshot> lookup) {
    Snapshot lastSnapshot = null;
    for (Snapshot snapshot : ancestorsOf(snapshotId, lookup)) {
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
   * Traverses the history of the table's current snapshot, finds the oldest snapshot that was
   * committed either at or after a given time.
   *
   * @param table a table
   * @param timestampMillis a timestamp in milliseconds
   * @return the first snapshot after the given timestamp, or null if the current snapshot is older
   *     than the timestamp
   * @throws IllegalStateException if the first ancestor after the given time can't be determined
   */
  public static Snapshot oldestAncestorAfter(Table table, long timestampMillis) {
    if (table.currentSnapshot() == null) {
      // there are no snapshots or ancestors
      return null;
    }

    Snapshot lastSnapshot = null;
    for (Snapshot snapshot : currentAncestors(table)) {
      if (snapshot.timestampMillis() < timestampMillis) {
        return lastSnapshot;
      } else if (snapshot.timestampMillis() == timestampMillis) {
        return snapshot;
      }

      lastSnapshot = snapshot;
    }

    if (lastSnapshot != null && lastSnapshot.parentId() == null) {
      // this is the first snapshot in the table, return it
      return lastSnapshot;
    }

    throw new IllegalStateException(
        "Cannot find snapshot older than " + DateTimeUtil.formatTimestampMillis(timestampMillis));
  }

  /**
   * Returns list of snapshot ids in the range - (fromSnapshotId, toSnapshotId]
   *
   * <p>This method assumes that fromSnapshotId is an ancestor of toSnapshotId.
   */
  public static List<Long> snapshotIdsBetween(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Long> snapshotIds =
        Lists.newArrayList(
            ancestorIds(
                table.snapshot(toSnapshotId),
                snapshotId -> snapshotId != fromSnapshotId ? table.snapshot(snapshotId) : null));
    return snapshotIds;
  }

  public static Iterable<Long> ancestorIdsBetween(
      long latestSnapshotId, Long oldestSnapshotId, Function<Long, Snapshot> lookup) {
    return toIds(ancestorsBetween(latestSnapshotId, oldestSnapshotId, lookup));
  }

  public static Iterable<Snapshot> ancestorsBetween(
      Table table, long latestSnapshotId, Long oldestSnapshotId) {
    return ancestorsBetween(latestSnapshotId, oldestSnapshotId, table::snapshot);
  }

  public static Iterable<Snapshot> ancestorsBetween(
      long latestSnapshotId, Long oldestSnapshotId, Function<Long, Snapshot> lookup) {
    if (oldestSnapshotId != null) {
      if (latestSnapshotId == oldestSnapshotId) {
        return ImmutableList.of();
      }

      return ancestorsOf(
          latestSnapshotId,
          snapshotId -> !oldestSnapshotId.equals(snapshotId) ? lookup.apply(snapshotId) : null);
    } else {
      return ancestorsOf(latestSnapshotId, lookup);
    }
  }

  private static Iterable<Snapshot> ancestorsOf(
      Snapshot snapshot, Function<Long, Snapshot> lookup) {
    if (snapshot != null) {
      return () ->
          new Iterator<Snapshot>() {
            private Snapshot next = snapshot;
            private boolean consumed = false; // include the snapshot in its history

            @Override
            public boolean hasNext() {
              if (!consumed) {
                return true;
              }

              if (next == null) {
                return false;
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

  public static List<DataFile> newFiles(
      Long baseSnapshotId, long latestSnapshotId, Function<Long, Snapshot> lookup, FileIO io) {
    List<DataFile> newFiles = Lists.newArrayList();
    Snapshot lastSnapshot = null;
    for (Snapshot currentSnapshot : ancestorsOf(latestSnapshotId, lookup)) {
      lastSnapshot = currentSnapshot;
      if (Objects.equals(currentSnapshot.snapshotId(), baseSnapshotId)) {
        return newFiles;
      }

      Iterables.addAll(newFiles, currentSnapshot.addedDataFiles(io));
    }

    ValidationException.check(
        Objects.equals(lastSnapshot.parentId(), baseSnapshotId),
        "Cannot determine history between read snapshot %s and the last known ancestor %s",
        baseSnapshotId,
        lastSnapshot.snapshotId());

    return newFiles;
  }

  /**
   * Traverses the history of the table's current snapshot and finds the snapshot with the given
   * snapshot id as its parent.
   *
   * @return the snapshot for which the given snapshot is the parent
   * @throws IllegalArgumentException when the given snapshotId is not found in the table
   * @throws IllegalStateException when the given snapshotId is not an ancestor of the current table
   *     state
   */
  public static Snapshot snapshotAfter(Table table, long snapshotId) {
    Preconditions.checkArgument(
        table.snapshot(snapshotId) != null, "Cannot find parent snapshot: %s", snapshotId);
    for (Snapshot current : currentAncestors(table)) {
      if (current.parentId() == snapshotId) {
        return current;
      }
    }

    throw new IllegalStateException(
        String.format(
            "Cannot find snapshot after %s: not an ancestor of table's current snapshot",
            snapshotId));
  }

  /**
   * Returns the ID of the most recent snapshot for the table as of the timestamp.
   *
   * @param table a {@link Table}
   * @param timestampMillis the timestamp in millis since the Unix epoch
   * @return the snapshot ID
   * @throws IllegalArgumentException when no snapshot is found in the table older than the
   *     timestamp
   */
  public static long snapshotIdAsOfTime(Table table, long timestampMillis) {
    Long snapshotId = nullableSnapshotIdAsOfTime(table, timestampMillis);

    Preconditions.checkArgument(
        snapshotId != null,
        "Cannot find a snapshot older than %s",
        DateTimeUtil.formatTimestampMillis(timestampMillis));

    return snapshotId;
  }

  public static Long nullableSnapshotIdAsOfTime(Table table, long timestampMillis) {
    Long snapshotId = null;
    for (HistoryEntry logEntry : table.history()) {
      if (logEntry.timestampMillis() <= timestampMillis) {
        snapshotId = logEntry.snapshotId();
      }
    }

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
      Preconditions.checkState(schema != null, "Cannot find schema with schema id %s", schemaId);
      return schema;
    }

    // TODO: recover the schema by reading previous metadata files
    return table.schema();
  }

  /**
   * Convenience method for returning the schema of the table for a snapshot, when we have a
   * snapshot id or a timestamp. Only one of them should be specified (non-null), or an
   * IllegalArgumentException is thrown.
   *
   * @param table a {@link Table}
   * @param snapshotId the ID of the snapshot
   * @param timestampMillis the timestamp in millis since the Unix epoch
   * @return the schema
   * @throws IllegalArgumentException if both snapshotId and timestampMillis are non-null
   */
  public static Schema schemaFor(Table table, Long snapshotId, Long timestampMillis) {
    Preconditions.checkArgument(
        snapshotId == null || timestampMillis == null,
        "Cannot use both snapshot id and timestamp to find a schema");

    if (snapshotId != null) {
      return schemaFor(table, snapshotId);
    }

    if (timestampMillis != null) {
      return schemaFor(table, snapshotIdAsOfTime(table, timestampMillis));
    }

    return table.schema();
  }

  /**
   * Return the schema of the snapshot at a given ref.
   *
   * <p>If the ref does not exist or the ref is a branch, the table schema is returned because it
   * will be the schema when the new branch is created. If the ref is a tag, then the snapshot
   * schema is returned.
   *
   * @param table a {@link Table}
   * @param ref ref name of the table (nullable)
   * @return schema of the specific snapshot at the given ref
   */
  public static Schema schemaFor(Table table, String ref) {
    if (ref == null || ref.equals(SnapshotRef.MAIN_BRANCH)) {
      return table.schema();
    }

    SnapshotRef snapshotRef = table.refs().get(ref);
    if (null == snapshotRef || snapshotRef.isBranch()) {
      return table.schema();
    }

    return schemaFor(table, snapshotRef.snapshotId());
  }

  /**
   * Return the schema of the snapshot at a given ref.
   *
   * <p>If the ref does not exist or the ref is a branch, the table schema is returned because it
   * will be the schema when the new branch is created. If the ref is a tag, then the snapshot
   * schema is returned.
   *
   * @param metadata a {@link TableMetadata}
   * @param ref ref name of the table (nullable)
   * @return schema of the specific snapshot at the given branch
   */
  public static Schema schemaFor(TableMetadata metadata, String ref) {
    if (ref == null || ref.equals(SnapshotRef.MAIN_BRANCH)) {
      return metadata.schema();
    }

    SnapshotRef snapshotRef = metadata.ref(ref);
    if (snapshotRef == null || snapshotRef.isBranch()) {
      return metadata.schema();
    }

    Snapshot snapshot = metadata.snapshot(snapshotRef.snapshotId());
    return metadata.schemas().get(snapshot.schemaId());
  }

  /**
   * Fetch the snapshot at the head of the given branch in the given table.
   *
   * <p>This method calls {@link Table#currentSnapshot()} instead of using branch API {@link
   * Table#snapshot(String)} for the main branch so that existing code still goes through the old
   * code path to ensure backwards compatibility.
   *
   * @param table a {@link Table}
   * @param branch branch name of the table (nullable)
   * @return the latest snapshot for the given branch
   */
  public static Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null || branch.equals(SnapshotRef.MAIN_BRANCH)) {
      return table.currentSnapshot();
    }

    return table.snapshot(branch);
  }

  /**
   * Fetch the snapshot at the head of the given branch in the given table.
   *
   * <p>This method calls {@link TableMetadata#currentSnapshot()} instead of using branch API {@link
   * TableMetadata#ref(String)}} for the main branch so that existing code still goes through the
   * old code path to ensure backwards compatibility.
   *
   * <p>If branch does not exist, the table's latest snapshot is returned it will be the schema when
   * the new branch is created.
   *
   * @param metadata a {@link TableMetadata}
   * @param branch branch name of the table metadata (nullable)
   * @return the latest snapshot for the given branch
   */
  public static Snapshot latestSnapshot(TableMetadata metadata, String branch) {
    if (branch == null || branch.equals(SnapshotRef.MAIN_BRANCH)) {
      return metadata.currentSnapshot();
    }

    SnapshotRef ref = metadata.ref(branch);
    if (ref == null) {
      return metadata.currentSnapshot();
    }

    return metadata.snapshot(ref.snapshotId());
  }
}
