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

import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class SnapshotUtil {
  private SnapshotUtil() {
  }

  /**
   * Returns whether ancestorSnapshotId is an ancestor of snapshotId.
   */
  public static boolean ancestorOf(Table table, long snapshotId, long ancestorSnapshotId) {
    Snapshot current = table.snapshot(snapshotId);
    while (current != null) {
      long id = current.snapshotId();
      if (ancestorSnapshotId == id) {
        return true;
      } else if (current.parentId() != null) {
        current = table.snapshot(current.parentId());
      } else {
        return false;
      }
    }
    return false;
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
  public static List<Long> currentAncestors(Table table) {
    return ancestorIds(table.currentSnapshot(), table::snapshot);
  }

  /**
   * Traverses the history of the table's current snapshot and finds the oldest Snapshot.
   * @return null if there is no current snapshot in the table, else the oldest Snapshot.
   */
  public static Snapshot oldestSnapshot(Table table) {
    Snapshot current = table.currentSnapshot();
    while (current != null && current.parentId() != null) {
      current = table.snapshot(current.parentId());
    }

    return current;
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

  public static List<Long> ancestorIds(Snapshot snapshot, Function<Long, Snapshot> lookup) {
    List<Long> ancestorIds = Lists.newArrayList();
    Snapshot current = snapshot;
    while (current != null) {
      ancestorIds.add(current.snapshotId());
      if (current.parentId() != null) {
        current = lookup.apply(current.parentId());
      } else {
        current = null;
      }
    }
    return ancestorIds;
  }

  public static List<DataFile> newFiles(Long baseSnapshotId, long latestSnapshotId, Function<Long, Snapshot> lookup) {
    List<DataFile> newFiles = Lists.newArrayList();

    Long currentSnapshotId = latestSnapshotId;
    while (currentSnapshotId != null && !currentSnapshotId.equals(baseSnapshotId)) {
      Snapshot currentSnapshot = lookup.apply(currentSnapshotId);

      if (currentSnapshot == null) {
        throw new ValidationException(
            "Cannot determine history between read snapshot %s and current %s",
            baseSnapshotId, currentSnapshotId);
      }

      Iterables.addAll(newFiles, currentSnapshot.addedFiles());
      currentSnapshotId = currentSnapshot.parentId();
    }

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

    Snapshot current = table.currentSnapshot();
    while (current != null) {
      if (current.parentId() == snapshotId) {
        return current;
      }

      current = table.snapshot(current.parentId());
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
