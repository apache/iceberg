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
package org.apache.iceberg.spark.source;

import java.util.Locale;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;

class MicroBatchUtils {

  private MicroBatchUtils() {}

  static StreamingOffset determineStartingOffset(
      Table table, long fromTimestamp, String fromSnapshot) {
    ValidationException.check(
        fromSnapshot == null || fromTimestamp == Long.MIN_VALUE,
        "Cannot set both '%s' and '%s' options",
        SparkReadOptions.STREAM_FROM_SNAPSHOT,
        SparkReadOptions.STREAM_FROM_TIMESTAMP);

    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (fromSnapshot != null) {
      return startingOffsetFromSnapshotOption(table, fromSnapshot);
    }

    if (fromTimestamp == Long.MIN_VALUE) {
      // read the initial snapshot in full, then continue with incremental changes
      Snapshot current = table.currentSnapshot();

      // Refuse row-level deletes rather than silently emit them
      ValidationException.check(
          current.deleteManifests(table.io()).isEmpty(),
          "Cannot stream initial snapshot %d in full: snapshot has row-level deletes "
              + "(V2 positional/equality delete files or V3 deletion vectors), which the "
              + "Iceberg streaming source does not apply. Set '%s' to one of: '%s' "
              + "(skip the backlog), '%s' (replay history snapshot-by-snapshot, with "
              + "'streaming-skip-overwrite-snapshots'/'streaming-skip-delete-snapshots' as "
              + "needed), or a specific snapshot id to start after.",
          current.snapshotId(),
          SparkReadOptions.STREAM_FROM_SNAPSHOT,
          SparkReadOptions.STREAM_FROM_SNAPSHOT_LATEST,
          SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST);
      return new StreamingOffset(current.snapshotId(), 0L, true);
    }

    if (table.currentSnapshot().timestampMillis() < fromTimestamp) {
      return StreamingOffset.START_OFFSET;
    }

    try {
      Snapshot snapshot = SnapshotUtil.oldestAncestorAfter(table, fromTimestamp);
      if (snapshot != null) {
        return new StreamingOffset(snapshot.snapshotId(), 0, false);
      } else {
        return StreamingOffset.START_OFFSET;
      }
    } catch (IllegalStateException e) {
      // could not determine the first snapshot after the timestamp. use the oldest ancestor instead
      return new StreamingOffset(SnapshotUtil.oldestAncestor(table).snapshotId(), 0, false);
    }
  }

  private static StreamingOffset startingOffsetFromSnapshotOption(
      Table table, String fromSnapshot) {
    String normalized = fromSnapshot.toLowerCase(Locale.ROOT);
    Snapshot currentSnapshot = table.currentSnapshot();

    if (SparkReadOptions.STREAM_FROM_SNAPSHOT_LATEST.equals(normalized)) {
      // start from current snapshot, skipping backlog
      return new StreamingOffset(
          currentSnapshot.snapshotId(), addedFilesCount(table, currentSnapshot), false);
    }

    if (SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST.equals(normalized)) {
      // start from oldest snapshot
      return new StreamingOffset(SnapshotUtil.oldestAncestor(table).snapshotId(), 0L, false);
    }

    long fromSnapshotId;
    try {
      fromSnapshotId = Long.parseLong(fromSnapshot);
    } catch (NumberFormatException e) {
      throw new ValidationException(
          "Invalid value for '%s': %s. Expected a snapshot id, '%s', or '%s'.",
          SparkReadOptions.STREAM_FROM_SNAPSHOT,
          fromSnapshot,
          SparkReadOptions.STREAM_FROM_SNAPSHOT_LATEST,
          SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST);
    }

    ValidationException.check(
        table.snapshot(fromSnapshotId) != null,
        "Cannot find snapshot for '%s': %s",
        SparkReadOptions.STREAM_FROM_SNAPSHOT,
        fromSnapshotId);
    ValidationException.check(
        SnapshotUtil.isAncestorOf(table, currentSnapshot.snapshotId(), fromSnapshotId),
        "Snapshot %s is not an ancestor of the current snapshot",
        fromSnapshotId);

    if (fromSnapshotId == currentSnapshot.snapshotId()) {
      // Requested snapshot is current — position past its end (same as `latest`)
      return new StreamingOffset(
          currentSnapshot.snapshotId(), addedFilesCount(table, currentSnapshot), false);
    }

    try {
      Snapshot next = SnapshotUtil.snapshotAfter(table, fromSnapshotId);
      return new StreamingOffset(next.snapshotId(), 0L, false);
    } catch (IllegalStateException e) {
      return StreamingOffset.START_OFFSET;
    }
  }

  static long addedFilesCount(Table table, Snapshot snapshot) {
    long addedFilesCount =
        PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, -1);
    return addedFilesCount == -1
        ? Iterables.size(
            SnapshotChanges.builderFor(table).snapshot(snapshot).build().addedDataFiles())
        : addedFilesCount;
  }

  static long totalFilesCount(Table table, Snapshot snapshot) {
    long total = 0L;
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      Integer added = manifest.addedFilesCount();
      Integer existing = manifest.existingFilesCount();
      total += (added == null ? 0L : (long) added) + (existing == null ? 0L : (long) existing);
    }
    return total;
  }

  static long endPositionFor(Table table, Snapshot snapshot, boolean scanAllFiles) {
    return scanAllFiles ? totalFilesCount(table, snapshot) : addedFilesCount(table, snapshot);
  }
}
