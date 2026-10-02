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
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;

class MicroBatchUtils {

  private MicroBatchUtils() {}

  static StreamingOffset determineInitialOffset(
      Table table, long fromTimestamp, String fromSnapshot) {
    Snapshot currentSnapshot = table.currentSnapshot();
    if (fromSnapshot == null) {
      return currentSnapshot != null && fromTimestamp == Long.MIN_VALUE
          ? new StreamingOffset(currentSnapshot.snapshotId(), 0L, true)
          : determineStartingOffset(table, fromTimestamp);
    }

    Preconditions.checkArgument(
        fromTimestamp == Long.MIN_VALUE,
        "Cannot set both %s and %s",
        SparkReadOptions.STREAM_FROM_SNAPSHOT,
        SparkReadOptions.STREAM_FROM_TIMESTAMP);

    String option = fromSnapshot.toLowerCase(Locale.ROOT);
    if (SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST.equals(option)) {
      return determineStartingOffset(table, fromTimestamp);
    }

    if (SparkReadOptions.STREAM_FROM_SNAPSHOT_LATEST.equals(option)) {
      // every file of the current snapshot counts as read, so the stream starts with the next one
      return currentSnapshot != null
          ? new StreamingOffset(
              currentSnapshot.snapshotId(), endPosition(table, currentSnapshot, true), true)
          : StreamingOffset.START_OFFSET;
    }

    long snapshotId;
    try {
      snapshotId = Long.parseLong(fromSnapshot);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value for %s: %s (supported: a snapshot ID, %s, %s)",
              SparkReadOptions.STREAM_FROM_SNAPSHOT,
              fromSnapshot,
              SparkReadOptions.STREAM_FROM_SNAPSHOT_LATEST,
              SparkReadOptions.STREAM_FROM_SNAPSHOT_EARLIEST),
          e);
    }

    Preconditions.checkArgument(
        table.snapshot(snapshotId) != null,
        "Cannot find snapshot for %s: %s",
        SparkReadOptions.STREAM_FROM_SNAPSHOT,
        snapshotId);
    Preconditions.checkArgument(
        currentSnapshot != null && SnapshotUtil.isAncestorOf(table, snapshotId),
        "Cannot stream from snapshot %s: not an ancestor of the current snapshot",
        snapshotId);
    return new StreamingOffset(snapshotId, 0L, false);
  }

  static StreamingOffset determineStartingOffset(Table table, long fromTimestamp) {
    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (fromTimestamp == Long.MIN_VALUE) {
      // start from the oldest snapshot, since default value is MIN_VALUE
      // avoids looping to find first snapshot
      return new StreamingOffset(SnapshotUtil.oldestAncestor(table).snapshotId(), 0, false);
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

  static long addedFilesCount(Table table, Snapshot snapshot) {
    long addedFilesCount =
        PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, -1);
    return addedFilesCount == -1
        ? Iterables.size(
            SnapshotChanges.builderFor(table).snapshot(snapshot).build().addedDataFiles())
        : addedFilesCount;
  }

  static long endPosition(Table table, Snapshot snapshot, boolean scanAllFiles) {
    return scanAllFiles
        ? MicroBatches.from(snapshot, table.io()).fullScanFileCount()
        : addedFilesCount(table, snapshot);
  }
}
