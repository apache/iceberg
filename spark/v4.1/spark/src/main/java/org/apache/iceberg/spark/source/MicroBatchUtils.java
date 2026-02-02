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

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;

class MicroBatchUtils {

  private MicroBatchUtils() {}

  static StreamingOffset determineStartingOffset(Table table, long fromTimestamp) {
    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (fromTimestamp == Long.MIN_VALUE) {
      // match existing behavior and start from the oldest snapshot
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
    // If snapshotSummary doesn't have SnapshotSummary.ADDED_FILES_PROP,
    // iterate through addedFiles iterator to find addedFilesCount.
    return addedFilesCount == -1
        ? Iterables.size(snapshot.addedDataFiles(table.io()))
        : addedFilesCount;
  }
}
