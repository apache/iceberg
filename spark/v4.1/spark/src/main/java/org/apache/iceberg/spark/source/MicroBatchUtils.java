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
import org.apache.iceberg.spark.StartingOffset;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MicroBatchUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MicroBatchUtils.class);

  private MicroBatchUtils() {}

  static StreamingOffset determineStartingOffset(
      Table table, long fromTimestamp, StartingOffset startFrom) {
    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (fromTimestamp != Long.MIN_VALUE && startFrom != StartingOffset.EARLIEST) {
      LOG.warn(
          "Both stream-from-timestamp and streaming-start-from are set; "
              + "stream-from-timestamp takes precedence and streaming-start-from is ignored");
    }

    Snapshot startSnapshot;

    if (fromTimestamp != Long.MIN_VALUE) {
      // stream-from-timestamp overrides earliest/latest selection
      if (table.currentSnapshot().timestampMillis() < fromTimestamp) {
        return StreamingOffset.START_OFFSET;
      }

      try {
        startSnapshot = SnapshotUtil.oldestAncestorAfter(table, fromTimestamp);
        if (startSnapshot == null) {
          return StreamingOffset.START_OFFSET;
        }
      } catch (IllegalStateException e) {
        // could not determine the first snapshot after the timestamp; use the oldest ancestor
        startSnapshot = SnapshotUtil.oldestAncestor(table);
      }
    } else if (startFrom.useLatest()) {
      startSnapshot = table.currentSnapshot();
    } else {
      startSnapshot = SnapshotUtil.oldestAncestor(table);
    }

    // When scanAllFiles=false (LATEST mode), the stream iterates only addedDataFiles() for
    // the starting snapshot. Setting position = addedFilesCount skips all of them, so
    // processing begins only from the next snapshot's added files.
    long position = 0;
    if (startFrom == StartingOffset.LATEST) {
      long addedFiles =
          PropertyUtil.propertyAsLong(
              startSnapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, -1L);
      position =
          addedFiles == -1L ? Iterables.size(startSnapshot.addedDataFiles(table.io())) : addedFiles;
    }

    return new StreamingOffset(startSnapshot.snapshotId(), position, startFrom.scanAllFiles());
  }

  static long addedFilesCount(Table table, Snapshot snapshot) {
    long addedFilesCount =
        PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, -1);
    return addedFilesCount == -1
        ? Iterables.size(snapshot.addedDataFiles(table.io()))
        : addedFilesCount;
  }
}
