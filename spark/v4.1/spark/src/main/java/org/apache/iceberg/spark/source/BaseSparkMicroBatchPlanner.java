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
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.connector.read.streaming.CompositeReadLimit;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.ReadMaxFiles;
import org.apache.spark.sql.connector.read.streaming.ReadMaxRows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseSparkMicroBatchPlanner implements SparkMicroBatchPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(BaseSparkMicroBatchPlanner.class);
  private final Table table;
  private final SparkReadConf readConf;

  BaseSparkMicroBatchPlanner(Table table, SparkReadConf readConf) {
    this.table = table;
    this.readConf = readConf;
  }

  protected Table table() {
    return table;
  }

  protected SparkReadConf readConf() {
    return readConf;
  }

  protected boolean shouldProcess(Snapshot snapshot) {
    String op = snapshot.operation();
    switch (op) {
      case DataOperations.APPEND:
        return true;
      case DataOperations.REPLACE:
        return false;
      case DataOperations.DELETE:
        Preconditions.checkState(
            readConf.streamingSkipDeleteSnapshots(),
            "Cannot process delete snapshot: %s, to ignore deletes, set %s=true",
            snapshot.snapshotId(),
            SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS);
        return false;
      case DataOperations.OVERWRITE:
        Preconditions.checkState(
            readConf.streamingSkipOverwriteSnapshots(),
            "Cannot process overwrite snapshot: %s, to ignore overwrites, set %s=true",
            snapshot.snapshotId(),
            SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS);
        return false;
      default:
        throw new IllegalStateException(
            String.format(
                "Cannot process unknown snapshot operation: %s (snapshot id %s)",
                op.toLowerCase(Locale.ROOT), snapshot.snapshotId()));
    }
  }

  /**
   * Get the next snapshot skipping over rewrite and delete snapshots. Async must handle nulls.
   *
   * @param curSnapshot the current snapshot
   * @return the next valid snapshot (not a rewrite or delete snapshot), returns null if all
   *     remaining snapshots should be skipped.
   */
  protected Snapshot nextValidSnapshot(Snapshot curSnapshot) {
    Snapshot nextSnapshot;
    // if there were no valid snapshots, check for an initialOffset again
    if (curSnapshot == null) {
      StreamingOffset startingOffset =
          MicroBatchUtils.determineStartingOffset(table, readConf.streamFromTimestamp());
      LOG.debug("determineStartingOffset picked startingOffset: {}", startingOffset);
      if (StreamingOffset.START_OFFSET.equals(startingOffset)) {
        return null;
      }
      nextSnapshot = table.snapshot(startingOffset.snapshotId());
    } else {
      if (curSnapshot.snapshotId() == table.currentSnapshot().snapshotId()) {
        return null;
      }
      nextSnapshot = SnapshotUtil.snapshotAfter(table, curSnapshot.snapshotId());
    }
    // skip over rewrite and delete snapshots
    while (!shouldProcess(nextSnapshot)) {
      LOG.debug("Skipping snapshot: {}", nextSnapshot);
      // if the currentSnapShot was also the mostRecentSnapshot then break
      // avoids snapshotAfter throwing exception since there are no more snapshots to process
      if (nextSnapshot.snapshotId() == table.currentSnapshot().snapshotId()) {
        return null;
      }
      nextSnapshot = SnapshotUtil.snapshotAfter(table, nextSnapshot.snapshotId());
    }
    return nextSnapshot;
  }

  static class UnpackedLimits {
    private long maxRows = Integer.MAX_VALUE;
    private long maxFiles = Integer.MAX_VALUE;

    UnpackedLimits(ReadLimit limit) {
      if (limit instanceof CompositeReadLimit) {
        ReadLimit[] compositeLimits = ((CompositeReadLimit) limit).getReadLimits();
        for (ReadLimit individualLimit : compositeLimits) {
          if (individualLimit instanceof ReadMaxRows) {
            ReadMaxRows readMaxRows = (ReadMaxRows) individualLimit;
            this.maxRows = Math.min(this.maxRows, readMaxRows.maxRows());
          } else if (individualLimit instanceof ReadMaxFiles) {
            ReadMaxFiles readMaxFiles = (ReadMaxFiles) individualLimit;
            this.maxFiles = Math.min(this.maxFiles, readMaxFiles.maxFiles());
          }
        }
      } else if (limit instanceof ReadMaxRows) {
        this.maxRows = ((ReadMaxRows) limit).maxRows();
      } else if (limit instanceof ReadMaxFiles) {
        this.maxFiles = ((ReadMaxFiles) limit).maxFiles();
      }
    }

    public long getMaxRows() {
      return maxRows;
    }

    public long getMaxFiles() {
      return maxFiles;
    }
  }
}
