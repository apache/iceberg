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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.connector.read.streaming.CompositeReadLimit;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.ReadMaxFiles;
import org.apache.spark.sql.connector.read.streaming.ReadMaxRows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncSparkMicroBatchPlanner implements SparkMicroBatchPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(SyncSparkMicroBatchPlanner.class);

  private final Table table;
  private final SparkReadConf readConf;
  private final boolean caseSensitive;
  private final boolean skipDelete;
  private final boolean skipOverwrite;
  private final long fromTimestamp;
  private final StreamingOffset lastOffsetForTriggerAvailableNow;

  public SyncSparkMicroBatchPlanner(
      Table table, SparkReadConf readConf, StreamingOffset lastOffsetForTriggerAvailableNow) {
    this.table = table;
    this.readConf = readConf;
    this.caseSensitive = readConf.caseSensitive();
    this.skipDelete = readConf.streamingSkipDeleteSnapshots();
    this.skipOverwrite = readConf.streamingSkipOverwriteSnapshots();
    this.fromTimestamp = readConf.streamFromTimestamp();
    this.lastOffsetForTriggerAvailableNow = lastOffsetForTriggerAvailableNow;
  }

  @Override
  public List<FileScanTask> planFiles(StreamingOffset start, StreamingOffset end) {
    List<FileScanTask> fileScanTasks = Lists.newArrayList();
    StreamingOffset batchStartOffset =
        StreamingOffset.START_OFFSET.equals(start)
            ? SparkMicroBatchStream.computeInitialOffset(table, fromTimestamp)
            : start;

    StreamingOffset currentOffset = null;

    // [(startOffset : startFileIndex), (endOffset : endFileIndex) )
    do {
      long endFileIndex;
      if (currentOffset == null) {
        currentOffset = batchStartOffset;
      } else {
        Snapshot snapshotAfter = SnapshotUtil.snapshotAfter(table, currentOffset.snapshotId());
        // it may happen that we need to read this snapshot partially in case it's equal to
        // endOffset.
        if (currentOffset.snapshotId() != end.snapshotId()) {
          currentOffset =
              new StreamingOffset(
                  snapshotAfter.snapshotId(),
                  0L,
                  false,
                  snapshotAfter.timestampMillis(),
                  PropertyUtil.propertyAsLong(
                      snapshotAfter.summary(), SnapshotSummary.TOTAL_RECORDS_PROP, -1));
        } else {
          currentOffset = end;
        }
      }

      Snapshot snapshot = table.snapshot(currentOffset.snapshotId());

      validateCurrentSnapshotExists(snapshot, currentOffset);

      if (!shouldProcess(snapshot)) {
        LOG.debug("Skipping snapshot: {} of table {}", currentOffset.snapshotId(), table.name());
        continue;
      }

      Snapshot currentSnapshot = table.snapshot(currentOffset.snapshotId());
      if (currentOffset.snapshotId() == end.snapshotId()) {
        endFileIndex = end.position();
      } else {
        endFileIndex = addedFilesCount(currentSnapshot);
      }

      MicroBatch latestMicroBatch =
          MicroBatches.from(currentSnapshot, table.io())
              .caseSensitive(caseSensitive)
              .specsById(table.specs())
              .generate(
                  currentOffset.position(),
                  endFileIndex,
                  Long.MAX_VALUE,
                  currentOffset.shouldScanAllFiles());

      fileScanTasks.addAll(latestMicroBatch.tasks());
    } while (currentOffset.snapshotId() != end.snapshotId());

    return fileScanTasks;
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public StreamingOffset latestOffset(StreamingOffset start, ReadLimit limit) {
    // calculate end offset get snapshotId from the startOffset
    Preconditions.checkArgument(start != null, "Invalid start offset: %s", start);

    table.refresh();
    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (table.currentSnapshot().timestampMillis() < fromTimestamp) {
      return StreamingOffset.START_OFFSET;
    }

    // end offset can expand to multiple snapshots
    StreamingOffset startingOffset = start;

    if (start.equals(StreamingOffset.START_OFFSET)) {
      startingOffset = SparkMicroBatchStream.computeInitialOffset(table, fromTimestamp);
    }

    Snapshot curSnapshot = table.snapshot(startingOffset.snapshotId());
    validateCurrentSnapshotExists(curSnapshot, startingOffset);

    // Use the pre-computed snapshotId when Trigger.AvailableNow is enabled.
    long latestSnapshotId =
        lastOffsetForTriggerAvailableNow != null
            ? lastOffsetForTriggerAvailableNow.snapshotId()
            : table.currentSnapshot().snapshotId();

    int startPosOfSnapOffset = (int) startingOffset.position();

    boolean scanAllFiles = startingOffset.shouldScanAllFiles();

    boolean shouldContinueReading = true;
    int curFilesAdded = 0;
    long curRecordCount = 0;
    int curPos = 0;

    // Extract limits once to avoid repeated calls in tight loop
    int maxFiles = getMaxFiles(limit);
    int maxRows = getMaxRows(limit);

    // Note : we produce nextOffset with pos as non-inclusive
    while (shouldContinueReading) {
      // generate manifest index for the curSnapshot
      List<Pair<ManifestFile, Integer>> indexedManifests =
          MicroBatches.skippedManifestIndexesFromSnapshot(
              table.io(), curSnapshot, startPosOfSnapOffset, scanAllFiles);
      // this is under assumption we will be able to add at-least 1 file in the new offset
      for (int idx = 0; idx < indexedManifests.size() && shouldContinueReading; idx++) {
        // be rest assured curPos >= startFileIndex
        curPos = indexedManifests.get(idx).second();
        try (CloseableIterable<FileScanTask> taskIterable =
                MicroBatches.openManifestFile(
                    table.io(),
                    table.specs(),
                    caseSensitive,
                    curSnapshot,
                    indexedManifests.get(idx).first(),
                    scanAllFiles);
            CloseableIterator<FileScanTask> taskIter = taskIterable.iterator()) {
          while (taskIter.hasNext()) {
            FileScanTask task = taskIter.next();
            if (curPos >= startPosOfSnapOffset) {
              if ((curFilesAdded + 1) > maxFiles) {
                // On including the file it might happen that we might exceed, the configured
                // soft limit on the number of records, since this is a soft limit its acceptable.
                shouldContinueReading = false;
                break;
              }

              curFilesAdded += 1;
              curRecordCount += task.file().recordCount();

              if (curRecordCount >= maxRows) {
                // we included the file, so increment the number of files
                // read in the current snapshot.
                if (curFilesAdded == 1 && curRecordCount > maxRows) {
                  LOG.warn(
                      "File {} contains {} records, exceeding maxRecordsPerMicroBatch limit of {}. "
                          + "This file will be processed entirely to guarantee forward progress. "
                          + "Consider increasing the limit or writing smaller files to avoid unexpected memory usage.",
                      task.file().path(),
                      task.file().recordCount(),
                      maxRows);
                }
                ++curPos;
                shouldContinueReading = false;
                break;
              }
            }
            ++curPos;
          }
        } catch (IOException ioe) {
          LOG.warn("Failed to close task iterable", ioe);
        }
      }
      // if the currentSnapShot was also the latestSnapshot then break
      if (curSnapshot.snapshotId() == latestSnapshotId) {
        break;
      }

      // if everything was OK and we consumed complete snapshot then move to next snapshot
      if (shouldContinueReading) {
        Snapshot nextValid = nextValidSnapshot(curSnapshot);
        if (nextValid == null) {
          // nextValid implies all the remaining snapshots should be skipped.
          break;
        }
        // we found the next available snapshot, continue from there.
        curSnapshot = nextValid;
        startPosOfSnapOffset = -1;
        // if anyhow we are moving to next snapshot we should only scan addedFiles
        scanAllFiles = false;
      }
    }

    StreamingOffset latestStreamingOffset =
        new StreamingOffset(
            curSnapshot.snapshotId(),
            curPos,
            scanAllFiles,
            curSnapshot.timestampMillis(),
            PropertyUtil.propertyAsLong(
                curSnapshot.summary(), SnapshotSummary.TOTAL_RECORDS_PROP, -1));

    // if no new data arrived, then return null.
    return latestStreamingOffset.equals(startingOffset) ? null : latestStreamingOffset;
  }

  @Override
  public Offset reportLatestOffset() {
    return null;
  }

  @Override
  public void stop() {
    // No-op for synchronous planner
  }

  private boolean shouldProcess(Snapshot snapshot) {
    String op = snapshot.operation();
    switch (op) {
      case DataOperations.APPEND:
        return true;
      case DataOperations.REPLACE:
        return false;
      case DataOperations.DELETE:
        Preconditions.checkState(
            skipDelete,
            "Cannot process delete snapshot: %s, to ignore deletes, set %s=true",
            snapshot.snapshotId(),
            SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS);
        return false;
      case DataOperations.OVERWRITE:
        Preconditions.checkState(
            skipOverwrite,
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
   * Get the next snapshot skiping over rewrite and delete snapshots.
   *
   * @param curSnapshot the current snapshot
   * @return the next valid snapshot (not a rewrite or delete snapshot), returns null if all
   *     remaining snapshots should be skipped.
   */
  private Snapshot nextValidSnapshot(Snapshot curSnapshot) {
    Snapshot nextSnapshot = SnapshotUtil.snapshotAfter(table, curSnapshot.snapshotId());
    // skip over rewrite and delete snapshots
    while (!shouldProcess(nextSnapshot)) {
      LOG.debug("Skipping snapshot: {} of table {}", nextSnapshot.snapshotId(), table.name());
      // if the currentSnapShot was also the mostRecentSnapshot then break
      if (nextSnapshot.snapshotId() == table.currentSnapshot().snapshotId()) {
        return null;
      }
      nextSnapshot = SnapshotUtil.snapshotAfter(table, nextSnapshot.snapshotId());
    }
    return nextSnapshot;
  }

  private long addedFilesCount(Snapshot snapshot) {
    long addedFilesCount =
        PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, -1);
    // If snapshotSummary doesn't have SnapshotSummary.ADDED_FILES_PROP,
    // iterate through addedFiles iterator to find addedFilesCount.
    return addedFilesCount == -1
        ? Iterables.size(snapshot.addedDataFiles(table.io()))
        : addedFilesCount;
  }

  private void validateCurrentSnapshotExists(Snapshot snapshot, StreamingOffset currentOffset) {
    if (snapshot == null) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Cannot load current offset at snapshot %d, the snapshot was expired or removed",
              currentOffset.snapshotId()));
    }
  }

  private static int getMaxFiles(ReadLimit readLimit) {
    if (readLimit instanceof ReadMaxFiles) {
      return ((ReadMaxFiles) readLimit).maxFiles();
    }

    if (readLimit instanceof CompositeReadLimit) {
      // We do not expect a CompositeReadLimit to contain a nested CompositeReadLimit.
      // In fact, it should only be a composite of two or more of ReadMinRows, ReadMaxRows and
      // ReadMaxFiles, with no more than one of each.
      ReadLimit[] limits = ((CompositeReadLimit) readLimit).getReadLimits();
      for (ReadLimit limit : limits) {
        if (limit instanceof ReadMaxFiles) {
          return ((ReadMaxFiles) limit).maxFiles();
        }
      }
    }

    // there is no ReadMaxFiles, so return the default
    return Integer.MAX_VALUE;
  }

  private static int getMaxRows(ReadLimit readLimit) {
    if (readLimit instanceof ReadMaxRows) {
      long maxRows = ((ReadMaxRows) readLimit).maxRows();
      return Math.toIntExact(maxRows);
    }

    if (readLimit instanceof CompositeReadLimit) {
      ReadLimit[] limits = ((CompositeReadLimit) readLimit).getReadLimits();
      for (ReadLimit limit : limits) {
        if (limit instanceof ReadMaxRows) {
          long maxRows = ((ReadMaxRows) limit).maxRows();
          return Math.toIntExact(maxRows);
        }
      }
    }

    // There is no ReadMaxRows, so return the default
    return Integer.MAX_VALUE;
  }
}
