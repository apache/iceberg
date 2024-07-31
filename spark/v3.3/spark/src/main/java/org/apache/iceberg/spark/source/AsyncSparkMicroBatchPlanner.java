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

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.cache.Cache;
import org.apache.iceberg.relocated.com.google.common.cache.CacheBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadAllAvailable;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncSparkMicroBatchPlanner implements SparkMicroBatchPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncSparkMicroBatchPlanner.class);
  private final Table table;
  private final long minQueuedFiles;
  private final long minQueuedRows;
  private final SparkReadConf readConf;
  private final Cache<Pair<StreamingOffset, StreamingOffset>, List<FileScanTask>> planFilesCache;
  private final LinkedBlockingQueue<Pair<StreamingOffset, FileScanTask>> queue;
  private final ScheduledExecutorService executor;
  private final AtomicLong queuedFileCount = new AtomicLong(0);
  private final AtomicLong queuedRowCount = new AtomicLong(0);
  private volatile Throwable refreshFailedThrowable;
  private volatile Throwable fillQueueFailedThrowable;
  private volatile Pair<StreamingOffset, FileScanTask> tail;
  private Snapshot lastQueuedSnapshot;
  private boolean stopped;

  /**
   * This class manages a queue of FileScanTask + StreamingOffset. On creation, it starts up an
   * asynchronous polling process which populates it to the queue when a new snapshot arrives or the
   * minimum amount of queued data is too low
   *
   * <p>Note: that this will in effect capture the state of the table when snapshots are added to
   * the queue. If a snapshot is expired after it has been added to the queue, the job will still
   * process it.
   */
  public AsyncSparkMicroBatchPlanner(
      Table table,
      SparkReadConf readConf,
      StreamingOffset initialOffset,
      StreamingOffset maybeEndOffset) {
    this.table = table;
    this.minQueuedFiles = readConf.maxFilesPerMicroBatch();
    this.minQueuedRows = readConf.maxRecordsPerMicroBatch();
    this.readConf = readConf;
    this.planFilesCache = CacheBuilder.newBuilder().maximumSize(10).build();
    this.queue = new LinkedBlockingQueue<>();
    this.executor = Executors.newSingleThreadScheduledExecutor();
    this.executor.scheduleWithFixedDelay(
        this::refreshAndTrapException,
        readConf.streamingSnapshotPollingIntervalMs(),
        readConf.streamingSnapshotPollingIntervalMs(),
        TimeUnit.MILLISECONDS);
    table.refresh();
    // synchronously data to the queue to meet our initial constraints
    fillQueue(initialOffset, maybeEndOffset);
    // schedule queue fill to run continuously with 10 ms pauses
    executor.scheduleWithFixedDelay(
        () -> fillQueueAndTrapException(lastQueuedSnapshot), 10, 10, TimeUnit.MILLISECONDS);
    LOG.info(
        "Started AsyncSparkMicroBatchPlanner for {} from initialOffset: {}",
        table.name(),
        initialOffset);
  }

  @Override
  public synchronized void stop() {
    Preconditions.checkArgument(
        !stopped, "AsyncSparkMicroBatchPlanner for {} was already stopped", table.name());
    stopped = true;
    LOG.info("stopping AsyncSparkMicroBatchPlanner for table: {}", table.name());
    executor.shutdownNow();
    try {
      stopped =
          executor.awaitTermination(
              readConf.streamingSnapshotPollingIntervalMs() * 2, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
      LOG.info("InterruptedException in AsyncMicroBatchPlanner.stop, this is safe to ignore");
    }
    LOG.info("AsyncSparkMicroBatchPlanner for table: {}, stopped: {}", table.name(), stopped);
  }

  /**
   * Spork can call this multiple times, it should produce the same answer every time
   *
   * @param startOffset the starting offset of this microbatch, position is inclusive
   * @param endOffset the end offset of this microbatch, position is exclusive
   * @return the list of files to scan between these offsets
   * @throws ExecutionException thrown if there is an exception planning
   */
  @Override
  public synchronized List<FileScanTask> planFiles(
      StreamingOffset startOffset, StreamingOffset endOffset) throws ExecutionException {
    return planFilesCache.get(
        Pair.of(startOffset, endOffset),
        () -> {
          LOG.info(
              "running planFiles for {}, startOffset: {}, endOffset: {}",
              table.name(),
              startOffset,
              endOffset);
          List<FileScanTask> result = new LinkedList<>();
          Pair<StreamingOffset, FileScanTask> elem;
          StreamingOffset currentOffset;
          boolean shouldTerminate = false;
          long filesInPlan = 0;
          long rowsInPlan = 0;
          do {
            // I have to synchronize here since I am polling, checking for empty and updating tail
            synchronized (queue) {
              elem = queue.poll(100, TimeUnit.MILLISECONDS);
              if (queue.isEmpty()) {
                tail = null;
              }
            }
            if (elem != null) {
              currentOffset = elem.first();
              LOG.debug("planFiles consumed: {}", currentOffset);
              FileScanTask currentTask = elem.second();
              filesInPlan += 1;
              long elemRows = currentTask.file().recordCount();
              rowsInPlan += elemRows;
              queuedFileCount.decrementAndGet();
              queuedRowCount.addAndGet(-elemRows);
              result.add(currentTask);
              // try to peek at the next entry of the queue and see if we should stop
              Pair<StreamingOffset, FileScanTask> nextElem = queue.peek();
              boolean endOffsetPeek = false;
              if (nextElem != null) {
                endOffsetPeek = endOffset.equals(nextElem.first());
              }
              // end offset may be synthetic and not exist in the queue
              boolean endOffsetSynthetic =
                  currentOffset.snapshotId() == endOffset.snapshotId()
                      && (currentOffset.position() + 1) == endOffset.position();
              shouldTerminate = endOffsetPeek || endOffsetSynthetic;
            } else {
              LOG.trace("planFiles hasn't reached {}, waiting", endOffset);
            }
          } while (!shouldTerminate && refreshFailedThrowable == null);
          if (refreshFailedThrowable != null) {
            throw new ExecutionException(refreshFailedThrowable);
          }
          LOG.info(
              "completed planFiles for {}, startOffset: {}, endOffset: {}, files: {}, rows: {}",
              table.name(),
              startOffset,
              endOffset,
              filesInPlan,
              rowsInPlan);
          return result;
        });
  }

  /**
   * This needs to be non destructive on the queue as spark could call this multiple times. Each
   * time, depending on the table state it could return something different
   *
   * @param startOffset the starting offset of the next microbatch
   * @param limit a limit for how many files/bytes/rows the next microbatch should include
   * @return The end offset to use for the next microbatch, null signals that no data is available
   */
  @Override
  public synchronized StreamingOffset latestOffset(StreamingOffset startOffset, ReadLimit limit) {
    LOG.info(
        "running latestOffset for {}, startOffset: {}, limit: {}",
        table.name(),
        startOffset,
        limit);
    // if any exceptions were encountered in the background process, raise them here
    if (refreshFailedThrowable != null) {
      throw new RuntimeException(refreshFailedThrowable);
    }
    if (fillQueueFailedThrowable != null) {
      throw new RuntimeException(fillQueueFailedThrowable);
    }
    // if we want to read all available we don't need to scan files, just snapshots
    if (limit instanceof ReadAllAvailable) {
      // find the last valid snapshot
      Snapshot lastValidSnapshot = table.snapshot(startOffset.snapshotId());
      Snapshot nextValidSnapshot;
      do {
        nextValidSnapshot = nextValidSnapshot(lastValidSnapshot);
        if (nextValidSnapshot != null) {
          lastValidSnapshot = nextValidSnapshot;
        }
      } while (nextValidSnapshot != null);
      return new StreamingOffset(
          lastValidSnapshot.snapshotId(),
          // offset is non-inclusive for end offsets
          // pos is 0 indexed
          addedFilesCount(lastValidSnapshot),
          false,
          lastValidSnapshot.timestampMillis(),
          totalRecords(lastValidSnapshot));
    }

    SparkMicroBatchStream.UnpackedLimits unpackedLimits =
        new SparkMicroBatchStream.UnpackedLimits(limit);
    long rowsSeen = 0;
    long filesSeen = 0;
    LOG.debug(
        "latestOffset queue status, queuedFiles: {}, queuedRows: {}",
        queuedFileCount.get(),
        queuedRowCount.get());
    for (Pair<StreamingOffset, FileScanTask> elem : queue) {
      long fileRows = elem.second().file().recordCount();
      // if adding this file would take us over our limits, return
      if ((rowsSeen + fileRows > unpackedLimits.getMaxRows())
          || (filesSeen + 1 > unpackedLimits.getMaxFiles())) {
        // special case, if this is the first element, we cannot advance
        if (filesSeen == 0) {
          return null;
        }
        LOG.debug("latestOffset found {}, rows: {}, files: {}", elem.first(), rowsSeen, filesSeen);
        return elem.first();
      }
      rowsSeen += fileRows;
      filesSeen += 1;
    }
    // if we got here there aren't enough files to exceed our limits
    if (tail != null) {
      StreamingOffset tailOffset = tail.first();
      // we have to increment the position by 1 since we want to include the tail in the read and
      // position is non-inclusive
      StreamingOffset latestOffset =
          new StreamingOffset(
              tailOffset.snapshotId(),
              tailOffset.position() + 1,
              tailOffset.shouldScanAllFiles(),
              tailOffset.snapshotTimestampMs(),
              tailOffset.snapshotTotalRows());
      LOG.debug("latestOffset tail {}", latestOffset);
      return latestOffset;
    }
    // if we got here the queue is empty
    LOG.debug("latestOffset no data, returning null");
    return null;
  }

  @Override
  public Offset reportLatestOffset() {
    // intentionally not calling table.refresh() here so the background thread is the only thing
    // making that call
    Snapshot latestSnapshot = table.currentSnapshot();
    StreamingOffset latestOffset = null;
    if (latestSnapshot != null) {
      latestOffset =
          new StreamingOffset(
              latestSnapshot.snapshotId(),
              addedFilesCount(latestSnapshot),
              false,
              latestSnapshot.timestampMillis(),
              totalRecords(latestSnapshot));
    }
    LOG.info("reportLatestOffset: {}", latestOffset);
    return latestOffset;
  }

  /** Generate a MicroBatch based on input parameters and add to the queue */
  private void addMicroBatchToQueue(
      Snapshot snapshot, long startFileIndex, long endFileIndex, boolean shouldScanAllFile) {
    LOG.info("Adding MicroBatch for snapshot: {} to the queue", snapshot.snapshotId());
    MicroBatches.MicroBatch microBatch =
        MicroBatches.from(snapshot, table.io())
            .caseSensitive(readConf.caseSensitive())
            .specsById(table.specs())
            .generate(startFileIndex, endFileIndex, Long.MAX_VALUE, shouldScanAllFile);

    long position = startFileIndex;
    for (FileScanTask task : microBatch.tasks()) {
      Pair<StreamingOffset, FileScanTask> elem =
          Pair.of(
              new StreamingOffset(
                  microBatch.snapshotId(),
                  position,
                  shouldScanAllFile,
                  snapshot.timestampMillis(),
                  totalRecords(snapshot)),
              task);
      queuedFileCount.incrementAndGet();
      queuedRowCount.addAndGet(task.file().recordCount());
      // I have to synchronize here so queue and tail can never be out of sync
      synchronized (queue) {
        queue.add(elem);
        tail = elem;
      }
      position += 1;
    }
    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder("\n");
      for (Pair<StreamingOffset, FileScanTask> elem : queue) {
        sb.append(elem.first()).append("\n");
      }
      LOG.debug(sb.toString());
    }
    lastQueuedSnapshot = snapshot;
  }

  /** Wrap refresh and trap exceptions since this is being run in the background */
  private void refreshAndTrapException() {
    try {
      table.refresh();
      fillQueue(lastQueuedSnapshot);
      LOG.info(
          "Refreshed table: {}, timestamp: {}",
          table.name(),
          table.currentSnapshot() != null ? table.currentSnapshot().timestampMillis() : -1);
    } catch (Throwable t) {
      refreshFailedThrowable = t;
    }
  }

  /** Wrap fillQueue and trap exceptions since this is being run in the background */
  private void fillQueueAndTrapException(Snapshot readFrom) {
    try {
      fillQueue(readFrom);
    } catch (Throwable t) {
      fillQueueFailedThrowable = t;
    }
  }

  private void fillQueue(StreamingOffset fromOffset, StreamingOffset toOffset) {
    Snapshot currentSnapshot = table.snapshot(fromOffset.snapshotId());
    // this could be a partial snapshot so add it outside the loop
    if (currentSnapshot != null) {
      addMicroBatchToQueue(
          currentSnapshot,
          fromOffset.position(),
          addedFilesCount(currentSnapshot),
          fromOffset.shouldScanAllFiles());
    }
    if (toOffset != null) {
      while (currentSnapshot.snapshotId() != toOffset.snapshotId()) {
        addMicroBatchToQueue(currentSnapshot, 0, addedFilesCount(currentSnapshot), false);
        currentSnapshot = nextValidSnapshot(currentSnapshot);
      }
      addMicroBatchToQueue(
          currentSnapshot, 0, addedFilesCount(currentSnapshot), toOffset.shouldScanAllFiles());
    }
  }

  /** Try to populate the queue with data from unread snapshots */
  private void fillQueue(Snapshot readFrom) {
    if ((queuedRowCount.get() > minQueuedRows) || (queuedFileCount.get() > minQueuedFiles)) {
      // we have enough data buffered, check back shortly
      LOG.debug(
          "Buffer is full, {} > {} or {} > {}",
          queuedRowCount.get(),
          minQueuedRows,
          queuedFileCount.get(),
          minQueuedFiles);
    } else {
      // add an entire snapshot to the queue
      Snapshot nextValidSnapshot = nextValidSnapshot(readFrom);
      if (nextValidSnapshot != null) {
        addMicroBatchToQueue(nextValidSnapshot, 0, addedFilesCount(nextValidSnapshot), false);
      } else {
        LOG.debug("No snapshots ready to be read");
      }
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
    Snapshot nextSnapshot;
    // if there were no valid snapshots, check for an initialOffset again
    if (curSnapshot == null) {
      StreamingOffset startingOffset = SparkMicroBatchStream.computeInitialOffset(table, readConf);
      LOG.debug("computeInitialOffset picked startingOffset: {}", startingOffset);
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
      if (nextSnapshot.snapshotId() == table.currentSnapshot().snapshotId()) {
        return null;
      }
      nextSnapshot = SnapshotUtil.snapshotAfter(table, nextSnapshot.snapshotId());
    }
    return nextSnapshot;
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

  private long addedFilesCount(Snapshot snapshot) {
    long addedFilesCount =
        PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, -1);
    // If snapshotSummary doesn't have SnapshotSummary.ADDED_FILES_PROP,
    // iterate through addedFiles iterator to find addedFilesCount.
    return addedFilesCount == -1
        ? Iterables.size(snapshot.addedDataFiles(table.io()))
        : addedFilesCount;
  }

  private long totalRecords(Snapshot snapshot) {
    return PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.TOTAL_RECORDS_PROP, -1);
  }
}
