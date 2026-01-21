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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.connector.read.streaming.ReadAllAvailable;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncSparkMicroBatchPlanner extends BaseSparkMicroBatchPlanner implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncSparkMicroBatchPlanner.class);
  private static final int PLAN_FILES_CACHE_MAX_SIZE = 10;
  private static final long QUEUE_POLL_TIMEOUT_MS = 100L; // 100 ms

  private final long minQueuedFiles;
  private final long minQueuedRows;

  // Cache for planFiles results to handle duplicate calls
  private final Cache<Pair<StreamingOffset, StreamingOffset>, List<FileScanTask>> planFilesCache;

  // Queue to buffer pre-fetched file scan tasks
  private final LinkedBlockingQueue<Pair<StreamingOffset, FileScanTask>> queue;

  // Background executor for async operations
  private final ScheduledExecutorService executor;

  // Error tracking
  private volatile Throwable refreshFailedThrowable;
  private volatile Throwable fillQueueFailedThrowable;

  // Tracking queue state
  private final AtomicLong queuedFileCount = new AtomicLong(0);
  private final AtomicLong queuedRowCount = new AtomicLong(0);
  private volatile Pair<StreamingOffset, FileScanTask> tail;
  private Snapshot lastQueuedSnapshot;
  private boolean stopped;

  // Cap for Trigger.AvailableNow - don't process beyond this offset
  private final StreamingOffset lastOffsetForTriggerAvailableNow;

  /**
   * This class manages a queue of FileScanTask + StreamingOffset. On creation, it starts up an
   * asynchronous polling process which populates the queue when a new snapshot arrives or the
   * minimum amount of queued data is too low.
   *
   * <p>Note: this will capture the state of the table when snapshots are added to the queue. If a
   * snapshot is expired after being added to the queue, the job will still process it.
   */
  AsyncSparkMicroBatchPlanner(
      Table table,
      SparkReadConf readConf,
      StreamingOffset initialOffset,
      StreamingOffset maybeEndOffset,
      StreamingOffset lastOffsetForTriggerAvailableNow) {
    super(table, readConf);
    this.minQueuedFiles = readConf().maxFilesPerMicroBatch();
    this.minQueuedRows = readConf().maxRecordsPerMicroBatch();
    this.lastOffsetForTriggerAvailableNow = lastOffsetForTriggerAvailableNow;
    this.planFilesCache = Caffeine.newBuilder().maximumSize(PLAN_FILES_CACHE_MAX_SIZE).build();
    this.queue = new LinkedBlockingQueue<>();

    table().refresh();
    // Synchronously add data to the queue to meet our initial constraints
    fillQueue(initialOffset, maybeEndOffset);

    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "iceberg-async-planner-" + table().name());
              thread.setDaemon(true);
              return thread;
            });
    // Schedule table refresh at configured interval
    long pollingIntervalMs = readConf().streamingSnapshotPollingIntervalMs();
    this.executor.scheduleWithFixedDelay(
        this::refreshAndTrapException, pollingIntervalMs, pollingIntervalMs, TimeUnit.MILLISECONDS);
    // Schedule queue fill to run frequently (use polling interval for tests, cap at 100ms for
    // production)
    long queueFillIntervalMs = Math.min(QUEUE_POLL_TIMEOUT_MS, pollingIntervalMs);
    executor.scheduleWithFixedDelay(
        () -> fillQueueAndTrapException(lastQueuedSnapshot),
        0,
        queueFillIntervalMs,
        TimeUnit.MILLISECONDS);

    LOG.info(
        "Started AsyncSparkMicroBatchPlanner for {} from initialOffset: {}",
        table().name(),
        initialOffset);
  }

  @Override
  public synchronized void stop() {
    Preconditions.checkArgument(
        !stopped, "AsyncSparkMicroBatchPlanner for {} was already stopped", table().name());
    stopped = true;
    LOG.info("Stopping AsyncSparkMicroBatchPlanner for table: {}", table().name());
    executor.shutdownNow();
    boolean terminated = false;
    try {
      terminated =
          executor.awaitTermination(
              readConf().streamingSnapshotPollingIntervalMs() * 2, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
      // Restore interrupt status
      Thread.currentThread().interrupt();
    }
    LOG.info("AsyncSparkMicroBatchPlanner for table: {}, stopped: {}", table().name(), terminated);
  }

  @Override
  public void close() {
    stop();
  }

  /**
   * Spark can call this multiple times; it should produce the same answer every time.
   *
   * @param startOffset the starting offset of this microbatch, position is inclusive
   * @param endOffset the end offset of this microbatch, position is exclusive
   * @return the list of files to scan between these offsets
   */
  @Override
  public synchronized List<FileScanTask> planFiles(StreamingOffset startOffset, StreamingOffset endOffset) {
    return planFilesCache.get(
        Pair.of(startOffset, endOffset),
        key -> {
          LOG.info(
              "running planFiles for {}, startOffset: {}, endOffset: {}",
              table().name(),
              startOffset,
              endOffset);
          List<FileScanTask> result = new LinkedList<>();
          Pair<StreamingOffset, FileScanTask> elem;
          StreamingOffset currentOffset;
          boolean shouldTerminate = false;
          long filesInPlan = 0;
          long rowsInPlan = 0;

          do {
            // Synchronize here since we are polling, checking for empty and updating tail
            synchronized (queue) {
              try {
                elem = queue.poll(QUEUE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while polling queue", e);
              }
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
            throw new RuntimeException("Table refresh failed", refreshFailedThrowable);
          }

          LOG.info(
              "completed planFiles for {}, startOffset: {}, endOffset: {}, files: {}, rows: {}",
              table().name(),
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
        table().name(),
        startOffset,
        limit);

    if (table().currentSnapshot() == null) {
      LOG.info("latestOffset returning START_OFFSET, currentSnapshot() is null");
      return StreamingOffset.START_OFFSET;
    }

    if (table().currentSnapshot().timestampMillis() < readConf().streamFromTimestamp()) {
      LOG.info("latestOffset returning START_OFFSET, currentSnapshot() < fromTimestamp");
      return StreamingOffset.START_OFFSET;
    }

    // if any exceptions were encountered in the background process, raise them here
    if (refreshFailedThrowable != null) {
      throw new RuntimeException(refreshFailedThrowable);
    }
    if (fillQueueFailedThrowable != null) {
      throw new RuntimeException(fillQueueFailedThrowable);
    }

    // if we want to read all available we don't need to scan files, just snapshots
    if (limit instanceof ReadAllAvailable) {
      // If Trigger.AvailableNow cap is set, return it directly
      if (this.lastOffsetForTriggerAvailableNow != null) {
        return this.lastOffsetForTriggerAvailableNow;
      }
      Snapshot lastValidSnapshot = table().snapshot(startOffset.snapshotId());
      Snapshot nextValidSnapshot;
      do {
        nextValidSnapshot = nextValidSnapshot(lastValidSnapshot);
        if (nextValidSnapshot != null) {
          lastValidSnapshot = nextValidSnapshot;
        }
      } while (nextValidSnapshot != null);
      return new StreamingOffset(
          lastValidSnapshot.snapshotId(),
          MicroBatchUtils.addedFilesCount(table(), lastValidSnapshot),
          false);
    }

    return computeLimitedOffset(limit);
  }

  private StreamingOffset computeLimitedOffset(ReadLimit limit) {
    UnpackedLimits unpackedLimits = new UnpackedLimits(limit);
    long rowsSeen = 0;
    long filesSeen = 0;
    LOG.debug(
        "latestOffset queue status, queuedFiles: {}, queuedRows: {}",
        queuedFileCount.get(),
        queuedRowCount.get());

    // Convert to list for indexed access
    List<Pair<StreamingOffset, FileScanTask>> queueList = Lists.newArrayList(queue);
    for (int i = 0; i < queueList.size(); i++) {
      Pair<StreamingOffset, FileScanTask> elem = queueList.get(i);
      long fileRows = elem.second().file().recordCount();

      // Hard limit on files - stop BEFORE exceeding
      if (filesSeen + 1 > unpackedLimits.getMaxFiles()) {
        if (filesSeen == 0) {
          return null;
        }
        LOG.debug("latestOffset hit file limit at {}, rows: {}, files: {}", elem.first(), rowsSeen, filesSeen);
        return elem.first();
      }

      // Soft limit on rows - include file FIRST, then check
      rowsSeen += fileRows;
      filesSeen += 1;

      // Check if we've hit the row limit after including this file
      if (rowsSeen >= unpackedLimits.getMaxRows()) {
        if (filesSeen == 1 && rowsSeen > unpackedLimits.getMaxRows()) {
          LOG.warn(
              "File {} at offset {} contains {} records, exceeding maxRecordsPerMicroBatch limit of {}. "
                  + "This file will be processed entirely to guarantee forward progress. "
                  + "Consider increasing the limit or writing smaller files to avoid unexpected memory usage.",
              elem.second().file().location(),
              elem.first(),
              fileRows,
              unpackedLimits.getMaxRows());
        }
        // Return the offset of the NEXT element (or synthesize tail+1)
        if (i + 1 < queueList.size()) {
          LOG.debug(
              "latestOffset hit row limit at {}, rows: {}, files: {}",
              queueList.get(i + 1).first(),
              rowsSeen,
              filesSeen);
          return queueList.get(i + 1).first();
        } else {
          // This is the last element - return tail+1
          StreamingOffset current = elem.first();
          StreamingOffset result =
              new StreamingOffset(
                  current.snapshotId(), current.position() + 1, current.shouldScanAllFiles());
          LOG.debug("latestOffset hit row limit at tail {}, rows: {}, files: {}", result, rowsSeen, filesSeen);
          return result;
        }
      }
    }

    // if we got here there aren't enough files to exceed our limits
    if (tail != null) {
      StreamingOffset tailOffset = tail.first();
      // we have to increment the position by 1 since we want to include the tail in the read and
      // position is non-inclusive
      StreamingOffset latestOffset =
          new StreamingOffset(
              tailOffset.snapshotId(), tailOffset.position() + 1, tailOffset.shouldScanAllFiles());
      LOG.debug("latestOffset returning all queued data {}", latestOffset);
      return latestOffset;
    }

    // if we got here the queue is empty
    LOG.debug("latestOffset no data, returning null");
    return null;
  }

  // Background task wrapper that traps exceptions
  private void refreshAndTrapException() {
    try {
      table().refresh();
    } catch (Throwable t) {
      LOG.error("Failed to refresh table {}", table().name(), t);
      refreshFailedThrowable = t;
    }
  }

  // Background task wrapper that traps exceptions
  private void fillQueueAndTrapException(Snapshot snapshot) {
    try {
      fillQueue(snapshot);
    } catch (Throwable t) {
      LOG.error("Failed to fill queue for table {}", table().name(), t);
      fillQueueFailedThrowable = t;
    }
  }

  /** Generate a MicroBatch based on input parameters and add to the queue */
  private void addMicroBatchToQueue(
      Snapshot snapshot, long startFileIndex, long endFileIndex, boolean shouldScanAllFile) {
    LOG.info("Adding MicroBatch for snapshot: {} to the queue", snapshot.snapshotId());
    MicroBatches.MicroBatch microBatch =
        MicroBatches.from(snapshot, table().io())
            .caseSensitive(readConf().caseSensitive())
            .specsById(table().specs())
            .generate(startFileIndex, endFileIndex, Long.MAX_VALUE, shouldScanAllFile);

    long position = startFileIndex;
    for (FileScanTask task : microBatch.tasks()) {
      Pair<StreamingOffset, FileScanTask> elem =
          Pair.of(new StreamingOffset(microBatch.snapshotId(), position, shouldScanAllFile), task);
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

  private void fillQueue(StreamingOffset fromOffset, StreamingOffset toOffset) {
    LOG.debug("filling queue from {}, to: {}", fromOffset, toOffset);
    Snapshot currentSnapshot = table().snapshot(fromOffset.snapshotId());
    // this could be a partial snapshot so add it outside the loop
    if (currentSnapshot != null) {
      addMicroBatchToQueue(
          currentSnapshot,
          fromOffset.position(),
          MicroBatchUtils.addedFilesCount(table(), currentSnapshot),
          fromOffset.shouldScanAllFiles());
    }
    if (toOffset != null) {
      if (currentSnapshot != null) {
        while (currentSnapshot.snapshotId() != toOffset.snapshotId()) {
          currentSnapshot = nextValidSnapshot(currentSnapshot);
          if (currentSnapshot != null) {
            addMicroBatchToQueue(
                currentSnapshot, 0, MicroBatchUtils.addedFilesCount(table(), currentSnapshot), false);
          } else {
            break;
          }
        }
      }
      // toOffset snapshot already added in loop when currentSnapshot == toOffset
    } else {
      fillQueueInitialBuffer(currentSnapshot);
    }
  }

  private void fillQueueInitialBuffer(Snapshot startSnapshot) {
    // toOffset is null - fill initial buffer to prevent queue starvation before background
    // thread starts. Use configured limits to avoid loading all snapshots
    // (which could cause OOM on tables with thousands of snapshots).
    long targetRows = readConf().asyncQueuePreloadRowLimit();
    long targetFiles = readConf().asyncQueuePreloadFileLimit();

    Snapshot tableCurrentSnapshot = table().currentSnapshot();
    if (tableCurrentSnapshot == null) {
      return; // Empty table
    }

    // START_OFFSET case: initialize using nextValidSnapshot which respects timestamp filtering
    Snapshot current = startSnapshot;
    if (current == null) {
      current = nextValidSnapshot(null);
      if (current != null) {
        addMicroBatchToQueue(current, 0, MicroBatchUtils.addedFilesCount(table(), current), false);
      }
    }

    // Continue loading more snapshots within safety limits
    if (current != null) {
      while ((queuedRowCount.get() < targetRows || queuedFileCount.get() < targetFiles)
          && current.snapshotId() != tableCurrentSnapshot.snapshotId()) {
        current = nextValidSnapshot(current);
        if (current != null) {
          addMicroBatchToQueue(
              current, 0, MicroBatchUtils.addedFilesCount(table(), current), false);
        } else {
          break;
        }
      }
    }
  }

  /** Try to populate the queue with data from unread snapshots */
  private void fillQueue(Snapshot readFrom) {
    // Don't add beyond cap for Trigger.AvailableNow
    if (this.lastOffsetForTriggerAvailableNow != null
        && readFrom != null
        && readFrom.snapshotId() >= this.lastOffsetForTriggerAvailableNow.snapshotId()) {
      LOG.debug(
          "Reached cap snapshot {}, not adding more",
          this.lastOffsetForTriggerAvailableNow.snapshotId());
      return;
    }

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
        addMicroBatchToQueue(
            nextValidSnapshot, 0, MicroBatchUtils.addedFilesCount(table(), nextValidSnapshot), false);
      } else {
        LOG.debug("No snapshots ready to be read");
      }
    }
  }
}
