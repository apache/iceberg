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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;

/**
 * A mirco-batch based Spark Structured Streaming reader for Iceberg table. It will track the added
 * files and generate tasks per batch to process newly added files. By default it will process
 * all the newly added files to the current snapshot in each batch, user could also set this
 * configuration "max-files-per-trigger" to control the number of files processed per batch.
 */
class StreamingReader extends Reader implements MicroBatchReader {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingReader.class);

  private StreamingOffset startOffset;
  private StreamingOffset endOffset;

  private final Table table;
  private final long maxSizePerBatch;
  private final Long startSnapshotId;
  private final long splitSize;
  private final int splitLookback;
  private final long splitOpenFileCost;
  private Boolean readUsingBatch = null;

  // Used to cache the pending batches for this streaming batch interval.
  private Pair<StreamingOffset, List<MicroBatch>> cachedPendingBatches = null;

  StreamingReader(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryptionManager,
                  boolean caseSensitive, DataSourceOptions options) {
    super(table, io, encryptionManager, caseSensitive, options);

    this.table = table;
    this.maxSizePerBatch = options.get("max-size-per-batch").map(Long::parseLong).orElse(Long.MAX_VALUE);
    Preconditions.checkArgument(maxSizePerBatch > 0L,
        "Option max-size-per-batch '%d' should > 0", maxSizePerBatch);

    this.startSnapshotId = options.get("starting-snapshot-id").map(Long::parseLong).orElse(null);
    if (startSnapshotId != null) {
      if (!SnapshotUtil.ancestorOf(table, table.currentSnapshot().snapshotId(), startSnapshotId)) {
        throw new IllegalStateException("The option starting-snapshot-id " + startSnapshotId +
            " is not an ancestor of the current snapshot");
      }
    }

    this.splitSize = Optional.ofNullable(splitSize())
        .orElseGet(() -> PropertyUtil.propertyAsLong(table.properties(), SPLIT_SIZE, SPLIT_SIZE_DEFAULT));
    this.splitLookback = Optional.ofNullable(splitLookback())
        .orElseGet(() -> PropertyUtil.propertyAsInt(table.properties(), SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT));
    this.splitOpenFileCost = Optional.ofNullable(splitOpenFileCost())
        .orElseGet(() ->
            PropertyUtil.propertyAsLong(table.properties(), SPLIT_OPEN_FILE_COST, SPLIT_OPEN_FILE_COST_DEFAULT));
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
    table.refresh();

    if (start.isPresent() && !StreamingOffset.START_OFFSET.equals(start.get())) {
      this.startOffset = (StreamingOffset) start.get();
      this.endOffset = (StreamingOffset) end.orElseGet(() -> calculateEndOffset(startOffset));
    } else {
      // If starting offset is "START_OFFSET" (there's no snapshot in the last batch), or starting
      // offset is not set, then we need to calculate the starting offset again.
      this.startOffset = calculateStartingOffset();
      this.endOffset = calculateEndOffset(startOffset);
    }
  }

  @Override
  public Offset getStartOffset() {
    if (startOffset == null) {
      throw new IllegalStateException("Start offset is not set");
    }

    return startOffset;
  }

  @Override
  public Offset getEndOffset() {
    if (endOffset == null) {
      throw new IllegalStateException("End offset is not set");
    }

    return endOffset;
  }

  @Override
  public Offset deserializeOffset(String json) {
    return StreamingOffset.fromJson(json);
  }

  @Override
  public void commit(Offset end) {
    // Since all the data and metadata of Iceberg is as it is, nothing needs to commit when
    // offset is processed, so no need to implement this method.
  }

  @Override
  public void stop() {
  }

  @Override
  public boolean enableBatchRead() {
    return readUsingBatch == null ? false : readUsingBatch;
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergStreamScan(table=%s, type=%s)", table, table.schema().asStruct());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected List<CombinedScanTask> tasks() {
    if (startOffset.equals(endOffset)) {
      LOG.info("Start offset {} equals to end offset {}, no data to process", startOffset, endOffset);
      return Collections.emptyList();
    }

    Preconditions.checkState(cachedPendingBatches != null,
        "pendingBatches is null, which is unexpected as it will be set when calculating end offset");

    List<MicroBatch> pendingBatches = cachedPendingBatches.second();
    if (pendingBatches.isEmpty()) {
      LOG.info("Current start offset {} and end offset {}, there's no task to process in this batch",
          startOffset, endOffset);
      return Collections.emptyList();
    }

    MicroBatch lastBatch = pendingBatches.get(pendingBatches.size() - 1);
    Preconditions.checkState(
        lastBatch.snapshotId() == endOffset.snapshotId() && lastBatch.endFileIndex() == endOffset.index(),
        "The cached pendingBatches doesn't match the current end offset " + endOffset);

    LOG.info("Processing data from {} to {}", startOffset, endOffset);
    List<FileScanTask> tasks = pendingBatches.stream()
        .flatMap(batch -> batch.tasks().stream())
        .collect(Collectors.toList());
    CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(CloseableIterable.withNoopClose(tasks),
        splitSize);
    List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(
        TableScanUtil.planTasks(splitTasks, splitSize, splitLookback, splitOpenFileCost));

    if (readUsingBatch == null) {
      this.readUsingBatch = checkEnableBatchRead(combinedScanTasks);
    }

    return combinedScanTasks;
  }

  private StreamingOffset calculateStartingOffset() {
    StreamingOffset startingOffset;
    if (startSnapshotId != null) {
      startingOffset = new StreamingOffset(startSnapshotId, 0, true, false);
    } else {
      List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
      if (snapshotIds.isEmpty()) {
        // there's no snapshot currently.
        startingOffset = StreamingOffset.START_OFFSET;
      } else {
        startingOffset = new StreamingOffset(snapshotIds.get(snapshotIds.size() - 1), 0, true, false);
      }
    }

    return startingOffset;
  }

  private StreamingOffset calculateEndOffset(StreamingOffset start) {
    if (start.equals(StreamingOffset.START_OFFSET)) {
      return StreamingOffset.START_OFFSET;
    }

    // Spark will invoke setOffsetRange more than once. If this is already calulated, use the cached one to avoid
    // calculating again.
    if (cachedPendingBatches == null || !cachedPendingBatches.first().equals(start)) {
      this.cachedPendingBatches = Pair.of(start, getChangesWithRateLimit(start, maxSizePerBatch));
    }

    List<MicroBatch> batches = cachedPendingBatches.second();
    MicroBatch lastBatch = batches.isEmpty() ? null : batches.get(batches.size() - 1);

    if (lastBatch == null) {
      return start;
    } else {
      boolean isStarting = lastBatch.snapshotId() == start.snapshotId() && start.shouldScanAllFiles();
      return new StreamingOffset(lastBatch.snapshotId(), lastBatch.endFileIndex(), isStarting,
          lastBatch.lastIndexOfSnapshot());
    }
  }

  @VisibleForTesting
  @SuppressWarnings("checkstyle:HiddenField")
  List<MicroBatch> getChangesWithRateLimit(StreamingOffset startOffset, long maxSize) {
    List<MicroBatch> batches = Lists.newArrayList();
    long currentLeftSize = maxSize;
    MicroBatch lastBatch = null;

    checkOverwrite(table.snapshot(startOffset.snapshotId()));
    if (shouldGenerateFromStartOffset(startOffset)) {
      currentLeftSize -= generateMicroBatch(startOffset.snapshotId(), startOffset.index(),
          startOffset.shouldScanAllFiles(), currentLeftSize, batches);
      lastBatch = batches.get(batches.size() - 1);
    }

    // Current snapshot can already satisfy the size needs.
    if (currentLeftSize <= 0L || (lastBatch != null && !lastBatch.lastIndexOfSnapshot())) {
      return batches;
    }

    long currentSnapshotId = table.currentSnapshot().snapshotId();
    if (currentSnapshotId == startOffset.snapshotId()) {
      // the snapshot of current offset is already the latest snapshot of this table.
      return batches;
    }

    ImmutableList<Long> snapshotIds = ImmutableList.<Long>builder()
        .addAll(SnapshotUtil.snapshotIdsBetween(table, startOffset.snapshotId(), currentSnapshotId))
        .build()
        .reverse();

    for (Long id : snapshotIds) {
      Snapshot snapshot = table.snapshot(id);
      checkOverwrite(snapshot);
      if (!isAppend(snapshot)) {
        continue;
      }

      int startIndex = lastBatch == null || lastBatch.lastIndexOfSnapshot() ? 0 : lastBatch.endFileIndex();
      currentLeftSize -= generateMicroBatch(id, startIndex, false, currentLeftSize, batches);
      lastBatch = batches.get(batches.size() - 1);

      // If the current request size is already satisfied, or none of the DataFile size can satisfy the current left
      // size, break the current loop.
      if (currentLeftSize <= 0L || !lastBatch.lastIndexOfSnapshot()) {
        break;
      }
    }

    return batches;
  }

  private long generateMicroBatch(long snapshotId, int startIndex, boolean isStart, long currentLeftSize,
      List<MicroBatch> batches) {
    MicroBatch batch = MicroBatches.from(table.snapshot(snapshotId), table.io())
        .caseSensitive(caseSensitive())
        .specsById(table.specs())
        .generate(startIndex, currentLeftSize, isStart);
    batches.add(batch);
    return batch.sizeInBytes();
  }

  @SuppressWarnings("checkstyle:HiddenField")
  private boolean shouldGenerateFromStartOffset(StreamingOffset startOffset) {
    return !startOffset.isSnapshotFullyProcessed() &&
        (startOffset.shouldScanAllFiles() || isAppend(table.snapshot(startOffset.snapshotId())));
  }

  private static void checkOverwrite(Snapshot snapshot) {
    if (snapshot.operation().equals(DataOperations.OVERWRITE)) {
      throw new UnsupportedOperationException(String.format("Found %s operation, cannot support incremental data for " +
          "snapshot %d", DataOperations.OVERWRITE, snapshot.snapshotId()));
    }
  }

  private static boolean isAppend(Snapshot snapshot) {
    return snapshot.operation().equals(DataOperations.APPEND);
  }
}
