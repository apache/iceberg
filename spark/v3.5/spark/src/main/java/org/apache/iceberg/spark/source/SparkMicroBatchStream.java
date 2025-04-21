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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkMicroBatchStream implements MicroBatchStream, SupportsAdmissionControl {
  private static final Joiner SLASH = Joiner.on("/");
  private static final Logger LOG = LoggerFactory.getLogger(SparkMicroBatchStream.class);
  private static final Types.StructType EMPTY_GROUPING_KEY_TYPE = Types.StructType.of();

  private final Table table;
  private final String branch;
  private final boolean caseSensitive;
  private final String expectedSchema;
  private final Broadcast<Table> tableBroadcast;
  private final long splitSize;
  private final int splitLookback;
  private final long splitOpenFileCost;
  private final boolean localityPreferred;
  private final StreamingOffset initialOffset;
  private final boolean skipDelete;
  private final boolean skipOverwrite;
  private final long fromTimestamp;
  private final int maxFilesPerMicroBatch;
  private final int maxRecordsPerMicroBatch;

  public SparkMicroBatchStream(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      String checkpointLocation) {
    this.table = table;
    this.branch = readConf.branch();
    this.caseSensitive = readConf.caseSensitive();
    this.expectedSchema = SchemaParser.toJson(expectedSchema);
    this.localityPreferred = readConf.localityEnabled();
    this.tableBroadcast = sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    this.splitSize = readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.splitOpenFileCost = readConf.splitOpenFileCost();
    this.fromTimestamp = readConf.streamFromTimestamp();
    this.maxFilesPerMicroBatch = readConf.maxFilesPerMicroBatch();
    this.maxRecordsPerMicroBatch = readConf.maxRecordsPerMicroBatch();

    LOG.debug(
        "Initializing SparkMicroBatchStream with params: branch={}, caseSensitive={}, "
            + "splitSize={}, splitLookback={}, splitOpenFileCost={}, fromTimestamp={}, "
            + "maxFilesPerMicroBatch={}, maxRecordsPerMicroBatch={}",
        branch,
        caseSensitive,
        splitSize,
        splitLookback,
        splitOpenFileCost,
        fromTimestamp,
        maxFilesPerMicroBatch,
        maxRecordsPerMicroBatch);

    InitialOffsetStore initialOffsetStore =
        new InitialOffsetStore(table, checkpointLocation, fromTimestamp);
    this.initialOffset = initialOffsetStore.initialOffset();
    LOG.debug("Initial offset set to {}", initialOffset);

    this.skipDelete = readConf.streamingSkipDeleteSnapshots();
    this.skipOverwrite = readConf.streamingSkipOverwriteSnapshots();
    LOG.debug("Skip delete snapshots={}, skip overwrite snapshots={}", skipDelete, skipOverwrite);
  }

  @Override
  public Offset latestOffset() {
    table.refresh();
    LOG.debug(
        "Refreshed table {}, current snapshot id={}",
        table.name(),
        table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : "null");

    if (table.currentSnapshot() == null
        || table.currentSnapshot().timestampMillis() < fromTimestamp) {
      LOG.debug(
          "No valid current snapshot or snapshot before fromTimestamp ({}), returning START_OFFSET",
          fromTimestamp);
      return StreamingOffset.START_OFFSET;
    }

    Snapshot latestSnapshot = table.currentSnapshot();
    long added = addedFilesCount(latestSnapshot);
    StreamingOffset offset = new StreamingOffset(latestSnapshot.snapshotId(), added, false);

    LOG.debug(
        "Computed latestOffset: snapshotId={}, addedFilesCount={}",
        latestSnapshot.snapshotId(),
        added);
    return offset;
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    Preconditions.checkArgument(
        end instanceof StreamingOffset, "Invalid end offset: %s is not a StreamingOffset", end);
    Preconditions.checkArgument(
        start instanceof StreamingOffset,
        "Invalid start offset: %s is not a StreamingOffset",
        start);

    StreamingOffset startOffset = (StreamingOffset) start;
    StreamingOffset endOffset = (StreamingOffset) end;

    if (endOffset.equals(StreamingOffset.START_OFFSET)) {
      LOG.debug("End offset is START_OFFSET, returning no partitions");
      return new InputPartition[0];
    }

    LOG.debug("Planning input partitions from {} to {}", startOffset, endOffset);
    List<FileScanTask> fileScanTasks = planFiles(startOffset, endOffset);
    LOG.debug("planFiles returned {} file scan tasks", fileScanTasks.size());

    CloseableIterable<FileScanTask> splitTasks =
        TableScanUtil.splitFiles(CloseableIterable.withNoopClose(fileScanTasks), splitSize);
    List<CombinedScanTask> combinedScanTasks =
        Lists.newArrayList(
            TableScanUtil.planTasks(splitTasks, splitSize, splitLookback, splitOpenFileCost));
    LOG.debug("Split into {} combined scan tasks", combinedScanTasks.size());

    String[][] locations = computePreferredLocations(combinedScanTasks);
    LOG.debug("Computed preferred locations for tasks: localityPreferred={}", localityPreferred);

    InputPartition[] partitions = new InputPartition[combinedScanTasks.size()];
    for (int index = 0; index < combinedScanTasks.size(); index++) {
      partitions[index] =
          new SparkInputPartition(
              EMPTY_GROUPING_KEY_TYPE,
              combinedScanTasks.get(index),
              tableBroadcast,
              branch,
              expectedSchema,
              caseSensitive,
              locations != null ? locations[index] : SparkPlanningUtil.NO_LOCATION_PREFERENCE);
    }
    LOG.debug("Created {} SparkInputPartitions", partitions.length);

    return partitions;
  }

  private String[][] computePreferredLocations(List<CombinedScanTask> taskGroups) {
    return localityPreferred ? SparkPlanningUtil.fetchBlockLocations(table.io(), taskGroups) : null;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SparkRowReaderFactory();
  }

  @Override
  public Offset initialOffset() {
    return initialOffset;
  }

  @Override
  public Offset deserializeOffset(String json) {
    StreamingOffset off = StreamingOffset.fromJson(json);
    LOG.debug("Deserialized offset from JSON {}: {}", json, off);
    return off;
  }

  @Override
  public void commit(Offset end) {
    LOG.debug("Commit called for offset {}", end);
  }

  @Override
  public void stop() {
    LOG.debug("Stop called on SparkMicroBatchStream");
  }

  private List<FileScanTask> planFiles(StreamingOffset startOffset, StreamingOffset endOffset) {
    LOG.debug("planFiles called with startOffset={}, endOffset={}", startOffset, endOffset);
    List<FileScanTask> fileScanTasks = Lists.newArrayList();
    StreamingOffset batchStartOffset =
        StreamingOffset.START_OFFSET.equals(startOffset)
            ? determineStartingOffset(table, fromTimestamp)
            : startOffset;
    LOG.debug("Batch start offset determined as {}", batchStartOffset);

    StreamingOffset currentOffset = null;
    do {
      if (currentOffset == null) {
        currentOffset = batchStartOffset;
      } else {
        Snapshot snapshotAfter = SnapshotUtil.snapshotAfter(table, currentOffset.snapshotId());
        if (currentOffset.snapshotId() != endOffset.snapshotId()) {
          currentOffset = new StreamingOffset(snapshotAfter.snapshotId(), 0L, false);
        } else {
          currentOffset = endOffset;
        }
      }

      Snapshot snapshot = table.snapshot(currentOffset.snapshotId());
      validateCurrentSnapshotExists(snapshot, currentOffset);

      if (!shouldProcess(snapshot)) {
        LOG.debug(
            "Skipping processing for snapshot id={} operation={}",
            snapshot.snapshotId(),
            snapshot.operation());
        continue;
      }

      long endFileIndex =
          currentOffset.snapshotId() == endOffset.snapshotId()
              ? endOffset.position()
              : addedFilesCount(snapshot);

      LOG.debug(
          "Processing snapshot [id={}, startFileIndex={}, endFileIndex={}]",
          currentOffset.snapshotId(),
          currentOffset.position(),
          endFileIndex);

      MicroBatch latestMicroBatch =
          MicroBatches.from(snapshot, table.io())
              .caseSensitive(caseSensitive)
              .specsById(table.specs())
              .generate(
                  currentOffset.position(),
                  endFileIndex,
                  Long.MAX_VALUE,
                  currentOffset.shouldScanAllFiles());
      List<FileScanTask> tasks = latestMicroBatch.tasks();
      LOG.debug("Snapshot id={} generated {} file scan tasks", snapshot.snapshotId(), tasks.size());

      fileScanTasks.addAll(tasks);
    } while (currentOffset.snapshotId() != endOffset.snapshotId());

    LOG.debug("planFiles returning total {} tasks", fileScanTasks.size());
    return fileScanTasks;
  }

  private boolean shouldProcess(Snapshot snapshot) {
    String op = snapshot.operation();
    switch (op) {
      case DataOperations.APPEND:
        LOG.debug("Snapshot {} is APPEND, will process", snapshot.snapshotId());
        return true;
      case DataOperations.REPLACE:
        LOG.debug("Snapshot {} is REPLACE, skipping", snapshot.snapshotId());
        return false;
      case DataOperations.DELETE:
        LOG.debug("Snapshot {} is DELETE, skipDelete={}", snapshot.snapshotId(), skipDelete);
        Preconditions.checkState(
            skipDelete,
            "Cannot process delete snapshot: %s, to ignore deletes, set %s=true",
            snapshot.snapshotId(),
            SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS);
        return false;
      case DataOperations.OVERWRITE:
        LOG.debug(
            "Snapshot {} is OVERWRITE, skipOverwrite={}", snapshot.snapshotId(), skipOverwrite);
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

  private static StreamingOffset determineStartingOffset(Table table, Long fromTimestamp) {
    table.refresh();
    LOG.debug("Determining starting offset for fromTimestamp={}", fromTimestamp);

    if (table.currentSnapshot() == null) {
      LOG.debug("No current snapshot, returning START_OFFSET");
      return StreamingOffset.START_OFFSET;
    }
    if (fromTimestamp == null) {
      long oldestId = SnapshotUtil.oldestAncestor(table).snapshotId();
      LOG.debug("No fromTimestamp specified, starting from oldest snapshot {}", oldestId);
      return new StreamingOffset(oldestId, 0L, false);
    }
    if (table.currentSnapshot().timestampMillis() < fromTimestamp) {
      LOG.debug("Current snapshot timestamp < fromTimestamp, returning START_OFFSET");
      return StreamingOffset.START_OFFSET;
    }
    try {
      Snapshot snapshot = SnapshotUtil.oldestAncestorAfter(table, fromTimestamp);
      if (snapshot != null) {
        LOG.debug("Found oldest ancestor after timestamp: {}", snapshot.snapshotId());
        return new StreamingOffset(snapshot.snapshotId(), 0L, false);
      } else {
        LOG.debug("No snapshot after timestamp, returning START_OFFSET");
        return StreamingOffset.START_OFFSET;
      }
    } catch (IllegalStateException e) {
      long oldestId = SnapshotUtil.oldestAncestor(table).snapshotId();
      LOG.warn("Could not find snapshot after timestamp, falling back to oldest {}", oldestId, e);
      return new StreamingOffset(oldestId, 0L, false);
    }
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    Preconditions.checkArgument(
        startOffset instanceof StreamingOffset,
        "Invalid start offset: %s is not a StreamingOffset",
        startOffset);

    LOG.debug("latestOffset(startOffset={}, limit={}) called", startOffset, limit);
    table.refresh();
    if (table.currentSnapshot() == null
        || table.currentSnapshot().timestampMillis() < fromTimestamp) {
      LOG.debug("No valid current snapshot or before fromTimestamp, returning START_OFFSET");
      return StreamingOffset.START_OFFSET;
    }

    StreamingOffset startingOffset = (StreamingOffset) startOffset;
    if (startingOffset.equals(StreamingOffset.START_OFFSET)) {
      startingOffset = determineStartingOffset(table, fromTimestamp);
    }
    LOG.debug("Effective startingOffset={}", startingOffset);

    Snapshot curSnapshot = table.snapshot(startingOffset.snapshotId());
    validateCurrentSnapshotExists(curSnapshot, startingOffset);

    int startPosOfSnapOffset = (int) startingOffset.position();
    boolean scanAllFiles = startingOffset.shouldScanAllFiles();
    LOG.debug("Start position in snapshot={}, scanAllFiles={}", startPosOfSnapOffset, scanAllFiles);

    boolean shouldContinueReading = true;
    int curFilesAdded = 0;
    long curRecordCount = 0;
    long curBytesProcessed = 0L;

    // Start curPos at the incoming offset’s position so file indexing is correct
    // when resuming mid‐snapshot. This ensures the first file processed is at or after
    // startPosOfSnapOffset.
    int curPos = startPosOfSnapOffset;
    int snapshotsProcessed = 0;
    while (shouldContinueReading) {
      snapshotsProcessed++;
      List<Pair<ManifestFile, Integer>> indexedManifests =
          MicroBatches.skippedManifestIndexesFromSnapshot(
              table.io(), curSnapshot, startPosOfSnapOffset, scanAllFiles);
      LOG.debug(
          "Snapshot {} has {} manifest files after skipping",
          curSnapshot.snapshotId(),
          indexedManifests.size());

      for (Pair<ManifestFile, Integer> manifestWithIndex : indexedManifests) {
        if (!shouldContinueReading) {
          break;
        }
        ManifestFile manifest = manifestWithIndex.first();
        curPos = manifestWithIndex.second();
        LOG.debug("Reading manifest {} at index {}", manifest.path(), curPos);

        try (CloseableIterable<FileScanTask> taskIterable =
                MicroBatches.openManifestFile(
                    table.io(), table.specs(), caseSensitive, curSnapshot, manifest, scanAllFiles);
            CloseableIterator<FileScanTask> taskIter = taskIterable.iterator()) {

          while (taskIter.hasNext()) {
            FileScanTask task = taskIter.next();
            long fileRecords = task.file().recordCount();

            // WARN if a single file’s record count exceeds the configured maxRecordsPerMicroBatch
            if (fileRecords > maxRecordsPerMicroBatch) {
              LOG.warn(
                  "File {} has {} records, which exceeds maxRecordsPerMicroBatch {}. "
                      + "This file will never fit into any micro-batch.",
                  task.file().location(),
                  fileRecords,
                  maxRecordsPerMicroBatch);
            }

            if (curPos >= startPosOfSnapOffset) {
              long nextFilesCount = curFilesAdded + 1;
              long nextRecordsCount = curRecordCount + fileRecords;
              if (nextFilesCount > maxFilesPerMicroBatch
                  || nextRecordsCount > maxRecordsPerMicroBatch) {
                LOG.debug(
                    "Limits reached: filesAdded={} (max={}), recordsCount={} (max={}), stopping",
                    curFilesAdded,
                    maxFilesPerMicroBatch,
                    curRecordCount,
                    maxRecordsPerMicroBatch);
                shouldContinueReading = false;
                break;
              }
              curFilesAdded++;
              curRecordCount += task.file().recordCount();
              curBytesProcessed += task.file().fileSizeInBytes(); // ← accumulate bytes
            }
            curPos++;
          }
        } catch (IOException ioe) {
          LOG.warn("Failed to close task iterable for manifest {}", manifest.path(), ioe);
        }
      }

      if (curSnapshot.snapshotId() == table.currentSnapshot().snapshotId()) {
        LOG.debug("Reached latest snapshot {}, breaking", curSnapshot.snapshotId());
        break;
      }

      if (shouldContinueReading) {
        Snapshot next = nextValidSnapshot(curSnapshot);
        if (next == null) {
          LOG.debug("No next valid snapshot after {}, stopping", curSnapshot.snapshotId());
          break;
        }
        curSnapshot = next;
        startPosOfSnapOffset = -1;
        scanAllFiles = false;
        LOG.debug(
            "Moving to next snapshot {}, scanAllFiles reset to false", curSnapshot.snapshotId());
      }
    }

    StreamingOffset latestStreamingOffset =
        new StreamingOffset(curSnapshot.snapshotId(), curPos, scanAllFiles);

    LOG.debug(
        "Computed next streaming offset [offset={}] after filling the batch: [files={}, records={}, bytes={}, snapshots={}]",
        latestStreamingOffset,
        curFilesAdded,
        curRecordCount,
        curBytesProcessed,
        snapshotsProcessed);

    if (latestStreamingOffset.equals(startingOffset)) {
      LOG.debug("No new data beyond startingOffset, returning null");
      return null;
    }
    return latestStreamingOffset;
  }

  private Snapshot nextValidSnapshot(Snapshot curSnapshot) {
    Snapshot nextSnapshot = SnapshotUtil.snapshotAfter(table, curSnapshot.snapshotId());
    while (!shouldProcess(nextSnapshot)) {
      LOG.debug(
          "Skipping next snapshot {} operation={}",
          nextSnapshot.snapshotId(),
          nextSnapshot.operation());
      if (nextSnapshot.snapshotId() == table.currentSnapshot().snapshotId()) {
        return null;
      }
      nextSnapshot = SnapshotUtil.snapshotAfter(table, nextSnapshot.snapshotId());
    }
    LOG.debug("Next valid snapshot is {}", nextSnapshot.snapshotId());
    return nextSnapshot;
  }

  private long addedFilesCount(Snapshot snapshot) {
    long countFromSummary =
        PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, -1);
    if (countFromSummary != -1) {
      LOG.debug(
          "addedFilesCount from summary for snapshot {} = {}",
          snapshot.snapshotId(),
          countFromSummary);
      return countFromSummary;
    }
    long counted = Iterables.size(snapshot.addedDataFiles(table.io()));
    LOG.debug("addedFilesCount by counting for snapshot {} = {}", snapshot.snapshotId(), counted);
    return counted;
  }

  private void validateCurrentSnapshotExists(Snapshot snapshot, StreamingOffset currentOffset) {
    if (snapshot == null) {
      throw new IllegalStateException(
          String.format(
              "Cannot load current offset at snapshot %d, the snapshot was expired or removed",
              currentOffset.snapshotId()));
    }
  }

  @Override
  public ReadLimit getDefaultReadLimit() {
    if (maxFilesPerMicroBatch != Integer.MAX_VALUE
        && maxRecordsPerMicroBatch != Integer.MAX_VALUE) {
      ReadLimit[] readLimits = new ReadLimit[2];
      readLimits[0] = ReadLimit.maxFiles(maxFilesPerMicroBatch);
      readLimits[1] = ReadLimit.maxRows(maxFilesPerMicroBatch);
      return ReadLimit.compositeLimit(readLimits);
    } else if (maxFilesPerMicroBatch != Integer.MAX_VALUE) {
      return ReadLimit.maxFiles(maxFilesPerMicroBatch);
    } else if (maxRecordsPerMicroBatch != Integer.MAX_VALUE) {
      return ReadLimit.maxRows(maxRecordsPerMicroBatch);
    } else {
      return ReadLimit.allAvailable();
    }
  }

  private static class InitialOffsetStore {
    private final Table table;
    private final FileIO io;
    private final String initialOffsetLocation;
    private final Long fromTimestamp;

    InitialOffsetStore(Table table, String checkpointLocation, Long fromTimestamp) {
      this.table = table;
      this.io = table.io();
      this.initialOffsetLocation = SLASH.join(checkpointLocation, "offsets/0");
      this.fromTimestamp = fromTimestamp;
      LOG.debug("InitialOffsetStore created with location {}", initialOffsetLocation);
    }

    public StreamingOffset initialOffset() {
      InputFile inputFile = io.newInputFile(initialOffsetLocation);
      if (inputFile.exists()) {
        LOG.debug("Found existing offset file at {}, reading", initialOffsetLocation);
        return readOffset(inputFile);
      }
      LOG.debug("No existing offset file, determining starting offset");
      table.refresh();
      StreamingOffset offset = determineStartingOffset(table, fromTimestamp);
      LOG.debug("Writing initial offset {} to {}", offset, initialOffsetLocation);
      OutputFile outputFile = io.newOutputFile(initialOffsetLocation);
      writeOffset(offset, outputFile);
      return offset;
    }

    private void writeOffset(StreamingOffset offset, OutputFile file) {
      try (OutputStream outputStream = file.create();
          BufferedWriter writer =
              new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
        writer.write(offset.json());
        writer.flush();
        LOG.debug("Successfully wrote offset {} to {}", offset, initialOffsetLocation);
      } catch (IOException ioException) {
        LOG.warn(
            "Failed writing offset to {}: {}", initialOffsetLocation, ioException.getMessage());
        throw new UncheckedIOException(
            String.format("Failed writing offset to: %s", initialOffsetLocation), ioException);
      }
    }

    private StreamingOffset readOffset(InputFile file) {
      try (InputStream in = file.newStream()) {
        StreamingOffset off = StreamingOffset.fromJson(in);
        LOG.debug("Read offset {} from {}", off, initialOffsetLocation);
        return off;
      } catch (IOException ioException) {
        LOG.warn(
            "Failed reading offset from {}: {}", initialOffsetLocation, ioException.getMessage());
        throw new UncheckedIOException(
            String.format("Failed reading offset from: %s", initialOffsetLocation), ioException);
      }
    }
  }
}
