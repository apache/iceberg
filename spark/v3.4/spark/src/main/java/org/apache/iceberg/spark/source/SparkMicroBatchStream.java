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

  SparkMicroBatchStream(
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

    InitialOffsetStore initialOffsetStore =
        new InitialOffsetStore(table, checkpointLocation, fromTimestamp);
    this.initialOffset = initialOffsetStore.initialOffset();

    this.skipDelete = readConf.streamingSkipDeleteSnapshots();
    this.skipOverwrite = readConf.streamingSkipOverwriteSnapshots();
  }

  @Override
  public Offset latestOffset() {
    table.refresh();
    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (table.currentSnapshot().timestampMillis() < fromTimestamp) {
      return StreamingOffset.START_OFFSET;
    }

    Snapshot latestSnapshot = table.currentSnapshot();

    return new StreamingOffset(latestSnapshot.snapshotId(), addedFilesCount(latestSnapshot), false);
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    Preconditions.checkArgument(
        end instanceof StreamingOffset, "Invalid end offset: %s is not a StreamingOffset", end);
    Preconditions.checkArgument(
        start instanceof StreamingOffset,
        "Invalid start offset: %s is not a StreamingOffset",
        start);

    if (end.equals(StreamingOffset.START_OFFSET)) {
      return new InputPartition[0];
    }

    StreamingOffset endOffset = (StreamingOffset) end;
    StreamingOffset startOffset = (StreamingOffset) start;

    List<FileScanTask> fileScanTasks = planFiles(startOffset, endOffset);

    CloseableIterable<FileScanTask> splitTasks =
        TableScanUtil.splitFiles(CloseableIterable.withNoopClose(fileScanTasks), splitSize);
    List<CombinedScanTask> combinedScanTasks =
        Lists.newArrayList(
            TableScanUtil.planTasks(splitTasks, splitSize, splitLookback, splitOpenFileCost));
    String[][] locations = computePreferredLocations(combinedScanTasks);

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
    return StreamingOffset.fromJson(json);
  }

  @Override
  public void commit(Offset end) {}

  @Override
  public void stop() {}

  private List<FileScanTask> planFiles(StreamingOffset startOffset, StreamingOffset endOffset) {
    List<FileScanTask> fileScanTasks = Lists.newArrayList();
    StreamingOffset batchStartOffset =
        StreamingOffset.START_OFFSET.equals(startOffset)
            ? determineStartingOffset(table, fromTimestamp)
            : startOffset;

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
        if (currentOffset.snapshotId() != endOffset.snapshotId()) {
          currentOffset = new StreamingOffset(snapshotAfter.snapshotId(), 0L, false);
        } else {
          currentOffset = endOffset;
        }
      }

      Snapshot snapshot = table.snapshot(currentOffset.snapshotId());

      validateCurrentSnapshotExists(snapshot, currentOffset);

      if (!shouldProcess(snapshot)) {
        LOG.debug("Skipping snapshot: {} of table {}", currentOffset.snapshotId(), table.name());
        continue;
      }

      Snapshot currentSnapshot = table.snapshot(currentOffset.snapshotId());
      if (currentOffset.snapshotId() == endOffset.snapshotId()) {
        endFileIndex = endOffset.position();
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
    } while (currentOffset.snapshotId() != endOffset.snapshotId());

    return fileScanTasks;
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

  private static StreamingOffset determineStartingOffset(Table table, Long fromTimestamp) {
    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (fromTimestamp == null) {
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

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    // calculate end offset get snapshotId from the startOffset
    Preconditions.checkArgument(
        startOffset instanceof StreamingOffset,
        "Invalid start offset: %s is not a StreamingOffset",
        startOffset);

    table.refresh();
    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (table.currentSnapshot().timestampMillis() < fromTimestamp) {
      return StreamingOffset.START_OFFSET;
    }

    // end offset can expand to multiple snapshots
    StreamingOffset startingOffset = (StreamingOffset) startOffset;

    if (startOffset.equals(StreamingOffset.START_OFFSET)) {
      startingOffset = determineStartingOffset(table, fromTimestamp);
    }

    Snapshot curSnapshot = table.snapshot(startingOffset.snapshotId());
    validateCurrentSnapshotExists(curSnapshot, startingOffset);

    int startPosOfSnapOffset = (int) startingOffset.position();

    boolean scanAllFiles = startingOffset.shouldScanAllFiles();

    boolean shouldContinueReading = true;
    int curFilesAdded = 0;
    int curRecordCount = 0;
    int curPos = 0;

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
              // TODO : use readLimit provided in function param, the readLimits are derived from
              // these 2 properties.
              if ((curFilesAdded + 1) > maxFilesPerMicroBatch
                  || (curRecordCount + task.file().recordCount()) > maxRecordsPerMicroBatch) {
                shouldContinueReading = false;
                break;
              }

              curFilesAdded += 1;
              curRecordCount += task.file().recordCount();
            }
            ++curPos;
          }
        } catch (IOException ioe) {
          LOG.warn("Failed to close task iterable", ioe);
        }
      }
      // if the currentSnapShot was also the mostRecentSnapshot then break
      if (curSnapshot.snapshotId() == table.currentSnapshot().snapshotId()) {
        break;
      }

      // if everything was OK and we consumed complete snapshot then move to next snapshot
      if (shouldContinueReading) {
        startPosOfSnapOffset = -1;
        curSnapshot = SnapshotUtil.snapshotAfter(table, curSnapshot.snapshotId());
        // if anyhow we are moving to next snapshot we should only scan addedFiles
        scanAllFiles = false;
      }
    }

    StreamingOffset latestStreamingOffset =
        new StreamingOffset(curSnapshot.snapshotId(), curPos, scanAllFiles);

    // if no new data arrived, then return null.
    return latestStreamingOffset.equals(startingOffset) ? null : latestStreamingOffset;
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
    }

    public StreamingOffset initialOffset() {
      InputFile inputFile = io.newInputFile(initialOffsetLocation);
      if (inputFile.exists()) {
        return readOffset(inputFile);
      }

      table.refresh();
      StreamingOffset offset = determineStartingOffset(table, fromTimestamp);

      OutputFile outputFile = io.newOutputFile(initialOffsetLocation);
      writeOffset(offset, outputFile);

      return offset;
    }

    private void writeOffset(StreamingOffset offset, OutputFile file) {
      try (OutputStream outputStream = file.create()) {
        BufferedWriter writer =
            new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        writer.write(offset.json());
        writer.flush();
      } catch (IOException ioException) {
        throw new UncheckedIOException(
            String.format("Failed writing offset to: %s", initialOffsetLocation), ioException);
      }
    }

    private StreamingOffset readOffset(InputFile file) {
      try (InputStream in = file.newStream()) {
        return StreamingOffset.fromJson(in);
      } catch (IOException ioException) {
        throw new UncheckedIOException(
            String.format("Failed reading offset from: %s", initialOffsetLocation), ioException);
      }
    }
  }
}
