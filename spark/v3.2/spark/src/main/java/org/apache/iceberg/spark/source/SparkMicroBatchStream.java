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
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkScan.ReaderFactory;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkMicroBatchStream implements MicroBatchStream {
  private static final Joiner SLASH = Joiner.on("/");
  private static final Logger LOG = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private final Table table;
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

  SparkMicroBatchStream(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      String checkpointLocation) {
    this.table = table;
    this.caseSensitive = readConf.caseSensitive();
    this.expectedSchema = SchemaParser.toJson(expectedSchema);
    this.localityPreferred = readConf.localityEnabled();
    this.tableBroadcast = sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    this.splitSize = readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.splitOpenFileCost = readConf.splitOpenFileCost();
    this.fromTimestamp = readConf.streamFromTimestamp();

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
    long addedFilesCount =
        PropertyUtil.propertyAsLong(latestSnapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, -1);
    // If snapshotSummary doesn't have SnapshotSummary.ADDED_FILES_PROP, iterate through addedFiles
    // iterator to find
    // addedFilesCount.
    addedFilesCount =
        addedFilesCount == -1
            ? Iterables.size(latestSnapshot.addedDataFiles(table.io()))
            : addedFilesCount;

    return new StreamingOffset(latestSnapshot.snapshotId(), addedFilesCount, false);
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

    InputPartition[] partitions = new InputPartition[combinedScanTasks.size()];

    Tasks.range(partitions.length)
        .stopOnFailure()
        .executeWith(localityPreferred ? ThreadPools.getWorkerPool() : null)
        .run(
            index ->
                partitions[index] =
                    new SparkInputPartition(
                        combinedScanTasks.get(index),
                        tableBroadcast,
                        expectedSchema,
                        caseSensitive,
                        localityPreferred));

    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new ReaderFactory(0);
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

    do {
      if (currentOffset == null) {
        currentOffset = batchStartOffset;
      } else {
        Snapshot snapshotAfter = SnapshotUtil.snapshotAfter(table, currentOffset.snapshotId());
        currentOffset = new StreamingOffset(snapshotAfter.snapshotId(), 0L, false);
      }

      Snapshot snapshot = table.snapshot(currentOffset.snapshotId());

      if (snapshot == null) {
        throw new IllegalStateException(
            String.format(
                "Cannot load current offset at snapshot %d, the snapshot was expired or removed",
                currentOffset.snapshotId()));
      }

      if (!shouldProcess(table.snapshot(currentOffset.snapshotId()))) {
        LOG.debug("Skipping snapshot: {} of table {}", currentOffset.snapshotId(), table.name());
        continue;
      }

      MicroBatch latestMicroBatch =
          MicroBatches.from(table.snapshot(currentOffset.snapshotId()), table.io())
              .caseSensitive(caseSensitive)
              .specsById(table.specs())
              .generate(
                  currentOffset.position(), Long.MAX_VALUE, currentOffset.shouldScanAllFiles());

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
