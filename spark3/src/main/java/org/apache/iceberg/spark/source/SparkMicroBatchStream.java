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
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkBatchScan.ReadTask;
import org.apache.iceberg.spark.source.SparkBatchScan.ReaderFactory;
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
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;

public class SparkMicroBatchStream implements MicroBatchStream {
  private static final Joiner SLASH = Joiner.on("/");
  private static final Logger LOG = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private final Table table;
  private final boolean caseSensitive;
  private final String expectedSchema;
  private final Broadcast<Table> tableBroadcast;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final boolean localityPreferred;
  private final StreamingOffset initialOffset;
  private final boolean skipDelete;

  SparkMicroBatchStream(JavaSparkContext sparkContext, Table table, boolean caseSensitive,
                        Schema expectedSchema, CaseInsensitiveStringMap options, String checkpointLocation) {
    this.table = table;
    this.caseSensitive = caseSensitive;
    this.expectedSchema = SchemaParser.toJson(expectedSchema);
    this.localityPreferred = Spark3Util.isLocalityEnabled(table.io(), table.location(), options);
    this.tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table));

    long tableSplitSize = PropertyUtil.propertyAsLong(table.properties(), SPLIT_SIZE, SPLIT_SIZE_DEFAULT);
    this.splitSize = Spark3Util.propertyAsLong(options, SparkReadOptions.SPLIT_SIZE, tableSplitSize);

    int tableSplitLookback = PropertyUtil.propertyAsInt(table.properties(), SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT);
    this.splitLookback = Spark3Util.propertyAsInt(options, SparkReadOptions.LOOKBACK, tableSplitLookback);

    long tableSplitOpenFileCost = PropertyUtil.propertyAsLong(
        table.properties(), SPLIT_OPEN_FILE_COST, SPLIT_OPEN_FILE_COST_DEFAULT);
    this.splitOpenFileCost = Spark3Util.propertyAsLong(
        options, SparkReadOptions.FILE_OPEN_COST, tableSplitOpenFileCost);

    InitialOffsetStore initialOffsetStore = new InitialOffsetStore(table, checkpointLocation);
    this.initialOffset = initialOffsetStore.initialOffset();

    this.skipDelete = Spark3Util.propertyAsBoolean(options, SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS, false);
  }

  @Override
  public Offset latestOffset() {
    table.refresh();
    Snapshot latestSnapshot = table.currentSnapshot();
    if (latestSnapshot == null) {
      return StreamingOffset.START_OFFSET;
    }

    return new StreamingOffset(latestSnapshot.snapshotId(), Iterables.size(latestSnapshot.addedFiles()), false);
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    Preconditions.checkArgument(end instanceof StreamingOffset, "Invalid end offset: %s is not a StreamingOffset", end);
    Preconditions.checkArgument(
        start instanceof StreamingOffset, "Invalid start offset: %s is not a StreamingOffset", start);

    if (end.equals(StreamingOffset.START_OFFSET)) {
      return new InputPartition[0];
    }

    StreamingOffset endOffset = (StreamingOffset) end;
    StreamingOffset startOffset = (StreamingOffset) start;

    List<FileScanTask> fileScanTasks = planFiles(startOffset, endOffset);

    CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
        CloseableIterable.withNoopClose(fileScanTasks),
        splitSize);
    List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(
        TableScanUtil.planTasks(splitTasks, splitSize, splitLookback, splitOpenFileCost));
    InputPartition[] readTasks = new InputPartition[combinedScanTasks.size()];

    Tasks.range(readTasks.length)
        .stopOnFailure()
        .executeWith(localityPreferred ? ThreadPools.getWorkerPool() : null)
        .run(index -> readTasks[index] = new ReadTask(
            combinedScanTasks.get(index), tableBroadcast, expectedSchema,
            caseSensitive, localityPreferred));

    return readTasks;
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
  public void commit(Offset end) {
  }

  @Override
  public void stop() {
  }

  private List<FileScanTask> planFiles(StreamingOffset startOffset, StreamingOffset endOffset) {
    List<FileScanTask> fileScanTasks = Lists.newArrayList();
    StreamingOffset batchStartOffset = StreamingOffset.START_OFFSET.equals(startOffset) ?
        new StreamingOffset(SnapshotUtil.oldestSnapshot(table).snapshotId(), 0, false) :
        startOffset;

    StreamingOffset currentOffset = null;

    do {
      if (currentOffset == null) {
        currentOffset = batchStartOffset;
      } else {
        Snapshot snapshotAfter = SnapshotUtil.snapshotAfter(table, currentOffset.snapshotId());
        currentOffset = new StreamingOffset(snapshotAfter.snapshotId(), 0L, false);
      }

      if (!shouldProcess(table.snapshot(currentOffset.snapshotId()))) {
        LOG.debug("Skipping snapshot: {} of table {}", currentOffset.snapshotId(), table.name());
        continue;
      }

      MicroBatch latestMicroBatch = MicroBatches.from(table.snapshot(currentOffset.snapshotId()), table.io())
          .caseSensitive(caseSensitive)
          .specsById(table.specs())
          .generate(currentOffset.position(), Long.MAX_VALUE, currentOffset.shouldScanAllFiles());

      fileScanTasks.addAll(latestMicroBatch.tasks());
    } while (currentOffset.snapshotId() != endOffset.snapshotId());

    return fileScanTasks;
  }

  private boolean shouldProcess(Snapshot snapshot) {
    String op = snapshot.operation();
    Preconditions.checkState(!op.equals(DataOperations.DELETE) || skipDelete,
        "Cannot process delete snapshot: %s", snapshot.snapshotId());
    Preconditions.checkState(
        op.equals(DataOperations.DELETE) || op.equals(DataOperations.APPEND) || op.equals(DataOperations.REPLACE),
        "Cannot process %s snapshot: %s", op.toLowerCase(Locale.ROOT), snapshot.snapshotId());
    return op.equals(DataOperations.APPEND);
  }

  private static class InitialOffsetStore {
    private final Table table;
    private final FileIO io;
    private final String initialOffsetLocation;

    InitialOffsetStore(Table table, String checkpointLocation) {
      this.table = table;
      this.io = table.io();
      this.initialOffsetLocation = SLASH.join(checkpointLocation, "offsets/0");
    }

    public StreamingOffset initialOffset() {
      InputFile inputFile = io.newInputFile(initialOffsetLocation);
      if (inputFile.exists()) {
        return readOffset(inputFile);
      }

      table.refresh();
      StreamingOffset offset = table.currentSnapshot() == null ?
          StreamingOffset.START_OFFSET :
          new StreamingOffset(SnapshotUtil.oldestSnapshot(table).snapshotId(), 0, false);

      OutputFile outputFile = io.newOutputFile(initialOffsetLocation);
      writeOffset(offset, outputFile);

      return offset;
    }

    private void writeOffset(StreamingOffset offset, OutputFile file) {
      try (OutputStream outputStream = file.create()) {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
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
