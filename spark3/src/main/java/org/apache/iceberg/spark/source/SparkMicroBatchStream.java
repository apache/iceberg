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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
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
  private static final Logger LOG = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final boolean caseSensitive;
  private final Schema expectedSchema;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final boolean localityPreferred;
  private final InitialOffsetStore initialOffsetStore;
  private final StreamingOffset initialOffset;

  SparkMicroBatchStream(JavaSparkContext sparkContext,
                        Table table, boolean caseSensitive, Schema expectedSchema,
                        CaseInsensitiveStringMap options, String checkpointLocation) {
    this.sparkContext = sparkContext;
    this.table = table;
    this.caseSensitive = caseSensitive;
    this.expectedSchema = expectedSchema;
    this.localityPreferred = Spark3Util.isLocalityEnabled(table.io(), table.location(), options);
    this.splitSize = Optional.ofNullable(Spark3Util.propertyAsLong(options, SparkReadOptions.SPLIT_SIZE, null))
        .orElseGet(() -> PropertyUtil.propertyAsLong(table.properties(), SPLIT_SIZE, SPLIT_SIZE_DEFAULT));
    this.splitLookback = Optional.ofNullable(Spark3Util.propertyAsInt(options, SparkReadOptions.LOOKBACK, null))
        .orElseGet(() -> PropertyUtil.propertyAsInt(table.properties(), SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT));
    this.splitOpenFileCost = Optional.ofNullable(
        Spark3Util.propertyAsLong(options, SparkReadOptions.FILE_OPEN_COST, null))
        .orElseGet(() -> PropertyUtil.propertyAsLong(table.properties(), SPLIT_OPEN_FILE_COST,
            SPLIT_OPEN_FILE_COST_DEFAULT));
    this.initialOffsetStore = InitialOffsetStore.getInstance(table.io(), checkpointLocation);
    this.initialOffset = getOrWriteInitialOffset(table, initialOffsetStore);
  }

  @Override
  public Offset latestOffset() {
    table.refresh();
    Snapshot latestSnapshot = table.currentSnapshot();
    if (latestSnapshot == null) {
      return StreamingOffset.START_OFFSET;
    }

    String addedFilesValue = latestSnapshot.summary().get(SnapshotSummary.ADDED_FILES_PROP);
    long addedFiles = addedFilesValue == null ?
        Iterables.size(latestSnapshot.addedFiles()) :
        Long.parseLong(addedFilesValue);

    // a readStream on an Iceberg table can be started from 2 types of snapshots
    // 1. a valid starting Snapshot:
    //      when this valid starting Snapshot is the initialOffset - then, scanAllFiles must be set to true;
    //      for all StreamingOffsets following this - scanAllFiles must be set to false
    // 2. START_OFFSET:
    //      if the stream started on the table from START_OFFSET - it implies - that all the subsequent Snapshots added
    //      will have all files as net New manifests & hence scanAllFiles can be false.
    boolean scanAllFiles = !StreamingOffset.START_OFFSET.equals(initialOffset) &&
        latestSnapshot.snapshotId() == initialOffset.snapshotId();
    return new StreamingOffset(latestSnapshot.snapshotId(), addedFiles, scanAllFiles);
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    if (end.equals(StreamingOffset.START_OFFSET)) {
      return new InputPartition[0];
    }

    // broadcast the table metadata as input partitions will be sent to executors
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table));
    String expectedSchemaString = SchemaParser.toJson(expectedSchema);

    Preconditions.checkState(
        end instanceof StreamingOffset,
        "The end offset passed to planInputPartitions() is not an instance of StreamingOffset.");

    Preconditions.checkState(
        start instanceof StreamingOffset,
        "The start offset passed to planInputPartitions() is not an instance of StreamingOffset.");

    StreamingOffset endOffset = (StreamingOffset) end;
    StreamingOffset startOffset = (StreamingOffset) start;

    List<FileScanTask> fileScanTasks = getFileScanTasks(startOffset, endOffset);

    CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(
        CloseableIterable.withNoopClose(fileScanTasks),
        splitSize);
    List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(
        TableScanUtil.planTasks(splitTasks, splitSize, splitLookback, splitOpenFileCost));
    InputPartition[] readTasks = new InputPartition[combinedScanTasks.size()];

    for (int i = 0; i < combinedScanTasks.size(); i++) {
      readTasks[i] = new ReadTask(
          combinedScanTasks.get(i), tableBroadcast, expectedSchemaString,
          caseSensitive, localityPreferred);
    }

    return readTasks;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    int batchSizeValueToDisableColumnarReads = 0;
    return new ReaderFactory(batchSizeValueToDisableColumnarReads);
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

  private static StreamingOffset getOrWriteInitialOffset(Table table, InitialOffsetStore initialOffsetStore) {
    if (initialOffsetStore.isOffsetStoreInitialized()) {
      return initialOffsetStore.getInitialOffset();
    }

    table.refresh();
    StreamingOffset offset = table.currentSnapshot() == null ?
        StreamingOffset.START_OFFSET :
        new StreamingOffset(SnapshotUtil.oldestSnapshot(table).snapshotId(), 0, true);
    initialOffsetStore.addInitialOffset(offset);

    return offset;
  }

  private List<FileScanTask> getFileScanTasks(StreamingOffset startOffset, StreamingOffset endOffset) {
    List<FileScanTask> fileScanTasks = new ArrayList<>();
    MicroBatch latestMicroBatch = null;
    StreamingOffset batchStartOffset = StreamingOffset.START_OFFSET.equals(startOffset) ?
        new StreamingOffset(SnapshotUtil.oldestSnapshot(table).snapshotId(), 0, false) :
        startOffset;

    do {
      final StreamingOffset currentOffset =
          latestMicroBatch != null && latestMicroBatch.lastIndexOfSnapshot() ?
          getNextAvailableSnapshot(latestMicroBatch.snapshotId()) :
              batchStartOffset;

      latestMicroBatch = MicroBatches.from(table.snapshot(currentOffset.snapshotId()), table.io())
          .caseSensitive(caseSensitive)
          .specsById(table.specs())
          .generate(currentOffset.position(), Long.MAX_VALUE, currentOffset.shouldScanAllFiles());

      fileScanTasks.addAll(latestMicroBatch.tasks());
    } while (latestMicroBatch.snapshotId() != endOffset.snapshotId());

    return fileScanTasks;
  }

  private StreamingOffset getNextAvailableSnapshot(long snapshotId) {
    Snapshot previousSnapshot = table.snapshot(snapshotId);
    Snapshot pointer = table.currentSnapshot();
    while (pointer != null && previousSnapshot.snapshotId() != pointer.parentId()) {
      Preconditions.checkState(pointer.operation().equals(DataOperations.APPEND),
          "Encountered Snapshot DataOperation other than APPEND.");

      pointer = table.snapshot(pointer.parentId());
    }

    Preconditions.checkState(pointer != null,
        "Cannot read data from snapshot which has already expired: %s", snapshotId);

    return new StreamingOffset(pointer.snapshotId(), 0L, false);
  }

  interface InitialOffsetStore {
    static InitialOffsetStore getInstance(FileIO io, String checkpointLocation) {
      return new InitialOffsetStoreImpl(io, checkpointLocation);
    }

    void addInitialOffset(StreamingOffset offset);

    boolean isOffsetStoreInitialized();

    StreamingOffset getInitialOffset();
  }

  private static class InitialOffsetStoreImpl implements InitialOffsetStore {
    private final FileIO io;
    private final String initialOffsetLocation;

    InitialOffsetStoreImpl(FileIO io, String checkpointLocation) {
      this.io = io;
      this.initialOffsetLocation = new Path(checkpointLocation, "offsets/0").toString();
    }

    @Override
    public void addInitialOffset(StreamingOffset offset) {
      OutputFile file = io.newOutputFile(initialOffsetLocation);
      serialize(offset, file);
    }

    @Override
    public boolean isOffsetStoreInitialized() {
      InputFile file = io.newInputFile(initialOffsetLocation);
      return file.exists();
    }

    @Override
    public StreamingOffset getInitialOffset() {
      InputFile file = io.newInputFile(initialOffsetLocation);
      return deserialize(file);
    }

    private void serialize(StreamingOffset metadata, OutputFile file) {
      try (OutputStream outputStream = file.create()) {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        writer.write(metadata.json());
        writer.flush();
      } catch (IOException ioException) {
        throw new IllegalArgumentException(
            String.format("Failed to serialize latest checkpoint to: %s, with error: %s.",
                initialOffsetLocation, ioException));
      }
    }

    private StreamingOffset deserialize(InputFile file) {
      try (InputStream in = file.newStream()) {
        return StreamingOffset.fromJson(in);
      } catch (IOException ioException) {
        throw new IllegalArgumentException(
            String.format("Failed to deserialize latest checkpoint from: %s, with error: %s.",
                initialOffsetLocation, ioException));
      }
    }
  }
}
