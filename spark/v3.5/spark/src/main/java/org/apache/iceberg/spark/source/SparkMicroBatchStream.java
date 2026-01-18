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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
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
import org.apache.spark.sql.connector.read.streaming.ReportsSourceMetrics;
import org.apache.spark.sql.connector.read.streaming.SupportsTriggerAvailableNow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkMicroBatchStream
    implements MicroBatchStream, SupportsTriggerAvailableNow, ReportsSourceMetrics {
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
  private final boolean cacheDeleteFilesOnExecutors;
  private final SparkReadConf readConf;
  private SparkMicroBatchPlanner microBatchPlanner;
  private StreamingOffset lastOffsetForTriggerAvailableNow;

  SparkMicroBatchStream(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      String checkpointLocation) {
    this.table = table;
    this.readConf = readConf;
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
    this.cacheDeleteFilesOnExecutors = readConf.cacheDeleteFilesOnExecutors();

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

    // Initialize planner if not already done (for resume scenarios)
    if (microBatchPlanner == null) {
      initializePlanner(startOffset, endOffset);
    }

    List<FileScanTask> fileScanTasks;
    try {
      fileScanTasks = microBatchPlanner.planFiles(startOffset, endOffset);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to plan files", e);
    }

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
              locations != null ? locations[index] : SparkPlanningUtil.NO_LOCATION_PREFERENCE,
              cacheDeleteFilesOnExecutors);
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
  public void stop() {
    if (microBatchPlanner != null) {
      microBatchPlanner.stop();
    }
  }

  static StreamingOffset determineStartingOffset(Table table, long fromTimestamp) {
    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (fromTimestamp == Long.MIN_VALUE) {
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
  public Map<String, String> metrics(Optional<Offset> latestConsumedOffset) {
    Map<String, String> metricsMap = Maps.newHashMap();
    metricsMap.put("table", table.name());
    metricsMap.put("branch", branch != null ? branch : "main");
    metricsMap.put(
        "asyncPlanningEnabled", String.valueOf(readConf.asyncMicroBatchPlanningEnabled()));
    metricsMap.put("maxFilesPerMicroBatch", String.valueOf(maxFilesPerMicroBatch));
    metricsMap.put("maxRecordsPerMicroBatch", String.valueOf(maxRecordsPerMicroBatch));
    if (microBatchPlanner != null) {
      Offset reportedOffset = microBatchPlanner.reportLatestOffset();
      if (reportedOffset != null) {
        metricsMap.put("latestOffset", reportedOffset.json());
      }
    }
    return Collections.unmodifiableMap(metricsMap);
  }

  private void initializePlanner(StreamingOffset startOffset, StreamingOffset endOffset) {
    if (readConf.asyncMicroBatchPlanningEnabled()) {
      this.microBatchPlanner =
          new AsyncSparkMicroBatchPlanner(
              table, readConf, startOffset, endOffset, lastOffsetForTriggerAvailableNow);
    } else {
      this.microBatchPlanner =
          new SyncSparkMicroBatchPlanner(table, readConf, lastOffsetForTriggerAvailableNow);
    }
  }

  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    // Initialize planner if not already done
    if (microBatchPlanner == null) {
      initializePlanner((StreamingOffset) startOffset, null);
    }

    return microBatchPlanner.latestOffset((StreamingOffset) startOffset, limit);
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

  @Override
  public ReadLimit getDefaultReadLimit() {
    if (maxFilesPerMicroBatch != Integer.MAX_VALUE
        && maxRecordsPerMicroBatch != Integer.MAX_VALUE) {
      ReadLimit[] readLimits = new ReadLimit[2];
      readLimits[0] = ReadLimit.maxFiles(maxFilesPerMicroBatch);
      readLimits[1] = ReadLimit.maxRows(maxRecordsPerMicroBatch);
      return ReadLimit.compositeLimit(readLimits);
    } else if (maxFilesPerMicroBatch != Integer.MAX_VALUE) {
      return ReadLimit.maxFiles(maxFilesPerMicroBatch);
    } else if (maxRecordsPerMicroBatch != Integer.MAX_VALUE) {
      return ReadLimit.maxRows(maxRecordsPerMicroBatch);
    } else {
      return ReadLimit.allAvailable();
    }
  }

  @Override
  public void prepareForTriggerAvailableNow() {
    LOG.info("The streaming query reports to use Trigger.AvailableNow");

    lastOffsetForTriggerAvailableNow =
        (StreamingOffset) latestOffset(initialOffset, ReadLimit.allAvailable());

    LOG.info("lastOffset for Trigger.AvailableNow is {}", lastOffsetForTriggerAvailableNow.json());

    // Reset planner so it gets recreated with the cap on next call
    if (microBatchPlanner != null) {
      microBatchPlanner.stop();
      microBatchPlanner = null;
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
