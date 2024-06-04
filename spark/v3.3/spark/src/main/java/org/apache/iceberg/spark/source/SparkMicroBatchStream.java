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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.CompositeReadLimit;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.ReadMaxFiles;
import org.apache.spark.sql.connector.read.streaming.ReadMaxRows;
import org.apache.spark.sql.connector.read.streaming.SupportsAdmissionControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkMicroBatchStream implements MicroBatchStream, SupportsAdmissionControl {
  private static final Logger LOG = LoggerFactory.getLogger(SparkMicroBatchStream.class);
  private static final Types.StructType EMPTY_GROUPING_KEY_TYPE = Types.StructType.of();

  private final Table table;
  private final SparkReadConf readConf;
  private final String branch;
  private final boolean caseSensitive;
  private final String expectedSchema;
  private final Broadcast<Table> tableBroadcast;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final boolean localityPreferred;
  private final boolean skipDelete;
  private final boolean skipOverwrite;
  private final Long fromTimestamp;
  private final Integer maxFilesPerMicroBatch;
  private final Integer maxRecordsPerMicroBatch;
  private SparkMicroBatchPlanner microBatchPlanner;

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
    this.tableBroadcast = sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    this.splitSize = readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.splitOpenFileCost = readConf.splitOpenFileCost();
    this.localityPreferred = readConf.localityEnabled();
    this.skipDelete = readConf.streamingSkipDeleteSnapshots();
    this.skipOverwrite = readConf.streamingSkipOverwriteSnapshots();
    this.fromTimestamp = readConf.streamFromTimestamp();
    this.maxFilesPerMicroBatch = readConf.maxFilesPerMicroBatch();
    this.maxRecordsPerMicroBatch = readConf.maxRecordsPerMicroBatch();
    // this refresh is important
    this.table.refresh();
    // microbatch planner isn't initialized here, rather when we wither get our first of either:
    // - planInputPartitions (called when resuming from a snapshot with committed offsets)
    // - latestOffset (called when starting without committed offsets)
  }

  public static StreamingOffset computeInitialOffset(Table table, SparkReadConf readConf) {
    Long fromTimestamp = readConf.streamFromTimestamp();

    if (table.currentSnapshot() == null) {
      LOG.debug("computeInitialOffset: Table is empty, returning START_OFFSET");
      return StreamingOffset.START_OFFSET;
    }

    if (fromTimestamp == null) {
      LOG.debug("computeInitialOffset: fromTimestamp is null, returning oldest snapshot");
      // match existing behavior and start from the oldest snapshot
      Snapshot oldestSnapshot = SnapshotUtil.oldestAncestor(table);
      return new StreamingOffset(
          oldestSnapshot.snapshotId(),
          0,
          false,
          oldestSnapshot.timestampMillis(),
          Long.parseLong(
              oldestSnapshot.summary().getOrDefault(CommitMetricsResult.TOTAL_RECORDS, "-1")));
    }

    if (table.currentSnapshot().timestampMillis() < fromTimestamp) {
      LOG.debug("computeInitialOffset: all snapshots are older than {}", fromTimestamp);
      return StreamingOffset.START_OFFSET;
    }

    try {
      Snapshot snapshot = SnapshotUtil.oldestAncestorAfter(table, fromTimestamp);
      if (snapshot != null) {
        return new StreamingOffset(
            snapshot.snapshotId(),
            0,
            false,
            snapshot.timestampMillis(),
            Long.parseLong(
                snapshot.summary().getOrDefault(CommitMetricsResult.TOTAL_RECORDS, "-1")));
      } else {

        LOG.debug("computeInitialOffset: could not find ancestor older than {}", fromTimestamp);
        return StreamingOffset.START_OFFSET;
      }
    } catch (IllegalStateException e) {
      LOG.debug("computeInitialOffset: exception", e);
      // could not determine the first snapshot after the timestamp. use the oldest ancestor instead
      Snapshot oldestSnapshot = SnapshotUtil.oldestAncestor(table);
      return new StreamingOffset(
          oldestSnapshot.snapshotId(),
          0,
          false,
          oldestSnapshot.timestampMillis(),
          Long.parseLong(
              oldestSnapshot.summary().getOrDefault(CommitMetricsResult.TOTAL_RECORDS, "-1")));
    }
  }

  @Override
  public Offset latestOffset() {
    throw new UnsupportedOperationException(
        "latestOffset(Offset, ReadLimit) should be called instead of this method");
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
    // this can happen on a startup with available offsets but nothing in the commit log
    if (microBatchPlanner == null) {
      // this will block while the planner fills the queue
      this.microBatchPlanner =
          new AsyncSparkMicroBatchPlanner(table, readConf, startOffset, endOffset);
    }

    List<FileScanTask> fileScanTasks;
    try {
      fileScanTasks = microBatchPlanner.planFiles(startOffset, endOffset);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

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
                        EMPTY_GROUPING_KEY_TYPE,
                        combinedScanTasks.get(index),
                        tableBroadcast,
                        branch,
                        expectedSchema,
                        caseSensitive,
                        localityPreferred));

    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new SparkRowReaderFactory();
  }

  /**
   * Spark will call this function if it is starting the stream without a checkpoint
   *
   * @return The offset to start from, given our read configuration, and no checkpoint
   */
  @Override
  public Offset initialOffset() {
    StreamingOffset computedInitialOffset = computeInitialOffset(table, readConf);
    return computedInitialOffset;
  }

  @Override
  public Offset deserializeOffset(String json) {
    return StreamingOffset.fromJson(json);
  }

  @Override
  public void commit(Offset end) {}

  @Override
  public void stop() {
    microBatchPlanner.stop();
  }

  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    if (microBatchPlanner == null) {
      // this will block while the planner fills the queue
      this.microBatchPlanner =
          new AsyncSparkMicroBatchPlanner(table, readConf, (StreamingOffset) startOffset, null);
    }
    LOG.debug("Getting endOffset, from: {}, limit: {}", startOffset.json(), limit.toString());
    // calculate end offset get snapshotId from the startOffset
    Preconditions.checkArgument(
        startOffset instanceof StreamingOffset,
        "Invalid start offset: %s is not a StreamingOffset",
        startOffset);

    if (table.currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (table.currentSnapshot().timestampMillis() < fromTimestamp) {
      return StreamingOffset.START_OFFSET;
    }

    StreamingOffset latestOffset =
        microBatchPlanner.latestOffset((StreamingOffset) startOffset, limit);
    LOG.info("latestOffset result: {}", latestOffset);
    return latestOffset;
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

  @Override
  public Offset reportLatestOffset() {
    return microBatchPlanner.reportLatestOffset();
  }

  @Override
  public String toString() {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.writeStartObject();
      generator.writeStringField("table", table.name());
      generator.writeStringField("branch", branch);
      generator.writeBooleanField("caseSensitive", caseSensitive);
      try {
        JsonNode parsedSchema = JsonUtil.mapper().readTree(expectedSchema);
        generator.writeObjectField("expectedSchema", parsedSchema);
      } catch (JacksonException schemaParseException) {
        throw new RuntimeException("exectedSchema was not valid json", schemaParseException);
      }
      generator.writeNumberField("splitSize", splitSize);
      generator.writeNumberField("splitLookback", splitLookback);
      generator.writeBooleanField("localityPreferred", localityPreferred);
      generator.writeBooleanField("skipDelete", skipDelete);
      generator.writeBooleanField("skipOverwrite", skipOverwrite);
      generator.writeNumberField("fromTimestamp", fromTimestamp);
      generator.writeNumberField("maxFilesPerMicroBatch", maxFilesPerMicroBatch);
      generator.writeNumberField("maxRecordsPerMicroBatch", maxRecordsPerMicroBatch);
      generator.writeEndObject();
      generator.flush();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write SparkMicroBatchStream to json", e);
    }
    return writer.toString();
  }

  public static class UnpackedLimits {
    private long maxRows = Integer.MAX_VALUE;
    private long maxFiles = Integer.MAX_VALUE;

    public UnpackedLimits(ReadLimit limit) {
      if (limit instanceof CompositeReadLimit) {
        ReadLimit[] compositeLimits = ((CompositeReadLimit) limit).getReadLimits();
        Arrays.stream(compositeLimits)
            .forEach(
                (individualLimit) -> {
                  if (individualLimit instanceof ReadMaxRows) {
                    ReadMaxRows readMaxRows = (ReadMaxRows) individualLimit;
                    this.maxRows = Math.min(this.maxRows, readMaxRows.maxRows());
                  } else if (individualLimit instanceof ReadMaxFiles) {
                    ReadMaxFiles readMaxFiles = (ReadMaxFiles) individualLimit;
                    this.maxFiles = Math.min(this.maxFiles, readMaxFiles.maxFiles());
                  } else {
                    throw new IllegalArgumentException(
                        String.format(
                            "Unknown ReadLimit type %s found inside a CompositeReadLimit",
                            limit.getClass().getCanonicalName()));
                  }
                });
      } else if (limit instanceof ReadMaxRows) {
        this.maxRows = ((ReadMaxRows) limit).maxRows();
      } else if (limit instanceof ReadMaxFiles) {
        this.maxFiles = ((ReadMaxFiles) limit).maxFiles();
      } else {
        throw new IllegalArgumentException(
            String.format("Unknown ReadLimit type %s", limit.getClass().getCanonicalName()));
      }
    }

    public long getMaxRows() {
      return maxRows;
    }

    public long getMaxFiles() {
      return maxFiles;
    }
  }
}
