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

import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.MicroBatches.MicroBatchBuilder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.OffsetSeq;
import org.apache.spark.sql.execution.streaming.OffsetSeqLog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;

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
  private final int batchSize;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final boolean localityPreferred;
  private final OffsetSeqLog offsetSeqLog;

  private StreamingOffset initialOffset = null;
  private PlannedEndOffset previousEndOffset = null;

  SparkMicroBatchStream(SparkSession spark, JavaSparkContext sparkContext,
                        Table table, boolean caseSensitive, Schema expectedSchema,
                        CaseInsensitiveStringMap options, String checkpointLocation) {
    this.sparkContext = sparkContext;
    this.table = table;
    this.caseSensitive = caseSensitive;
    this.expectedSchema = expectedSchema;
    this.batchSize = Spark3Util.batchSize(table.properties(), options);
    this.localityPreferred = Spark3Util.isLocalityEnabled(table.io(), table.location(), options);
    this.splitSize = Optional.ofNullable(Spark3Util.propertyAsLong(options, SparkReadOptions.SPLIT_SIZE, null))
        .orElseGet(() -> PropertyUtil.propertyAsLong(table.properties(), SPLIT_SIZE, SPLIT_SIZE_DEFAULT));
    this.splitLookback = Optional.ofNullable(Spark3Util.propertyAsInt(options, SparkReadOptions.LOOKBACK, null))
        .orElseGet(() -> PropertyUtil.propertyAsInt(table.properties(), SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT));
    this.splitOpenFileCost = Optional.ofNullable(
        Spark3Util.propertyAsLong(options, SparkReadOptions.FILE_OPEN_COST, null))
        .orElseGet(() -> PropertyUtil.propertyAsLong(table.properties(), SPLIT_OPEN_FILE_COST,
            SPLIT_OPEN_FILE_COST_DEFAULT));
    this.offsetSeqLog = checkpointLocation != null ?
        new OffsetSeqLog(spark, getOffsetLogLocation(checkpointLocation)) :
        null;
  }

  @Override
  public Offset latestOffset() {
    initialOffset();

    if (isTableEmpty()) {
      return StreamingOffset.START_OFFSET;
    }

    StreamingOffset microBatchStartOffset = isFirstBatch() ? initialOffset : previousEndOffset;
    if (isEndOfSnapshot(microBatchStartOffset)) {
      microBatchStartOffset = getNextAvailableSnapshot(microBatchStartOffset);
    }

    previousEndOffset = calculateEndOffset(microBatchStartOffset);
    return previousEndOffset;
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
        end instanceof PlannedEndOffset,
        "The end offset passed to planInputPartitions() is not the one that is returned by lastOffset()");
    PlannedEndOffset endOffset = (PlannedEndOffset) end;

    List<FileScanTask> fileScanTasks = endOffset.getMicroBatch().tasks();

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
    if (isInitialOffsetResolved()) {
      return initialOffset;
    }

    if (isStreamResumedFromCheckpoint()) {
      initialOffset = calculateInitialOffsetFromCheckpoint();
      return initialOffset;
    }

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    if (snapshotIds.isEmpty()) {
      initialOffset = StreamingOffset.START_OFFSET;
      Preconditions.checkState(isTableEmpty(),
          "criteria behind isTableEmpty() changed.");
    } else {
      initialOffset = new StreamingOffset(Iterables.getLast(snapshotIds), 0, true);
    }

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

  private String getOffsetLogLocation(String checkpointLocation) {
    return new Path(checkpointLocation.replace("/sources/0", ""), "offsets").toString();
  }

  private boolean isInitialOffsetResolved() {
    return initialOffset != null;
  }

  private StreamingOffset calculateInitialOffsetFromCheckpoint() {
    Preconditions.checkState(isStreamResumedFromCheckpoint(),
        "Stream is not resumed from checkpoint.");

    OffsetSeq offsetSeq = offsetSeqLog.getLatest().get()._2;

    List<Option<Offset>> offsetSeqCol = JavaConverters.seqAsJavaList(offsetSeq.offsets());
    Option<Offset> optionalOffset = offsetSeqCol.get(0);

    StreamingOffset checkpointedOffset = StreamingOffset.fromJson(optionalOffset.get().json());
    return checkpointedOffset;
  }

  private boolean isStreamResumedFromCheckpoint() {
    Preconditions.checkState(!isInitialOffsetResolved(),
        "isStreamResumedFromCheckpoint() is invoked without resolving initialOffset");

    return offsetSeqLog != null && offsetSeqLog.getLatest() != null && offsetSeqLog.getLatest().isDefined();
  }

  private boolean isFirstBatch() {
    return previousEndOffset == null || previousEndOffset.equals(StreamingOffset.START_OFFSET);
  }

  private boolean isTableEmpty() {
    Preconditions.checkState(isInitialOffsetResolved(),
        "isTableEmpty() is invoked without resolving initialOffset");

    return initialOffset.equals(StreamingOffset.START_OFFSET);
  }

  private StreamingOffset getNextAvailableSnapshot(StreamingOffset microBatchStartOffset) {
    if (table.currentSnapshot().snapshotId() == microBatchStartOffset.snapshotId()) {
      return microBatchStartOffset;
    }

    Snapshot previousSnapshot = table.snapshot(microBatchStartOffset.snapshotId());
    Snapshot pointer = table.currentSnapshot();
    while (pointer != null && previousSnapshot.snapshotId() != pointer.parentId()) {
      pointer = table.snapshot(pointer.parentId());
    }

    return new StreamingOffset(pointer.snapshotId(), 0L, false);
  }

  private PlannedEndOffset calculateEndOffset(StreamingOffset microBatchStartOffset) {
    MicroBatch microBatch = MicroBatches.from(table.snapshot(microBatchStartOffset.snapshotId()), table.io())
        .caseSensitive(caseSensitive)
        .specsById(table.specs())
        .generate(microBatchStartOffset.position(), batchSize, microBatchStartOffset.shouldScanAllFiles());

    return new PlannedEndOffset(
        microBatch.snapshotId(),
        microBatch.endFileIndex(),
        microBatchStartOffset.shouldScanAllFiles(),
        microBatch);
  }

  private boolean isEndOfSnapshot(StreamingOffset microBatchStartOffset) {
    MicroBatchBuilder microBatchBuilder = MicroBatches.from(
        table.snapshot(microBatchStartOffset.snapshotId()), table.io())
        .caseSensitive(caseSensitive)
        .specsById(table.specs());

    MicroBatch microBatchStart = microBatchBuilder.generate(
        microBatchStartOffset.position(),
        1,
        microBatchStartOffset.shouldScanAllFiles());

    return microBatchStartOffset.position() == microBatchStart.startFileIndex() &&
        microBatchStartOffset.position() == microBatchStart.endFileIndex() &&
        microBatchStart.lastIndexOfSnapshot();
  }

  private static class PlannedEndOffset extends StreamingOffset {

    private final MicroBatch microBatch;

    PlannedEndOffset(long snapshotId, long position, boolean scanAllFiles, MicroBatch microBatch) {
      super(snapshotId, position, scanAllFiles);
      this.microBatch = microBatch;
    }

    public MicroBatch getMicroBatch() {
      return microBatch;
    }
  }
}
