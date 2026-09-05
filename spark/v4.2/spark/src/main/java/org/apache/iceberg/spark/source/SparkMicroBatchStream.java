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
import java.util.function.Supplier;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkMicroBatchStream extends SparkMicroBatchStreamBase {
  private static final Logger LOG = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private final long splitSize;
  private final int splitLookback;
  private final long splitOpenFileCost;
  private final boolean localityPreferred;
  private final long fromTimestamp;
  private final int maxFilesPerMicroBatch;
  private final int maxRecordsPerMicroBatch;
  private SparkMicroBatchPlanner planner;

  SparkMicroBatchStream(
      JavaSparkContext sparkContext,
      Table table,
      Supplier<FileIO> fileIO,
      SparkReadConf readConf,
      Schema projection,
      String checkpointLocation) {
    super(
        sparkContext,
        table,
        fileIO,
        readConf,
        projection,
        checkpointLocation,
        () -> {
          table.refresh();
          return MicroBatchUtils.determineStartingOffset(table, readConf.streamFromTimestamp());
        });
    this.localityPreferred = readConf.localityEnabled();
    this.splitSize = readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.splitOpenFileCost = readConf.splitOpenFileCost();
    this.fromTimestamp = readConf.streamFromTimestamp();
    this.maxFilesPerMicroBatch = readConf.maxFilesPerMicroBatch();
    this.maxRecordsPerMicroBatch = readConf.maxRecordsPerMicroBatch();
  }

  @Override
  protected StreamingOffset latestStreamingOffset() {
    table().refresh();
    if (table().currentSnapshot() == null) {
      return StreamingOffset.START_OFFSET;
    }

    if (table().currentSnapshot().timestampMillis() < fromTimestamp) {
      return StreamingOffset.START_OFFSET;
    }

    Snapshot latestSnapshot = table().currentSnapshot();

    return new StreamingOffset(
        latestSnapshot.snapshotId(),
        MicroBatchUtils.addedFilesCount(table(), latestSnapshot),
        false);
  }

  @Override
  protected List<CombinedScanTask> planTaskGroups(
      StreamingOffset startOffset, StreamingOffset endOffset) {
    if (endOffset.equals(StreamingOffset.START_OFFSET)) {
      return Lists.newArrayList();
    }

    // Initialize planner if not already done (for resume scenarios)
    if (planner == null) {
      initializePlanner(startOffset, endOffset);
    }

    List<FileScanTask> fileScanTasks = planner.planFiles(startOffset, endOffset);

    CloseableIterable<FileScanTask> splitTasks =
        TableScanUtil.splitFiles(CloseableIterable.withNoopClose(fileScanTasks), splitSize);
    return Lists.newArrayList(
        TableScanUtil.planTasks(splitTasks, splitSize, splitLookback, splitOpenFileCost));
  }

  @Override
  protected boolean localityPreferred() {
    return localityPreferred;
  }

  @Override
  protected void stopStream() {
    if (planner != null) {
      planner.stop();
    }
  }

  private void initializePlanner(StreamingOffset startOffset, StreamingOffset endOffset) {
    if (readConf().asyncMicroBatchPlanningEnabled()) {
      this.planner =
          new AsyncSparkMicroBatchPlanner(
              table(), readConf(), startOffset, endOffset, lastOffsetForTriggerAvailableNow());
    } else {
      this.planner =
          new SyncSparkMicroBatchPlanner(table(), readConf(), lastOffsetForTriggerAvailableNow());
    }
  }

  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    Preconditions.checkArgument(
        startOffset instanceof StreamingOffset,
        "Invalid start offset: %s is not a StreamingOffset",
        startOffset);

    // Initialize planner if not already done
    if (planner == null) {
      initializePlanner((StreamingOffset) startOffset, null);
    }

    return planner.latestOffset((StreamingOffset) startOffset, limit);
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
  protected StreamingOffset availableNowEndOffset() {
    LOG.info("The streaming query reports to use Trigger.AvailableNow");

    StreamingOffset endOffset =
        (StreamingOffset) latestOffset(initialStreamingOffset(), ReadLimit.allAvailable());

    LOG.info("lastOffset for Trigger.AvailableNow is {}", endOffset.json());
    return endOffset;
  }

  @Override
  protected void availableNowPrepared() {
    // Reset planner so it gets recreated with the cap on next call
    if (planner != null) {
      planner.stop();
      planner = null;
    }
  }
}
