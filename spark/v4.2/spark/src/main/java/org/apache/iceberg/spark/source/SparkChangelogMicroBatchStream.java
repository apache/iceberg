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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadAllAvailable;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;

/**
 * A minimal changelog stream that advances at Iceberg snapshot boundaries.
 *
 * <p>Each planned range contains complete snapshots, ensuring that all rows from a commit remain in
 * the same Spark micro-batch.
 */
class SparkChangelogMicroBatchStream extends SparkMicroBatchStreamBase {

  private Broadcast<Table> plannedTableBroadcast = null;
  private final Schema dataSchema;
  private final SparkChangelogRange range;

  SparkChangelogMicroBatchStream(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      Schema projection,
      String checkpointLocation,
      SparkChangelogRange range) {
    super(
        sparkContext,
        table,
        table::io,
        readConf,
        projection,
        checkpointLocation,
        () -> StreamingOffset.START_OFFSET);
    this.dataSchema = SparkChangelogTable.dropCdcMetadata(projection);
    this.range = range;
  }

  @Override
  protected StreamingOffset latestStreamingOffset() {
    table().refresh();
    Snapshot latest = table().currentSnapshot();
    return latest != null
        ? new StreamingOffset(latest.snapshotId(), 0, false)
        : StreamingOffset.START_OFFSET;
  }

  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    Preconditions.checkArgument(
        startOffset instanceof StreamingOffset, "Invalid start offset: %s", startOffset);

    StreamingOffset latestOffset = (StreamingOffset) latestOffset();
    if (latestOffset.equals(StreamingOffset.START_OFFSET) || latestOffset.equals(startOffset)) {
      return null;
    } else if (limit instanceof ReadAllAvailable) {
      return latestOffset;
    }

    BaseSparkMicroBatchPlanner.UnpackedLimits limits =
        new BaseSparkMicroBatchPlanner.UnpackedLimits(limit);
    List<Snapshot> snapshots = snapshotsBetween((StreamingOffset) startOffset, latestOffset);
    long rows = 0;
    long files = 0;
    Snapshot last = null;
    for (Snapshot snapshot : snapshots) {
      // Admission limits are soft: a single snapshot is never split across batches.
      if (last != null
          && (rows >= limits.getMaxRows() || files >= limits.getMaxFiles())
          && snapshot.timestampMillis() != last.timestampMillis()) {
        break;
      }

      last = snapshot;
      if (range.includes(snapshot)) {
        rows +=
            PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.ADDED_RECORDS_PROP, 0)
                + PropertyUtil.propertyAsLong(
                    snapshot.summary(), SnapshotSummary.DELETED_RECORDS_PROP, 0);
        files +=
            PropertyUtil.propertyAsLong(snapshot.summary(), SnapshotSummary.ADDED_FILES_PROP, 0)
                + PropertyUtil.propertyAsLong(
                    snapshot.summary(), SnapshotSummary.DELETED_FILES_PROP, 0);
      }
    }

    return last != null ? new StreamingOffset(last.snapshotId(), 0, false) : null;
  }

  @Override
  public ReadLimit getDefaultReadLimit() {
    return ReadLimit.compositeLimit(
        new ReadLimit[] {
          ReadLimit.maxFiles(readConf().maxFilesPerMicroBatch()),
          ReadLimit.maxRows(readConf().maxRecordsPerMicroBatch())
        });
  }

  @Override
  protected List<ScanTaskGroup<ChangelogScanTask>> planTaskGroups(
      StreamingOffset startOffset, StreamingOffset endOffset) {
    if (endOffset.equals(StreamingOffset.START_OFFSET) || startOffset.equals(endOffset)) {
      return Lists.newArrayList();
    }

    table().refresh();
    if (!startOffset.equals(StreamingOffset.START_OFFSET)) {
      Preconditions.checkState(
          table().snapshot(startOffset.snapshotId()) != null,
          "Cannot load changelog start offset at expired or removed snapshot: %s",
          startOffset.snapshotId());
    }

    Preconditions.checkState(
        table().snapshot(endOffset.snapshotId()) != null,
        "Cannot load changelog end offset at expired or removed snapshot: %s",
        endOffset.snapshotId());

    validateReadSchema();
    if (startOffset.equals(StreamingOffset.START_OFFSET)) {
      range.validateVersions(table());
    }

    if (range.requiresPostProcessing()) {
      validateCommitTimestamps(startOffset, endOffset);
    }

    IncrementalChangelogScan scan =
        table()
            .newIncrementalChangelogScan()
            .caseSensitive(readConf().caseSensitive())
            .project(ChangelogUtil.changelogSchema(dataSchema))
            .option(TableProperties.SPLIT_SIZE, String.valueOf(readConf().splitSize()))
            .option(TableProperties.SPLIT_LOOKBACK, String.valueOf(readConf().splitLookback()))
            .option(
                TableProperties.SPLIT_OPEN_FILE_COST,
                String.valueOf(readConf().splitOpenFileCost()));
    Long startSnapshotId =
        startOffset.equals(StreamingOffset.START_OFFSET)
            ? null
            : Long.valueOf(startOffset.snapshotId());
    return range.planTasks(table(), scan, startSnapshotId, endOffset.snapshotId());
  }

  private List<Snapshot> snapshotsBetween(StreamingOffset start, StreamingOffset end) {
    Long startSnapshotId =
        start.equals(StreamingOffset.START_OFFSET) ? null : Long.valueOf(start.snapshotId());
    if (startSnapshotId != null) {
      Preconditions.checkState(
          SnapshotUtil.isParentAncestorOf(table(), end.snapshotId(), startSnapshotId),
          "Cannot read CDC: snapshot %s is not an ancestor of %s",
          startSnapshotId,
          end.snapshotId());
    }

    List<Snapshot> snapshots = new ArrayList<>();
    SnapshotUtil.ancestorsBetween(table(), end.snapshotId(), startSnapshotId)
        .forEach(snapshots::add);
    Collections.reverse(snapshots);
    return snapshots;
  }

  private void validateCommitTimestamps(StreamingOffset start, StreamingOffset end) {
    Long previousTimestamp =
        start.equals(StreamingOffset.START_OFFSET)
            ? null
            : Long.valueOf(table().snapshot(start.snapshotId()).timestampMillis());
    boolean first = true;
    for (Snapshot snapshot : snapshotsBetween(start, end)) {
      long timestamp = snapshot.timestampMillis();
      // Spark's zero-delay CDC watermark drops timestamps <= the preceding batch's maximum.
      Preconditions.checkState(
          previousTimestamp == null
              || (first ? timestamp > previousTimestamp : timestamp >= previousTimestamp),
          "Cannot stream CDC post-processing at snapshot %s: commit timestamp %s does not advance "
              + "past %s. Use batch CDC or deduplicationMode=none with computeUpdates=false",
          snapshot.snapshotId(),
          timestamp,
          previousTimestamp);
      previousTimestamp = timestamp;
      first = false;
    }
  }

  private void validateReadSchema() {
    for (Types.NestedField field : dataSchema.columns()) {
      if (field.fieldId() != MetadataColumns.ROW_ID.fieldId()
          && field.fieldId() != MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()) {
        Types.NestedField current = table().schema().findField(field.fieldId());
        Preconditions.checkState(
            current != null && current.type().equals(field.type()),
            "Cannot continue CDC after an incompatible schema change to field %s",
            field.name());
      }
    }
  }

  @Override
  protected Broadcast<Table> tableBroadcast() {
    if (plannedTableBroadcast != null) {
      plannedTableBroadcast.unpersist(false);
    }

    this.plannedTableBroadcast =
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table()));
    return plannedTableBroadcast;
  }

  @Override
  protected void stopStream() {
    if (plannedTableBroadcast != null) {
      plannedTableBroadcast.unpersist(false);
      plannedTableBroadcast = null;
    }
  }
}
