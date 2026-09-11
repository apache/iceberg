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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;

/**
 * A minimal changelog stream that advances at Iceberg snapshot boundaries.
 *
 * <p>Each planned range contains complete snapshots, ensuring that all rows from a commit remain in
 * the same Spark micro-batch.
 */
class SparkChangelogMicroBatchStream extends SparkMicroBatchStreamBase {

  private Broadcast<Table> plannedTableBroadcast = null;

  SparkChangelogMicroBatchStream(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      Schema projection,
      String checkpointLocation) {
    super(
        sparkContext,
        table,
        table::io,
        readConf,
        projection,
        checkpointLocation,
        () -> configuredInitialOffset(readConf));
  }

  private static StreamingOffset configuredInitialOffset(SparkReadConf readConf) {
    return readConf.startSnapshotId() != null
        ? new StreamingOffset(readConf.startSnapshotId(), 0, false)
        : StreamingOffset.START_OFFSET;
  }

  @Override
  protected StreamingOffset latestStreamingOffset() {
    table().refresh();
    Snapshot latest = table().currentSnapshot();
    Long configuredEndSnapshotId = readConf().endSnapshotId();
    if (configuredEndSnapshotId != null) {
      latest = table().snapshot(configuredEndSnapshotId);
    }

    return latest != null
        ? new StreamingOffset(latest.snapshotId(), 0, false)
        : StreamingOffset.START_OFFSET;
  }

  @Override
  public Offset latestOffset(Offset startOffset, ReadLimit limit) {
    Preconditions.checkArgument(
        startOffset instanceof StreamingOffset, "Invalid start offset: %s", startOffset);

    StreamingOffset latestOffset = (StreamingOffset) latestOffset();
    return latestOffset.equals(StreamingOffset.START_OFFSET) || latestOffset.equals(startOffset)
        ? null
        : latestOffset;
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

    IncrementalChangelogScan scan =
        table()
            .newIncrementalChangelogScan()
            .caseSensitive(readConf().caseSensitive())
            .project(ChangelogUtil.changelogSchema(SparkChangelogTable.cdcDataSchema(table())));
    if (!startOffset.equals(StreamingOffset.START_OFFSET)) {
      scan = scan.fromSnapshotExclusive(startOffset.snapshotId());
    }
    scan = scan.toSnapshot(endOffset.snapshotId());

    List<ScanTaskGroup<ChangelogScanTask>> taskGroups;
    try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> groups = scan.planTasks()) {
      taskGroups = Lists.newArrayList(groups);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close Iceberg changelog task groups", e);
    }

    return taskGroups;
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
