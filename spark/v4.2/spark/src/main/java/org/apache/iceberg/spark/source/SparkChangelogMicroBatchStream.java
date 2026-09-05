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
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.SupportsTriggerAvailableNow;

/**
 * A minimal changelog stream that advances at Iceberg snapshot boundaries.
 *
 * <p>Each planned range contains complete snapshots, ensuring that all rows from a commit remain in
 * the same Spark micro-batch.
 */
class SparkChangelogMicroBatchStream
    implements MicroBatchStream, SupportsTriggerAvailableNow {

  private static final Types.StructType EMPTY_GROUPING_KEY_TYPE = Types.StructType.of();

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final SparkReadConf readConf;
  private final Schema projection;
  private final StreamingOffset initialOffset;
  private StreamingOffset lastOffsetForTriggerAvailableNow = null;

  SparkChangelogMicroBatchStream(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      Schema projection,
      String checkpointLocation) {
    this.sparkContext = sparkContext;
    this.table = table;
    this.readConf = readConf;
    this.projection = projection;
    StreamingOffset configuredInitialOffset =
        readConf.startSnapshotId() != null
            ? new StreamingOffset(readConf.startSnapshotId(), 0, false)
            : StreamingOffset.START_OFFSET;
    this.initialOffset =
        new StreamingInitialOffsetStore(
                checkpointLocation,
                sparkContext.hadoopConfiguration(),
                () -> configuredInitialOffset)
            .initialOffset();
  }

  @Override
  public Offset latestOffset() {
    if (lastOffsetForTriggerAvailableNow != null) {
      return lastOffsetForTriggerAvailableNow;
    }

    table.refresh();
    Snapshot latest = table.currentSnapshot();
    Long configuredEndSnapshotId = readConf.endSnapshotId();
    if (configuredEndSnapshotId != null) {
      latest = table.snapshot(configuredEndSnapshotId);
    }

    return latest != null
        ? new StreamingOffset(latest.snapshotId(), 0, false)
        : StreamingOffset.START_OFFSET;
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    Preconditions.checkArgument(
        start instanceof StreamingOffset, "Invalid changelog start offset: %s", start);
    Preconditions.checkArgument(
        end instanceof StreamingOffset, "Invalid changelog end offset: %s", end);

    StreamingOffset startOffset = (StreamingOffset) start;
    StreamingOffset endOffset = (StreamingOffset) end;
    if (endOffset.equals(StreamingOffset.START_OFFSET) || startOffset.equals(endOffset)) {
      return new InputPartition[0];
    }

    table.refresh();
    if (!startOffset.equals(StreamingOffset.START_OFFSET)) {
      Preconditions.checkState(
          table.snapshot(startOffset.snapshotId()) != null,
          "Cannot load changelog start offset at expired or removed snapshot: %s",
          startOffset.snapshotId());
    }

    Preconditions.checkState(
        table.snapshot(endOffset.snapshotId()) != null,
        "Cannot load changelog end offset at expired or removed snapshot: %s",
        endOffset.snapshotId());

    IncrementalChangelogScan scan =
        table
            .newIncrementalChangelogScan()
            .caseSensitive(readConf.caseSensitive())
            .project(ChangelogUtil.changelogSchema(SparkChangelogTable.cdcDataSchema(table)));
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

    Broadcast<Table> tableBroadcast =
        sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    Broadcast<FileIO> fileIOBroadcast =
        sparkContext.broadcast(SerializableFileIOWithSize.wrap(table.io()));
    String projectionJson = SchemaParser.toJson(projection);
    InputPartition[] partitions = new InputPartition[taskGroups.size()];
    for (int index = 0; index < taskGroups.size(); index++) {
      partitions[index] =
          new SparkInputPartition(
              EMPTY_GROUPING_KEY_TYPE,
              taskGroups.get(index),
              tableBroadcast,
              fileIOBroadcast,
              projectionJson,
              readConf.caseSensitive(),
              SparkPlanningUtil.NO_LOCATION_PREFERENCE,
              readConf.cacheDeleteFilesOnExecutors());
    }

    return partitions;
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

  @Override
  public void prepareForTriggerAvailableNow() {
    this.lastOffsetForTriggerAvailableNow = (StreamingOffset) latestOffset();
  }
}
