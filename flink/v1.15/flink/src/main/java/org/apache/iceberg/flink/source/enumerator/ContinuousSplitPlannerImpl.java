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
package org.apache.iceberg.flink.source.enumerator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.FlinkSplitPlanner;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class ContinuousSplitPlannerImpl implements ContinuousSplitPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousSplitPlannerImpl.class);

  private final Table table;
  private final ScanContext scanContext;
  private final boolean isSharedPool;
  private final ExecutorService workerPool;

  /**
   * @param threadName thread name prefix for worker pool to run the split planning. If null, a
   *     shared worker pool will be used.
   */
  public ContinuousSplitPlannerImpl(Table table, ScanContext scanContext, String threadName) {
    this.table = table;
    this.scanContext = scanContext;
    this.isSharedPool = threadName == null;
    this.workerPool =
        isSharedPool
            ? ThreadPools.getWorkerPool()
            : ThreadPools.newWorkerPool(
                "iceberg-plan-worker-pool-" + threadName, scanContext.planParallelism());
  }

  @Override
  public void close() throws IOException {
    if (!isSharedPool) {
      workerPool.shutdown();
    }
  }

  @Override
  public ContinuousEnumerationResult planSplits(IcebergEnumeratorPosition lastPosition) {
    table.refresh();
    if (lastPosition != null) {
      return discoverIncrementalSplits(lastPosition);
    } else {
      return discoverInitialSplits();
    }
  }

  /** Discover incremental changes between @{code lastPosition} and current table snapshot */
  private ContinuousEnumerationResult discoverIncrementalSplits(
      IcebergEnumeratorPosition lastPosition) {
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot == null) {
      // empty table
      Preconditions.checkArgument(
          lastPosition.snapshotId() == null,
          "Invalid last enumerated position for an empty table: not null");
      LOG.info("Skip incremental scan because table is empty");
      return new ContinuousEnumerationResult(Collections.emptyList(), lastPosition, lastPosition);
    } else if (lastPosition.snapshotId() != null
        && currentSnapshot.snapshotId() == lastPosition.snapshotId()) {
      LOG.info("Current table snapshot is already enumerated: {}", currentSnapshot.snapshotId());
      return new ContinuousEnumerationResult(Collections.emptyList(), lastPosition, lastPosition);
    } else {
      IcebergEnumeratorPosition newPosition =
          IcebergEnumeratorPosition.of(
              currentSnapshot.snapshotId(), currentSnapshot.timestampMillis());
      ScanContext incrementalScan =
          scanContext.copyWithAppendsBetween(
              lastPosition.snapshotId(), currentSnapshot.snapshotId());
      List<IcebergSourceSplit> splits =
          FlinkSplitPlanner.planIcebergSourceSplits(table, incrementalScan, workerPool);
      LOG.info(
          "Discovered {} splits from incremental scan: "
              + "from snapshot (exclusive) is {}, to snapshot (inclusive) is {}",
          splits.size(),
          lastPosition,
          newPosition);
      return new ContinuousEnumerationResult(splits, lastPosition, newPosition);
    }
  }

  /**
   * Discovery initial set of splits based on {@link StreamingStartingStrategy}.
   * <li>{@link ContinuousEnumerationResult#splits()} should contain initial splits discovered from
   *     table scan for {@link StreamingStartingStrategy#TABLE_SCAN_THEN_INCREMENTAL}. For all other
   *     strategies, splits collection should be empty.
   * <li>{@link ContinuousEnumerationResult#toPosition()} points to the starting position for the
   *     next incremental split discovery with exclusive behavior. Meaning files committed by the
   *     snapshot from the position in {@code ContinuousEnumerationResult} won't be included in the
   *     next incremental scan.
   */
  private ContinuousEnumerationResult discoverInitialSplits() {
    Optional<Snapshot> startSnapshotOptional = startSnapshot(table, scanContext);
    if (!startSnapshotOptional.isPresent()) {
      return new ContinuousEnumerationResult(
          Collections.emptyList(), null, IcebergEnumeratorPosition.empty());
    }

    Snapshot startSnapshot = startSnapshotOptional.get();
    LOG.info(
        "Get starting snapshot id {} based on strategy {}",
        startSnapshot.snapshotId(),
        scanContext.streamingStartingStrategy());
    List<IcebergSourceSplit> splits;
    IcebergEnumeratorPosition toPosition;
    if (scanContext.streamingStartingStrategy()
        == StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL) {
      // do a batch table scan first
      splits = FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext, workerPool);
      LOG.info(
          "Discovered {} splits from initial batch table scan with snapshot Id {}",
          splits.size(),
          startSnapshot.snapshotId());
      // For TABLE_SCAN_THEN_INCREMENTAL, incremental mode starts exclusive from the startSnapshot
      toPosition =
          IcebergEnumeratorPosition.of(startSnapshot.snapshotId(), startSnapshot.timestampMillis());
    } else {
      // For all other modes, starting snapshot should be consumed inclusively.
      // Use parentId to achieve the inclusive behavior. It is fine if parentId is null.
      splits = Collections.emptyList();
      Long parentSnapshotId = startSnapshot.parentId();
      if (parentSnapshotId != null) {
        Snapshot parentSnapshot = table.snapshot(parentSnapshotId);
        Long parentSnapshotTimestampMs =
            parentSnapshot != null ? parentSnapshot.timestampMillis() : null;
        toPosition = IcebergEnumeratorPosition.of(parentSnapshotId, parentSnapshotTimestampMs);
      } else {
        toPosition = IcebergEnumeratorPosition.empty();
      }

      LOG.info(
          "Start incremental scan with start snapshot (inclusive): id = {}, timestamp = {}",
          startSnapshot.snapshotId(),
          startSnapshot.timestampMillis());
    }

    return new ContinuousEnumerationResult(splits, null, toPosition);
  }

  /**
   * Calculate the starting snapshot based on the {@link StreamingStartingStrategy} defined in
   * {@code ScanContext}.
   *
   * <p>If the {@link StreamingStartingStrategy} is not {@link
   * StreamingStartingStrategy#TABLE_SCAN_THEN_INCREMENTAL}, the start snapshot should be consumed
   * inclusively.
   */
  @VisibleForTesting
  static Optional<Snapshot> startSnapshot(Table table, ScanContext scanContext) {
    switch (scanContext.streamingStartingStrategy()) {
      case TABLE_SCAN_THEN_INCREMENTAL:
      case INCREMENTAL_FROM_LATEST_SNAPSHOT:
        return Optional.ofNullable(table.currentSnapshot());
      case INCREMENTAL_FROM_EARLIEST_SNAPSHOT:
        return Optional.ofNullable(SnapshotUtil.oldestAncestor(table));
      case INCREMENTAL_FROM_SNAPSHOT_ID:
        Snapshot matchedSnapshotById = table.snapshot(scanContext.startSnapshotId());
        Preconditions.checkArgument(
            matchedSnapshotById != null,
            "Start snapshot id not found in history: " + scanContext.startSnapshotId());
        return Optional.of(matchedSnapshotById);
      case INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP:
        long snapshotIdAsOfTime =
            SnapshotUtil.snapshotIdAsOfTime(table, scanContext.startSnapshotTimestamp());
        Snapshot matchedSnapshotByTimestamp = table.snapshot(snapshotIdAsOfTime);
        if (matchedSnapshotByTimestamp.timestampMillis() == scanContext.startSnapshotTimestamp()) {
          return Optional.of(matchedSnapshotByTimestamp);
        } else {
          // if the snapshotIdAsOfTime has the timestamp value smaller than the
          // scanContext.startSnapshotTimestamp(),
          // return the child snapshot whose timestamp value is larger
          return Optional.of(SnapshotUtil.snapshotAfter(table, snapshotIdAsOfTime));
        }
      default:
        throw new IllegalArgumentException(
            "Unknown starting strategy: " + scanContext.streamingStartingStrategy());
    }
  }
}
