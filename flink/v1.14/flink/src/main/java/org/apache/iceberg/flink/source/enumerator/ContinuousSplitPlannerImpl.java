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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
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
  private final ExecutorService workerPool;

  public ContinuousSplitPlannerImpl(Table table, ScanContext scanContext, String threadPoolName) {
    this.table = table;
    this.scanContext = scanContext;
    this.workerPool = ThreadPools.newWorkerPool(
        "iceberg-plan-worker-pool-" + threadPoolName, scanContext.planParallelism());
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

  /**
   * Discover incremental changes between @{code lastPosition} and current table snapshot
   */
  private ContinuousEnumerationResult discoverIncrementalSplits(IcebergEnumeratorPosition lastPosition) {
    // incremental discovery mode
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot.snapshotId() == lastPosition.endSnapshotId()) {
      LOG.info("Current table snapshot is already enumerated: {}", currentSnapshot.snapshotId());
      return new ContinuousEnumerationResult(Collections.emptyList(), lastPosition);
    } else {
      ScanContext incrementalScan = scanContext
          .copyWithAppendsBetween(lastPosition.endSnapshotId(), currentSnapshot.snapshotId());
      LOG.info("Incremental scan: startSnapshotId = {}, endSnapshotId = {}",
          incrementalScan.startSnapshotId(), incrementalScan.endSnapshotId());
      List<IcebergSourceSplit> splits = FlinkSplitPlanner.planIcebergSourceSplits(table, incrementalScan, workerPool);
      IcebergEnumeratorPosition position = IcebergEnumeratorPosition.builder()
          .startSnapshotId(lastPosition.endSnapshotId())
          .startSnapshotTimestampMs(lastPosition.endSnapshotTimestampMs())
          .endSnapshotId(currentSnapshot.snapshotId())
          .endSnapshotTimestampMs(currentSnapshot.timestampMillis())
          .build();
      LOG.info("Discovered {} splits from incremental scan: {}", splits.size(), position);
      return new ContinuousEnumerationResult(splits, position);
    }
  }

  /**
   * Discovery initial set of splits based on {@link StreamingStartingStrategy}.
   *
   * <li>{@link ContinuousEnumerationResult#splits()} should contain initial splits
   * discovered from table scan for {@link StreamingStartingStrategy#TABLE_SCAN_THEN_INCREMENTAL}.
   * For all other strategies, splits collection should be empty.
   * <li>{@link ContinuousEnumerationResult#position()} points to the starting position
   * for the next incremental split discovery with exclusive behavior. Meaning files committed
   * by the snapshot from the position in {@code ContinuousEnumerationResult} won't be included
   * in the next incremental scan.
   */
  private ContinuousEnumerationResult discoverInitialSplits() {
    Optional<Snapshot> startSnapshotOptional = getStartSnapshot(table, scanContext);
    if (!startSnapshotOptional.isPresent()) {
      // set IcebergEnumeratorPosition to null in this case
      return new ContinuousEnumerationResult(Collections.emptyList(), null);
    }
    Snapshot startSnapshot = startSnapshotOptional.get();
    LOG.info("Get starting snapshot id {} based on strategy {}",
        startSnapshot.snapshotId(), scanContext.startingStrategy());
    List<IcebergSourceSplit> splits;
    if (scanContext.startingStrategy() ==
        StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL) {
      // do a full table scan first
      splits = FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext, workerPool);
      LOG.info("Discovered {} splits from initial full table scan with snapshot Id {}",
          splits.size(), startSnapshot.snapshotId());
    } else {
      splits = Collections.emptyList();
    }

    IcebergEnumeratorPosition position = IcebergEnumeratorPosition.builder()
        .endSnapshotId(startSnapshot.snapshotId())
        .endSnapshotTimestampMs(startSnapshot.timestampMillis())
        .build();
    return new ContinuousEnumerationResult(splits, position);
  }

  /**
   * Optional is used because table may be empty and has no snapshot
   */
  @VisibleForTesting
  static Optional<Snapshot> getStartSnapshot(Table table, ScanContext scanContext) {
    switch (scanContext.startingStrategy()) {
      case TABLE_SCAN_THEN_INCREMENTAL:
      case LATEST_SNAPSHOT:
        return Optional.ofNullable(table.currentSnapshot());
      case EARLIEST_SNAPSHOT:
        return Optional.ofNullable(SnapshotUtil.oldestAncestor(table));
      case SPECIFIC_START_SNAPSHOT_ID:
        Snapshot matchedSnapshotById = table.snapshot(scanContext.startSnapshotId());
        if (matchedSnapshotById != null) {
          return Optional.of(matchedSnapshotById);
        } else {
          throw new IllegalArgumentException("Snapshot id not found in history: " + scanContext.startSnapshotId());
        }
      case SPECIFIC_START_SNAPSHOT_TIMESTAMP:
        // snapshotIdAsOfTime returns valid snapshot id only.
        // it throws IllegalArgumentException when a matching snapshot not found.
        long snapshotIdAsOfTime = SnapshotUtil.snapshotIdAsOfTime(table, scanContext.startSnapshotTimestamp());
        Snapshot matchedSnapshotByTimestamp = table.snapshot(snapshotIdAsOfTime);
        if (matchedSnapshotByTimestamp != null) {
          return Optional.of(matchedSnapshotByTimestamp);
        } else {
          throw new IllegalArgumentException("Snapshot id not found in history: " + snapshotIdAsOfTime);
        }
      default:
        throw new IllegalArgumentException("Unknown starting strategy: " + scanContext.startingStrategy());
    }
  }
}
