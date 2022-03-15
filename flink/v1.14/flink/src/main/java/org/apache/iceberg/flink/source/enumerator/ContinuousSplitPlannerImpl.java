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
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.FlinkSplitPlanner;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class ContinuousSplitPlannerImpl implements ContinuousSplitPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(ContinuousSplitPlannerImpl.class);

  private final Table table;
  private final IcebergEnumeratorConfig config;
  private final ScanContext scanContext;
  private final ExecutorService workerPool;

  public ContinuousSplitPlannerImpl(Table table, IcebergEnumeratorConfig config, ScanContext scanContext) {
    this.table = table;
    this.config = config;
    this.scanContext = scanContext;
    // Within a JVM, table name should be unique across sources.
    // Hence it is used as worker pool thread name prefix.
    this.workerPool = ThreadPools.newWorkerPool("iceberg-worker-pool-" + table.name(), scanContext.planParallelism());
    validate();
  }

  private void validate() {
    Preconditions.checkArgument(scanContext.snapshotId() == null,
        "Can't set snapshotId in ScanContext for continuous enumerator");
    Preconditions.checkArgument(scanContext.asOfTimestamp() == null,
        "Can't set asOfTimestamp in ScanContext for continuous enumerator");
    Preconditions.checkArgument(scanContext.startSnapshotId() == null,
        "Can't set startSnapshotId in ScanContext for continuous enumerator");
    Preconditions.checkArgument(scanContext.endSnapshotId() == null,
        "Can't set endSnapshotId in ScanContext for continuous enumerator");
  }

  @Override
  public ContinuousEnumerationResult planSplits(IcebergEnumeratorPosition lastPosition) {
    table.refresh();
    if (lastPosition != null) {
      return discoverDeltaSplits(lastPosition);
    } else {
      return discoverInitialSplits();
    }
  }

  /**
   * Discover delta changes between @{code lastPosition} and current table snapshot
   */
  private ContinuousEnumerationResult discoverDeltaSplits(IcebergEnumeratorPosition lastPosition) {
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
   * Discovery initial set of splits based on {@link IcebergEnumeratorConfig.StartingStrategy}
   */
  private ContinuousEnumerationResult discoverInitialSplits() {
    HistoryEntry startSnapshotEntry = getStartSnapshot(table, config);
    LOG.info("get startSnapshotId {} based on starting strategy {}",
        startSnapshotEntry.snapshotId(), config.startingStrategy());
    List<IcebergSourceSplit> splits;
    if (config.startingStrategy() ==
        IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL) {
      // do a full table scan first
      splits = FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext, workerPool);
      LOG.info("Discovered {} splits from initial full table scan with snapshotId {}",
          splits.size(), startSnapshotEntry);
    } else {
      splits = Collections.emptyList();
    }

    IcebergEnumeratorPosition position = IcebergEnumeratorPosition.builder()
        .endSnapshotId(startSnapshotEntry.snapshotId())
        .endSnapshotTimestampMs(startSnapshotEntry.timestampMillis())
        .build();
    return new ContinuousEnumerationResult(splits, position);
  }

  @VisibleForTesting
  static HistoryEntry getStartSnapshot(Table table, IcebergEnumeratorConfig enumeratorConfig) {
    List<HistoryEntry> historyEntries = table.history();
    HistoryEntry startEntry;
    switch (enumeratorConfig.startingStrategy()) {
      case TABLE_SCAN_THEN_INCREMENTAL:
      case LATEST_SNAPSHOT:
        startEntry = historyEntries.get(historyEntries.size() - 1);
        break;
      case EARLIEST_SNAPSHOT:
        startEntry = historyEntries.get(0);
        break;
      case SPECIFIC_START_SNAPSHOT_ID:
        Optional<HistoryEntry> matchedEntry = historyEntries.stream()
            .filter(entry -> entry.snapshotId() == enumeratorConfig.startSnapshotId())
            .findFirst();
        if (matchedEntry.isPresent()) {
          startEntry = matchedEntry.get();
        } else {
          throw new IllegalArgumentException(
              "Snapshot id not found in history: {}" + enumeratorConfig.startSnapshotId());
        }
        break;
      case SPECIFIC_START_SNAPSHOT_TIMESTAMP:
        Optional<HistoryEntry> opt = findSnapshotHistoryEntryByTimestamp(
            enumeratorConfig.startSnapshotTimeMs(), historyEntries);
        if (opt.isPresent()) {
          startEntry = opt.get();
        } else {
          throw new IllegalArgumentException(
              "Failed to find a start snapshot in history using timestamp: " +
                  enumeratorConfig.startSnapshotTimeMs());
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown starting strategy: " +
            enumeratorConfig.startingStrategy());
    }

    return startEntry;
  }

  @VisibleForTesting
  static Optional<HistoryEntry> findSnapshotHistoryEntryByTimestamp(long timestamp, List<HistoryEntry> historyEntries) {
    HistoryEntry matchedEntry = null;
    for (HistoryEntry logEntry : historyEntries) {
      if (logEntry.timestampMillis() <= timestamp) {
        matchedEntry = logEntry;
      }
    }

    return Optional.ofNullable(matchedEntry);
  }

}
