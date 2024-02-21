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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.util.ElapsedTimeGauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class ContinuousIcebergEnumerator extends AbstractIcebergEnumerator {

  private static final Logger LOG = LoggerFactory.getLogger(ContinuousIcebergEnumerator.class);
  /**
   * This is hardcoded, as {@link ScanContext#maxPlanningSnapshotCount()} could be the knob to
   * control the total number of snapshots worth of splits tracked by assigner.
   */
  private static final int ENUMERATION_SPLIT_COUNT_HISTORY_SIZE = 3;

  private final SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext;
  private final SplitAssigner assigner;
  private final ScanContext scanContext;
  private final ContinuousSplitPlanner splitPlanner;

  /**
   * snapshotId for the last enumerated snapshot. next incremental enumeration should be based off
   * this as the starting position.
   */
  private final AtomicReference<IcebergEnumeratorPosition> enumeratorPosition;

  /** Track enumeration result history for split discovery throttling. */
  private final EnumerationHistory enumerationHistory;

  /** Count the consecutive failures and throw exception if the max allowed failres are reached */
  private transient int consecutiveFailures = 0;

  private final ElapsedTimeGauge elapsedSecondsSinceLastSplitDiscovery;

  public ContinuousIcebergEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext,
      SplitAssigner assigner,
      ScanContext scanContext,
      ContinuousSplitPlanner splitPlanner,
      @Nullable IcebergEnumeratorState enumState) {
    super(enumeratorContext, assigner);

    this.enumeratorContext = enumeratorContext;
    this.assigner = assigner;
    this.scanContext = scanContext;
    this.splitPlanner = splitPlanner;
    this.enumeratorPosition = new AtomicReference<>();
    this.enumerationHistory = new EnumerationHistory(ENUMERATION_SPLIT_COUNT_HISTORY_SIZE);
    this.elapsedSecondsSinceLastSplitDiscovery = new ElapsedTimeGauge(TimeUnit.SECONDS);
    this.enumeratorContext
        .metricGroup()
        .gauge("elapsedSecondsSinceLastSplitDiscovery", elapsedSecondsSinceLastSplitDiscovery);

    if (enumState != null) {
      this.enumeratorPosition.set(enumState.lastEnumeratedPosition());
      this.enumerationHistory.restore(enumState.enumerationSplitCountHistory());
    }
  }

  @Override
  public void start() {
    super.start();
    enumeratorContext.callAsync(
        this::discoverSplits,
        this::processDiscoveredSplits,
        0L,
        scanContext.monitorInterval().toMillis());
  }

  @Override
  public void close() throws IOException {
    splitPlanner.close();
    super.close();
  }

  @Override
  protected boolean shouldWaitForMoreSplits() {
    return true;
  }

  @Override
  public IcebergEnumeratorState snapshotState(long checkpointId) {
    return new IcebergEnumeratorState(
        enumeratorPosition.get(), assigner.state(), enumerationHistory.snapshot());
  }

  /** This method is executed in an IO thread pool. */
  private ContinuousEnumerationResult discoverSplits() {
    int pendingSplitCountFromAssigner = assigner.pendingSplitCount();
    if (enumerationHistory.shouldPauseSplitDiscovery(pendingSplitCountFromAssigner)) {
      // If the assigner already has many pending splits, it is better to pause split discovery.
      // Otherwise, eagerly discovering more splits will just increase assigner memory footprint
      // and enumerator checkpoint state size.
      LOG.info(
          "Pause split discovery as the assigner already has too many pending splits: {}",
          pendingSplitCountFromAssigner);
      return new ContinuousEnumerationResult(
          Collections.emptyList(), enumeratorPosition.get(), enumeratorPosition.get());
    } else {
      return splitPlanner.planSplits(enumeratorPosition.get());
    }
  }

  /** This method is executed in a single coordinator thread. */
  private void processDiscoveredSplits(ContinuousEnumerationResult result, Throwable error) {
    if (error == null) {
      consecutiveFailures = 0;
      if (!Objects.equals(result.fromPosition(), enumeratorPosition.get())) {
        // Multiple discoverSplits() may be triggered with the same starting snapshot to the I/O
        // thread pool. E.g., the splitDiscoveryInterval is very short (like 10 ms in some unit
        // tests) or the thread pool is busy and multiple discovery actions are executed
        // concurrently. Discovery result should only be accepted if the starting position
        // matches the enumerator position (like compare-and-swap).
        LOG.info(
            "Skip {} discovered splits because the scan starting position doesn't match "
                + "the current enumerator position: enumerator position = {}, scan starting position = {}",
            result.splits().size(),
            enumeratorPosition.get(),
            result.fromPosition());
      } else {
        elapsedSecondsSinceLastSplitDiscovery.refreshLastRecordedTime();
        // Sometimes, enumeration may yield no splits for a few reasons.
        // - upstream paused or delayed streaming writes to the Iceberg table.
        // - enumeration frequency is higher than the upstream write frequency.
        if (!result.splits().isEmpty()) {
          assigner.onDiscoveredSplits(result.splits());
          // EnumerationHistory makes throttling decision on split discovery
          // based on the total number of splits discovered in the last a few cycles.
          // Only update enumeration history when there are some discovered splits.
          enumerationHistory.add(result.splits().size());
          LOG.info(
              "Added {} splits discovered between ({}, {}] to the assigner",
              result.splits().size(),
              result.fromPosition(),
              result.toPosition());
        } else {
          LOG.info(
              "No new splits discovered between ({}, {}]",
              result.fromPosition(),
              result.toPosition());
        }
        // update the enumerator position even if there is no split discovered
        // or the toPosition is empty (e.g. for empty table).
        enumeratorPosition.set(result.toPosition());
        LOG.info("Update enumerator position to {}", result.toPosition());
      }
    } else {
      consecutiveFailures++;
      if (scanContext.maxAllowedPlanningFailures() < 0
          || consecutiveFailures <= scanContext.maxAllowedPlanningFailures()) {
        LOG.error("Failed to discover new splits", error);
      } else {
        throw new RuntimeException("Failed to discover new splits", error);
      }
    }
  }
}
