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

package org.apache.iceberg.flink.source.assigner.ordered;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.iceberg.flink.source.assigner.GetSplitResult;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventTimeAlignmentAssigner implements SplitAssigner, WatermarkTracker.Listener {
  private static final Logger log = LoggerFactory.getLogger(EventTimeAlignmentAssigner.class);
  private final Duration maxMisalignmentThreshold;

  private final WatermarkTracker watermarkTracker;
  private final TimestampAssigner<IcebergSourceSplit> timestampAssigner;

  private final EventTimeAlignmentAssignerState assignerState;
  private final UnassignedSplitsMaintainer unassignedSplitsMaintainer;
  private final WatermarkUpdater watermarkUpdater;

  private CompletableFuture<Void> availableFuture;
  private boolean closed = false;

  EventTimeAlignmentAssigner(
      Duration maxMisalignmentThreshold,
      TimestampAssigner<IcebergSourceSplit> timestampAssigner,
      Clock clock, WatermarkTracker watermarkTracker) {
    this(maxMisalignmentThreshold, Collections.emptyList(), watermarkTracker, timestampAssigner, clock);
  }

  EventTimeAlignmentAssigner(
      Duration maxMisalignmentThreshold,
      Collection<IcebergSourceSplitState> currentState,
      WatermarkTracker watermarkTracker,
      TimestampAssigner<IcebergSourceSplit> timestampAssigner,
      Clock clock) {
    this.maxMisalignmentThreshold = maxMisalignmentThreshold;
    this.watermarkTracker = watermarkTracker;
    this.timestampAssigner = timestampAssigner;
    this.assignerState = new EventTimeAlignmentAssignerState(currentState, clock);
    this.unassignedSplitsMaintainer =
        new UnassignedSplitsMaintainer(new AscendingTimestampSplitComparator(timestampAssigner), this.assignerState);
    this.watermarkUpdater = new WatermarkUpdater(watermarkTracker, timestampAssigner, this.assignerState);
  }

  @Override
  public void start() {
    watermarkTracker.addListener(this);
  }

  @Override
  public void close() {
    watermarkTracker.removeListener(this);
    closed = true;
    completeAvailableFuturesIfNeeded();
  }

  @Override
  public GetSplitResult getNext(@Nullable String hostname) {
    try {
      Long watermark = watermarkTracker.getGlobalWatermark();

      for (IcebergSourceSplit pendingSplit : unassignedSplitsMaintainer.getUnassignedSplits()) {
        // break early if you encounter a split that's ahead of the misalignment threshold.
        if (!isWithinBounds(pendingSplit, watermark)) {
          log.info(
              "split {} is not within bounds {} {}",
              pendingSplit,
              watermark,
              timestampAssigner.extractTimestamp(pendingSplit, -1));
          return GetSplitResult.constrained();
        }

        assignerState.assignSplits(ImmutableList.of(pendingSplit), hostname);
        return GetSplitResult.forSplit(pendingSplit);
      }

      return GetSplitResult.unavailable();
    } catch (Exception e) {
      log.error("Couldn't obtain the watermark from the tracker", e);
      return GetSplitResult.constrained();
    }
  }

  private boolean isWithinBounds(IcebergSourceSplit split, Long watermark) {
    if (maxMisalignmentThreshold == null) {
      return true;
    }

    if (watermark == null) {
      return true;
    }

    long splitTs = timestampAssigner.extractTimestamp(split, -1);
    if (splitTs < watermark) {
      log.warn("splitTs at {} is lower than the watermark {}", splitTs, watermark);
    }

    return Math.max(splitTs - watermark, 0L) <= maxMisalignmentThreshold.toMillis();
  }

  @Override
  public void onDiscoveredSplits(Collection<IcebergSourceSplit> splits) {
    assignerState.addSplits(splits);
  }

  @Override
  public void onUnassignedSplits(Collection<IcebergSourceSplit> splits) {
    assignerState.unassignSplits(splits);
  }

  @Override
  public void onCompletedSplits(Collection<String> completedSplitIds) {
    assignerState.completeSplits(completedSplitIds);
  }

  @Override
  public Collection<IcebergSourceSplitState> state() {
    return assignerState.snapshotState();
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    if (availableFuture == null) {
      availableFuture = new CompletableFuture<>();
    }
    return availableFuture;
  }

  @Override
  public void onWatermarkChange(Long watermark) {
    Preconditions.checkArgument(!closed, "strategy is already closed");
    log.info("Global watermark changed to {}; letting listeners know", watermark);
    completeAvailableFuturesIfNeeded();
  }

  /**
   * For now, we just simply complete the available future.
   * Let enumerator to try {@link #getNext(String)} again
   * and see if there is any assignable split.
   */
  private synchronized void completeAvailableFuturesIfNeeded() {
    if (availableFuture != null) {
      availableFuture.complete(null);
    }
    availableFuture = null;
  }
}
