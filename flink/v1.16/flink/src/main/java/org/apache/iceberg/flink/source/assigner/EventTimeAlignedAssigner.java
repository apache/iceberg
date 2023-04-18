/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.source.assigner;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.calcite.shaded.com.google.common.collect.Iterables.getFirst;

public class EventTimeAlignedAssigner implements SplitAssigner, WatermarkTracker.Listener {
  private static final Logger log = LoggerFactory.getLogger(EventTimeAlignedAssigner.class);
  private final WatermarkTracker watermarkTracker;
  private final State state;
  private final TimestampAssigner<IcebergSourceSplit> timestampAssigner;
  private final FutureNotifier futureNotifier;
  private final Options options;

  // flag that informs the state that there are no more splits to be added by the Enumerator
  private volatile boolean noMoreSplits;

  public EventTimeAlignedAssigner(
      WatermarkTracker watermarkTracker,
      TimestampAssigner<IcebergSourceSplit> timestampAssigner,
      Options options) {
    this.watermarkTracker = watermarkTracker;
    this.state = new State(watermarkTracker, timestampAssigner);
    this.options = options;
    this.timestampAssigner = timestampAssigner;
    this.futureNotifier = new FutureNotifier();
    this.watermarkTracker.onInitialization();
  }

  @Override
  public void start() {
    watermarkTracker.addListener(this);
  }

  @Override
  public void close() {
    watermarkTracker.removeListener(this);
    notifyListener();
  }

  @Override
  public GetSplitResult getNext(@Nullable String hostname) {
    GetSplitResult result = getNextInternal();
    if (result.status() == GetSplitResult.Status.AVAILABLE) {
      state.onSplitAssigned(result.split());
    }
    return result;
  }

  @Override
  public void onDiscoveredSplits(Collection<IcebergSourceSplit> splits) {
    state.onDiscoveredSplits(splits);
  }

  @Override
  public void onUnassignedSplits(Collection<IcebergSourceSplit> splits) {
    state.onUnassignedSplits(splits);
  }

  @Override
  public void onCompletedSplits(Collection<String> completedSplitIds) {
    state.onCompletedSplits(completedSplitIds);
    if (!completedSplitIds.isEmpty() && isTerminal()) {
      updateListenersOnTerminalCondition();
    }
  }

  @Override
  public void onNoMoreSplits() {
    noMoreSplits = true;
    if (isTerminal()) {
      updateListenersOnTerminalCondition();
    }
  }

  private boolean isWithinBounds(IcebergSourceSplit split, Long watermark) {
    if (watermark == null) {
      return true;
    }

    long splitTs = timestampAssigner.extractTimestamp(split, -1);
    if (splitTs < watermark) {
      log.warn("splitTs at {} is lower than the watermark {}", splitTs, watermark);
    }

    return Math.max(splitTs - watermark, 0L) <= options.getThresholdInMs();
  }

  private GetSplitResult getNextInternal() {
    if (state.getUnassignedSplits().isEmpty()) {
      log.info("looks like there are no splits to be assigned");
      return GetSplitResult.unavailable();
    }

    try {
      Long watermark = watermarkTracker.getGlobalWatermark();
      IcebergSourceSplit pendingSplit = getFirst(state.getUnassignedSplits(), null);
      if (!isWithinBounds(pendingSplit, watermark)) {
        log.info(
            "split {} is not within bounds {} {}",
            pendingSplit,
            watermark,
            timestampAssigner.extractTimestamp(pendingSplit, -1));
        return GetSplitResult.constrained();
      }

      return GetSplitResult.forSplit(pendingSplit);
    } catch (Exception e) {
      log.error("Couldn't obtain the watermark from the tracker", e);
      return GetSplitResult.unavailable();
    }
  }

  private void updateListenersOnTerminalCondition() {
    state.onNoMoreStatusChanges();
  }

  @Override
  public Collection<IcebergSourceSplitState> state() {
    return
        state
            .getUnassignedSplits()
            .stream()
            .map(split -> new IcebergSourceSplitState(split, IcebergSourceSplitStatus.UNASSIGNED))
            .collect(Collectors.toList());
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    CompletableFuture<Void> result = futureNotifier.future();
    checkAndNotifyListener();
    return result;
  }

  private boolean hasNext() {
    return getNextInternal().status().equals(GetSplitResult.Status.AVAILABLE);
  }

  private void checkAndNotifyListener() {
    if (hasNext()) {
      log.info("Looks like the future can be completed with an assignment; completing the future");
      notifyListener();
    }
  }

  private void notifyListener() {
    // Simply complete the future and return;
    futureNotifier.notifyComplete();
  }

  @Override
  public int pendingSplitCount() {
    return state.getUnassignedSplits().size();
  }

  @Override
  public void onWatermarkChange(Long watermark) {
    notifyListener();
  }

  // checks if the assigner state has reached the terminal condition
  private boolean isTerminal() {
    // check if there are no more splits to be added by the system
    // check if all the splits have been completed
    return (noMoreSplits && state.getUnassignedSplits().isEmpty());
  }

  public static class Options implements Serializable {

    private final long thresholdInMs;

    public Options(Duration maxMisalignmentThreshold) {
      this.thresholdInMs = maxMisalignmentThreshold.toMillis();
    }

    public long getThresholdInMs() {
      return thresholdInMs;
    }
  }

  /**
   * Keeps track of unassigned splits ordered by certain comparator.
   */
  @NotThreadSafe
  static
  class State {
    private static final Logger log = LoggerFactory.getLogger(State.class);

    private final WatermarkTracker tracker;

    private final TreeSet<IcebergSourceSplit> unassignedSplits;
    private final TreeSet<IcebergSourceSplit> assignedSplits;
    private final TimestampAssigner<IcebergSourceSplit> timestampAssigner;

    public State(
        WatermarkTracker tracker,
        TimestampAssigner<IcebergSourceSplit> timestampAssigner) {
      this.tracker = tracker;
      this.timestampAssigner = timestampAssigner;
      Comparator<IcebergSourceSplit> comparator =
          new AscendingTimestampSplitComparator(timestampAssigner);
      this.unassignedSplits = new TreeSet<>(comparator);
      this.assignedSplits = new TreeSet<>(comparator);
    }

    public Collection<IcebergSourceSplit> getUnassignedSplits() {
      return Collections.unmodifiableCollection(unassignedSplits);
    }

    void onDiscoveredSplits(Collection<IcebergSourceSplit> splits) {
      unassignedSplits.addAll(splits);
      updateWatermark();
    }

    public void onSplitAssigned(IcebergSourceSplit split) {
      if (!unassignedSplits.contains(split)) {
        log.error("split={} not found in unassigned splits", split);
        throw new IllegalArgumentException("split not found in unassigned splits");
      }
      unassignedSplits.remove(split);
      assignedSplits.add(split);
      updateWatermark();
    }

    public void onUnassignedSplits(Collection<IcebergSourceSplit> splits) {
      assignedSplits.removeAll(splits);
      unassignedSplits.addAll(splits);
      updateWatermark();
    }

    public void onCompletedSplits(Collection<String> completedSplitIds) {
      Set<IcebergSourceSplit> completed =
          assignedSplits.stream()
              .filter(icebergSourceSplit -> completedSplitIds.contains(icebergSourceSplit.splitId()))
              .collect(Collectors.toSet());

      assignedSplits.removeAll(completed);
      updateWatermark();
    }

    public void onNoMoreStatusChanges() {
      try {
        tracker.onCompletion();
      } catch (Exception e) {
        log.error("Failed to update watermark", e);
      }
    }

    private void updateWatermark() {
      try {
        long v1 = (!assignedSplits.isEmpty()) ?
            timestampAssigner.extractTimestamp(assignedSplits.first(), -1) : Long.MAX_VALUE;
        long v2 = (!unassignedSplits.isEmpty()) ?
            timestampAssigner.extractTimestamp(unassignedSplits.first(), -1) : Long.MAX_VALUE;
        long v = Math.min(v1, v2);
        if (v != Long.MAX_VALUE) {
          tracker.updateWatermark(v);
        }
      } catch (Exception e) {
        log.error("Failed to update watermark", e);
      }
    }
  }
}
