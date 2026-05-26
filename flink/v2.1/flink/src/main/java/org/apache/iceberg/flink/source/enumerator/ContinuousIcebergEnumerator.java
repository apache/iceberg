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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
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

  /**
   * Last lazy-bulk-scan cursor that was committed atomically with an assigner update. Updated only
   * in {@link #processDiscoveredSplits} (event loop), read by {@link #snapshotState} (also event
   * loop). Kept here rather than on the planner so the cursor advance is always atomic with the
   * corresponding {@code assigner.onDiscoveredSplits} call — a checkpoint that races with
   * planSplits cannot capture a cursor ahead of the assigner state.
   */
  @Nullable private LazyBulkScanCursor latestCommittedLazyBulkScanCursor;

  /** Count the consecutive failures and throw exception if the max allowed failres are reached */
  private transient int consecutiveFailures = 0;

  private final ElapsedTimeGauge elapsedSecondsSinceLastSplitDiscovery;

  /**
   * Single-flight latch preventing concurrent {@code planSplits} calls. Necessary because:
   *
   * <ol>
   *   <li>Without it, the reactive watermark trigger and the periodic timer can dispatch
   *       overlapping IO callables. Even though the planner's {@code synchronized planSplits}
   *       serialises them inside its monitor, their handler queueing in the coordinator event loop
   *       can race when the IO thread is preempted between {@code synchronized} exit and the
   *       framework's handler dispatch.
   *   <li>Out-of-order handler delivery would commit cursors out of order (regressing the
   *       checkpoint-visible cursor below the actual iterator position) and could trigger silent
   *       split loss when a final-bulk-page handler advances the position past sentinel before
   *       earlier mid-bulk handlers' CAS checks run.
   * </ol>
   *
   * <p>The handler that <em>acquired</em> the latch releases it. Handlers receiving the sentinel
   * no-op result (because the latch was already held when their callable started) short-circuit
   * without touching the flag.
   */
  private final AtomicBoolean planningInFlight = new AtomicBoolean(false);

  /**
   * Identity sentinel returned by {@link #discoverSplits} when the latch is already held. Compared
   * by reference in {@link #processDiscoveredSplits} to short-circuit without advancing position,
   * applying splits, or releasing the latch.
   */
  private static final ContinuousEnumerationResult LATCH_HELD_NOOP =
      new ContinuousEnumerationResult(
          Collections.emptyList(), null, IcebergEnumeratorPosition.empty());

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
      this.latestCommittedLazyBulkScanCursor = enumState.lazyBulkScanCursor();
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
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    super.handleSourceEvent(subtaskId, sourceEvent);
    // Iceberg uses SplitRequestEvent (not the default handleSplitRequest path). After the
    // parent has assigned a split to the reader, the assigner's pending count drops. If it's
    // below the planner's low-watermark, fire an extra planSplits without waiting for the
    // next monitorInterval tick.
    int lowWatermark = splitPlanner.recommendedLowWatermark();
    if (lowWatermark <= 0 || assigner.pendingSplitCount() >= lowWatermark) {
      return;
    }
    // Pre-check the single-flight latch so N parallel readers don't dispatch N callAsync tasks
    // when one is already running. The check is intentionally racy — the CAS inside
    // discoverSplits is the actual gatekeeper — but it suppresses the common case of a burst
    // of SplitRequestEvents arriving during a single in-flight plan.
    if (planningInFlight.get()) {
      return;
    }
    enumeratorContext.callAsync(this::discoverSplits, this::processDiscoveredSplits);
  }

  @Override
  protected boolean shouldWaitForMoreSplits() {
    return true;
  }

  @Override
  public IcebergEnumeratorState snapshotState(long checkpointId) {
    return new IcebergEnumeratorState(
        enumeratorPosition.get(),
        assigner.state(),
        enumerationHistory.snapshot(),
        latestCommittedLazyBulkScanCursor);
  }

  /**
   * Whether two enumerator positions are compatible for CAS-style result acceptance. Both bulk
   * sentinels ({@code null} or a position with {@code snapshotId() == null}) are mutually
   * compatible; otherwise strict {@link Objects#equals} applies. See the call site for why bulk
   * sentinels are treated as equivalent.
   */
  private static boolean positionsCompatible(
      @Nullable IcebergEnumeratorPosition left, @Nullable IcebergEnumeratorPosition right) {
    // "Bulk sentinel" is null-or-empty: see IcebergEnumeratorPosition.isEmpty(), which returns
    // true exactly when snapshotId() == null.
    boolean leftBulkSentinel = (left == null || left.isEmpty());
    boolean rightBulkSentinel = (right == null || right.isEmpty());
    if (leftBulkSentinel && rightBulkSentinel) {
      return true;
    }
    return Objects.equals(left, right);
  }

  /** This method is executed in an IO thread pool. */
  private ContinuousEnumerationResult discoverSplits() {
    if (!planningInFlight.compareAndSet(false, true)) {
      // Another planSplits call is in flight (callable already running or its handler not yet
      // executed). Return the identity sentinel; the handler will see it and short-circuit
      // without touching the latch.
      return LATCH_HELD_NOOP;
    }
    // Latch release happens exclusively in processDiscoveredSplits' finally block. Releasing
    // here on exception too would double-release: Flink still dispatches the handler with
    // (result=null, error=e), and that handler's finally would clear the flag a second time,
    // potentially clearing a concurrent caller's freshly-acquired latch.
    int pendingSplitCountFromAssigner = assigner.pendingSplitCount();
    if (enumerationHistory.shouldPauseSplitDiscovery(pendingSplitCountFromAssigner)) {
      // If the assigner already has many pending splits, it is better to pause split
      // discovery. Otherwise, eagerly discovering more splits will just increase assigner
      // memory footprint and enumerator checkpoint state size.
      LOG.info(
          "Pause split discovery as the assigner already has too many pending splits: {}",
          pendingSplitCountFromAssigner);
      return new ContinuousEnumerationResult(
          Collections.emptyList(), enumeratorPosition.get(), enumeratorPosition.get());
    }
    return splitPlanner.planSplits(enumeratorPosition.get());
  }

  /** This method is executed in a single coordinator thread. */
  private void processDiscoveredSplits(ContinuousEnumerationResult result, Throwable error) {
    if (result == LATCH_HELD_NOOP) {
      // Latched-out call; not the one holding the latch. Don't release, don't process.
      return;
    }
    try {
      processDiscoveredSplitsImpl(result, error);
    } finally {
      planningInFlight.set(false);
    }
  }

  private void processDiscoveredSplitsImpl(ContinuousEnumerationResult result, Throwable error) {
    if (error == null) {
      consecutiveFailures = 0;
      if (!positionsCompatible(result.fromPosition(), enumeratorPosition.get())) {
        // Multiple discoverSplits() may be triggered with the same starting snapshot to the I/O
        // thread pool. E.g., the splitDiscoveryInterval is very short (like 10 ms in some unit
        // tests) or the thread pool is busy and multiple discovery actions are executed
        // concurrently. Discovery result should only be accepted if the starting position
        // matches the enumerator position (like compare-and-swap).
        //
        // During the lazy-mode bulk phase, fromPosition observed when the IO callable started
        // can be null while enumeratorPosition has since advanced to empty() (or vice versa).
        // Both are semantically "still in bulk" — bulk-sentinel positions. Treating them as
        // equal is required to avoid dropping legitimate bulk pages on the floor (which would
        // silently lose splits even though the planner's iterator has already advanced past
        // them). Non-sentinel positions still require strict equality.
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
        // Commit the lazy-bulk cursor atomically with the assigner+position update.
        //   SET   → adopt new cursor (mid-bulk page).
        //   CLEAR → drop cursor (final bulk page).
        //   NONE  → eager-mode / incremental-mode / throttle no-op; leave it alone.
        switch (result.cursorAction()) {
          case SET:
            latestCommittedLazyBulkScanCursor = result.lazyBulkScanCursor();
            break;
          case CLEAR:
            latestCommittedLazyBulkScanCursor = null;
            break;
          case NONE:
          default:
            // Leave cursor alone.
            break;
        }
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
