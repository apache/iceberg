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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.SplitHelpers;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.DefaultSplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.SplitRequestEvent;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the lazy-mode integration with {@link ContinuousIcebergEnumerator} — specifically the
 * reactive low-watermark trigger and atomic cursor commit. Uses a fake planner so the test is fast
 * and doesn't depend on table state.
 */
public class TestLazyEnumerator {

  @TempDir protected Path temporaryFolder;

  @Test
  public void testWatermarkTriggerFiresWhenAssignerBelowThreshold() throws Exception {
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext = lazyScanContext();
    FakeLazyPlanner planner = new FakeLazyPlanner(1, 5);
    planner.queueSplits(SplitHelpers.createSplitsFromTransientHadoopTable(temporaryFolder, 1, 1));

    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, planner);

    enumeratorContext.registerReader(0, "localhost");
    enumerator.addReader(0);
    int callsBefore = planner.planSplitsCallCount();
    enumerator.handleSourceEvent(0, new SplitRequestEvent());
    enumeratorContext.triggerAllActions();

    assertThat(planner.planSplitsCallCount()).isGreaterThan(callsBefore);
  }

  @Test
  public void testEagerPlannerHasNoWatermarkTrigger() throws Exception {
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext = lazyScanContext();
    ManualContinuousSplitPlanner planner = new ManualContinuousSplitPlanner(scanContext, 0);
    planner.addSplits(SplitHelpers.createSplitsFromTransientHadoopTable(temporaryFolder, 1, 1));

    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, planner);

    enumeratorContext.registerReader(0, "localhost");
    enumerator.addReader(0);
    enumeratorContext.triggerAllActions();
    enumerator.handleSourceEvent(0, new SplitRequestEvent());
    enumeratorContext.triggerAllActions();
    assertThat(enumerator.snapshotState(1).lazyBulkScanCursor()).isNull();
  }

  @Test
  public void testCursorCommittedAtomicallyOnBulkPage() throws Exception {
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext = lazyScanContext();
    FakeLazyPlanner planner = new FakeLazyPlanner(1, Integer.MAX_VALUE);
    List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(temporaryFolder, 2, 1);
    planner.queueSplitsWithCursor(splits.subList(0, 1), new LazyBulkScanCursor(99L, 1L, 0L));
    planner.queueSplitsWithCursor(splits.subList(1, 2), new LazyBulkScanCursor(99L, 2L, 0L));

    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, planner);

    // First page commits cursor=(99,1).
    enumeratorContext.triggerAllActions();
    LazyBulkScanCursor c1 = enumerator.snapshotState(1).lazyBulkScanCursor();
    assertThat(c1).isNotNull();
    assertThat(c1.combinedTasksEnumerated()).isEqualTo(1L);

    // Second page commits cursor=(99,2).
    enumeratorContext.triggerAllActions();
    LazyBulkScanCursor c2 = enumerator.snapshotState(2).lazyBulkScanCursor();
    assertThat(c2).isNotNull();
    assertThat(c2.combinedTasksEnumerated()).isEqualTo(2L);
  }

  @Test
  public void testCursorClearedOnFinalBulkPage() throws Exception {
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext = lazyScanContext();
    FakeLazyPlanner planner = new FakeLazyPlanner(1, Integer.MAX_VALUE);
    List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(temporaryFolder, 1, 1);
    // Final page: cursor=null signals "bulk done, clear the committed cursor".
    planner.queueSplitsWithCursor(splits, null);

    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, planner);
    enumeratorContext.triggerAllActions();

    assertThat(enumerator.snapshotState(1).lazyBulkScanCursor()).isNull();
  }

  @Test
  public void testConcurrentBulkPagesNotDroppedByCasCheck() throws Exception {
    // Regression for the bug where the CAS check in processDiscoveredSplits would drop a
    // bulk-phase page whose fromPosition was null while enumeratorPosition had advanced to
    // empty(). The planner had already advanced its internal iterator, so the dropped page's
    // splits were silently lost.
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext = lazyScanContext();
    FakeLazyPlanner planner = new FakeLazyPlanner(1, Integer.MAX_VALUE);
    List<IcebergSourceSplit> splits =
        SplitHelpers.createSplitsFromTransientHadoopTable(temporaryFolder, 4, 1);

    // Page 1: fromPosition=null, toPosition=empty(). Mid-bulk.
    planner.queueResult(
        ContinuousEnumerationResult.forLazyBulkPage(
            splits.subList(0, 1),
            null,
            IcebergEnumeratorPosition.empty(),
            new LazyBulkScanCursor(99L, 1L, 0xA1L)));
    // Page 2: also fromPosition=null (scheduled before page 1 handler ran). Mid-bulk.
    planner.queueResult(
        ContinuousEnumerationResult.forLazyBulkPage(
            splits.subList(1, 2),
            null,
            IcebergEnumeratorPosition.empty(),
            new LazyBulkScanCursor(99L, 2L, 0xA2L)));
    // Page 3: fromPosition=empty() (page 1 handler ran, advancing position). Mid-bulk.
    planner.queueResult(
        ContinuousEnumerationResult.forLazyBulkPage(
            splits.subList(2, 3),
            IcebergEnumeratorPosition.empty(),
            IcebergEnumeratorPosition.empty(),
            new LazyBulkScanCursor(99L, 3L, 0xA3L)));

    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, planner);

    // Drive all three pages through the enumerator.
    enumeratorContext.triggerAllActions();
    enumeratorContext.triggerAllActions();
    enumeratorContext.triggerAllActions();

    // All three pages must be accepted — the bulk-sentinel CAS relaxation lets page 2 through.
    IcebergEnumeratorState state = enumerator.snapshotState(1);
    assertThat(state.pendingSplits()).hasSize(3);
    assertThat(state.lazyBulkScanCursor()).isNotNull();
    assertThat(state.lazyBulkScanCursor().combinedTasksEnumerated()).isEqualTo(3L);
    assertThat(state.lazyBulkScanCursor().rollingHash()).isEqualTo(0xA3L);
  }

  @Test
  public void testCursorNotTouchedByEagerResults() throws Exception {
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    ScanContext scanContext = lazyScanContext();
    FakeLazyPlanner planner = new FakeLazyPlanner(1, Integer.MAX_VALUE);
    List<IcebergSourceSplit> bulkSplits =
        SplitHelpers.createSplitsFromTransientHadoopTable(temporaryFolder, 1, 1);
    planner.queueSplitsWithCursor(bulkSplits, new LazyBulkScanCursor(123L, 7L, 0L));

    ContinuousIcebergEnumerator enumerator =
        createEnumerator(enumeratorContext, scanContext, planner);
    enumeratorContext.triggerAllActions();
    LazyBulkScanCursor afterBulk = enumerator.snapshotState(1).lazyBulkScanCursor();
    assertThat(afterBulk).isNotNull();
    assertThat(afterBulk.combinedTasksEnumerated()).isEqualTo(7L);

    // Now queue a "regular" (eager-style, no cursor update) result. Cursor must NOT be touched.
    planner.queueSplitsAsEager(
        SplitHelpers.createSplitsFromTransientHadoopTable(temporaryFolder, 1, 1));
    enumeratorContext.triggerAllActions();

    LazyBulkScanCursor afterEager = enumerator.snapshotState(2).lazyBulkScanCursor();
    assertThat(afterEager).isNotNull();
    assertThat(afterEager.combinedTasksEnumerated()).isEqualTo(7L);
  }

  // -----------------------------------------------------------------------------------------

  private static ScanContext lazyScanContext() {
    return ScanContext.builder()
        .streaming(true)
        .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
  }

  private static ContinuousIcebergEnumerator createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> context,
      ScanContext scanContext,
      ContinuousSplitPlanner splitPlanner) {
    ContinuousIcebergEnumerator enumerator =
        new ContinuousIcebergEnumerator(
            context,
            new DefaultSplitAssigner(null, Collections.emptyList()),
            scanContext,
            splitPlanner,
            null);
    enumerator.start();
    return enumerator;
  }

  /**
   * Fake planner that returns pre-queued results. Each queued entry specifies the splits and the
   * cursor semantics to attach to the result.
   */
  private static class FakeLazyPlanner implements ContinuousSplitPlanner {

    private final int pageSize;
    private final int lowWatermark;
    private final List<QueuedResult> queue = Lists.newArrayList();
    private final AtomicInteger planSplitsCalls = new AtomicInteger(0);

    FakeLazyPlanner(int pageSize, int lowWatermark) {
      this.pageSize = pageSize;
      this.lowWatermark = lowWatermark;
    }

    /** Convenience for tests that don't care about cursor semantics — treated as eager. */
    synchronized void queueSplits(Collection<IcebergSourceSplit> splits) {
      queueSplitsAsEager(splits);
    }

    synchronized void queueSplitsAsEager(Collection<IcebergSourceSplit> splits) {
      queue.add(new QueuedResult(Lists.newArrayList(splits), false, null));
    }

    synchronized void queueSplitsWithCursor(
        Collection<IcebergSourceSplit> splits, @Nullable LazyBulkScanCursor cursor) {
      queue.add(new QueuedResult(Lists.newArrayList(splits), true, cursor));
    }

    /** Queue a fully-formed result. Used by tests that want precise control over positions. */
    synchronized void queueResult(ContinuousEnumerationResult result) {
      queue.add(new QueuedResult(result));
    }

    int planSplitsCallCount() {
      return planSplitsCalls.get();
    }

    @Override
    public synchronized ContinuousEnumerationResult planSplits(
        IcebergEnumeratorPosition lastPosition) {
      planSplitsCalls.incrementAndGet();
      if (queue.isEmpty()) {
        return new ContinuousEnumerationResult(
            Collections.emptyList(), lastPosition, IcebergEnumeratorPosition.empty());
      }
      QueuedResult head = queue.remove(0);
      if (head.prebuilt != null) {
        return head.prebuilt;
      }
      // Decide toPosition like the real planner: mid-bulk = empty(), final = real id.
      boolean finalBulkPage = head.cursorUpdate && head.cursor == null;
      IcebergEnumeratorPosition toPosition =
          finalBulkPage
              ? IcebergEnumeratorPosition.of(99L, 99L)
              : IcebergEnumeratorPosition.empty();
      if (head.cursorUpdate) {
        List<IcebergSourceSplit> page =
            head.splits.subList(0, Math.min(pageSize, head.splits.size()));
        if (finalBulkPage) {
          return ContinuousEnumerationResult.forLazyBulkClear(page, lastPosition, toPosition);
        }
        return ContinuousEnumerationResult.forLazyBulkPage(
            page, lastPosition, toPosition, head.cursor);
      }
      return new ContinuousEnumerationResult(head.splits, lastPosition, toPosition);
    }

    @Override
    public int recommendedLowWatermark() {
      return lowWatermark;
    }

    @Override
    public void close() throws IOException {}

    private static class QueuedResult {

      final List<IcebergSourceSplit> splits;
      final boolean cursorUpdate;

      @Nullable final LazyBulkScanCursor cursor;

      @Nullable final ContinuousEnumerationResult prebuilt;

      QueuedResult(
          List<IcebergSourceSplit> splits,
          boolean cursorUpdate,
          @Nullable LazyBulkScanCursor cursor) {
        this.splits = splits;
        this.cursorUpdate = cursorUpdate;
        this.cursor = cursor;
        this.prebuilt = null;
      }

      QueuedResult(ContinuousEnumerationResult prebuilt) {
        this.splits = null;
        this.cursorUpdate = false;
        this.cursor = null;
        this.prebuilt = prebuilt;
      }
    }
  }
}
