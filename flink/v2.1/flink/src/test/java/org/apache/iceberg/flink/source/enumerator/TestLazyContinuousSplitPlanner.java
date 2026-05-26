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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.DefaultSplitAssigner;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.SplitRequestEvent;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class TestLazyContinuousSplitPlanner {

  @TempDir protected Path temporaryFolder;

  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;

  @RegisterExtension
  private static final HadoopTableExtension TABLE_RESOURCE =
      new HadoopTableExtension(TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  private GenericAppenderHelper dataAppender;

  @BeforeEach
  public void before() throws IOException {
    dataAppender = new GenericAppenderHelper(TABLE_RESOURCE.table(), FILE_FORMAT, temporaryFolder);
  }

  private void appendFiles(int count) throws IOException {
    for (int i = 0; i < count; i++) {
      List<Record> batch = RandomGenericData.generate(TestFixtures.SCHEMA, 2, (long) i);
      DataFile file = dataAppender.writeFile(null, batch);
      dataAppender.appendToTable(file);
    }
  }

  private ScanContext tableScanScanContext() {
    // splitSize(1L) prevents per-target-size combining so each appended file becomes its own
    // CombinedScanTask, making per-page assertions concrete.
    return ScanContext.builder()
        .streaming(true)
        .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .splitSize(1L)
        .build();
  }

  @Test
  public void testEmptyTableFinishesBulkPhaseImmediately() throws Exception {
    ScanContext scanContext = tableScanScanContext();
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 100, "test", null)) {
      ContinuousEnumerationResult result = planner.planSplits(null);
      assertThat(result.splits()).isEmpty();
      // Empty table: no cursor action, no cursor.
      assertThat(result.cursorAction()).isEqualTo(ContinuousEnumerationResult.CursorAction.NONE);
      assertThat(result.lazyBulkScanCursor()).isNull();

      // Subsequent call after the table got a snapshot should be served by the incremental
      // delegate (bulkPhaseFinished was set on the empty-table path), not re-enter the bulk
      // path against the new snapshot.
      appendFiles(2);
      ContinuousEnumerationResult next = planner.planSplits(IcebergEnumeratorPosition.empty());
      // Incremental result, not a lazy bulk page. Cursor is untouched.
      assertThat(next.cursorAction()).isEqualTo(ContinuousEnumerationResult.CursorAction.NONE);
      assertThat(next.lazyBulkScanCursor()).isNull();

      // recommendedLowWatermark() drops to 0 once the bulk phase is done.
      assertThat(planner.recommendedLowWatermark()).isZero();
    }
  }

  @Test
  public void testMultiPageEmitsPagesThenTransitions() throws Exception {
    appendFiles(7);
    Snapshot snapshot = TABLE_RESOURCE.table().currentSnapshot();
    ScanContext scanContext = tableScanScanContext();

    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 3, "test", null)) {
      Set<String> splitIds = Sets.newHashSet();
      IcebergEnumeratorPosition position = null;

      // First page: 3 splits, mid-bulk sentinel toPosition.
      ContinuousEnumerationResult p1 = planner.planSplits(position);
      assertThat(p1.splits()).hasSize(3);
      assertThat(p1.toPosition().snapshotId()).isNull();
      assertThat(p1.cursorAction()).isEqualTo(ContinuousEnumerationResult.CursorAction.SET);
      assertThat(p1.lazyBulkScanCursor()).isNotNull();
      assertThat(p1.lazyBulkScanCursor().combinedTasksEnumerated()).isEqualTo(3);
      p1.splits().forEach(s -> splitIds.add(s.splitId()));
      position = p1.toPosition();

      // Second page: 3 more, still mid-bulk.
      ContinuousEnumerationResult p2 = planner.planSplits(position);
      assertThat(p2.splits()).hasSize(3);
      assertThat(p2.toPosition().snapshotId()).isNull();
      assertThat(p2.cursorAction()).isEqualTo(ContinuousEnumerationResult.CursorAction.SET);
      assertThat(p2.lazyBulkScanCursor().combinedTasksEnumerated()).isEqualTo(6);
      p2.splits().forEach(s -> splitIds.add(s.splitId()));
      position = p2.toPosition();

      // Third page: 1 remaining, final page — transitions to real snapshot position.
      ContinuousEnumerationResult p3 = planner.planSplits(position);
      assertThat(p3.splits()).hasSize(1);
      assertThat(p3.toPosition().snapshotId()).isEqualTo(snapshot.snapshotId());
      assertThat(p3.cursorAction()).isEqualTo(ContinuousEnumerationResult.CursorAction.CLEAR);
      assertThat(p3.lazyBulkScanCursor()).isNull();
      p3.splits().forEach(s -> splitIds.add(s.splitId()));

      // No duplicates across pages.
      assertThat(splitIds).hasSize(7);
    }
  }

  @Test
  public void testCursorRecoveryResumesAtRightOffset() throws Exception {
    appendFiles(10);
    Snapshot snapshot = TABLE_RESOURCE.table().currentSnapshot();
    ScanContext scanContext = tableScanScanContext();

    // First incarnation: emit one page, capture cursor and remember which split ids we saw.
    Set<String> firstIncarnationIds = Sets.newHashSet();
    LazyBulkScanCursor restoredCursor;
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 4, "test", null)) {
      ContinuousEnumerationResult page = planner.planSplits(null);
      assertThat(page.splits()).hasSize(4);
      page.splits().forEach(s -> firstIncarnationIds.add(s.splitId()));
      // The cursor for the next checkpoint comes from the page result, not the planner.
      restoredCursor = page.lazyBulkScanCursor();
      assertThat(restoredCursor).isNotNull();
      assertThat(restoredCursor.bulkSnapshotId()).isEqualTo(snapshot.snapshotId());
      assertThat(restoredCursor.combinedTasksEnumerated()).isEqualTo(4);
    }

    // Second incarnation: construct with the restored cursor and drain the rest.
    Set<String> secondIncarnationIds = Sets.newHashSet();
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 4, "test", restoredCursor)) {
      ContinuousEnumerationResult p1 = planner.planSplits(null);
      p1.splits().forEach(s -> secondIncarnationIds.add(s.splitId()));
      ContinuousEnumerationResult p2 = planner.planSplits(IcebergEnumeratorPosition.empty());
      p2.splits().forEach(s -> secondIncarnationIds.add(s.splitId()));
      // Final bulk page signals "clear cursor" via CLEAR action.
      assertThat(p2.cursorAction()).isEqualTo(ContinuousEnumerationResult.CursorAction.CLEAR);
      assertThat(p2.lazyBulkScanCursor()).isNull();
    }

    // The two incarnations together cover exactly all 10 splits, no duplicates.
    assertThat(firstIncarnationIds).hasSize(4);
    assertThat(secondIncarnationIds).hasSize(6);
    Set<String> intersection = Sets.newHashSet(firstIncarnationIds);
    intersection.retainAll(secondIncarnationIds);
    assertThat(intersection).isEmpty();
  }

  @Test
  public void testCursorRecoveryFailsWhenSnapshotExpired() throws Exception {
    appendFiles(3);
    long missingSnapshotId = -1L;
    LazyBulkScanCursor cursor = new LazyBulkScanCursor(missingSnapshotId, 2L, 0L);
    ScanContext scanContext = tableScanScanContext();

    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 100, "test", cursor)) {
      assertThatThrownBy(() -> planner.planSplits(null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("snapshot " + missingSnapshotId)
          .hasMessageContaining("no longer exists");
    }
  }

  @Test
  public void testNonTableScanStrategyIsPassthrough() throws Exception {
    appendFiles(3);
    Snapshot snapshot = TABLE_RESOURCE.table().currentSnapshot();
    ScanContext scanContext =
        ScanContext.builder()
            .streaming(true)
            .startingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
            .build();

    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 100, "test", null)) {
      // INCREMENTAL_FROM_LATEST_SNAPSHOT has no bulk phase — first call returns empty initial
      // result with the current snapshot's parent position (inclusive semantics). We just
      // verify the planner doesn't claim mid-bulk state.
      ContinuousEnumerationResult result = planner.planSplits(null);
      // Non-TABLE_SCAN strategies pass through to the incremental delegate, which doesn't
      // touch the cursor.
      assertThat(result.cursorAction()).isEqualTo(ContinuousEnumerationResult.CursorAction.NONE);
      assertThat(result.toPosition()).isNotNull();
      // Critical: even though bulkPhaseFinished is still false (the bulk phase never ran),
      // recommendedLowWatermark() must return 0 for non-TABLE_SCAN strategies — otherwise the
      // enumerator would dispatch reactive planSplits calls on every SplitRequestEvent for what is
      // a pure passthrough to the incremental delegate.
      assertThat(planner.recommendedLowWatermark()).isZero();
    }
  }

  @Test
  public void testRecommendedLowWatermark() throws Exception {
    appendFiles(2);
    ScanContext scanContext = tableScanScanContext();
    // pageSize=1000 → half. pageSize=1 must clamp to 1 (lowWatermark=0 would mean "reactive
    // trigger disabled", the opposite of what a tiny page size implies).
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 1000, "test", null)) {
      assertThat(planner.recommendedLowWatermark()).isEqualTo(500);
    }
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 1, "test", null)) {
      assertThat(planner.recommendedLowWatermark()).isEqualTo(1);
    }
    // After the bulk phase completes, the watermark drops to 0 so reactive triggering stops.
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 100, "test", null)) {
      assertThat(planner.recommendedLowWatermark()).isEqualTo(50);
      ContinuousEnumerationResult page = planner.planSplits(null);
      assertThat(page.cursorAction()).isEqualTo(ContinuousEnumerationResult.CursorAction.CLEAR);
      assertThat(planner.recommendedLowWatermark()).isZero();
    }
  }

  @Test
  public void testIterationOrderIsDeterministicAcrossRuns() throws Exception {
    appendFiles(20);
    ScanContext scanContext = tableScanScanContext();

    List<String> firstRunOrder = new java.util.ArrayList<>();
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 1000, "test", null)) {
      ContinuousEnumerationResult result = planner.planSplits(null);
      for (IcebergSourceSplit split : result.splits()) {
        firstRunOrder.add(split.splitId());
      }
    }

    List<String> secondRunOrder = new java.util.ArrayList<>();
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 1000, "test", null)) {
      ContinuousEnumerationResult result = planner.planSplits(null);
      for (IcebergSourceSplit split : result.splits()) {
        secondRunOrder.add(split.splitId());
      }
    }

    // If the bulk-scan worker pool weren't pinned to one thread, parallel manifest reads could
    // produce a different CombinedScanTask order, breaking count-based cursor recovery.
    assertThat(secondRunOrder).containsExactlyElementsOf(firstRunOrder);
  }

  @Test
  public void testReopenAfterTransientFailureDoesNotDuplicateOrLoseSplits() throws Exception {
    appendFiles(8);
    ScanContext scanContext = tableScanScanContext();

    Set<String> allEmittedIds = Sets.newHashSet();
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 3, "test", null)) {
      // Page 1: 3 splits.
      ContinuousEnumerationResult p1 = planner.planSplits(null);
      assertThat(p1.splits()).hasSize(3);
      p1.splits().forEach(s -> allEmittedIds.add(s.splitId()));

      // Page 2: 3 splits.
      ContinuousEnumerationResult p2 = planner.planSplits(IcebergEnumeratorPosition.empty());
      assertThat(p2.splits()).hasSize(3);
      p2.splits().forEach(s -> allEmittedIds.add(s.splitId()));

      // Simulate a transient mid-page failure: directly close the iterator while keeping the
      // planner's internal bookkeeping intact. This is the same state planNextBulkPage's catch
      // block leaves the planner in after a failed bulkIterator.next().
      java.lang.reflect.Method close =
          LazyContinuousSplitPlanner.class.getDeclaredMethod("closeBulkIterator");
      close.setAccessible(true);
      close.invoke(planner);

      // Page 3 (after reopen): 2 remaining splits. Must NOT re-emit any from pages 1-2.
      ContinuousEnumerationResult p3 = planner.planSplits(IcebergEnumeratorPosition.empty());
      assertThat(p3.splits()).hasSize(2);
      assertThat(p3.cursorAction()).isEqualTo(ContinuousEnumerationResult.CursorAction.CLEAR);
      assertThat(p3.lazyBulkScanCursor()).isNull(); // final page clears
      p3.splits().forEach(s -> allEmittedIds.add(s.splitId()));

      assertThat(allEmittedIds).hasSize(8);
    }
  }

  @Test
  public void testRecoveryWithTamperedRollingHashAborts() throws Exception {
    appendFiles(5);
    ScanContext scanContext = tableScanScanContext();

    // First incarnation: emit one page, capture cursor (real hash).
    LazyBulkScanCursor genuineCursor;
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 2, "test", null)) {
      ContinuousEnumerationResult page = planner.planSplits(null);
      genuineCursor = page.lazyBulkScanCursor();
      assertThat(genuineCursor).isNotNull();
      assertThat(genuineCursor.rollingHash()).isNotZero();
    }

    // Tamper: keep the snapshot id and count, change the hash to something else (simulating
    // either a checkpoint forgery, a snapshot tamper, or — the case we actually care about —
    // an Iceberg version change that altered planTasks() iteration order such that the replay
    // hash no longer matches what was committed previously).
    LazyBulkScanCursor tampered =
        new LazyBulkScanCursor(
            genuineCursor.bulkSnapshotId(),
            genuineCursor.combinedTasksEnumerated(),
            genuineCursor.rollingHash() ^ 0xDEADBEEFDEADBEEFL);

    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 2, "test", tampered)) {
      assertThatThrownBy(() -> planner.planSplits(null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("iteration-order check failed");
    }
  }

  @Test
  public void testRecoveryWithMatchingRollingHashSucceeds() throws Exception {
    appendFiles(5);
    ScanContext scanContext = tableScanScanContext();

    LazyBulkScanCursor genuineCursor;
    Set<String> emittedIds = Sets.newHashSet();
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 2, "test", null)) {
      ContinuousEnumerationResult page = planner.planSplits(null);
      page.splits().forEach(sp -> emittedIds.add(sp.splitId()));
      genuineCursor = page.lazyBulkScanCursor();
    }

    // Second incarnation reuses the genuine cursor; replay should match and recovery succeeds.
    try (LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 100, "test", genuineCursor)) {
      ContinuousEnumerationResult page = planner.planSplits(null);
      page.splits().forEach(sp -> emittedIds.add(sp.splitId()));
    }

    assertThat(emittedIds).hasSize(5);
  }

  @Test
  public void testConstructorArgumentValidation() {
    // Pass the loader without an explicit clone: the Preconditions checks run before any
    // tableLoader access, so we don't leak a cloned loader when the assertion fires.
    ScanContext scanContext = tableScanScanContext();
    assertThatThrownBy(
            () ->
                new LazyContinuousSplitPlanner(
                    TABLE_RESOURCE.tableLoader(), scanContext, 0, "test", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("pageSize must be > 0");
    assertThatThrownBy(
            () ->
                new LazyContinuousSplitPlanner(
                    TABLE_RESOURCE.tableLoader(), scanContext, 10, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("threadName must not be null");
  }

  @Test
  public void testRestoreEnumeratorRejectsMidBulkCursorWithLazyDisabled() throws Exception {
    // Restoring an enumerator from a Phase-1 checkpoint (cursor != null) with lazy mode
    // disabled would hand the cursor to an eager ContinuousSplitPlannerImpl, which has no
    // notion of bulk-scan cursors. The eager planner would interpret the bulk-sentinel
    // lastPosition as "initial scan", materialise the full file list, and overflow the 2 GB
    // serialized-state cap on the next checkpoint — the exact failure mode lazy mode exists
    // to prevent. IcebergSource.createEnumerator must refuse this configuration loudly.
    appendFiles(1);
    LazyBulkScanCursor cursor = new LazyBulkScanCursor(42L, 7L, 0xABCDEFL);
    IcebergEnumeratorState state =
        new IcebergEnumeratorState(
            IcebergEnumeratorPosition.empty(), Collections.emptyList(), new int[0], cursor);

    IcebergSource<?> source =
        IcebergSource.forRowData()
            .tableLoader(TABLE_RESOURCE.tableLoader())
            .assignerFactory(new SimpleSplitAssignerFactory())
            .streaming(true)
            .streamingStartingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            // lazyInitialBulkScanPageSize left at the default 0 (eager).
            .build();

    assertThatThrownBy(() -> source.restoreEnumerator(null, state))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("lazy-bulk-scan checkpoint")
        .hasMessageContaining("lazy mode disabled");
  }

  @Test
  public void testRestoreEnumeratorAcceptsBulkSentinelPositionWithLazyDisabled() throws Exception {
    // Pre-existing eager planners produce position=empty() when the initial scan hits an empty
    // table (see ContinuousSplitPlannerImpl.discoverInitialSplits). A lazy planner that also hit
    // an empty initial table produces the same state (cursor=null, position=empty). The two are
    // indistinguishable on disk, so we can't reject the lazy variant without also breaking the
    // pre-existing eager → eager restart path. Accept the state and let the eager planner
    // handle empty() position as it always has.
    appendFiles(1);
    IcebergEnumeratorState state =
        new IcebergEnumeratorState(
            IcebergEnumeratorPosition.empty(), Collections.emptyList(), new int[0], null);

    IcebergSource<?> source =
        IcebergSource.forRowData()
            .tableLoader(TABLE_RESOURCE.tableLoader())
            .assignerFactory(new SimpleSplitAssignerFactory())
            .streaming(true)
            .streamingStartingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
            // lazyInitialBulkScanPageSize left at default 0 (eager).
            .build();

    // Must not throw: the empty-position eager checkpoint was previously valid and must remain
    // so. The enumerator is created, but we don't drive it (the test only verifies the validation
    // path doesn't trip; behavioral testing of empty-table-then-grew is out of scope for this PR).
    TestingSplitEnumeratorContext<IcebergSourceSplit> enumContext =
        new TestingSplitEnumeratorContext<>(1);
    SplitEnumerator<?, ?> enumerator = source.restoreEnumerator(enumContext, state);
    enumerator.close();
  }

  @Test
  public void testRealPlannerEndToEndWithEnumerator() throws Exception {
    // End-to-end: real LazyContinuousSplitPlanner + real ContinuousIcebergEnumerator. Verifies
    // the handshake between forLazyBulkPage / forLazyBulkClear results and the enumerator (CAS
    // relaxation, atomic
    // cursor commit, transition to incremental) — none of which is covered by isolated planner
    // or FakeLazyPlanner-based enumerator tests.
    appendFiles(5);
    Snapshot snapshot = TABLE_RESOURCE.table().currentSnapshot();
    ScanContext scanContext = tableScanScanContext();

    TestingSplitEnumeratorContext<IcebergSourceSplit> enumeratorContext =
        new TestingSplitEnumeratorContext<>(4);
    LazyContinuousSplitPlanner planner =
        new LazyContinuousSplitPlanner(
            TABLE_RESOURCE.tableLoader().clone(), scanContext, 2, "test", null);
    ContinuousIcebergEnumerator enumerator =
        new ContinuousIcebergEnumerator(
            enumeratorContext,
            new DefaultSplitAssigner(null, Collections.emptyList()),
            scanContext,
            planner,
            null);
    try {
      enumerator.start();
      enumeratorContext.registerReader(0, "localhost");
      enumerator.addReader(0);

      // Drive bulk paging by alternating periodic ticks and reader split-request events. Each
      // SplitRequestEvent drains the assigner, dipping below the reactive watermark and firing
      // an extra planSplits. Repeat until the cursor is cleared (bulk done) or we give up.
      boolean transitionedToIncremental = false;
      for (int i = 0; i < 20; i++) {
        enumeratorContext.triggerAllActions();
        enumerator.handleSourceEvent(0, new SplitRequestEvent());
        enumeratorContext.triggerAllActions();
        IcebergEnumeratorState snap = enumerator.snapshotState(i + 1);
        if (snap.lazyBulkScanCursor() == null
            && snap.lastEnumeratedPosition() != null
            && snap.lastEnumeratedPosition().snapshotId() != null) {
          transitionedToIncremental = true;
          break;
        }
      }
      assertThat(transitionedToIncremental)
          .as("Expected bulk phase to complete and transition to incremental")
          .isTrue();

      IcebergEnumeratorState finalState = enumerator.snapshotState(99);
      // Cursor cleared on final bulk page.
      assertThat(finalState.lazyBulkScanCursor()).isNull();
      // lastEnumeratedPosition advanced to the bulk snapshot's id (real, not the empty sentinel).
      assertThat(finalState.lastEnumeratedPosition().snapshotId()).isEqualTo(snapshot.snapshotId());
      // All 5 splits made it through the enumerator — sum of assigned (delivered to the reader)
      // and pendingSplits (still in the assigner). This sharply verifies the count rather than
      // just "transitioned": no duplicate or lost splits across the bulk → incremental boundary.
      int assigned =
          enumeratorContext.getSplitAssignments().values().stream()
              .mapToInt(a -> a.getAssignedSplits().size())
              .sum();
      assertThat(assigned + finalState.pendingSplits().size()).isEqualTo(5);
      // Post-bulk, recommendedLowWatermark drops to 0 so reactive triggering stops spamming the
      // incremental delegate.
      assertThat(planner.recommendedLowWatermark()).isZero();
    } finally {
      enumerator.close();
    }
  }

  @Test
  public void testRestoreEnumeratorRejectsMidBulkRecoveryWithDifferentStrategy() throws Exception {
    // A Phase-1 checkpoint pins to a specific snapshot via the cursor. The lazy planner only
    // consults the cursor when the strategy is TABLE_SCAN_THEN_INCREMENTAL; other strategies are
    // pure passthroughs to the incremental delegate, which would silently drop the cursor and
    // route empty() through ContinuousSplitPlannerImpl.discoverIncrementalSplits — that calls
    // appendsBetween(null, currentSnapshot) which materialises the entire table as one
    // incremental scan and overflows the 2 GB checkpoint, the exact failure mode lazy mode
    // exists to prevent.
    appendFiles(1);
    LazyBulkScanCursor cursor = new LazyBulkScanCursor(42L, 7L, 0xABCDEFL);
    IcebergEnumeratorState state =
        new IcebergEnumeratorState(
            IcebergEnumeratorPosition.empty(), Collections.emptyList(), new int[0], cursor);

    IcebergSource<?> source =
        IcebergSource.forRowData()
            .tableLoader(TABLE_RESOURCE.tableLoader())
            .assignerFactory(new SimpleSplitAssignerFactory())
            .streaming(true)
            // Switched away from TABLE_SCAN_THEN_INCREMENTAL while a bulk checkpoint exists.
            .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
            .lazyInitialBulkScanPageSize(10)
            .build();

    assertThatThrownBy(() -> source.restoreEnumerator(null, state))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("different starting strategy")
        .hasMessageContaining("TABLE_SCAN_THEN_INCREMENTAL");
  }
}
