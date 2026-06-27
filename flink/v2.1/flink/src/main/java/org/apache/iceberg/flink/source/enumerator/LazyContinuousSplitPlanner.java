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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSplitPlanner;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lazy variant of {@link ContinuousSplitPlannerImpl} for the {@link
 * StreamingStartingStrategy#TABLE_SCAN_THEN_INCREMENTAL} strategy.
 *
 * <p>Pages through the initial bulk scan one {@code pageSize}-sized batch at a time instead of
 * materialising the full split list up front. This keeps the enumerator's pending-split state (and
 * the resulting checkpoint) bounded regardless of table size — critical for tables with millions of
 * data files where the eager approach overflows the 2GB single-checkpoint-blob limit.
 *
 * <p>After the bulk phase completes, all further {@link #planSplits} calls delegate to a wrapped
 * {@link ContinuousSplitPlannerImpl} so the existing incremental-scan path is reused unchanged.
 *
 * <p>Bulk-phase progress is communicated via {@link ContinuousEnumerationResult#lazyBulkScanCursor}
 * — the enumerator advances the checkpointed cursor atomically with the assigner update, so a
 * checkpoint that races with split discovery can never capture a cursor that's ahead of the
 * assigner (which would cause silent data loss on restart). The planner itself holds no
 * checkpoint-visible cursor state; on restart it receives the previously-committed cursor through
 * the constructor.
 *
 * <p><b>Iteration determinism.</b> Count-based cursor recovery works only when {@code planTasks()}
 * produces the same {@link CombinedScanTask} sequence — same combined tasks in the same order, each
 * combined task containing the same files with the same {@code start}/{@code length} and delete
 * files — across job incarnations. This is not a contract of the Iceberg API; it's an
 * implementation-level property of the current Iceberg core (manifest-list order, deterministic
 * bin-packing) which the planner relies on. Two layers defend it:
 *
 * <ol>
 *   <li>The bulk-scan worker pool is hard-pinned to a single thread regardless of {@link
 *       ScanContext#planParallelism()}, so parallel-manifest-read non-determinism cannot leak in.
 *   <li>The cursor carries a 64-bit rolling FNV-1a hash over each emitted {@link FileScanTask}'s
 *       location + {@code start} + {@code length} and each attached delete file's location, in
 *       iteration order. On recovery the planner re-iterates the first N {@code CombinedScanTask}s
 *       and recomputes the hash; mismatch (an Iceberg refactor changed iteration order, the bulk
 *       snapshot's underlying data drifted, or scan context options like {@code splitSize} changed
 *       between incarnations) causes a loud abort instead of silent data loss. The hash is
 *       non-cryptographic and is purely a drift detector — not a tampering defence.
 * </ol>
 *
 * <p>Cost of a hash mismatch is a full re-scan from scratch (~30 min for a ~1.87M-file table in the
 * empirical run). A manifest-position cursor walking the manifest list directly would give a
 * contract-level guarantee instead of an implementation-level one; deferred to a follow-up because
 * it conflicts non-trivially with split combining.
 *
 * <p>Fails fast if the bulk snapshot has been expired between the checkpoint and recovery.
 */
@Internal
public class LazyContinuousSplitPlanner implements ContinuousSplitPlanner {

  private static final Logger LOG = LoggerFactory.getLogger(LazyContinuousSplitPlanner.class);

  private final ContinuousSplitPlannerImpl incrementalDelegate;
  private final TableLoader ownedTableLoader;
  private final Table table;
  private final ScanContext scanContext;
  private final int pageSize;
  private final ExecutorService workerPool;

  // Restored cursor from a previous incarnation. Drives openBulkScan's skip count. Never mutated
  // after construction (the live iteration position lives in bulkIterator + bulkTasksEnumerated).
  @Nullable private final LazyBulkScanCursor restoredCursor;

  // Bulk-phase state. Written under planSplits()'s monitor; volatile so the lock-free
  // recommendedLowWatermark() (called from the coordinator thread) sees the flip-to-true.
  private volatile boolean bulkPhaseFinished;

  /**
   * Total combined tasks emitted across all pages so far in this incarnation, including any skipped
   * during recovery. Used to compute the cursor for the next result and as the skip target on
   * iterator reopens after transient failures. Strictly monotonic. Only accessed under the {@code
   * planSplits} monitor.
   */
  private long bulkTasksEnumerated = 0L;

  /**
   * Rolling FNV-1a hash of every emitted file's path/offset + attached delete files' locations, in
   * iteration order. Initialised from the restored cursor on construction; verified during the skip
   * loop in {@link #openBulkScan()}; updated for every file emitted in {@link #planNextBulkPage}.
   */
  private long rollingHash = 0L;

  @Nullable private CloseableIterable<CombinedScanTask> bulkIterable;

  @Nullable private CloseableIterator<CombinedScanTask> bulkIterator;

  @Nullable private Snapshot bulkSnapshot;

  /**
   * Snapshot ID resolved on the first {@link #openBulkScan} call. Cached so iterator reopens after
   * transient failures use the same snapshot — never re-resolve via {@code
   * table.currentSnapshot()}, which could pick a newer snapshot and mix files from different
   * snapshots into the same bulk scan.
   */
  @Nullable private Long resolvedBulkSnapshotId;

  /**
   * @param tableLoader caller-owned table loader. Cloned internally; the clone is closed by {@link
   *     #close()}. The original is passed unchanged to the incremental delegate, which clones it
   *     again — so the caller's loader survives the planner lifecycle and remains the caller's
   *     responsibility to close.
   * @param scanContext scan context (filters, projection, splitSize, etc.)
   * @param pageSize maximum number of combined tasks emitted per {@link #planSplits} call
   * @param threadName non-null thread name prefix for worker pools. Used verbatim for the
   *     single-threaded bulk-scan pool and forwarded to the incremental delegate.
   * @param restoredCursor non-null only on recovery from a mid-bulk-scan checkpoint
   */
  public LazyContinuousSplitPlanner(
      TableLoader tableLoader,
      ScanContext scanContext,
      int pageSize,
      String threadName,
      @Nullable LazyBulkScanCursor restoredCursor) {
    Preconditions.checkArgument(pageSize > 0, "pageSize must be > 0, got: %s", pageSize);
    Preconditions.checkArgument(threadName != null, "threadName must not be null");
    this.ownedTableLoader = tableLoader.clone();
    this.ownedTableLoader.open();
    this.table = this.ownedTableLoader.loadTable();
    this.scanContext = scanContext;
    this.pageSize = pageSize;
    // Hard-pin the bulk-scan worker pool to a single thread so planTasks() produces the same
    // CombinedScanTask order across job incarnations. This is what makes count-based cursor
    // recovery correct under arbitrary checkpoint/restore cycles.
    this.workerPool =
        ThreadPools.newFixedThreadPool("iceberg-lazy-bulk-plan-worker-pool-" + threadName, 1);
    this.restoredCursor = restoredCursor;
    this.bulkTasksEnumerated =
        restoredCursor != null ? restoredCursor.combinedTasksEnumerated() : 0L;
    this.rollingHash = restoredCursor != null ? restoredCursor.rollingHash() : 0L;
    this.bulkPhaseFinished = false;
    // Use a separate ContinuousSplitPlannerImpl for the incremental tail. It manages its own
    // tableLoader clone, worker pool, and table reference. Incremental scans are bounded by
    // maxPlanningSnapshotCount, so they can run multi-threaded without affecting cursor
    // correctness (the cursor only applies to the bulk phase).
    this.incrementalDelegate = new ContinuousSplitPlannerImpl(tableLoader, scanContext, threadName);
  }

  @Override
  public synchronized ContinuousEnumerationResult planSplits(
      IcebergEnumeratorPosition lastPosition) {
    // Lazy paging only applies to TABLE_SCAN_THEN_INCREMENTAL. Other strategies have no bulk
    // phase, so transparently delegate.
    if (scanContext.streamingStartingStrategy()
        != StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL) {
      return incrementalDelegate.planSplits(lastPosition);
    }

    // Once the bulk phase completes the enumerator advances to a real snapshot id and every
    // subsequent call carries a non-empty lastPosition. From then on, the incremental delegate
    // owns the discovery loop. During the bulk phase the planner returns toPosition=empty() to
    // keep the enumerator pinned to the bulk scan; the early-return CAS check in the enumerator
    // passes because both sides match.
    boolean lastPositionIsBulkSentinel = lastPosition == null || lastPosition.snapshotId() == null;
    if (bulkPhaseFinished || !lastPositionIsBulkSentinel) {
      return incrementalDelegate.planSplits(lastPosition);
    }

    if (bulkIterator == null) {
      openBulkScan();
    }

    // Empty table: openBulkScan() left bulkSnapshot=null and bulkPhaseFinished=true.
    if (bulkPhaseFinished) {
      return new ContinuousEnumerationResult(
          Collections.emptyList(), null, IcebergEnumeratorPosition.empty());
    }

    return planNextBulkPage(lastPosition);
  }

  @Override
  public int recommendedLowWatermark() {
    // Reactive paging only applies to the bulk phase. For non-TABLE_SCAN_THEN_INCREMENTAL
    // strategies, planSplits is a pure passthrough to the incremental delegate and the bulk
    // phase never runs — return 0 so SplitRequestEvents don't spam the delegate with extra
    // calls. Once the bulk phase finishes (TABLE_SCAN_THEN_INCREMENTAL case), drop back to
    // purely-periodic discovery for the same reason.
    if (scanContext.streamingStartingStrategy()
            != StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL
        || bulkPhaseFinished) {
      return 0;
    }
    // Math.max guards pageSize=1: a literal pageSize/2 would be 0, which the enumerator
    // interprets as "reactive trigger disabled" — the opposite of what a tiny page size
    // implies.
    return Math.max(1, pageSize / 2);
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      closeBulkIterator();
    } finally {
      closeDelegateAndPool();
    }
  }

  private void closeDelegateAndPool() throws IOException {
    try {
      incrementalDelegate.close();
    } finally {
      closePoolAndLoader();
    }
  }

  private void closePoolAndLoader() throws IOException {
    try {
      workerPool.shutdown();
    } finally {
      ownedTableLoader.close();
    }
  }

  private void openBulkScan() {
    table.refresh();
    Snapshot resolved;
    if (resolvedBulkSnapshotId != null) {
      // Reopen after a transient mid-page failure (closeBulkIterator was called from the catch
      // block in planNextBulkPage). Use the snapshot we already pinned to — never re-resolve
      // via table.currentSnapshot(), which could pick a newer snapshot and produce a bulk scan
      // that mixes files from different snapshots.
      resolved = table.snapshot(resolvedBulkSnapshotId);
      Preconditions.checkArgument(
          resolved != null,
          "Lazy bulk scan: snapshot %s disappeared mid-scan from table %s.",
          resolvedBulkSnapshotId,
          table.name());
    } else if (restoredCursor != null) {
      // First open in a recovered incarnation. The cursor pins us to the same snapshot the
      // previous incarnation was scanning. If it has been expired, fail loudly — silent
      // re-scan against current snapshot would produce inconsistent results downstream.
      resolved = table.snapshot(restoredCursor.bulkSnapshotId());
      Preconditions.checkArgument(
          resolved != null,
          "Cannot recover lazy bulk scan: snapshot %s no longer exists in table %s. "
              + "Increase snapshot retention or restart the job from scratch.",
          restoredCursor.bulkSnapshotId(),
          table.name());
    } else {
      // Initial start, fresh job. Resolve the bulk-scan snapshot the same way
      // ContinuousSplitPlannerImpl would: branch first, otherwise current.
      resolved =
          scanContext.branch() != null
              ? table.snapshot(scanContext.branch())
              : table.currentSnapshot();
    }

    if (resolved == null) {
      // Empty table — nothing to scan. Match ContinuousSplitPlannerImpl#discoverInitialSplits()
      // by signalling "no work" via bulkPhaseFinished. Subsequent planSplits calls will fall
      // through to the incremental delegate, which also handles the empty-table case.
      LOG.info(
          "Lazy bulk scan: table {} has no current snapshot, skipping bulk phase", table.name());
      bulkPhaseFinished = true;
      return;
    }

    this.bulkSnapshot = resolved;
    this.resolvedBulkSnapshotId = resolved.snapshotId();
    ScanContext snapshotScan = scanContext.copyWithSnapshotId(resolved.snapshotId());

    // Wrap iterator creation + skip in a single try/catch. Any failure here must close whatever
    // we already opened — otherwise the next planSplits call would see a half-open
    // bulkIterator, bypass openBulkScan, and start emitting from the wrong position (silently
    // skipping or duplicating splits depending on where the failure landed).
    try {
      this.bulkIterable = FlinkSplitPlanner.planTasks(table, snapshotScan, workerPool);
      this.bulkIterator = bulkIterable.iterator();

      // Skip target is bulkTasksEnumerated, which has been:
      //   - initialised from restoredCursor.combinedTasksEnumerated() in the constructor (so
      //     first open in a recovered incarnation skips the right amount);
      //   - kept current across mid-page failures (the failed attempt's catch block does NOT
      //     advance bulkTasksEnumerated, so it still reflects the last successfully-emitted
      //     total).
      long skipTarget = bulkTasksEnumerated;
      if (skipTarget > 0) {
        long expectedHash = rollingHash;
        LOG.info(
            "Lazy bulk scan: opening iterator on snapshot {}, skipping {} tasks, verifying hash {}",
            resolved.snapshotId(),
            skipTarget,
            String.format(Locale.ROOT, "0x%016x", expectedHash));
        long replayHash = 0L;
        long skipped = 0;
        while (skipped < skipTarget && bulkIterator.hasNext()) {
          CombinedScanTask skippedTask = bulkIterator.next();
          replayHash = updateRollingHash(replayHash, skippedTask);
          skipped++;
        }
        Preconditions.checkState(
            skipped == skipTarget,
            "Lazy bulk scan: expected to skip %s tasks but iterator exhausted after %s. "
                + "Snapshot %s may have been compacted between checkpoint and recovery.",
            skipTarget,
            skipped,
            resolved.snapshotId());
        Preconditions.checkState(
            replayHash == expectedHash,
            "Lazy bulk scan: iteration-order check failed on recovery. Snapshot %s scan produced "
                + "a different sequence of files than the previous incarnation (replay hash %s, "
                + "expected %s). Likely causes: an Iceberg version upgrade changed planTasks() "
                + "iteration order; the scan context's splitSize/delete-file set changed between "
                + "incarnations; or the snapshot's underlying metadata drifted. Refusing to "
                + "resume — restart the job from scratch (discard the checkpoint) to avoid "
                + "silent data loss.",
            resolved.snapshotId(),
            String.format(Locale.ROOT, "0x%016x", replayHash),
            String.format(Locale.ROOT, "0x%016x", expectedHash));
      } else {
        LOG.info(
            "Lazy bulk scan: starting fresh on snapshot {} with pageSize {}",
            resolved.snapshotId(),
            pageSize);
      }
    } catch (RuntimeException e) {
      closeBulkIterator();
      throw e;
    }
  }

  private ContinuousEnumerationResult planNextBulkPage(IcebergEnumeratorPosition lastPosition) {
    // Invariant: openBulkScan() sets bulkSnapshot before bulkIterator, and closeBulkIterator()
    // only nulls the iterator. So whenever we reach this method via planSplits (which checks
    // bulkIterator != null), bulkSnapshot must be non-null. Defensive check to catch future
    // refactors that break this invariant.
    Preconditions.checkState(
        bulkSnapshot != null, "Lazy bulk scan: bulkSnapshot is null in planNextBulkPage");
    List<IcebergSourceSplit> page = Lists.newArrayListWithCapacity(pageSize);
    int collected = 0;
    long pageHash = rollingHash;
    // Track exhaustion inside the try so the post-loop branch never calls hasNext() outside
    // the catch. A manifest-read failure on the page-boundary hasNext() would otherwise leave
    // bulkIterator half-open with no closeBulkIterator() call, and the next planSplits would
    // bypass openBulkScan() and resume from the wrong position.
    boolean iteratorExhausted = false;
    try {
      while (collected < pageSize) {
        if (!bulkIterator.hasNext()) {
          iteratorExhausted = true;
          break;
        }
        CombinedScanTask task = bulkIterator.next();
        page.add(IcebergSourceSplit.fromCombinedScanTask(task));
        pageHash = updateRollingHash(pageHash, task);
        collected++;
      }
    } catch (RuntimeException e) {
      // Transient manifest-read failure (e.g. HDFS hiccup). Drop this partial page so the
      // failure surfaces to ContinuousIcebergEnumerator.processDiscoveredSplits, which retries
      // via maxAllowedPlanningFailures. Close the now-possibly-corrupt iterator; the next
      // planSplits call will reopen from the last committed cursor passed in via the
      // restoredCursor constructor arg (effectively the same as restart, since the partial page
      // was never returned to the enumerator).
      LOG.warn(
          "Lazy bulk scan: iterator threw mid-page after {} new tasks; reopening on next call",
          collected,
          e);
      closeBulkIterator();
      throw e;
    }

    long newTotal = bulkTasksEnumerated + collected;
    bulkTasksEnumerated = newTotal;
    rollingHash = pageHash;

    // ContinuousEnumerationResult requires non-null toPosition. During the bulk phase we use
    // IcebergEnumeratorPosition.empty() (which has null snapshotId) as the sentinel that means
    // "still inside the bulk scan, don't advance". Once the iterator is drained we emit the
    // real bulk-snapshot position so the enumerator advances and subsequent calls delegate to
    // the incremental planner.
    // Echo lastPosition as fromPosition so the enumerator's CAS check passes — by construction
    // it equals the current enumeratorPosition.
    if (!iteratorExhausted) {
      LazyBulkScanCursor cursorForCheckpoint =
          new LazyBulkScanCursor(bulkSnapshot.snapshotId(), newTotal, pageHash);
      LOG.info(
          "Lazy bulk scan: emitted page of {} tasks (total {}, snapshot {})",
          collected,
          newTotal,
          bulkSnapshot.snapshotId());
      return ContinuousEnumerationResult.forLazyBulkPage(
          page, lastPosition, IcebergEnumeratorPosition.empty(), cursorForCheckpoint);
    } else {
      IcebergEnumeratorPosition toPosition =
          IcebergEnumeratorPosition.of(bulkSnapshot.snapshotId(), bulkSnapshot.timestampMillis());
      LOG.info(
          "Lazy bulk scan: emitted final page of {} tasks (total {}, snapshot {}), transitioning to incremental",
          collected,
          newTotal,
          bulkSnapshot.snapshotId());
      bulkPhaseFinished = true;
      closeBulkIterator();
      // Final page: clear the cursor so subsequent checkpoints carry no mid-bulk state.
      return ContinuousEnumerationResult.forLazyBulkClear(page, lastPosition, toPosition);
    }
  }

  /**
   * 64-bit FNV-1a hash update over each file path (UTF-8 bytes), {@code start}, {@code length}, and
   * each attached delete file's location, for every {@link FileScanTask} in the given combined
   * task. Order-sensitive across calls (running hash) and order-sensitive within a task (we iterate
   * files in the task's declared order and delete files in the file scan task's declared order), so
   * any drift in combined-task order, file-within-task order, split offsets, or delete-file
   * attachment changes the result and triggers the loud abort on recovery.
   */
  private static long updateRollingHash(long current, CombinedScanTask task) {
    long hash = current;
    for (FileScanTask file : task.files()) {
      hash = fnv1a64UpdateString(hash, file.file().location());
      hash = fnv1a64UpdateLong(hash, file.start());
      hash = fnv1a64UpdateLong(hash, file.length());
      for (DeleteFile delete : file.deletes()) {
        hash = fnv1a64UpdateString(hash, delete.location());
      }
    }
    return hash;
  }

  // Standard FNV-1a-64 update: XOR each byte then multiply by the 64-bit FNV prime. The path
  // is encoded to UTF-8 bytes so the hash is well-defined regardless of platform charset and
  // matches the canonical FNV-1a definition (which is byte-oriented, not char-oriented).
  private static long fnv1a64UpdateString(long hash, String path) {
    long acc = hash;
    for (byte b : path.getBytes(StandardCharsets.UTF_8)) {
      acc ^= (b & 0xffL);
      acc *= 0x100000001b3L;
    }
    return acc;
  }

  // Fold a 64-bit value into the rolling hash big-endian byte-by-byte so the result is
  // independent of platform byte order.
  private static long fnv1a64UpdateLong(long hash, long value) {
    long acc = hash;
    for (int shift = 56; shift >= 0; shift -= 8) {
      acc ^= ((value >>> shift) & 0xffL);
      acc *= 0x100000001b3L;
    }
    return acc;
  }

  private void closeBulkIterator() {
    CloseableIterator<CombinedScanTask> iterator = bulkIterator;
    CloseableIterable<CombinedScanTask> iterable = bulkIterable;
    bulkIterator = null;
    bulkIterable = null;
    if (iterator != null) {
      try {
        iterator.close();
      } catch (IOException e) {
        LOG.warn("Failed to close lazy bulk scan iterator", e);
      }
    }
    if (iterable != null) {
      try {
        iterable.close();
      } catch (IOException e) {
        LOG.warn("Failed to close lazy bulk scan iterable", e);
      }
    }
  }
}
