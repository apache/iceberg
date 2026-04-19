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
package org.apache.iceberg.deletes;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Coalesces consecutive position deletes into range inserts on a {@link PositionDeleteIndex}.
 *
 * <p>The consumer is agnostic to input sortedness: callers can stream any {@code Iterable} or
 * {@code long[]} of positions and the API flushes them onto the target {@link PositionDeleteIndex}
 * without requiring a sorted contract. Monotone runs become a single range insert, which touches
 * fewer bitmap pages than a per-position {@code target.delete(pos)} loop; non-monotone input still
 * produces a correct index via an internal per-position fallback.
 *
 * <p>External callers see three operations: construct with a target index, feed positions via
 * {@link #acceptAll(long[], int, int)} (one or more calls), and {@link #flush()} when done. The
 * sniff/escape state and the buffering policy used by the package-private {@code forEach} helpers
 * are internal and may change.
 *
 * <p><b>Note:</b> this class is tied to the V2 position delete file lifecycle. Format-version 3
 * deletion vectors arrive pre-bitmap via {@link PositionDeleteIndex#deserialize} and bypass this
 * class entirely; this class can retire once V2 position delete files are no longer read.
 */
public final class PositionDeleteRangeConsumer {

  /**
   * Batch size for {@link #forEach}. Sized to fit comfortably in L1 (512 bytes). Smaller buffers
   * miss the bulk-path branch elision; larger buffers add allocation cost without improving the
   * inner-loop throughput (the {@code acceptAll} body is the same regardless of slice length).
   */
  private static final int FOREACH_BATCH_SIZE = 64;

  /**
   * Source-cardinality cutoff below which the batched path's setup cost (buffer allocation,
   * consumer state, {@code Preconditions} checks) outweighs the per-position savings from
   * coalescing. Sources at or below this size are drained directly via {@code target::delete}.
   */
  private static final int SMALL_SOURCE_THRESHOLD = 8;

  /** The number of positions to sniff for boundary density. */
  private static final int SNIFF_SIZE = 256;

  /** The threshold for boundary density at which to switch to the bulk path, as a percentage. */
  private static final int BOUNDARY_THRESHOLD_PERCENT = 30;

  private final PositionDeleteIndex target;
  private boolean hasRun;
  private long rangeStart;
  private long lastPosition;

  private int processed;
  private int boundaries;
  private boolean escaped;

  /**
   * Creates a new consumer that writes coalesced ranges into {@code target}. Single-threaded; one
   * instance per target index.
   */
  public PositionDeleteRangeConsumer(PositionDeleteIndex target) {
    Preconditions.checkArgument(target != null, "Invalid target index: null");
    this.target = target;
  }

  /**
   * Adds the half-open slice {@code positions[from, to)} to the target index, coalescing
   * consecutive positions into range inserts. Safe to call repeatedly; the consumer keeps the
   * pending run across calls and only emits it on the next gap or on {@link #flush()}.
   */
  public void acceptAll(long[] positions, int from, int to) {
    Preconditions.checkArgument(positions != null, "Invalid positions array: null");
    Preconditions.checkPositionIndexes(from, to, positions.length);
    if (from >= to) {
      return;
    }

    int cursor = from;

    if (escaped) {
      drainEscaped(positions, cursor, to);
      return;
    }

    if (!hasRun) {
      initRun(positions[cursor++]);
    }

    while (cursor < to && processed < SNIFF_SIZE) {
      coalesceSniff(positions[cursor++]);
    }

    if (processed == SNIFF_SIZE && shouldEscape()) {
      enterEscape();
      drainEscaped(positions, cursor, to);
      return;
    }

    while (cursor < to) {
      coalesce(positions[cursor++]);
    }
  }

  /** Emits the active run, if any. The escape decision is sticky across flushes. */
  public void flush() {
    if (hasRun) {
      emit();
      hasRun = false;
    }
  }

  // Per-element entry used internally by the package-private forEach helpers below and exercised
  // directly by in-package tests. Not exposed to other modules: callers there should buffer into
  // a long[] and use acceptAll, which keeps the state-machine dispatch out of the inner loop.
  void accept(long pos) {
    if (escaped) {
      target.delete(pos);
      return;
    }
    if (!hasRun) {
      initRun(pos);
      return;
    }
    coalesceSniff(pos);
    if (processed == SNIFF_SIZE && shouldEscape()) {
      enterEscape();
    }
  }

  /**
   * Drains a boxed {@code Iterable<Long>} into {@code target}. Buffers into a small primitive slice
   * and hands chunks to {@link #acceptAll(long[], int, int)}; keeps the sniff/escape state machine
   * out of the inner loop. Package-private: external callers should construct the consumer directly
   * and feed it via {@code acceptAll}.
   */
  public static void forEach(Iterable<Long> positions, PositionDeleteIndex target) {
    PositionDeleteRangeConsumer consumer = new PositionDeleteRangeConsumer(target);
    long[] buffer = new long[FOREACH_BATCH_SIZE];
    int filled = 0;
    for (Long pos : positions) {
      buffer[filled++] = pos;
      if (filled == FOREACH_BATCH_SIZE) {
        consumer.acceptAll(buffer, 0, FOREACH_BATCH_SIZE);
        filled = 0;
      }
    }
    if (filled > 0) {
      consumer.acceptAll(buffer, 0, filled);
    }
    consumer.flush();
  }

  /**
   * Drains all positions from {@code source} into {@code target} via the batched accumulator.
   * Equivalent to {@code source.forEach(target::delete)} but coalesces consecutive runs into range
   * inserts. Sources that emit in ascending order (such as {@code BitmapPositionDeleteIndex}, which
   * iterates a Roaring bitmap) feed straight into the coalescing fast path; out-of-order sources
   * still produce a correct result via the per-position fallback. Package-private: see the note on
   * {@link #forEach(Iterable, PositionDeleteIndex)}.
   */
  public static void forEach(PositionDeleteIndex source, PositionDeleteIndex target) {
    if (source.isEmpty()) {
      return;
    }
    if (isSmallSource(source)) {
      source.forEach(target::delete);
      return;
    }
    PositionDeleteRangeConsumer consumer = new PositionDeleteRangeConsumer(target);
    long[] buffer = new long[FOREACH_BATCH_SIZE];
    int[] filled = {0};
    source.forEach(
        pos -> {
          buffer[filled[0]++] = pos;
          if (filled[0] == FOREACH_BATCH_SIZE) {
            consumer.acceptAll(buffer, 0, FOREACH_BATCH_SIZE);
            filled[0] = 0;
          }
        });
    if (filled[0] > 0) {
      consumer.acceptAll(buffer, 0, filled[0]);
    }
    consumer.flush();
  }

  // cardinality() is a default method that throws when the impl does not implement it; treat
  // "unknown" as "use the batched path" so we never regress on bitmap-backed sources.
  private static boolean isSmallSource(PositionDeleteIndex source) {
    try {
      return source.cardinality() <= SMALL_SOURCE_THRESHOLD;
    } catch (UnsupportedOperationException ignored) {
      return false;
    }
  }

  /** Starts a new active run anchored at {@code first}. */
  private void initRun(long first) {
    rangeStart = first;
    lastPosition = first;
    hasRun = true;
    processed = 1;
  }

  /** Extends the active run with {@code pos} during sniffing; counts gaps to inform escape. */
  private void coalesceSniff(long pos) {
    if (pos - lastPosition != 1) {
      boundaries++;
      emit();
      rangeStart = pos;
    }
    lastPosition = pos;
    processed++;
  }

  /** Extends the active run with {@code pos} after sniffing has decided not to escape. */
  private void coalesce(long pos) {
    if (pos - lastPosition != 1) {
      emit();
      rangeStart = pos;
    }
    lastPosition = pos;
  }

  /** True if the sniffed prefix has too many gaps to make coalescing worthwhile. */
  private boolean shouldEscape() {
    return boundaries * 100 > (SNIFF_SIZE - 1) * BOUNDARY_THRESHOLD_PERCENT;
  }

  /** Permanently switches to per-position deletes; flushes any pending run. */
  private void enterEscape() {
    emit();
    hasRun = false;
    escaped = true;
  }

  /** Per-position fallback used once the consumer has escaped coalescing. */
  private void drainEscaped(long[] positions, int from, int to) {
    for (int cursor = from; cursor < to; cursor++) {
      target.delete(positions[cursor]);
    }
  }

  private void emit() {
    if (rangeStart == lastPosition) {
      target.delete(rangeStart);
    } else {
      target.delete(rangeStart, lastPosition + 1);
    }
  }
}
