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

import java.util.Objects;
import java.util.PrimitiveIterator;

/**
 * Coalesces consecutive position deletes into range inserts on a {@link PositionDeleteIndex}.
 *
 * <p>Position delete files are spec-sorted, but this consumer accepts any input ordering and still
 * produces a correct index: callers can stream any {@code Iterable} or {@code long[]} of positions
 * through this API. Out-of-order input is handled by an internal per-position fallback.
 *
 * <p>The coalescing benefit scales with monotone runs in the input. Each ascending run is emitted
 * as a single range insert, which touches fewer bitmap pages than a per-position {@code
 * target.delete(pos)} loop. A small sniff window measures gap density at the start of the stream
 * and switches to the per-position path when coalescing would no longer pay for the bookkeeping.
 *
 * <p>External callers see three operations: construct with a target index, feed positions via
 * {@link #acceptAll(long[], int, int)} (one or more calls), and {@link #flush()} when done.
 *
 * <p><b>Note:</b> this class is tied to the V2 position delete file lifecycle. Format-version 3
 * deletion vectors arrive pre-bitmap via {@link PositionDeleteIndex#deserialize} and bypass this
 * class entirely; this class can retire once V2 position delete files are no longer read.
 *
 * <p><b>API note:</b> cross-module helper used directly by {@code VectorizedPositionDeleteReader}
 * in the {@code iceberg-arrow} module; the constructor + {@link #acceptAll} + {@link #flush} shape
 * is not yet a stable public API and may evolve as the Arrow stack lands. The static {@code
 * forEach} overloads remain package-private since all callers live in this package.
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
   * consumer state) outweighs the per-position savings from coalescing. Sources at or below this
   * size are drained directly via {@code target::delete}.
   */
  private static final int SMALL_SOURCE_THRESHOLD = 8;

  /** The number of positions to sniff for boundary density. */
  private static final int SNIFF_SIZE = 256;

  /**
   * Gap-density threshold for the sniff window, as a percentage. If more than this fraction of
   * adjacent pairs in the first {@link #SNIFF_SIZE} positions are gaps, the consumer escapes
   * coalescing for the rest of the stream and routes positions through the per-position fallback.
   */
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
    this.target = target;
  }

  /**
   * Adds the half-open slice {@code positions[from, to)} to the target index, coalescing
   * consecutive positions into range inserts. Safe to call repeatedly; the consumer keeps the
   * pending run across calls and only emits it on the next gap or on {@link #flush()}.
   */
  public void acceptAll(long[] positions, int from, int to) {
    Objects.checkFromToIndex(from, to, positions.length);
    if (from == to) {
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

  /**
   * Drains a boxed {@code Iterable<Long>} into {@code target}, buffering into a primitive slice and
   * forwarding chunks to {@link #acceptAll(long[], int, int)}.
   */
  static void forEach(Iterable<Long> positions, PositionDeleteIndex target) {
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
   * Drains a {@link PrimitiveIterator.OfLong} into {@code target}, buffering into a primitive slice
   * and forwarding chunks to {@link #acceptAll(long[], int, int)}. Equivalent to {@link
   * #forEach(Iterable, PositionDeleteIndex)} but avoids autoboxing each position, which matters
   * when the caller is itself unboxing from a {@code StructLike} accessor.
   */
  static void forEach(PrimitiveIterator.OfLong positions, PositionDeleteIndex target) {
    PositionDeleteRangeConsumer consumer = new PositionDeleteRangeConsumer(target);
    long[] buffer = new long[FOREACH_BATCH_SIZE];
    int filled = 0;
    while (positions.hasNext()) {
      buffer[filled++] = positions.nextLong();
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
   * Drains all positions from {@code source} into {@code target}, coalescing consecutive runs into
   * range inserts. Equivalent to {@code source.forEach(target::delete)} but cheaper for ascending
   * sources (such as {@code BitmapPositionDeleteIndex}); out-of-order sources still produce a
   * correct result via the per-position fallback.
   */
  static void forEach(PositionDeleteIndex source, PositionDeleteIndex target) {
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

  /**
   * Returns {@code true} when {@code source} is small enough that the per-position drain wins over
   * the batched path's setup cost. {@link UnsupportedOperationException} from {@link
   * PositionDeleteIndex#cardinality()} is part of the contract here: implementations that don't
   * report cardinality are treated as "size unknown" and routed through the batched path, so
   * bitmap-backed sources never regress to per-position dispatch.
   */
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
