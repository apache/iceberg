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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class TestPositionDeleteRangeConsumer {

  @Test
  void emptyInput() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(Collections.<Long>emptyList(), index);
    assertThat(index.isEmpty()).isTrue();
  }

  @Test
  void singlePosition() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(42L), index);

    assertThat(index.isDeleted(42)).isTrue();
    assertThat(index.cardinality()).isEqualTo(1);
  }

  @Test
  void consecutivePositionsCoalesced() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(10L, 11L, 12L, 13L, 14L), index);

    for (long pos = 10; pos <= 14; pos++) {
      assertThat(index.isDeleted(pos)).isTrue();
    }

    assertThat(index.isDeleted(9)).isFalse();
    assertThat(index.isDeleted(15)).isFalse();
    assertThat(index.cardinality()).isEqualTo(5);
  }

  @Test
  void multipleDisjointRanges() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(3L, 4L, 5L, 10L, 11L, 100L), index);

    assertThat(index.isDeleted(3)).isTrue();
    assertThat(index.isDeleted(4)).isTrue();
    assertThat(index.isDeleted(5)).isTrue();
    assertThat(index.isDeleted(6)).isFalse();
    assertThat(index.isDeleted(10)).isTrue();
    assertThat(index.isDeleted(11)).isTrue();
    assertThat(index.isDeleted(12)).isFalse();
    assertThat(index.isDeleted(100)).isTrue();
    assertThat(index.cardinality()).isEqualTo(6);
  }

  @Test
  void unsortedInputProducesCorrectResult() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(50L, 10L, 11L, 12L, 1L, 2L, 3L), index);

    assertThat(index.isDeleted(50)).isTrue();
    assertThat(index.isDeleted(10)).isTrue();
    assertThat(index.isDeleted(11)).isTrue();
    assertThat(index.isDeleted(12)).isTrue();
    assertThat(index.isDeleted(1)).isTrue();
    assertThat(index.isDeleted(2)).isTrue();
    assertThat(index.isDeleted(3)).isTrue();
    assertThat(index.cardinality()).isEqualTo(7);
  }

  @Test
  void duplicatePositions() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(5L, 5L, 6L, 6L), index);

    assertThat(index.isDeleted(5)).isTrue();
    assertThat(index.isDeleted(6)).isTrue();
    assertThat(index.cardinality()).isEqualTo(2);
  }

  @Test
  void largeConsecutiveRange() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    int rangeSize = 100_000;
    long[] positions = new long[rangeSize];
    for (int i = 0; i < rangeSize; i++) {
      positions[i] = i;
    }

    PositionDeleteRangeConsumer.forEach(asList(positions), index);

    assertThat(index.cardinality()).isEqualTo(rangeSize);
    assertThat(index.isDeleted(0)).isTrue();
    assertThat(index.isDeleted(rangeSize - 1)).isTrue();
    assertThat(index.isDeleted(rangeSize)).isFalse();
  }

  @Test
  void positionZero() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(0L, 1L, 2L), index);

    assertThat(index.isDeleted(0)).isTrue();
    assertThat(index.isDeleted(1)).isTrue();
    assertThat(index.isDeleted(2)).isTrue();
    assertThat(index.cardinality()).isEqualTo(3);
  }

  @Test
  void alternatingPositionsNoCoalescing() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(0L, 2L, 4L, 6L), index);

    assertThat(index.isDeleted(0)).isTrue();
    assertThat(index.isDeleted(1)).isFalse();
    assertThat(index.isDeleted(2)).isTrue();
    assertThat(index.isDeleted(3)).isFalse();
    assertThat(index.isDeleted(4)).isTrue();
    assertThat(index.isDeleted(5)).isFalse();
    assertThat(index.isDeleted(6)).isTrue();
    assertThat(index.cardinality()).isEqualTo(4);
  }

  @Test
  void largeSparseInputTakesNaivePath() {
    // 4096 alternating positions -- sniff sees 100% boundaries on the 256-long prefix and
    // dispatches to the naive path. The result must still be identical to coalescing.
    int count = 4096;
    long[] positions = new long[count];
    for (int i = 0; i < count; i++) {
      positions[i] = i * 2L;
    }

    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(positions), index);

    assertThat(index.cardinality()).isEqualTo(count);
    for (int i = 0; i < count; i++) {
      assertThat(index.isDeleted(positions[i])).isTrue();
      assertThat(index.isDeleted(positions[i] + 1)).isFalse();
    }
  }

  @Test
  void sparsePrefixWithDenseTail() {
    // First 512 positions alternate, then 2048 consecutive positions. The sniff sees a sparse
    // prefix and picks the naive path; the dense tail is still emitted correctly.
    int prefixCount = 512;
    int tailCount = 2048;
    long[] positions = new long[prefixCount + tailCount];
    for (int i = 0; i < prefixCount; i++) {
      positions[i] = i * 2L;
    }

    long tailStart = 10_000L;
    for (int i = 0; i < tailCount; i++) {
      positions[prefixCount + i] = tailStart + i;
    }

    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(positions), index);

    assertThat(index.cardinality()).isEqualTo(prefixCount + tailCount);
    for (int i = 0; i < prefixCount; i++) {
      assertThat(index.isDeleted(positions[i])).isTrue();
    }

    for (int i = 0; i < tailCount; i++) {
      assertThat(index.isDeleted(tailStart + i)).isTrue();
    }

    assertThat(index.isDeleted(tailStart + tailCount)).isFalse();
  }

  @Test
  void densePrefixWithSparseTail() {
    // First 512 positions are contiguous, then 2048 alternating. The sniff sees a dense prefix
    // and picks the coalesce path; the sparse tail is still emitted correctly.
    int prefixCount = 512;
    int tailCount = 2048;
    long[] positions = new long[prefixCount + tailCount];
    for (int i = 0; i < prefixCount; i++) {
      positions[i] = i;
    }

    long tailStart = 10_000L;
    for (int i = 0; i < tailCount; i++) {
      positions[prefixCount + i] = tailStart + i * 2L;
    }

    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(positions), index);

    assertThat(index.cardinality()).isEqualTo(prefixCount + tailCount);
    for (int i = 0; i < prefixCount; i++) {
      assertThat(index.isDeleted(i)).isTrue();
    }

    for (int i = 0; i < tailCount; i++) {
      assertThat(index.isDeleted(tailStart + i * 2L)).isTrue();
      assertThat(index.isDeleted(tailStart + i * 2L + 1)).isFalse();
    }
  }

  @Test
  void lengthAtExactSniffBoundary() {
    // Input length matches the sniff window exactly: the prefix fills, the sniff runs, and the
    // streaming tail loop never executes. Both dispatch decisions must produce correct output.
    int count = 256;
    long[] dense = new long[count];
    long[] sparse = new long[count];
    for (int i = 0; i < count; i++) {
      dense[i] = i;
      sparse[i] = i * 2L;
    }

    BitmapPositionDeleteIndex denseIndex = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(dense), denseIndex);
    assertThat(denseIndex.cardinality()).isEqualTo(count);
    assertThat(denseIndex.isDeleted(0)).isTrue();
    assertThat(denseIndex.isDeleted(count - 1)).isTrue();

    BitmapPositionDeleteIndex sparseIndex = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(sparse), sparseIndex);
    assertThat(sparseIndex.cardinality()).isEqualTo(count);
    for (int i = 0; i < count; i++) {
      assertThat(sparseIndex.isDeleted(i * 2L)).isTrue();
    }
  }

  @Test
  void smallAdversarialInputSkipsSniff() {
    // Alternating input shorter than the sniff window -- the prefix never fills, so the sniff
    // is skipped and we coalesce directly. Verifies correctness on the small-list fast path.
    int count = 100;
    long[] positions = new long[count];
    for (int i = 0; i < count; i++) {
      positions[i] = i * 2L;
    }

    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(asList(positions), index);

    assertThat(index.cardinality()).isEqualTo(count);
    for (int i = 0; i < count; i++) {
      assertThat(index.isDeleted(i * 2L)).isTrue();
      assertThat(index.isDeleted(i * 2L + 1)).isFalse();
    }
  }

  @Test
  void accumulatorRejectsNullTarget() {
    assertThatThrownBy(() -> new PositionDeleteRangeConsumer(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid target index");
  }

  @Test
  void accumulatorEmitsSinglePositionOnFlush() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    acc.accept(42L);
    acc.flush();

    assertThat(index.isDeleted(42L)).isTrue();
    assertThat(index.cardinality()).isEqualTo(1);
  }

  @Test
  void accumulatorCoalescesConsecutivePositions() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    for (long pos = 10L; pos <= 14L; pos++) {
      acc.accept(pos);
    }
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(5);
    assertThat(index.isDeleted(9L)).isFalse();
    assertThat(index.isDeleted(15L)).isFalse();
  }

  @Test
  void accumulatorBreaksRunOnGap() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    acc.accept(10L);
    acc.accept(11L);
    acc.accept(20L);
    acc.accept(21L);
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(4);
    assertThat(index.isDeleted(12L)).isFalse();
    assertThat(index.isDeleted(19L)).isFalse();
  }

  @Test
  void accumulatorFlushAllowsContinuationWithoutCoalescing() {
    // Explicit flush mid-stream emulates a logical boundary (e.g. a path change in a multi-file
    // delete file). The next accept must not be coalesced with the just-emitted run even when
    // its position is consecutive with the previous one.
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    acc.accept(10L);
    acc.accept(11L);
    acc.flush();
    acc.accept(12L);
    acc.accept(13L);
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(4);
    assertThat(index.isDeleted(10L)).isTrue();
    assertThat(index.isDeleted(13L)).isTrue();
  }

  @Test
  void doubleFlushIsNoOp() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    acc.accept(7L);
    acc.flush();
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(1);
    assertThat(index.isDeleted(7L)).isTrue();
  }

  @Test
  void escapedAccumulatorIgnoresFlush() {
    // Drive the accumulator into escape mode with a fully-alternating prefix, then flush after
    // every accept. The result must be identical to the per-position path -- no coalescing
    // across or within flushes.
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    int sniff = 256;
    for (int i = 0; i < sniff; i++) {
      acc.accept(i * 2L);
    }

    // After the sniff window is full and >30% boundaries observed, the accumulator escapes.
    // Subsequent accepts go through the per-position path, and flush should be a no-op.
    acc.accept(10_000L);
    acc.flush();
    acc.accept(10_001L);
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(sniff + 2);
    for (int i = 0; i < sniff; i++) {
      assertThat(index.isDeleted(i * 2L)).isTrue();
    }
    assertThat(index.isDeleted(10_000L)).isTrue();
    assertThat(index.isDeleted(10_001L)).isTrue();
  }

  @Test
  void acceptAllRejectsNullArray() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    assertThatThrownBy(() -> acc.acceptAll(null, 0, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid positions array");
  }

  @Test
  void acceptAllValidatesIndexes() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    long[] positions = {1L, 2L, 3L};
    assertThatThrownBy(() -> acc.acceptAll(positions, 0, 4))
        .isInstanceOf(IndexOutOfBoundsException.class)
        .hasMessageContaining("end index (4)");
    assertThatThrownBy(() -> acc.acceptAll(positions, 2, 1))
        .isInstanceOf(IndexOutOfBoundsException.class)
        .hasMessageContaining("end index (1)");
  }

  @Test
  void acceptAllEmptyRangeIsNoOp() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    acc.acceptAll(new long[] {1L, 2L, 3L}, 1, 1);
    acc.flush();
    assertThat(index.isEmpty()).isTrue();
  }

  @Test
  void acceptAllCoalescesContiguousRange() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    long[] positions = {10L, 11L, 12L, 13L, 14L};
    acc.acceptAll(positions, 0, positions.length);
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(5);
    assertThat(index.isDeleted(9L)).isFalse();
    assertThat(index.isDeleted(15L)).isFalse();
  }

  @Test
  void acceptAllHandlesMultipleDisjointRanges() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    long[] positions = {3L, 4L, 5L, 10L, 11L, 100L};
    acc.acceptAll(positions, 0, positions.length);
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(6);
    for (long p : positions) {
      assertThat(index.isDeleted(p)).isTrue();
    }
    assertThat(index.isDeleted(6L)).isFalse();
    assertThat(index.isDeleted(12L)).isFalse();
  }

  @Test
  void acceptAllHonoursFromAndTo() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    long[] positions = {1L, 2L, 100L, 101L, 999L};
    acc.acceptAll(positions, 2, 4);
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(2);
    assertThat(index.isDeleted(100L)).isTrue();
    assertThat(index.isDeleted(101L)).isTrue();
    assertThat(index.isDeleted(1L)).isFalse();
    assertThat(index.isDeleted(999L)).isFalse();
  }

  @Test
  void acceptAllChunksProduceSameResultAsSingleCall() {
    int rangeSize = 10_000;
    long[] positions = new long[rangeSize];
    for (int i = 0; i < rangeSize; i++) {
      positions[i] = i;
    }

    BitmapPositionDeleteIndex bulkIndex = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer bulk = new PositionDeleteRangeConsumer(bulkIndex);
    bulk.acceptAll(positions, 0, rangeSize);
    bulk.flush();

    BitmapPositionDeleteIndex chunkedIndex = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer chunked = new PositionDeleteRangeConsumer(chunkedIndex);
    int chunk = 137;
    int cursor = 0;
    while (cursor < rangeSize) {
      int end = Math.min(rangeSize, cursor + chunk);
      chunked.acceptAll(positions, cursor, end);
      cursor = end;
    }
    chunked.flush();

    assertThat(chunkedIndex.cardinality()).isEqualTo(bulkIndex.cardinality());
    for (long p : positions) {
      assertThat(chunkedIndex.isDeleted(p)).isTrue();
    }
  }

  @Test
  void acceptAllAndAcceptInterleaveCorrectly() {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    acc.acceptAll(new long[] {10L, 11L}, 0, 2);
    acc.accept(12L);
    acc.acceptAll(new long[] {13L, 14L, 20L}, 0, 3);
    acc.accept(21L);
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(7);
    for (long p : new long[] {10L, 11L, 12L, 13L, 14L, 20L, 21L}) {
      assertThat(index.isDeleted(p)).isTrue();
    }
  }

  @Test
  void acceptAllDispatchesToEscapedPath() {
    int sniff = 256;
    int total = 4096;
    long[] positions = new long[total];
    for (int i = 0; i < total; i++) {
      positions[i] = i * 2L;
    }

    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer acc = new PositionDeleteRangeConsumer(index);
    acc.acceptAll(positions, 0, total);
    acc.flush();

    assertThat(index.cardinality()).isEqualTo(total);
    for (int i = 0; i < total; i++) {
      assertThat(index.isDeleted(i * 2L)).isTrue();
      assertThat(index.isDeleted(i * 2L + 1)).isFalse();
    }
    // Unused: silences an unused-variable warning if sniff size assumptions ever change.
    assertThat(sniff).isLessThan(total);
  }

  @Test
  void forEachIndexEmptySourceLeavesTargetUntouched() {
    BitmapPositionDeleteIndex source = new BitmapPositionDeleteIndex();
    BitmapPositionDeleteIndex target = new BitmapPositionDeleteIndex();
    target.delete(99L);

    PositionDeleteRangeConsumer.forEach(source, target);

    assertThat(target.cardinality()).isEqualTo(1);
    assertThat(target.isDeleted(99L)).isTrue();
  }

  @Test
  void forEachIndexBitmapSourceCoalescesRuns() {
    BitmapPositionDeleteIndex source = new BitmapPositionDeleteIndex();
    for (long p = 10; p <= 14; p++) {
      source.delete(p);
    }
    source.delete(100L);
    source.delete(101L);

    BitmapPositionDeleteIndex target = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(source, target);

    assertThat(target.cardinality()).isEqualTo(source.cardinality());
    for (long p = 10; p <= 14; p++) {
      assertThat(target.isDeleted(p)).isTrue();
    }
    assertThat(target.isDeleted(100L)).isTrue();
    assertThat(target.isDeleted(101L)).isTrue();
    assertThat(target.isDeleted(9L)).isFalse();
    assertThat(target.isDeleted(15L)).isFalse();
  }

  @Test
  void forEachIndexSmallSourceShortCircuitsToPerPosition() {
    BitmapPositionDeleteIndex source = new BitmapPositionDeleteIndex();
    long[] positions = {1L, 2L, 3L, 5L};
    for (long p : positions) {
      source.delete(p);
    }

    BitmapPositionDeleteIndex target = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(source, target);

    assertThat(target.cardinality()).isEqualTo(positions.length);
    for (long p : positions) {
      assertThat(target.isDeleted(p)).isTrue();
    }
  }

  @Test
  void forEachIndexCardinalityUnknownStillCorrect() {
    BitmapPositionDeleteIndex backing = new BitmapPositionDeleteIndex();
    long[] positions = {7L, 8L, 9L, 100L};
    for (long p : positions) {
      backing.delete(p);
    }
    // Wrap so cardinality() throws -- forces the batched path even though the source is tiny.
    PositionDeleteIndex source = forwardingWithoutCardinality(backing);

    BitmapPositionDeleteIndex target = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(source, target);

    assertThat(target.cardinality()).isEqualTo(positions.length);
    for (long p : positions) {
      assertThat(target.isDeleted(p)).isTrue();
    }
  }

  @Test
  void forEachIndexNonBitmapSourceTakesFallbackPath() {
    BitmapPositionDeleteIndex backing = new BitmapPositionDeleteIndex();
    for (long p = 1; p <= 5; p++) {
      backing.delete(p);
    }
    backing.delete(42L);
    PositionDeleteIndex source = forwarding(backing);

    BitmapPositionDeleteIndex target = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(source, target);

    assertThat(target.cardinality()).isEqualTo(6);
    for (long p = 1; p <= 5; p++) {
      assertThat(target.isDeleted(p)).isTrue();
    }
    assertThat(target.isDeleted(42L)).isTrue();
  }

  @Test
  void forEachIndexExceedsBatchSize() {
    BitmapPositionDeleteIndex source = new BitmapPositionDeleteIndex();
    int total = 1_024; // > FOREACH_BATCH_SIZE = 64
    for (long p = 0; p < total; p++) {
      source.delete(p);
    }

    BitmapPositionDeleteIndex target = new BitmapPositionDeleteIndex();
    PositionDeleteRangeConsumer.forEach(source, target);

    assertThat(target.cardinality()).isEqualTo(total);
    assertThat(target.isDeleted(0L)).isTrue();
    assertThat(target.isDeleted(total - 1L)).isTrue();
    assertThat(target.isDeleted((long) total)).isFalse();
  }

  private static PositionDeleteIndex forwarding(PositionDeleteIndex backing) {
    return new PositionDeleteIndex() {
      @Override
      public void delete(long position) {
        backing.delete(position);
      }

      @Override
      public void delete(long posStart, long posEnd) {
        backing.delete(posStart, posEnd);
      }

      @Override
      public boolean isDeleted(long position) {
        return backing.isDeleted(position);
      }

      @Override
      public boolean isEmpty() {
        return backing.isEmpty();
      }

      @Override
      public void forEach(java.util.function.LongConsumer consumer) {
        backing.forEach(consumer);
      }

      @Override
      public long cardinality() {
        return backing.cardinality();
      }
    };
  }

  private static PositionDeleteIndex forwardingWithoutCardinality(PositionDeleteIndex backing) {
    return new PositionDeleteIndex() {
      @Override
      public void delete(long position) {
        backing.delete(position);
      }

      @Override
      public void delete(long posStart, long posEnd) {
        backing.delete(posStart, posEnd);
      }

      @Override
      public boolean isDeleted(long position) {
        return backing.isDeleted(position);
      }

      @Override
      public boolean isEmpty() {
        return backing.isEmpty();
      }

      @Override
      public void forEach(java.util.function.LongConsumer consumer) {
        backing.forEach(consumer);
      }
      // Intentionally does not override cardinality(); inherits the default that throws.
    };
  }

  private static List<Long> asList(long... positions) {
    return Arrays.stream(positions).boxed().collect(Collectors.toList());
  }
}
