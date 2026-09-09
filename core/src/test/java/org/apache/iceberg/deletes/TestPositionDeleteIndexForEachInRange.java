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

import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link PositionDeleteIndex#forEachInRange(long, int,
 * java.util.function.LongConsumer)} returns exactly the positions that {@link
 * PositionDeleteIndex#isDeleted(long)} reports as deleted in the same range, in ascending order,
 * for every implementation.
 */
public class TestPositionDeleteIndexForEachInRange {

  private static final long CHUNK_SIZE = 65536L;
  private static final long BITMAP_SIZE = 0xFFFFFFFFL + 1L;

  @Test
  public void testEmptyIndex() {
    assertThat(collect(PositionDeleteIndex.empty(), 0L, 1000)).isEmpty();
    assertThat(collect(PositionDeleteIndex.empty(), 12345L, 1000)).isEmpty();
  }

  @Test
  public void testEmptyBitmapIndex() {
    assertThat(collect(new BitmapPositionDeleteIndex(), 0L, 1000)).isEmpty();
  }

  @Test
  public void testZeroLength() {
    PositionDeleteIndex index = index(0L, 1L, 2L);
    assertThat(collect(index, 0L, 0)).isEmpty();
  }

  @Test
  public void testNegativeLength() {
    PositionDeleteIndex index = index(0L, 1L, 2L);
    assertThat(collect(index, 0L, -5)).isEmpty();
  }

  @Test
  public void testRangeBoundariesAreInclusiveExclusive() {
    PositionDeleteIndex index = index(10L, 11L, 12L, 13L);

    // [11, 13) must return 11 and 12 only
    assertThat(collect(index, 11L, 2)).containsExactly(11L, 12L);
    // the position right before the range must not be returned
    assertThat(collect(index, 11L, 1)).containsExactly(11L);
    // a range that ends exactly where the deletes start
    assertThat(collect(index, 0L, 10)).isEmpty();
    // a range that starts exactly where the deletes end
    assertThat(collect(index, 14L, 10)).isEmpty();
  }

  @Test
  public void testAscendingOrder() {
    PositionDeleteIndex index = index(9L, 3L, 7L, 1L, 5L);
    assertThat(collect(index, 0L, 20)).containsExactly(1L, 3L, 5L, 7L, 9L);
  }

  @Test
  public void testRangeSpanningRoaringChunks() {
    // Roaring splits a 32-bit bitmap into 65536-position chunks; a batch may straddle one
    PositionDeleteIndex index = index(CHUNK_SIZE - 2, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1);
    assertThat(collect(index, CHUNK_SIZE - 3, 6))
        .containsExactly(CHUNK_SIZE - 2, CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1);
  }

  @Test
  public void testRangeSpanningBitmapKeys() {
    // positions are split into a 32-bit key and a 32-bit position; a range may straddle the key
    long lastInFirstBitmap = BITMAP_SIZE - 1;
    PositionDeleteIndex index =
        index(lastInFirstBitmap - 1, lastInFirstBitmap, BITMAP_SIZE, BITMAP_SIZE + 1);

    assertThat(collect(index, lastInFirstBitmap - 2, 5))
        .containsExactly(lastInFirstBitmap - 1, lastInFirstBitmap, BITMAP_SIZE, BITMAP_SIZE + 1);
  }

  @Test
  public void testRangeBeyondAllocatedBitmaps() {
    PositionDeleteIndex index = index(1L, 2L, 3L);
    // no bitmap is allocated for this key, the range must simply produce nothing
    assertThat(collect(index, BITMAP_SIZE * 3, 1000)).isEmpty();
  }

  @Test
  public void testRunLengthEncodedIndex() {
    // deletes stored as run containers must behave identically
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    index.delete(1000L, 40000L);
    index.serialize(); // triggers runLengthEncode

    List<Long> actual = collect(index, 999L, 5);
    assertThat(actual).containsExactly(1000L, 1001L, 1002L, 1003L);
    assertThat(collect(index, 39998L, 5)).containsExactly(39998L, 39999L);
  }

  @Test
  public void testMatchesIsDeletedOnRandomData() {
    Random random = new Random(20260818L);
    Set<Long> positions = Sets.newHashSet();
    for (int i = 0; i < 20000; i++) {
      positions.add((long) random.nextInt(300000));
    }

    BitmapPositionDeleteIndex bitmapIndex = new BitmapPositionDeleteIndex();
    positions.forEach(bitmapIndex::delete);

    // an index that only implements isDeleted, exercising the default implementation
    PositionDeleteIndex defaultIndex = new SetBackedIndex(positions);

    for (int i = 0; i < 500; i++) {
      long start = random.nextInt(300000);
      int length = 1 + random.nextInt(6000);
      List<Long> expected = referenceScan(positions, start, length);
      assertThat(collect(bitmapIndex, start, length)).as("bitmap at %s", start).isEqualTo(expected);
      assertThat(collect(defaultIndex, start, length))
          .as("default at %s", start)
          .isEqualTo(expected);
    }
  }

  @Test
  public void testMatchesIsDeletedAfterRunLengthEncoding() {
    Random random = new Random(20260818L);
    Set<Long> positions = Sets.newHashSet();
    // long runs so that run containers are chosen
    for (int block = 0; block < 200; block++) {
      long start = random.nextInt(300000);
      for (long pos = start; pos < start + 500; pos++) {
        positions.add(pos);
      }
    }

    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    positions.forEach(index::delete);
    index.serialize(); // triggers runLengthEncode

    for (int i = 0; i < 500; i++) {
      long start = random.nextInt(320000);
      int length = 1 + random.nextInt(6000);
      assertThat(collect(index, start, length))
          .as("run container at %s", start)
          .isEqualTo(referenceScan(positions, start, length));
    }
  }

  private static List<Long> referenceScan(Set<Long> positions, long start, int length) {
    List<Long> expected = Lists.newArrayList();
    for (int index = 0; index < length; index++) {
      long pos = start + index;
      if (positions.contains(pos)) {
        expected.add(pos);
      }
    }
    return expected;
  }

  private static List<Long> collect(PositionDeleteIndex index, long posStart, int length) {
    List<Long> collected = Lists.newArrayList();
    index.forEachInRange(posStart, length, collected::add);
    return collected;
  }

  private static PositionDeleteIndex index(long... positions) {
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    for (long position : positions) {
      index.delete(position);
    }
    return index;
  }

  /** An index that implements only the required methods, so the default traversal is used. */
  private static class SetBackedIndex implements PositionDeleteIndex {
    private final Set<Long> positions;

    SetBackedIndex(Set<Long> positions) {
      this.positions = positions;
    }

    @Override
    public void delete(long position) {
      positions.add(position);
    }

    @Override
    public void delete(long posStart, long posEnd) {
      for (long pos = posStart; pos < posEnd; pos++) {
        positions.add(pos);
      }
    }

    @Override
    public boolean isDeleted(long position) {
      return positions.contains(position);
    }

    @Override
    public boolean isEmpty() {
      return positions.isEmpty();
    }
  }
}
