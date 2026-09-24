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

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.io.Resources;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBitmapPositionDeleteIndex {

  private static final long BITMAP_OFFSET = 0xFFFFFFFFL + 1L;
  private static final long CONTAINER_OFFSET = Character.MAX_VALUE + 1L;
  private static final long SEED = 20260818L;

  @Test
  public void testForEach() {
    long pos1 = 10L; // Container 0 (high bits = 0)
    long pos2 = 1L << 33; // Container 1 (high bits = 1)
    long pos3 = pos2 + 1; // Container 1 (high bits = 1)
    long pos4 = 2L << 33; // Container 2 (high bits = 2)
    long pos5 = pos4 + 1; // Container 2 (high bits = 2)
    long pos6 = 3L << 33; // Container 3 (high bits = 3)

    PositionDeleteIndex index = new BitmapPositionDeleteIndex();

    // add in any order
    index.delete(pos1);
    index.delete(pos6);
    index.delete(pos2);
    index.delete(pos3);
    index.delete(pos5);
    index.delete(pos4);

    // output must be sorted in ascending order across containers
    List<Long> positions = collect(index);
    assertThat(positions).containsExactly(pos1, pos2, pos3, pos4, pos5, pos6);
  }

  @Test
  public void testForEachEmptyBitmapIndex() {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    List<Long> positions = collect(index);
    assertThat(positions).isEmpty();
  }

  @Test
  public void testForEachEmptyIndex() {
    PositionDeleteIndex index = PositionDeleteIndex.empty();
    List<Long> positions = collect(index);
    assertThat(positions).isEmpty();
  }

  @Test
  public void testForEachInRange() {
    PositionDeleteIndex index = indexOf(10L, 11L, 12L, 13L);

    // the beginning is inclusive and the end is exclusive
    assertThat(positionsInRange(index, 11L, 13L)).containsExactly(11L, 12L);
    assertThat(positionsInRange(index, 11L, 12L)).containsExactly(11L);

    // a range that ends where the deletes start
    assertThat(positionsInRange(index, 0L, 10L)).isEmpty();

    // a range that starts after the deletes end
    assertThat(positionsInRange(index, 14L, 24L)).isEmpty();
  }

  @Test
  public void testForEachInRangeAscendingOrder() {
    PositionDeleteIndex index = indexOf(9L, 3L, 7L, 1L, 5L);
    assertThat(positionsInRange(index, 0L, 20L)).containsExactly(1L, 3L, 5L, 7L, 9L);
  }

  @Test
  public void testForEachInRangeEmptyRange() {
    PositionDeleteIndex index = indexOf(0L, 1L, 2L);
    assertThat(positionsInRange(index, 0L, 0L)).isEmpty();
  }

  @Test
  public void testForEachInRangeEmptyBitmapIndex() {
    assertThat(positionsInRange(new BitmapPositionDeleteIndex(), 0L, 1000L)).isEmpty();
  }

  @Test
  public void testForEachInRangeEmptyIndex() {
    assertThat(positionsInRange(PositionDeleteIndex.empty(), 0L, 1000L)).isEmpty();
    assertThat(positionsInRange(PositionDeleteIndex.empty(), 12345L, 13345L)).isEmpty();
  }

  @Test
  public void testForEachInRangeInvalidRange() {
    // each implementation validates the range on its own, so check all of them
    assertThatThrownBy(() -> positionsInRange(indexOf(1L, 2L, 3L), 5L, 3L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Start position must not exceed end position");

    assertThatThrownBy(() -> positionsInRange(PositionDeleteIndex.empty(), 5L, 3L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Start position must not exceed end position");

    assertThatThrownBy(
            () ->
                positionsInRange(
                    new SetBackedPositionDeleteIndex(Sets.newHashSet(1L, 2L, 3L)), 5L, 3L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Start position must not exceed end position");
  }

  @Test
  public void testForEachInRangeAcrossBitmaps() {
    long lastPosInFirstBitmap = BITMAP_OFFSET - 1;
    PositionDeleteIndex index =
        indexOf(lastPosInFirstBitmap - 1, lastPosInFirstBitmap, BITMAP_OFFSET, BITMAP_OFFSET + 1);

    assertThat(positionsInRange(index, lastPosInFirstBitmap - 2, BITMAP_OFFSET + 3))
        .containsExactly(
            lastPosInFirstBitmap - 1, lastPosInFirstBitmap, BITMAP_OFFSET, BITMAP_OFFSET + 1);
  }

  @Test
  public void testForEachInRangeAcrossContainers() {
    PositionDeleteIndex index =
        indexOf(CONTAINER_OFFSET - 2, CONTAINER_OFFSET - 1, CONTAINER_OFFSET, CONTAINER_OFFSET + 1);

    assertThat(positionsInRange(index, CONTAINER_OFFSET - 3, CONTAINER_OFFSET + 3))
        .containsExactly(
            CONTAINER_OFFSET - 2, CONTAINER_OFFSET - 1, CONTAINER_OFFSET, CONTAINER_OFFSET + 1);
  }

  @Test
  public void testForEachInRangeMatchesIsDeleted() {
    Random random = new Random(SEED);
    Set<Long> positions = Sets.newHashSet();
    for (int index = 0; index < 20000; index++) {
      positions.add((long) random.nextInt(300000));
    }

    PositionDeleteIndex bitmapIndex = new BitmapPositionDeleteIndex();
    positions.forEach(bitmapIndex::delete);

    // an index that implements only isDeleted, so the default traversal is used
    PositionDeleteIndex defaultIndex = new SetBackedPositionDeleteIndex(positions);

    for (int index = 0; index < 500; index++) {
      long posStart = random.nextInt(300000);
      long posEnd = posStart + 1 + random.nextInt(6000);
      List<Long> expected = deletedPositionsInRange(positions, posStart, posEnd);
      assertThat(positionsInRange(bitmapIndex, posStart, posEnd))
          .as("bitmap at %s", posStart)
          .isEqualTo(expected);
      assertThat(positionsInRange(defaultIndex, posStart, posEnd))
          .as("default at %s", posStart)
          .isEqualTo(expected);
    }
  }

  @Test
  public void testForEachInRangeMatchesIsDeletedAcrossSignedIntBoundary() {
    // a 32-bit position at or above Integer.MAX_VALUE does not fit in a signed int, and neither
    // does the end of a range that reaches it, so sweep every range around the boundary
    long boundary = Integer.MAX_VALUE + 1L;
    Set<Long> positions = Sets.newHashSet();

    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    index.delete(boundary - 4, boundary + 4);
    for (long pos = boundary - 4; pos < boundary + 4; pos++) {
      positions.add(pos);
    }

    index.serialize(); // triggers runLengthEncode

    for (long posStart = boundary - 12; posStart <= boundary + 6; posStart++) {
      for (long posEnd = posStart; posEnd <= posStart + 20; posEnd++) {
        assertThat(positionsInRange(index, posStart, posEnd))
            .as("range [%s, %s)", posStart, posEnd)
            .isEqualTo(deletedPositionsInRange(positions, posStart, posEnd));
      }
    }
  }

  @Test
  public void testMergeBitmapIndexWithNonEmpty() {
    long pos1 = 10L; // Container 0 (high bits = 0)
    long pos2 = 1L << 33; // Container 1 (high bits = 1)
    long pos3 = pos2 + 1; // Container 1 (high bits = 1)
    long pos4 = 2L << 33; // Container 2 (high bits = 2)

    BitmapPositionDeleteIndex index1 = new BitmapPositionDeleteIndex();
    index1.delete(pos2);
    index1.delete(pos1);

    BitmapPositionDeleteIndex index2 = new BitmapPositionDeleteIndex();
    index2.delete(pos4);
    index2.delete(pos3);

    index1.merge(index2);

    // output must be sorted in ascending order across containers
    List<Long> positions = collect(index1);
    assertThat(positions).containsExactly(pos1, pos2, pos3, pos4);
  }

  @Test
  public void testMergeBitmapIndexWithEmpty() {
    long pos1 = 10L; // Container 0 (high bits = 0)
    long pos2 = 1L << 33; // Container 1 (high bits = 1)
    long pos3 = pos2 + 1; // Container 1 (high bits = 1)
    long pos4 = 2L << 33; // Container 2 (high bits = 2)

    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex();
    index.delete(pos2);
    index.delete(pos1);
    index.delete(pos3);
    index.delete(pos4);
    index.merge(PositionDeleteIndex.empty());

    // output must be sorted in ascending order across containers
    List<Long> positions = collect(index);
    assertThat(positions).containsExactly(pos1, pos2, pos3, pos4);
  }

  @Test
  public void testEmptyIndexSerialization() throws Exception {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    validate(index, "empty-position-index.bin");
  }

  @Test
  public void testSmallAlternatingValuesIndexSerialization() throws Exception {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    index.delete(1L);
    index.delete(3L);
    index.delete(5L);
    index.delete(7L);
    index.delete(9L);
    validate(index, "small-alternating-values-position-index.bin");
  }

  @Test
  public void testSmallAndLargeValuesIndexSerialization() throws Exception {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    index.delete(100L);
    index.delete(101L);
    index.delete(Integer.MAX_VALUE + 100L);
    index.delete(Integer.MAX_VALUE + 101L);
    validate(index, "small-and-large-values-position-index.bin");
  }

  @Test
  public void testAllContainerTypesIndexSerialization() throws Exception {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();

    // bitmap 0, container 0 (array)
    index.delete(position(0 /* bitmap */, 0 /* container */, 5L));
    index.delete(position(0 /* bitmap */, 0 /* container */, 7L));

    // bitmap 0, container 1 (array that can be compressed)
    index.delete(
        position(0 /* bitmap */, 1 /* container */, 1L),
        position(0 /* bitmap */, 1 /* container */, 1000L));

    // bitmap 1, container 2 (bitset)
    index.delete(
        position(0 /* bitmap */, 2 /* container */, 1L),
        position(0 /* bitmap */, 2 /* container */, CONTAINER_OFFSET - 1L));

    // bitmap 1, container 0 (array)
    index.delete(position(1 /* bitmap */, 0 /* container */, 10L));
    index.delete(position(1 /* bitmap */, 0 /* container */, 20L));

    // bitmap 1, container 1 (array that can be compressed)
    index.delete(
        position(1 /* bitmap */, 1 /* container */, 10L),
        position(1 /* bitmap */, 1 /* container */, 500L));

    // bitmap 1, container 2 (bitset)
    index.delete(
        position(1 /* bitmap */, 2 /* container */, 1L),
        position(1 /* bitmap */, 2 /* container */, CONTAINER_OFFSET - 1));

    validate(index, "all-container-types-position-index.bin");
  }

  @Test
  public void testDeserializeInvalidCrc() {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    index.delete(1L);
    index.delete(2L);
    byte[] bytes = index.serialize().array();

    // corrupt the last CRC byte so the computed checksum no longer matches
    bytes[bytes.length - 1] ^= 0x01;

    DeleteFile dv = mockDV(bytes.length, index.cardinality());
    Mockito.when(dv.location()).thenReturn("s3://bucket/dv.puffin");

    assertThatThrownBy(() -> PositionDeleteIndex.deserialize(bytes, dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid CRC for deletion vector s3://bucket/dv.puffin: 0x712fa6e8, expected 0x712fa6e9");
  }

  private static void validate(PositionDeleteIndex index, String goldenFile) throws Exception {
    ByteBuffer buffer = index.serialize();
    byte[] bytes = buffer.array();
    DeleteFile dv = mockDV(bytes.length, index.cardinality());
    PositionDeleteIndex indexCopy = PositionDeleteIndex.deserialize(bytes, dv);
    assertEqual(index, indexCopy);
    byte[] goldenBytes = readTestResource(goldenFile);
    assertThat(bytes).isEqualTo(goldenBytes);
    PositionDeleteIndex goldenIndex = PositionDeleteIndex.deserialize(goldenBytes, dv);
    assertEqual(index, goldenIndex);
  }

  private static DeleteFile mockDV(long contentSize, long cardinality) {
    DeleteFile mock = Mockito.mock(DeleteFile.class);
    Mockito.when(mock.contentSizeInBytes()).thenReturn(contentSize);
    Mockito.when(mock.recordCount()).thenReturn(cardinality);
    return mock;
  }

  private static void assertEqual(PositionDeleteIndex index, PositionDeleteIndex thatIndex) {
    assertThat(index.cardinality()).isEqualTo(thatIndex.cardinality());
    index.forEach(position -> assertThat(thatIndex.isDeleted(position)).isTrue());
    thatIndex.forEach(position -> assertThat(index.isDeleted(position)).isTrue());
  }

  private static long position(int bitmapIndex, int containerIndex, long value) {
    return bitmapIndex * BITMAP_OFFSET + containerIndex * CONTAINER_OFFSET + value;
  }

  private static byte[] readTestResource(String resourceName) throws IOException {
    URL resource = Resources.getResource(TestRoaringPositionBitmap.class, resourceName);
    return Resources.toByteArray(resource);
  }

  private List<Long> collect(PositionDeleteIndex index) {
    List<Long> positions = Lists.newArrayList();
    index.forEach(positions::add);
    return positions;
  }

  private static List<Long> positionsInRange(
      PositionDeleteIndex index, long posStartInclusive, long posEndExclusive) {
    List<Long> positions = Lists.newArrayList();
    index.forEachInRange(posStartInclusive, posEndExclusive, positions::add);
    return positions;
  }

  private static List<Long> deletedPositionsInRange(
      Set<Long> positions, long posStart, long posEnd) {
    List<Long> deleted = Lists.newArrayList();
    for (long pos = posStart; pos < posEnd; pos++) {
      if (positions.contains(pos)) {
        deleted.add(pos);
      }
    }
    return deleted;
  }

  private static PositionDeleteIndex indexOf(long... positions) {
    PositionDeleteIndex index = new BitmapPositionDeleteIndex();
    for (long position : positions) {
      index.delete(position);
    }
    return index;
  }

  /** An index that implements only the required methods, so the default traversal is used. */
  private static class SetBackedPositionDeleteIndex implements PositionDeleteIndex {
    private final Set<Long> positions;

    SetBackedPositionDeleteIndex(Set<Long> positions) {
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
