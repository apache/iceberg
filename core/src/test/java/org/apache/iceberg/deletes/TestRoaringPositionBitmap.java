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
import java.nio.ByteOrder;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.io.Resources;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRoaringPositionBitmap {

  private static final long BITMAP_SIZE = 0xFFFFFFFFL;
  private static final long BITMAP_OFFSET = BITMAP_SIZE + 1L;
  private static final long CONTAINER_SIZE = Character.MAX_VALUE;
  private static final long CONTAINER_OFFSET = CONTAINER_SIZE + 1L;
  private static final int VALIDATION_LOOKUP_COUNT = 20_000;
  private static final Set<String> SUPPORTED_OFFICIAL_EXAMPLE_FILES =
      ImmutableSet.of("64map32bitvals.bin", "64mapempty.bin", "64mapspreadvals.bin");

  @Parameters(name = "seed = {0}, validationSeed = {1}")
  protected static List<Object> parameters() {
    List<Object> parameters = Lists.newArrayList();
    Random random = new Random();
    long seed = random.nextLong();
    long validationSeed = random.nextLong();
    parameters.add(new Object[] {seed, validationSeed});
    return parameters;
  }

  @Parameter(index = 0)
  private long seed;

  @Parameter(index = 1)
  private long validationSeed;

  @TestTemplate
  public void testAdd() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    bitmap.add(10L);
    assertThat(bitmap.contains(10L)).isTrue();

    bitmap.add(0L);
    assertThat(bitmap.contains(0L)).isTrue();

    bitmap.add(10L);
    assertThat(bitmap.contains(10L)).isTrue();
  }

  @TestTemplate
  public void testAddPositionsRequiringMultipleBitmaps() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    // construct positions that differ in their high 32-bit parts (i.e. keys)
    long pos1 = ((long) 0 << 32) | 10L; // high = 0, low = 10
    long pos2 = ((long) 1 << 32) | 20L; // high = 1, low = 20
    long pos3 = ((long) 2 << 32) | 30L; // high = 2, low = 30
    long pos4 = ((long) 100 << 32) | 40L; // high = 100, low = 40

    bitmap.add(pos1);
    bitmap.add(pos2);
    bitmap.add(pos3);
    bitmap.add(pos4);

    assertThat(bitmap.contains(pos1)).isTrue();
    assertThat(bitmap.contains(pos2)).isTrue();
    assertThat(bitmap.contains(pos3)).isTrue();
    assertThat(bitmap.contains(pos4)).isTrue();
    assertThat(bitmap.cardinality()).isEqualTo(4);
    assertThat(bitmap.serializedSizeInBytes()).isGreaterThan(4);
    assertThat(bitmap.allocatedBitmapCount()).isEqualTo(101 /* max high + 1 */);
  }

  @TestTemplate
  public void testAddRange() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    long posStartInclusive = 10L;
    long posEndExclusive = 20L;
    bitmap.addRange(posStartInclusive, posEndExclusive);

    // assert that all positions in the range [10, 20) are added
    for (long pos = posStartInclusive; pos < posEndExclusive; pos++) {
      assertThat(bitmap.contains(pos)).isTrue();
    }

    // assert that positions outside the range are not present
    assertThat(bitmap.contains(9L)).isFalse();
    assertThat(bitmap.contains(20L)).isFalse();

    // assert that the cardinality is correct (10 positions in range [10, 20))
    assertThat(bitmap.cardinality()).isEqualTo(10);
  }

  @TestTemplate
  public void testAddRangeAcrossKeys() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    long posStartInclusive = ((long) 1 << 32) - 5L;
    long posEndExclusive = ((long) 1 << 32) + 5L;
    bitmap.addRange(posStartInclusive, posEndExclusive);

    // assert that all positions in the range are added
    for (long pos = posStartInclusive; pos < posEndExclusive; pos++) {
      assertThat(bitmap.contains(pos)).isTrue();
    }

    // assert that positions outside the range are not present
    assertThat(bitmap.contains(0)).isFalse();
    assertThat(bitmap.contains(posEndExclusive)).isFalse();

    // assert that the cardinality is correct
    assertThat(bitmap.cardinality()).isEqualTo(10);
  }

  @TestTemplate
  public void testAddEmptyRange() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.addRange(10, 10);
    assertThat(bitmap.isEmpty()).isTrue();
  }

  @TestTemplate
  public void testAddAll() {
    RoaringPositionBitmap bitmap1 = new RoaringPositionBitmap();
    bitmap1.add(10L);
    bitmap1.add(20L);

    RoaringPositionBitmap bitmap2 = new RoaringPositionBitmap();
    bitmap2.add(30L);
    bitmap2.add(40L);
    bitmap2.add((long) 2 << 32);

    bitmap1.addAll(bitmap2);

    assertThat(bitmap1.contains(10L)).isTrue();
    assertThat(bitmap1.contains(20L)).isTrue();
    assertThat(bitmap1.contains(30L)).isTrue();
    assertThat(bitmap1.contains(40L)).isTrue();
    assertThat(bitmap1.contains((long) 2 << 32)).isTrue();
    assertThat(bitmap1.cardinality()).isEqualTo(5);

    assertThat(bitmap2.contains(10L)).isFalse();
    assertThat(bitmap2.contains(20L)).isFalse();
    assertThat(bitmap2.cardinality()).isEqualTo(3);
  }

  @TestTemplate
  public void testAddAllWithEmptyBitmap() {
    RoaringPositionBitmap bitmap1 = new RoaringPositionBitmap();
    bitmap1.add(10L);
    bitmap1.add(20L);

    RoaringPositionBitmap emptyBitmap = new RoaringPositionBitmap();

    bitmap1.addAll(emptyBitmap);

    assertThat(bitmap1.contains(10L)).isTrue();
    assertThat(bitmap1.contains(20L)).isTrue();
    assertThat(bitmap1.cardinality()).isEqualTo(2);

    assertThat(emptyBitmap.contains(10L)).isFalse();
    assertThat(emptyBitmap.contains(20L)).isFalse();
    assertThat(emptyBitmap.cardinality()).isEqualTo(0);
    assertThat(emptyBitmap.isEmpty()).isTrue();
  }

  @TestTemplate
  public void testAddAllWithOverlappingBitmap() {
    RoaringPositionBitmap bitmap1 = new RoaringPositionBitmap();
    bitmap1.add(10L);
    bitmap1.add(20L);
    bitmap1.add(30L);

    RoaringPositionBitmap bitmap2 = new RoaringPositionBitmap();
    bitmap2.add(20L);
    bitmap2.add(40L);

    bitmap1.addAll(bitmap2);

    assertThat(bitmap1.contains(10L)).isTrue();
    assertThat(bitmap1.contains(20L)).isTrue();
    assertThat(bitmap1.contains(30L)).isTrue();
    assertThat(bitmap1.contains(40L)).isTrue();
    assertThat(bitmap1.cardinality()).isEqualTo(4);

    assertThat(bitmap2.contains(10L)).isFalse();
    assertThat(bitmap2.contains(20L)).isTrue();
    assertThat(bitmap2.contains(30L)).isFalse();
    assertThat(bitmap2.contains(40L)).isTrue();
    assertThat(bitmap2.cardinality()).isEqualTo(2);
  }

  @TestTemplate
  public void testAddAllSparseBitmaps() {
    RoaringPositionBitmap bitmap1 = new RoaringPositionBitmap();
    bitmap1.add((long) 0 << 32 | 100L); // High = 0, Low = 100
    bitmap1.add((long) 1 << 32 | 200L); // High = 1, Low = 200

    RoaringPositionBitmap bitmap2 = new RoaringPositionBitmap();
    bitmap2.add((long) 2 << 32 | 300L); // High = 2, Low = 300
    bitmap2.add((long) 3 << 32 | 400L); // High = 3, Low = 400

    bitmap1.addAll(bitmap2);

    assertThat(bitmap1.contains((long) 0 << 32 | 100L)).isTrue();
    assertThat(bitmap1.contains((long) 1 << 32 | 200L)).isTrue();
    assertThat(bitmap1.contains((long) 2 << 32 | 300L)).isTrue();
    assertThat(bitmap1.contains((long) 3 << 32 | 400L)).isTrue();
    assertThat(bitmap1.cardinality()).isEqualTo(4);
  }

  @TestTemplate
  public void testCardinality() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    assertThat(bitmap.cardinality()).isEqualTo(0);

    bitmap.add(10L);
    bitmap.add(20L);
    bitmap.add(30L);

    assertThat(bitmap.cardinality()).isEqualTo(3);

    bitmap.add(10L); // already exists

    assertThat(bitmap.cardinality()).isEqualTo(3);
  }

  @TestTemplate
  public void testCardinalitySparseBitmaps() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    bitmap.add((long) 0 << 32 | 100L); // high = 0, low = 100
    bitmap.add((long) 0 << 32 | 101L); // high = 0, low = 101
    bitmap.add((long) 0 << 32 | 105L); // high = 0, low = 101
    bitmap.add((long) 1 << 32 | 200L); // high = 1, low = 200
    bitmap.add((long) 100 << 32 | 300L); // high = 100, low = 300

    assertThat(bitmap.cardinality()).isEqualTo(5);
  }

  @TestTemplate
  public void testSerializeDeserializeAllContainerBitmap() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    // bitmap 0, container 0 (array)
    bitmap.add(position(0 /* bitmap */, 0 /* container */, 5L));
    bitmap.add(position(0 /* bitmap */, 0 /* container */, 7L));

    // bitmap 0, container 1 (array that can be compressed)
    bitmap.addRange(
        position(0 /* bitmap */, 1 /* container */, 1L),
        position(0 /* bitmap */, 1 /* container */, 1000L));

    // bitmap 1, container 2 (bitset)
    bitmap.addRange(
        position(0 /* bitmap */, 2 /* container */, 1L),
        position(0 /* bitmap */, 2 /* container */, CONTAINER_OFFSET - 1L));

    // bitmap 1, container 0 (array)
    bitmap.add(position(1 /* bitmap */, 0 /* container */, 10L));
    bitmap.add(position(1 /* bitmap */, 0 /* container */, 20L));

    // bitmap 1, container 1 (array that can be compressed)
    bitmap.addRange(
        position(1 /* bitmap */, 1 /* container */, 10L),
        position(1 /* bitmap */, 1 /* container */, 500L));

    // bitmap 1, container 2 (bitset)
    bitmap.addRange(
        position(1 /* bitmap */, 2 /* container */, 1L),
        position(1 /* bitmap */, 2 /* container */, CONTAINER_OFFSET - 1));

    assertThat(bitmap.runOptimize()).as("Bitmap must be RLE encoded").isTrue();

    RoaringPositionBitmap bitmapCopy = roundTripSerialize(bitmap);

    assertThat(bitmapCopy.cardinality()).isEqualTo(bitmap.cardinality());
    bitmapCopy.forEach(position -> assertThat(bitmap.contains(position)).isTrue());
    bitmap.forEach(position -> assertThat(bitmapCopy.contains(position)).isTrue());
  }

  @TestTemplate
  public void testDeserializeSupportedRoaringExamples() throws IOException {
    for (String file : SUPPORTED_OFFICIAL_EXAMPLE_FILES) {
      RoaringPositionBitmap bitmap = readBitmap(file);
      assertThat(bitmap).isNotNull();
    }
  }

  @TestTemplate
  public void testDeserializeUnsupportedRoaringExample() {
    // this file contains a value that is larger than the max supported value in our impl
    assertThatThrownBy(() -> readBitmap("64maphighvals.bin"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid unsigned key");
  }

  @TestTemplate
  public void testUnsupportedPositions() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    assertThatThrownBy(() -> bitmap.add(-1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Bitmap supports positions that are >= 0 and <= %s",
            RoaringPositionBitmap.MAX_POSITION);

    assertThatThrownBy(() -> bitmap.contains(-1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Bitmap supports positions that are >= 0 and <= %s",
            RoaringPositionBitmap.MAX_POSITION);

    assertThatThrownBy(() -> bitmap.add(RoaringPositionBitmap.MAX_POSITION + 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Bitmap supports positions that are >= 0 and <= %s",
            RoaringPositionBitmap.MAX_POSITION);

    assertThatThrownBy(() -> bitmap.contains(RoaringPositionBitmap.MAX_POSITION + 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Bitmap supports positions that are >= 0 and <= %s",
            RoaringPositionBitmap.MAX_POSITION);
  }

  @TestTemplate
  public void testRandomSparseBitmap() {
    Pair<RoaringPositionBitmap, Set<Long>> bitmapAndPositions =
        generateSparseBitmap(
            0L /* min position */,
            (long) 5 << 32 /* max position must not need more than 5 bitmaps */,
            100_000 /* cardinality */);
    RoaringPositionBitmap bitmap = bitmapAndPositions.first();
    Set<Long> positions = bitmapAndPositions.second();
    assertEqual(bitmap, positions);
    assertRandomPositions(bitmap, positions);
  }

  @TestTemplate
  public void testRandomDenseBitmap() {
    Pair<RoaringPositionBitmap, Set<Long>> bitmapAndPositions = generateDenseBitmap(7);
    RoaringPositionBitmap bitmap = bitmapAndPositions.first();
    Set<Long> positions = bitmapAndPositions.second();
    assertEqual(bitmap, positions);
    assertRandomPositions(bitmap, positions);
  }

  @TestTemplate
  public void testRandomMixedBitmap() {
    Pair<RoaringPositionBitmap, Set<Long>> bitmapAndPositions =
        generateSparseBitmap(
            (long) 3 << 32 /* min position must need at least 3 bitmaps */,
            (long) 5 << 32 /* max position must not need more than 5 bitmaps */,
            100_000 /* cardinality */);
    RoaringPositionBitmap bitmap = bitmapAndPositions.first();
    Set<Long> positions = bitmapAndPositions.second();

    Pair<RoaringPositionBitmap, Set<Long>> pair1 = generateDenseBitmap(9);
    bitmap.addAll(pair1.first());
    positions.addAll(pair1.second());

    Pair<RoaringPositionBitmap, Set<Long>> pair2 =
        generateSparseBitmap(
            0 /* min position */,
            (long) 3 << 32 /* max position must not need more than 3 bitmaps */,
            25_000 /* cardinality */);
    bitmap.addAll(pair2.first());
    positions.addAll(pair2.second());

    Pair<RoaringPositionBitmap, Set<Long>> pair3 = generateDenseBitmap(3);
    bitmap.addAll(pair3.first());
    positions.addAll(pair3.second());

    Pair<RoaringPositionBitmap, Set<Long>> pair4 =
        generateSparseBitmap(
            0 /* min position */,
            (long) 1 << 32 /* max position must not need more than 1 bitmap */,
            5_000 /* cardinality */);
    bitmap.addAll(pair4.first());
    positions.addAll(pair4.second());

    assertEqual(bitmap, positions);
    assertRandomPositions(bitmap, positions);
  }

  private Pair<RoaringPositionBitmap, Set<Long>> generateSparseBitmap(
      long minInclusive, long maxExclusive, int size) {
    Random random = new Random(seed);
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    Set<Long> positions = Sets.newHashSet();

    while (positions.size() < size) {
      long position = nextLong(random, minInclusive, maxExclusive);
      positions.add(position);
      bitmap.add(position);
    }

    return Pair.of(bitmap, positions);
  }

  private Pair<RoaringPositionBitmap, Set<Long>> generateDenseBitmap(int requiredBitmapCount) {
    Random random = new Random(seed);
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    Set<Long> positions = Sets.newHashSet();
    long currentPosition = 0;

    while (bitmap.allocatedBitmapCount() <= requiredBitmapCount) {
      long maxRunPosition = currentPosition + nextLong(random, 1000, 2 * CONTAINER_SIZE);
      for (long position = currentPosition; position <= maxRunPosition; position++) {
        bitmap.add(position);
        positions.add(position);
      }
      long shift = nextLong(random, (long) (0.1 * BITMAP_SIZE), (long) (0.25 * BITMAP_SIZE));
      currentPosition = maxRunPosition + shift;
    }

    return Pair.of(bitmap, positions);
  }

  private void assertRandomPositions(RoaringPositionBitmap bitmap, Set<Long> positions) {
    Random random = new Random(validationSeed);
    for (int ordinal = 0; ordinal < VALIDATION_LOOKUP_COUNT; ordinal++) {
      long position = nextLong(random, 0, RoaringPositionBitmap.MAX_POSITION);
      assertThat(bitmap.contains(position)).isEqualTo(positions.contains(position));
    }
  }

  private static long nextLong(Random random, long minInclusive, long maxExclusive) {
    return minInclusive + (long) (random.nextDouble() * (maxExclusive - minInclusive));
  }

  private static long position(int bitmapIndex, int containerIndex, long value) {
    return bitmapIndex * BITMAP_OFFSET + containerIndex * CONTAINER_OFFSET + value;
  }

  private static RoaringPositionBitmap roundTripSerialize(RoaringPositionBitmap bitmap) {
    ByteBuffer buffer = ByteBuffer.allocate((int) bitmap.serializedSizeInBytes());
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    bitmap.serialize(buffer);
    buffer.flip();
    return RoaringPositionBitmap.deserialize(buffer);
  }

  private static RoaringPositionBitmap readBitmap(String resourceName) throws IOException {
    byte[] bytes = readTestResource(resourceName);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    return RoaringPositionBitmap.deserialize(buffer);
  }

  private static byte[] readTestResource(String resourceName) throws IOException {
    URL resource = Resources.getResource(TestRoaringPositionBitmap.class, resourceName);
    return Resources.toByteArray(resource);
  }

  private static void assertEqual(RoaringPositionBitmap bitmap, Set<Long> positions) {
    assertThat(bitmap.cardinality()).isEqualTo(positions.size());
    positions.forEach(position -> assertThat(bitmap.contains(position)).isTrue());
    bitmap.forEach(position -> assertThat(positions.contains(position)).isTrue());

    RoaringPositionBitmap bitmapCopy1 = roundTripSerialize(bitmap);
    assertThat(bitmapCopy1.cardinality()).isEqualTo(positions.size());
    positions.forEach(position -> assertThat(bitmapCopy1.contains(position)).isTrue());
    bitmapCopy1.forEach(position -> assertThat(positions.contains(position)).isTrue());

    bitmap.runOptimize();

    RoaringPositionBitmap bitmapCopy2 = roundTripSerialize(bitmap);
    assertThat(bitmapCopy2.cardinality()).isEqualTo(positions.size());
    positions.forEach(position -> assertThat(bitmapCopy2.contains(position)).isTrue());
    bitmapCopy2.forEach(position -> assertThat(positions.contains(position)).isTrue());
  }
}
