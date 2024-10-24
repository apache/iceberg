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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

public class TestRoaringPositionBitmap {

  private static final long BITMAP_OFFSET = 0xFFFFFFFFL + 1L;
  private static final long CONTAINER_OFFSET = Character.MAX_VALUE + 1L;
  private static final Set<String> SUPPORTED_OFFICIAL_EXAMPLE_FILES =
      ImmutableSet.of("64map32bitvals.bin", "64mapempty.bin", "64mapspreadvals.bin");

  @Test
  public void testAdd() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    bitmap.add(10L);
    assertThat(bitmap.contains(10L)).isTrue();

    bitmap.add(0L);
    assertThat(bitmap.contains(0L)).isTrue();

    bitmap.add(10L);
    assertThat(bitmap.contains(10L)).isTrue();
  }

  @Test
  public void testAddPositionsRequiringMultipleBitmaps() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    // construct positions that differ in their high 32-bit parts (i.e. keys)
    long pos1 = ((long) 0 << 32) | 10L; // high = 0, low = 10
    long pos2 = ((long) 1 << 32) | 20L; // high = 1, low = 20
    long pos3 = ((long) 2 << 32) | 30L; // high = 2, low = 30
    long pos4 = ((long) 100 << 32) | 40L; // high = 1000, low = 40

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
  }

  @Test
  public void testAddRange() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    long startInclusive = 10L;
    long endExclusive = 20L;
    bitmap.addRange(startInclusive, endExclusive);

    // assert that all positions in the range [10, 20) are added
    for (long pos = startInclusive; pos < endExclusive; pos++) {
      assertThat(bitmap.contains(pos)).isTrue();
    }

    // assert that positions outside the range are not present
    assertThat(bitmap.contains(9L)).isFalse();
    assertThat(bitmap.contains(20L)).isFalse();

    // assert that the cardinality is correct (10 positions in range [10, 20))
    assertThat(bitmap.cardinality()).isEqualTo(10);
  }

  @Test
  public void testAddEmptyRange() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.addRange(10, 10);
    assertThat(bitmap.isEmpty()).isTrue();
  }

  @Test
  public void testOr() {
    RoaringPositionBitmap bitmap1 = new RoaringPositionBitmap();
    bitmap1.add(10L);
    bitmap1.add(20L);

    RoaringPositionBitmap bitmap2 = new RoaringPositionBitmap();
    bitmap2.add(30L);
    bitmap2.add(40L);

    bitmap1.or(bitmap2);

    assertThat(bitmap1.contains(10L)).isTrue();
    assertThat(bitmap1.contains(20L)).isTrue();
    assertThat(bitmap1.contains(30L)).isTrue();
    assertThat(bitmap1.contains(40L)).isTrue();

    assertThat(bitmap1.cardinality()).isEqualTo(4);
  }

  @Test
  public void testOrWithEmptyBitmap() {
    RoaringPositionBitmap bitmap1 = new RoaringPositionBitmap();
    bitmap1.add(10L);
    bitmap1.add(20L);

    RoaringPositionBitmap emptyBitmap = new RoaringPositionBitmap();

    bitmap1.or(emptyBitmap);

    assertThat(bitmap1.contains(10L)).isTrue();
    assertThat(bitmap1.contains(20L)).isTrue();

    assertThat(bitmap1.cardinality()).isEqualTo(2);
  }

  @Test
  public void testOrWithOverlappingBitmap() {
    RoaringPositionBitmap bitmap1 = new RoaringPositionBitmap();
    bitmap1.add(10L);
    bitmap1.add(20L);
    bitmap1.add(30L);

    RoaringPositionBitmap bitmap2 = new RoaringPositionBitmap();
    bitmap2.add(20L);
    bitmap2.add(40L);

    bitmap1.or(bitmap2);

    assertThat(bitmap1.contains(10L)).isTrue();
    assertThat(bitmap1.contains(20L)).isTrue();
    assertThat(bitmap1.contains(30L)).isTrue();
    assertThat(bitmap1.contains(40L)).isTrue();

    assertThat(bitmap1.cardinality()).isEqualTo(4);
  }

  @Test
  public void testOrSparseBitmaps() {
    RoaringPositionBitmap bitmap1 = new RoaringPositionBitmap();
    bitmap1.add((long) 0 << 32 | 100L); // High = 0, Low = 100
    bitmap1.add((long) 1 << 32 | 200L); // High = 1, Low = 200

    RoaringPositionBitmap bitmap2 = new RoaringPositionBitmap();
    bitmap2.add((long) 2 << 32 | 300L); // High = 2, Low = 300
    bitmap2.add((long) 3 << 32 | 400L); // High = 3, Low = 400

    bitmap1.or(bitmap2);

    assertThat(bitmap1.contains((long) 0 << 32 | 100L)).isTrue();
    assertThat(bitmap1.contains((long) 1 << 32 | 200L)).isTrue();
    assertThat(bitmap1.contains((long) 2 << 32 | 300L)).isTrue();
    assertThat(bitmap1.contains((long) 3 << 32 | 400L)).isTrue();

    assertThat(bitmap1.cardinality()).isEqualTo(4);
  }

  @Test
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

  @Test
  public void testCardinalitySparseBitmaps() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    bitmap.add((long) 0 << 32 | 100L); // high = 0, low = 100
    bitmap.add((long) 0 << 32 | 101L); // high = 0, low = 101
    bitmap.add((long) 0 << 32 | 105L); // high = 0, low = 101
    bitmap.add((long) 1 << 32 | 200L); // high = 1, low = 200
    bitmap.add((long) 100 << 32 | 300L); // high = 1000, low = 300

    assertThat(bitmap.cardinality()).isEqualTo(5);
  }

  @Test
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

  @Test
  public void testDeserializeSupportedRoaringExamples() throws IOException {
    for (String file : SUPPORTED_OFFICIAL_EXAMPLE_FILES) {
      RoaringPositionBitmap bitmap = readBitmap(file);
      assertThat(bitmap).isNotNull();
    }
  }

  @Test
  public void testDeserializeUnsupportedRoaringExample() {
    // this file contains a value that is larger than the max supported value in our impl
    assertThatThrownBy(() -> readBitmap("64maphighvals.bin"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid unsigned key");
  }

  @Test
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
}
