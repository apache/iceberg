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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.function.LongConsumer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.roaringbitmap.RoaringBitmap;

/**
 * A bitmap that supports positive 64-bit positions, but is optimized for cases where most positions
 * fit in 32 bits by using an array of 32-bit Roaring bitmaps. The bitmap array is grown as needed
 * to accommodate the largest value.
 *
 * <p>Incoming 64-bit positions are divided into a 32-bit "key" using the most significant 4 bytes
 * and a 32-bit position using the least significant 4 bytes. For each key in the set of positions,
 * a 32-bit Roaring bitmap is maintained to store a set of 32-bit positions for that key.
 *
 * <p>To test whether a certain position is set, its most significant 4 bytes (the key) are used to
 * find a 32-bit bitmap and the least significant 4 bytes are tested for inclusion in the bitmap. If
 * a bitmap is not found for the key, then the position is not set.
 *
 * <p>Note that positions must be greater than or equal to 0 add less than {@link #MAX_POSITION}.
 * This bitmap does not support positions larger than {@link #MAX_POSITION} as the bitmap array
 * length is a signed 32-bit integer that must be greater than or equal to 0.
 */
class RoaringPositionBitmap {

  static final long MAX_POSITION = toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE);
  private static final RoaringBitmap[] EMPTY_BITMAP_ARRAY = new RoaringBitmap[] {};
  private static final long BITMAP_COUNT_SIZE_BYTES = 8L;
  private static final long BITMAP_KEY_SIZE_BYTES = 4L;

  private RoaringBitmap[] bitmaps;

  RoaringPositionBitmap() {
    this.bitmaps = EMPTY_BITMAP_ARRAY;
  }

  private RoaringPositionBitmap(RoaringBitmap[] bitmaps) {
    this.bitmaps = bitmaps;
  }

  /**
   * Adds a position to the bitmap.
   *
   * @param pos the row position
   */
  public void add(long pos) {
    validatePosition(pos);
    int high = highBytes(pos);
    int low = lowBytes(pos);
    allocateBitmapsIfNeeded(high + 1 /* required number of bitmaps */);
    bitmaps[high].add(low);
  }

  /**
   * Adds a range of positions to the bitmap.
   *
   * @param posStartInclusive the start position of the range (inclusive)
   * @param posEndExclusive the end position of the range (exclusive)
   */
  public void addRange(long posStartInclusive, long posEndExclusive) {
    for (long pos = posStartInclusive; pos < posEndExclusive; pos++) {
      add(pos);
    }
  }

  /**
   * Checks if the bitmap contains a position.
   *
   * @param pos the row position
   * @return true if the position is in this bitmap, false otherwise
   */
  public boolean contains(long pos) {
    validatePosition(pos);
    int high = highBytes(pos);
    int low = lowBytes(pos);
    return high < bitmaps.length && bitmaps[high].contains(low);
  }

  /**
   * Adds all positions from the other bitmap to this bitmap, modifying this bitmap in place.
   *
   * @param that the other bitmap
   */
  public void or(RoaringPositionBitmap that) {
    allocateBitmapsIfNeeded(that.bitmaps.length);
    for (int index = 0; index < that.bitmaps.length; index++) {
      bitmaps[index].or(that.bitmaps[index]);
    }
  }

  /**
   * Indicates whether the bitmap is empty.
   *
   * @return true if the bitmap is empty, false otherwise
   */
  public boolean isEmpty() {
    return cardinality() == 0;
  }

  /**
   * Returns the number of positions in the bitmap.
   *
   * @return the number of set positions
   */
  public long cardinality() {
    long cardinality = 0L;
    for (RoaringBitmap bitmap : bitmaps) {
      cardinality += bitmap.getLongCardinality();
    }
    return cardinality;
  }

  /**
   * Run-length compresses the bitmap if it is more space efficient.
   *
   * @return whether the bitmap was compressed
   */
  public boolean runOptimize() {
    boolean changed = false;
    for (RoaringBitmap bitmap : bitmaps) {
      changed |= bitmap.runOptimize();
    }
    return changed;
  }

  /**
   * Iterates over all positions in the bitmap.
   *
   * @param consumer a consumer for positions
   */
  public void forEach(LongConsumer consumer) {
    for (int index = 0; index < bitmaps.length; index++) {
      forEach(bitmaps[index], index, consumer);
    }
  }

  /**
   * Returns the number of bytes required to serialize the bitmap.
   *
   * @return the serialized size in bytes
   */
  public long serializedSizeInBytes() {
    long size = BITMAP_COUNT_SIZE_BYTES;
    for (RoaringBitmap bitmap : bitmaps) {
      size += BITMAP_KEY_SIZE_BYTES + bitmap.serializedSizeInBytes();
    }
    return size;
  }

  /**
   * Serializes the bitmap using the portable serialization format described below.
   *
   * <ul>
   *   <li>The number of 32-bit Roaring bitmaps, serialized as 8 bytes
   *   <li>For each 32-bit Roaring bitmap, ordered by unsigned comparison of the 32-bit keys:
   *       <ul>
   *         <li>The key stored as 4 bytes
   *         <li>Serialized 32-bit Roaring bitmap using the standard format
   *       </ul>
   * </ul>
   *
   * <p>Note the byte order of the buffer must be little-endian.
   *
   * @param buffer the buffer to write to
   * @see <a href="https://github.com/RoaringBitmap/RoaringFormatSpe">Roaring bitmap spec</a>
   */
  public void serialize(ByteBuffer buffer) {
    validateByteOrder(buffer);
    buffer.putLong(bitmaps.length);
    for (int index = 0; index < bitmaps.length; index++) {
      buffer.putInt(index);
      bitmaps[index].serialize(buffer);
    }
  }

  private void allocateBitmapsIfNeeded(int requiredLength) {
    if (bitmaps.length < requiredLength) {
      if (bitmaps.length == 0 && requiredLength == 1) {
        this.bitmaps = new RoaringBitmap[] {new RoaringBitmap()};
      } else {
        RoaringBitmap[] newBitmaps = new RoaringBitmap[requiredLength];
        System.arraycopy(bitmaps, 0, newBitmaps, 0, bitmaps.length);
        for (int index = bitmaps.length; index < requiredLength; index++) {
          newBitmaps[index] = new RoaringBitmap();
        }
        this.bitmaps = newBitmaps;
      }
    }
  }

  /**
   * Deserializes a bitmap from a buffer, assuming the portable serialization format.
   *
   * @param buffer the buffer to read from
   * @return a new bitmap instance with the deserialized data
   */
  public static RoaringPositionBitmap deserialize(ByteBuffer buffer) {
    validateByteOrder(buffer);

    // the bitmap array may be sparse with more elements than the number of read bitmaps
    int remainingBitmapCount = readBitmapCount(buffer);
    List<RoaringBitmap> bitmaps = Lists.newArrayListWithExpectedSize(remainingBitmapCount);
    int lastKey = 0;

    while (remainingBitmapCount > 0) {
      int key = readKey(buffer, lastKey);

      // fill gaps as the bitmap array may be sparse
      while (lastKey < key) {
        bitmaps.add(new RoaringBitmap());
        lastKey++;
      }

      RoaringBitmap bitmap = readBitmap(buffer);
      bitmaps.add(bitmap);

      lastKey++;
      remainingBitmapCount--;
    }

    return new RoaringPositionBitmap(bitmaps.toArray(EMPTY_BITMAP_ARRAY));
  }

  private static void validateByteOrder(ByteBuffer buffer) {
    Preconditions.checkArgument(
        buffer.order() == ByteOrder.LITTLE_ENDIAN,
        "Roaring bitmap serialization requires little-endian byte order");
  }

  private static int readBitmapCount(ByteBuffer buffer) {
    long bitmapCount = buffer.getLong();
    Preconditions.checkArgument(
        bitmapCount >= 0 && bitmapCount <= Integer.MAX_VALUE,
        "Invalid bitmap count: %s",
        bitmapCount);
    return (int) bitmapCount;
  }

  private static int readKey(ByteBuffer buffer, int lastKey) {
    int key = buffer.getInt();
    Preconditions.checkArgument(key >= 0, "Invalid unsigned key: %s", key);
    Preconditions.checkArgument(key >= lastKey, "Keys must be sorted in ascending order");
    return key;
  }

  private static RoaringBitmap readBitmap(ByteBuffer buffer) {
    try {
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.deserialize(buffer);
      buffer.position(buffer.position() + bitmap.serializedSizeInBytes());
      return bitmap;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // extracts high 32 bits from a 64-bit position
  private static int highBytes(long pos) {
    return (int) (pos >> 32);
  }

  // extracts low 32 bits from a 64-bit position
  private static int lowBytes(long pos) {
    return (int) pos;
  }

  // combines high and low 32-bit values into a 64-bit position
  // the low value must be bit-masked to avoid sign extension
  private static long toPosition(int high, int low) {
    return (((long) high) << 32) | (((long) low) & 0xFFFFFFFFL);
  }

  // iterates over 64-bit positions, reconstructing them from high and low 32-bit values
  private static void forEach(RoaringBitmap bitmap, int high, LongConsumer consumer) {
    bitmap.forEach((int low) -> consumer.accept(toPosition(high, low)));
  }

  private static void validatePosition(long pos) {
    Preconditions.checkArgument(
        pos >= 0 && pos <= MAX_POSITION,
        "Bitmap supports positions that are >= 0 and <= %s: %s",
        MAX_POSITION,
        pos);
  }
}
