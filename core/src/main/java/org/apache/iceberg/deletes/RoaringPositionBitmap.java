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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.roaringbitmap.RoaringBitmap;

/**
 * A bitmap that supports positive 64-bit positions (the most significant bit must be 0), but is
 * optimized for cases where most positions fit in 32 bits by using an array of 32-bit Roaring
 * bitmaps. The internal bitmap array is grown as needed to accommodate the largest position.
 *
 * <p>Incoming 64-bit positions are divided into a 32-bit "key" using the most significant 4 bytes
 * and a 32-bit position using the least significant 4 bytes. For each key in the set of positions,
 * a 32-bit Roaring bitmap is maintained to store a set of 32-bit positions for that key.
 *
 * <p>To test whether a certain position is set, its most significant 4 bytes (the key) are used to
 * find a 32-bit bitmap and the least significant 4 bytes are tested for inclusion in the bitmap. If
 * a bitmap is not found for the key, then the position is not set.
 *
 * <p>Positions must range from 0 (inclusive) to {@link #MAX_POSITION} (inclusive). This class
 * cannot handle positions with the key equal to Integer.MAX_VALUE because the length of the
 * internal bitmap array is a signed 32-bit integer, which must be greater than or equal to 0.
 * Supporting Integer.MAX_VALUE as a key would require allocating a bitmap array with size
 * Integer.MAX_VALUE + 1, triggering an integer overflow.
 */
class RoaringPositionBitmap {

  static final long MAX_POSITION = toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE);
  private static final RoaringBitmap[] EMPTY_BITMAP_ARRAY = new RoaringBitmap[0];
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
   * Sets a position in the bitmap.
   *
   * @param pos the position
   */
  public void set(long pos) {
    validatePosition(pos);
    int key = key(pos);
    int pos32Bits = pos32Bits(pos);
    allocateBitmapsIfNeeded(key + 1 /* required bitmap array length */);
    bitmaps[key].add(pos32Bits);
  }

  /**
   * Sets a range of positions in the bitmap.
   *
   * @param posStartInclusive the start position of the range (inclusive)
   * @param posEndExclusive the end position of the range (exclusive)
   */
  public void setRange(long posStartInclusive, long posEndExclusive) {
    for (long pos = posStartInclusive; pos < posEndExclusive; pos++) {
      set(pos);
    }
  }

  /**
   * Sets all positions from the other bitmap in this bitmap, modifying this bitmap in place.
   *
   * @param that the other bitmap
   */
  public void setAll(RoaringPositionBitmap that) {
    allocateBitmapsIfNeeded(that.bitmaps.length);
    for (int key = 0; key < that.bitmaps.length; key++) {
      bitmaps[key].or(that.bitmaps[key]);
    }
  }

  /**
   * Checks if a position is set in the bitmap.
   *
   * @param pos the position
   * @return true if the position is set in this bitmap, false otherwise
   */
  public boolean contains(long pos) {
    validatePosition(pos);
    int key = key(pos);
    int pos32Bits = pos32Bits(pos);
    return key < bitmaps.length && bitmaps[key].contains(pos32Bits);
  }

  /**
   * Indicates whether the bitmap has any positions set.
   *
   * @return true if the bitmap is empty, false otherwise
   */
  public boolean isEmpty() {
    return cardinality() == 0;
  }

  /**
   * Returns the number of set positions in the bitmap.
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
   * Applies run-length encoding wherever it is more space efficient.
   *
   * @return whether the bitmap was changed
   */
  public boolean runLengthEncode() {
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
    for (int key = 0; key < bitmaps.length; key++) {
      forEach(key, bitmaps[key], consumer);
    }
  }

  @VisibleForTesting
  int allocatedBitmapCount() {
    return bitmaps.length;
  }

  private void allocateBitmapsIfNeeded(int requiredLength) {
    if (bitmaps.length < requiredLength) {
      if (bitmaps.length == 0 && requiredLength == 1) {
        this.bitmaps = new RoaringBitmap[] {new RoaringBitmap()};
      } else {
        RoaringBitmap[] newBitmaps = new RoaringBitmap[requiredLength];
        System.arraycopy(bitmaps, 0, newBitmaps, 0, bitmaps.length);
        for (int key = bitmaps.length; key < requiredLength; key++) {
          newBitmaps[key] = new RoaringBitmap();
        }
        this.bitmaps = newBitmaps;
      }
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
   * @see <a href="https://github.com/RoaringBitmap/RoaringFormatSpec">Roaring bitmap spec</a>
   */
  public void serialize(ByteBuffer buffer) {
    validateByteOrder(buffer);
    buffer.putLong(bitmaps.length);
    for (int key = 0; key < bitmaps.length; key++) {
      buffer.putInt(key);
      bitmaps[key].serialize(buffer);
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
    int lastKey = -1;

    while (remainingBitmapCount > 0) {
      int key = readKey(buffer, lastKey);

      // fill gaps as the bitmap array may be sparse
      while (lastKey < key - 1) {
        bitmaps.add(new RoaringBitmap());
        lastKey++;
      }

      RoaringBitmap bitmap = readBitmap(buffer);
      bitmaps.add(bitmap);

      lastKey = key;
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
    Preconditions.checkArgument(key <= Integer.MAX_VALUE - 1, "Key is too large: %s", key);
    Preconditions.checkArgument(key > lastKey, "Keys must be sorted in ascending order");
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

  // extracts high 32 bits from a 64-bit position (i.e. key)
  private static int key(long pos) {
    return (int) (pos >> 32);
  }

  // extracts low 32 bits from a 64-bit position (i.e. 32-bit position)
  private static int pos32Bits(long pos) {
    return (int) pos;
  }

  // combines high and low 32 bits into a 64-bit position
  // the low 32 bits must be bit-masked to avoid sign extension
  private static long toPosition(int key, int pos32Bits) {
    return (((long) key) << 32) | (((long) pos32Bits) & 0xFFFFFFFFL);
  }

  // iterates over 64-bit positions, reconstructing them from keys and 32-bit positions
  private static void forEach(int key, RoaringBitmap bitmap, LongConsumer consumer) {
    bitmap.forEach((int pos32Bits) -> consumer.accept(toPosition(key, pos32Bits)));
  }

  private static void validatePosition(long pos) {
    Preconditions.checkArgument(
        pos >= 0 && pos <= MAX_POSITION,
        "Bitmap supports positions that are >= 0 and <= %s: %s",
        MAX_POSITION,
        pos);
  }
}
