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
package org.apache.iceberg.util;

import java.nio.ByteBuffer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Maps a set of columns (each already converted to fixed-width, lexicographically-ordered unsigned
 * bytes by {@link ZOrderByteUtils}) onto a single byte array whose unsigned big-endian
 * lexicographic ordering follows the multi-dimensional Hilbert space-filling curve.
 *
 * <p>Unlike Z-ordering, the Hilbert transform requires every dimension to contribute the same
 * number of bits, so each column is read to a fixed {@code bitsPerColumn} precision.
 *
 * <p>The transform is the standard "axes to transposed Hilbert index" algorithm from J. Skilling,
 * "Programming the Hilbert curve" (2004); the transposed index is then serialized to a scalar with
 * {@link ZOrderByteUtils#interleaveBits}.
 */
public class HilbertByteUtils {

  private HilbertByteUtils() {}

  /** See {@link #hilbertIndex(byte[][], int, ByteBuffer)}. */
  public static byte[] hilbertIndex(byte[][] columnsBinary, int bitsPerColumn) {
    int outputBytes = columnsBinary.length * (bitsPerColumn / 8);
    return hilbertIndex(columnsBinary, bitsPerColumn, ByteBuffer.allocate(outputBytes));
  }

  /**
   * Compute the Hilbert index for the given columns.
   *
   * @param columnsBinary one ordered-byte array per column; each must be at least {@code
   *     bitsPerColumn / 8} bytes long (only the leading bytes are used)
   * @param bitsPerColumn bits taken from each column; a positive multiple of 8, no greater than 64
   * @param reuse a buffer with capacity at least {@code numColumns * bitsPerColumn / 8}
   * @return the Hilbert index, of length {@code numColumns * bitsPerColumn / 8}
   */
  public static byte[] hilbertIndex(byte[][] columnsBinary, int bitsPerColumn, ByteBuffer reuse) {
    Preconditions.checkArgument(
        bitsPerColumn > 0 && bitsPerColumn % 8 == 0,
        "Hilbert bitsPerColumn must be a positive multiple of 8, was %s",
        bitsPerColumn);
    Preconditions.checkArgument(
        bitsPerColumn <= 64,
        "Hilbert bitsPerColumn must be no greater than 64, was %s",
        bitsPerColumn);
    Preconditions.checkArgument(
        columnsBinary.length > 0, "Cannot compute a Hilbert index for zero columns");

    int numColumns = columnsBinary.length;
    int bytesPerColumn = bitsPerColumn / 8;

    long[] transpose = new long[numColumns];
    for (int i = 0; i < numColumns; i++) {
      Preconditions.checkArgument(
          columnsBinary[i].length >= bytesPerColumn,
          "Column %s contributes %s bytes but %s are required",
          i,
          columnsBinary[i].length,
          bytesPerColumn);
      transpose[i] = readBigEndian(columnsBinary[i], bytesPerColumn);
    }

    axesToTranspose(transpose, bitsPerColumn);

    byte[][] transposedBytes = new byte[numColumns][bytesPerColumn];
    for (int i = 0; i < numColumns; i++) {
      writeBigEndian(transpose[i], transposedBytes[i], bytesPerColumn);
    }

    return ZOrderByteUtils.interleaveBits(transposedBytes, numColumns * bytesPerColumn, reuse);
  }

  // please refer to the paper
  private static void axesToTranspose(long[] x, int bits) {
    int numColumns = x.length;

    for (int b = bits - 1; b > 0; b--) {
      long q = 1L << b;
      long p = q - 1;
      for (int i = 0; i < numColumns; i++) {
        if ((x[i] & q) != 0) {
          x[0] ^= p;
        } else {
          long t = (x[0] ^ x[i]) & p;
          x[0] ^= t;
          x[i] ^= t;
        }
      }
    }

    // Gray encode.
    for (int i = 1; i < numColumns; i++) {
      x[i] ^= x[i - 1];
    }
    long t = 0;
    for (int b = bits - 1; b > 0; b--) {
      long q = 1L << b;
      if ((x[numColumns - 1] & q) != 0) {
        t ^= q - 1;
      }
    }
    for (int i = 0; i < numColumns; i++) {
      x[i] ^= t;
    }
  }

  private static long readBigEndian(byte[] bytes, int len) {
    long value = 0;
    for (int i = 0; i < len; i++) {
      value = (value << 8) | (bytes[i] & 0xFF);
    }
    return value;
  }

  private static void writeBigEndian(long value, byte[] dst, int len) {
    long remaining = value;
    for (int i = len - 1; i >= 0; i--) {
      dst[i] = (byte) (remaining & 0xFF);
      remaining >>>= 8;
    }
  }
}
