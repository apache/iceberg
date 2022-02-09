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
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Within Z-Ordering the byte representations of objects being compared must be ordered,
 * this requires several types to be transformed when converted to bytes. The goal is to
 * map object's whose byte representation are not lexicographically ordered into representations
 * that are lexicographically ordered. Bytes produced should be compared lexicographically as
 * unsigned bytes, big-endian.
 * <p>
 * Most of these techniques are derived from
 * https://aws.amazon.com/blogs/database/z-order-indexing-for-multifaceted-queries-in-amazon-dynamodb-part-2/
 * <p>
 * Some implementation is taken from
 * https://github.com/apache/hbase/blob/master/hbase-common/src/main/java/org/apache/hadoop/hbase/util/OrderedBytes.java
 */
public class ZOrderByteUtils {

  private ZOrderByteUtils() {

  }

  /**
   * Signed ints do not have their bytes in magnitude order because of the sign bit.
   * To fix this, flip the sign bit so that all negatives are ordered before positives. This essentially
   * shifts the 0 value so that we don't break our ordering when we cross the new 0 value.
   */
  public static ByteBuffer intToOrderedBytes(int val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, Integer.BYTES);
    bytes.putInt(val ^ 0x80000000);
    return bytes;
  }

  /**
   * Signed longs are treated the same as the signed ints in {@link #intToOrderedBytes(int, ByteBuffer)}
   */
  public static ByteBuffer longToOrderedBytes(long val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, Long.BYTES);
    bytes.putLong(val ^ 0x8000000000000000L);
    return bytes;
  }

  /**
   * Signed shorts are treated the same as the signed ints in {@link #intToOrderedBytes(int, ByteBuffer)}
   */
  public static ByteBuffer shortToOrderedBytes(short val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, Short.BYTES);
    bytes.putShort((short) (val ^ (0x8000)));
    return bytes;
  }

  /**
   * Signed tiny ints are treated the same as the signed ints in {@link #intToOrderedBytes(int, ByteBuffer)}
   */
  public static ByteBuffer tinyintToOrderedBytes(byte val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, Byte.BYTES);
    bytes.put((byte) (val ^ (0x80)));
    return bytes;
  }

  /**
   * IEEE 754 :
   * “If two floating-point numbers in the same format are ordered (say, x {@literal <} y),
   * they are ordered the same way when their bits are reinterpreted as sign-magnitude integers.”
   *
   * Which means floats can be treated as sign magnitude integers which can then be converted into lexicographically
   * comparable bytes
   */
  public static ByteBuffer floatToOrderedBytes(float val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, Float.BYTES);
    int ival = Float.floatToIntBits(val);
    ival ^= ((ival >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);
    bytes.putInt(ival);
    return bytes;
  }

  /**
   * Doubles are treated the same as floats in {@link #floatToOrderedBytes(float, ByteBuffer)}
   */
  public static ByteBuffer doubleToOrderedBytes(double val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, Double.BYTES);
    long lng = Double.doubleToLongBits(val);
    lng ^= ((lng >> (Long.SIZE - 1)) | Long.MIN_VALUE);
    bytes.putLong(lng);
    return bytes;
  }

  /**
   * Strings are lexicographically sortable BUT if different byte array lengths will
   * ruin the Z-Ordering. (ZOrder requires that a given column contribute the same number of bytes every time).
   * This implementation just uses a set size to for all output byte representations. Truncating longer strings
   * and right padding 0 for shorter strings.
   */
  public static ByteBuffer stringToOrderedBytes(String val, int length, ByteBuffer reuse, CharsetEncoder encoder) {
    Preconditions.checkArgument(encoder.charset().equals(StandardCharsets.UTF_8),
        "Cannot use an encoder not using UTF_8 as it's Charset");

    ByteBuffer bytes = ByteBuffers.reuse(reuse, length);
    Arrays.fill(bytes.array(), 0, length, (byte) 0x00);
    if (val != null) {
      CharBuffer inputBuffer = CharBuffer.wrap(val);
      encoder.encode(inputBuffer, bytes, true);
    }
    return bytes;
  }

  /**
   * For Testing interleave all available bytes
   */
  static byte[] interleaveBits(byte[][] columnsBinary) {
    return interleaveBits(columnsBinary,
        Arrays.stream(columnsBinary).mapToInt(column -> column.length).sum());
  }

  /**
   * Interleave bits using a naive loop. Variable length inputs are allowed but to get a consistent ordering it is
   * required that every column contribute the same number of bytes in each invocation. Bits are interleaved from all
   * columns that have a bit available at that position. Once a Column has no more bits to produce it is skipped in the
   * interleaving.
   * @param columnsBinary an array of ordered byte representations of the columns being ZOrdered
   * @param interleavedSize the number of bytes to use in the output
   * @return the columnbytes interleaved
   */
  public static byte[] interleaveBits(byte[][] columnsBinary, int interleavedSize) {
    byte[] interleavedBytes = new byte[interleavedSize];
    int sourceColumn = 0;
    int sourceByte = 0;
    int sourceBit = 7;
    int interleaveByte = 0;
    int interleaveBit = 7;

    while (interleaveByte < interleavedSize) {
      // Take the source bit from source byte and move it to the output bit position
      interleavedBytes[interleaveByte] |=
              (columnsBinary[sourceColumn][sourceByte] & 1 << sourceBit) >>> sourceBit << interleaveBit;
      --interleaveBit;

      // Check if an output byte has been completed
      if (interleaveBit == -1) {
        // Move to the next output byte
        interleaveByte++;
        // Move to the highest order bit of the new output byte
        interleaveBit = 7;
      }

      // Check if the last output byte has been completed
      if (interleaveByte == interleavedSize) {
        break;
      }

      // Find the next source bit to interleave
      do {
        // Move to next column
        ++sourceColumn;
        if (sourceColumn == columnsBinary.length) {
          // If the last source column was used, reset to next bit of first column
          sourceColumn = 0;
          --sourceBit;
          if (sourceBit == -1) {
            // If the last bit of the source byte was used, reset to the highest bit of the next byte
            sourceByte++;
            sourceBit = 7;
          }
        }
      } while (columnsBinary[sourceColumn].length <= sourceByte);
    }
    return interleavedBytes;
  }
}
