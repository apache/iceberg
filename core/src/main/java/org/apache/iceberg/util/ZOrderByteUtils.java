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
import java.util.Arrays;

/**
 * Within Z-Ordering the byte representations of objects being compared must be ordered,
 * this requires several types to be transformed when converted to bytes. The goal is to
 * map object's whose byte representation are not lexicographically ordered into representations
 * that are lexicographically ordered.
 * Most of these techniques are derived from
 * https://aws.amazon.com/blogs/database/z-order-indexing-for-multifaceted-queries-in-amazon-dynamodb-part-2/
 *
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
  public static byte[] intToOrderedBytes(int val) {
    ByteBuffer bytes = ByteBuffer.allocate(Integer.BYTES);
    bytes.putInt(val ^ 0x80000000);
    return bytes.array();
  }

  /**
   * Signed longs are treated the same as the signed ints
   */
  public static byte[] longToOrderBytes(long val) {
    ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES);
    bytes.putLong(val ^ 0x8000000000000000L);
    return bytes.array();
  }

  /**
   * Signed shorts are treated the same as the signed ints
   */
  public static byte[] shortToOrderBytes(short val) {
    ByteBuffer bytes = ByteBuffer.allocate(Short.BYTES);
    bytes.putShort((short) (val ^ (0x8000)));
    return bytes.array();
  }

  /**
   * Signed tiny ints are treated the same as the signed ints
   */
  public static byte[] tinyintToOrderedBytes(byte val) {
    ByteBuffer bytes = ByteBuffer.allocate(Byte.BYTES);
    bytes.put((byte) (val ^ (0x80)));
    return bytes.array();
  }

  /**
   * IEEE 754 :
   * “If two floating-point numbers in the same format are ordered (say, x {@literal <} y),
   * they are ordered the same way when their bits are reinterpreted as sign-magnitude integers.”
   *
   * Which means floats can be treated as sign magnitude integers which can then be converted into lexicographically
   * comparable bytes
   */
  public static byte[] floatToOrderedBytes(float val) {
    ByteBuffer bytes = ByteBuffer.allocate(Integer.BYTES);
    int ival = Float.floatToIntBits(val);
    ival ^= ((ival >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);
    bytes.putInt(ival);
    return bytes.array();
  }

  /**
   * Doubles are treated the same as floats
   */
  public static byte[] doubleToOrderedBytes(double val) {
    ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES);
    long lng = Double.doubleToLongBits(val);
    lng ^= ((lng >> (Long.SIZE - 1)) | Long.MIN_VALUE);
    bytes.putLong(lng);
    return bytes.array();
  }

  /**
   * Strings are lexicographically sortable BUT if different byte array lengths will
   * ruin the Z-Ordering. (ZOrder requires that a given column contribute the same number of bytes every time).
   * This implementation just uses a set size to for all output byte representations. Truncating longer strings
   * and right padding 0 for shorter strings.
   */
  public static byte[] stringToOrderedBytes(String val, int length) {
    ByteBuffer bytes = ByteBuffer.allocate(length);
    if (val != null) {
      int maxLength = Math.min(length, val.length());
      bytes.put(val.getBytes(), 0, maxLength);
    }
    return bytes.array();
  }

  /**
   * Interleave bits using a naive loop.
   * @param columnsBinary an array of byte arrays, none of which are empty
   * @return their bits interleaved
   */
  public static byte[] interleaveBits(byte[][] columnsBinary) {
    int interleavedSize = Arrays.stream(columnsBinary).mapToInt(a -> a.length).sum();
    byte[] interleavedBytes = new byte[interleavedSize];
    int sourceBit = 7;
    int sourceByte = 0;
    int sourceColumn = 0;
    int interleaveBit = 7;
    int interleaveByte = 0;
    while (interleaveByte < interleavedSize) {
      // Take what we have, Get the source Bit of the source Byte, move it to the interleaveBit position
      interleavedBytes[interleaveByte] =
          (byte) (interleavedBytes[interleaveByte] |
              (columnsBinary[sourceColumn][sourceByte] & 1 << sourceBit) >> sourceBit << interleaveBit);

      --interleaveBit;
      if (interleaveBit == -1) {
        // Finished a byte in our interleave byte array start a new byte
        interleaveByte++;
        interleaveBit = 7;
      }

      // Find next column with a byte we can use
      do {
        ++sourceColumn;
        if (sourceColumn == columnsBinary.length) {
          sourceColumn = 0;
          if (--sourceBit == -1) {
            sourceByte++;
            sourceBit = 7;
          }
        }
      } while (columnsBinary[sourceColumn].length <= sourceByte && interleaveByte < interleavedSize);
    }
    return interleavedBytes;
  }
}
