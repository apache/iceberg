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
 * Within Z-Ordering the byte representations of objects being compared must be ordered, this
 * requires several types to be transformed when converted to bytes. The goal is to map object's
 * whose byte representation are not lexicographically ordered into representations that are
 * lexicographically ordered. Bytes produced should be compared lexicographically as unsigned bytes,
 * big-endian.
 *
 * <p>All types except for String are stored within an 8 Byte Buffer
 *
 * <p>Most of these techniques are derived from
 * https://aws.amazon.com/blogs/database/z-order-indexing-for-multifaceted-queries-in-amazon-dynamodb-part-2/
 *
 * <p>Some implementation is taken from
 * https://github.com/apache/hbase/blob/master/hbase-common/src/main/java/org/apache/hadoop/hbase/util/OrderedBytes.java
 */
public class ZOrderByteUtils {

  public static final int PRIMITIVE_BUFFER_SIZE = 8;

  /**
   * The most columns the table based interleaving supports. Interleaving N columns spreads the 8
   * bits of a source byte over 8 * N bits, which has to fit in the long holding one output group.
   */
  private static final int MAX_TABLE_COLUMNS = Long.SIZE / Byte.SIZE;

  /**
   * SPREAD[n][b] holds the bits of the byte b spread n positions apart, the highest order bit of b
   * first, so that OR-ing the spread bytes of n columns interleaves them. Only the low order n
   * bytes are used.
   */
  private static final long[][] SPREAD = buildSpreadTables();

  private ZOrderByteUtils() {}

  private static long[][] buildSpreadTables() {
    long[][] tables = new long[MAX_TABLE_COLUMNS + 1][];
    for (int numColumns = 1; numColumns <= MAX_TABLE_COLUMNS; numColumns += 1) {
      long[] table = new long[1 << Byte.SIZE];
      for (int value = 0; value < table.length; value += 1) {
        long spread = 0L;
        for (int bit = 0; bit < Byte.SIZE; bit += 1) {
          if ((value & (1 << (Byte.SIZE - 1 - bit))) != 0) {
            spread |= 1L << (Byte.SIZE * numColumns - 1 - bit * numColumns);
          }
        }
        table[value] = spread;
      }
      tables[numColumns] = table;
    }

    return tables;
  }

  static ByteBuffer allocatePrimitiveBuffer() {
    return ByteBuffer.allocate(PRIMITIVE_BUFFER_SIZE);
  }

  /** Internally just calls {@link #wholeNumberOrderedBytes(long, ByteBuffer)} */
  public static ByteBuffer intToOrderedBytes(int val, ByteBuffer reuse) {
    return wholeNumberOrderedBytes(val, reuse);
  }

  /** Internally just calls {@link #wholeNumberOrderedBytes(long, ByteBuffer)} */
  public static ByteBuffer longToOrderedBytes(long val, ByteBuffer reuse) {
    return wholeNumberOrderedBytes(val, reuse);
  }

  /** Internally just calls {@link #wholeNumberOrderedBytes(long, ByteBuffer)} */
  public static ByteBuffer shortToOrderedBytes(short val, ByteBuffer reuse) {
    return wholeNumberOrderedBytes(val, reuse);
  }

  /** Internally just calls {@link #wholeNumberOrderedBytes(long, ByteBuffer)} */
  public static ByteBuffer tinyintToOrderedBytes(byte val, ByteBuffer reuse) {
    return wholeNumberOrderedBytes(val, reuse);
  }

  /**
   * Signed longs do not have their bytes in magnitude order because of the sign bit. To fix this,
   * flip the sign bit so that all negatives are ordered before positives. This essentially shifts
   * the 0 value so that we don't break our ordering when we cross the new 0 value.
   */
  public static ByteBuffer wholeNumberOrderedBytes(long val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
    bytes.putLong(val ^ 0x8000000000000000L);
    return bytes;
  }

  /** Internally just calls {@link #floatingPointOrderedBytes(double, ByteBuffer)} */
  public static ByteBuffer floatToOrderedBytes(float val, ByteBuffer reuse) {
    return floatingPointOrderedBytes(val, reuse);
  }

  /** Internally just calls {@link #floatingPointOrderedBytes(double, ByteBuffer)} */
  public static ByteBuffer doubleToOrderedBytes(double val, ByteBuffer reuse) {
    return floatingPointOrderedBytes(val, reuse);
  }

  /**
   * IEEE 754 : “If two floating-point numbers in the same format are ordered (say, x {@literal <}
   * y), they are ordered the same way when their bits are reinterpreted as sign-magnitude
   * integers.”
   *
   * <p>Which means doubles can be treated as sign magnitude integers which can then be converted
   * into lexicographically comparable bytes
   */
  public static ByteBuffer floatingPointOrderedBytes(double val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
    long lval = Double.doubleToLongBits(val);
    lval ^= ((lval >> (Long.SIZE - 1)) | Long.MIN_VALUE);
    bytes.putLong(lval);
    return bytes;
  }

  /**
   * Strings are lexicographically sortable BUT if different byte array lengths will ruin the
   * Z-Ordering. (ZOrder requires that a given column contribute the same number of bytes every
   * time). This implementation just uses a set size to for all output byte representations.
   * Truncating longer strings and right padding 0 for shorter strings.
   */
  @SuppressWarnings("ByteBufferBackingArray")
  public static ByteBuffer stringToOrderedBytes(
      String val, int length, ByteBuffer reuse, CharsetEncoder encoder) {
    Preconditions.checkArgument(
        encoder.charset().equals(StandardCharsets.UTF_8),
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
   * Return a bytebuffer with the given bytes truncated to length, or filled with 0's to length
   * depending on whether the given bytes are larger or smaller than the given length.
   */
  @SuppressWarnings("ByteBufferBackingArray")
  public static ByteBuffer byteTruncateOrFill(byte[] val, int length, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, length);
    if (val == null) {
      Arrays.fill(bytes.array(), 0, length, (byte) 0x00);
      return bytes;
    }

    if (val.length < length) {
      bytes.put(val, 0, val.length);
      Arrays.fill(bytes.array(), val.length, length, (byte) 0x00);
    } else {
      bytes.put(val, 0, length);
    }
    return bytes;
  }

  static byte[] interleaveBits(byte[][] columnsBinary, int interleavedSize) {
    return interleaveBits(columnsBinary, interleavedSize, ByteBuffer.allocate(interleavedSize));
  }

  /**
   * Interleave bits from the given columns. Variable length inputs are allowed but to get a
   * consistent ordering it is required that every column contribute the same number of bytes in
   * each invocation. Bits are interleaved from all columns that have a bit available at that
   * position. Once a Column has no more bits to produce it is skipped in the interleaving.
   *
   * <p>When every column contributes the same number of bytes, which is what all callers of this
   * method produce, the interleaving is a fixed permutation of the source bits and is computed a
   * byte at a time through {@link #SPREAD}. Any other input falls back to interleaving one bit at a
   * time.
   *
   * @param columnsBinary an array of ordered byte representations of the columns being ZOrdered
   * @param interleavedSize the number of bytes to use in the output
   * @return the columnbytes interleaved
   */
  @SuppressWarnings("ByteBufferBackingArray")
  public static byte[] interleaveBits(
      byte[][] columnsBinary, int interleavedSize, ByteBuffer reuse) {
    byte[] interleavedBytes = reuse.array();
    int uniformColumnLength = uniformColumnLength(columnsBinary);
    if (uniformColumnLength > 0 && interleavedSize <= uniformColumnLength * columnsBinary.length) {
      return interleaveUniformColumns(columnsBinary, interleavedSize, interleavedBytes);
    }

    return interleaveBitwise(columnsBinary, interleavedSize, interleavedBytes);
  }

  /**
   * Returns the shared length of the given columns if the table based interleaving applies to them,
   * and 0 otherwise. It applies when there is at least one column and at most {@link
   * #MAX_TABLE_COLUMNS} of them, and all of them are of the same non-zero length.
   */
  private static int uniformColumnLength(byte[][] columnsBinary) {
    if (columnsBinary.length < 1 || columnsBinary.length > MAX_TABLE_COLUMNS) {
      return 0;
    }

    int columnLength = columnsBinary[0].length;
    for (int column = 1; column < columnsBinary.length; column += 1) {
      if (columnsBinary[column].length != columnLength) {
        return 0;
      }
    }

    return columnLength;
  }

  /**
   * Interleave columns that all contribute the same number of bytes.
   *
   * <p>With N columns, the N bytes of output starting at offset {@code index * N} are produced
   * entirely from byte {@code index} of each column: bit {@code bit} of that source byte, counted
   * from the most significant one, lands at bit {@code bit * N + column} of that output group. That
   * permutation depends only on the source byte and on N, so it is read from {@link #SPREAD} rather
   * than being applied a bit at a time. Shifting the spread bits right by the column index puts
   * each column at its offset within the group.
   *
   * <p>Every output byte is written, so unlike {@link #interleaveBitwise} this does not need to
   * zero the output first. An interleavedSize smaller than the full interleaving truncates the last
   * group: no column is exhausted before another one here, so the leading bytes of the interleaving
   * do not depend on where it is cut off.
   */
  private static byte[] interleaveUniformColumns(
      byte[][] columnsBinary, int interleavedSize, byte[] interleavedBytes) {
    int numColumns = columnsBinary.length;
    long[] spread = SPREAD[numColumns];
    int completeGroups = interleavedSize / numColumns;

    int interleaveByte = 0;
    for (int sourceByte = 0; sourceByte < completeGroups; sourceByte += 1) {
      long group = interleaveGroup(columnsBinary, spread, sourceByte);
      for (int groupByte = numColumns - 1; groupByte >= 0; groupByte -= 1) {
        interleavedBytes[interleaveByte] = (byte) (group >>> (Byte.SIZE * groupByte));
        interleaveByte += 1;
      }
    }

    // The output can end in the middle of a group, in which case only its leading bytes are kept
    if (interleaveByte < interleavedSize) {
      long group = interleaveGroup(columnsBinary, spread, completeGroups);
      for (int groupByte = numColumns - 1; interleaveByte < interleavedSize; groupByte -= 1) {
        interleavedBytes[interleaveByte] = (byte) (group >>> (Byte.SIZE * groupByte));
        interleaveByte += 1;
      }
    }

    return interleavedBytes;
  }

  /**
   * Interleaves the given byte of every column into a single group of {@code columnsBinary.length}
   * bytes, held in the low order bytes of the returned long.
   */
  private static long interleaveGroup(byte[][] columnsBinary, long[] spread, int sourceByte) {
    long group = 0L;
    for (int column = 0; column < columnsBinary.length; column += 1) {
      group |= spread[columnsBinary[column][sourceByte] & 0xFF] >>> column;
    }

    return group;
  }

  /**
   * Interleave bits using a naive loop, one output bit at a time. This handles columns of differing
   * lengths, where a column that has run out of bytes is skipped by the interleaving.
   */
  // NarrowingCompoundAssignment is intended here. See
  // https://github.com/apache/iceberg/pull/5200#issuecomment-1176226163
  @SuppressWarnings("NarrowingCompoundAssignment")
  private static byte[] interleaveBitwise(
      byte[][] columnsBinary, int interleavedSize, byte[] interleavedBytes) {
    Arrays.fill(interleavedBytes, 0, interleavedSize, (byte) 0x00);

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
            // If the last bit of the source byte was used, reset to the highest bit of the next
            // byte
            sourceByte++;
            sourceBit = 7;
          }
        }
      } while (columnsBinary[sourceColumn].length <= sourceByte);
    }
    return interleavedBytes;
  }
}
