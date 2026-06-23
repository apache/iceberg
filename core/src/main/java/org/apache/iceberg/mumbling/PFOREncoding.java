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
package org.apache.iceberg.mumbling;

import java.nio.ByteBuffer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;

/**
 * Patched Frame of Reference (PFOR) encoding for arrays of unsigned byte values.
 *
 * <p>Implements the encoding described in Appendix A of the Mumbling bitmap specification. The
 * input array is split into 256-value chunks (the last chunk may be shorter). Each chunk is
 * independently encoded using 4 configuration values:
 *
 * <ul>
 *   <li>{@code b1}: number of bits stored in the primary array for every normalized value
 *   <li>{@code b2}: number of bits stored per exception value (normalized value of &gt; b1 bits)
 *   <li>{@code e}: number of exceptions with more than b1 bits
 *   <li>{@code m}: chunk-local minimum value, subtracted from all values to normalize
 * </ul>
 *
 * <p>Each chunk is stored as:
 *
 * <ul>
 *   <li>3-byte header: {@code b1|b2} primary and exception bit widths (byte 0), {@code e} exception
 *       count (byte 1), {@code m} normalization base value (byte 2)
 *   <li>Primary array: the low {@code b1} bits of every normalized value, packed MSB-first ({@code
 *       b1 * n} bits, padded to a byte)
 *   <li>Exception offsets: chunk-relative positions of exception values ({@code e} bytes)
 *   <li>Exception values: the high {@code b2} bits of every exception value, packed MSB-first
 *       ({@code e * b2} bits, padded to a byte.
 * </ul>
 */
class PFOREncoding {
  private static final int CHUNK_SIZE = 256;

  private PFOREncoding() {}

  /**
   * Encodes {@code count} values from an array of unsigned byte values.
   *
   * @param values unsigned byte values to encode
   * @param count number of values to encode
   * @return a {@link ByteBuffer} of the encoded values with position and limit set for reading
   */
  static ByteBuffer encode(int[] values, int count) {
    ByteBuffer out = ByteBuffer.allocate(estimateEncodedSize(count));
    int bytesWritten = encode(values, 0, out, 0, count);
    return out.slice(0, bytesWritten);
  }

  /**
   * Encode {@code count} unsigned byte values from {@code values} into a buffer.
   *
   * <p>The buffer's position and limit are not modified.
   *
   * @param values unsigned byte values to encode
   * @param valueOffset starting offset of values to encode
   * @param out buffer to write encoded values to
   * @param outOffset starting offset in the output buffer
   * @param count number of values to encode
   * @return the number of bytes written to the buffer
   */
  static int encode(int[] values, int valueOffset, ByteBuffer out, int outOffset, int count) {
    // outOffset is relative to the buffer's position; check the encoded data fits
    Preconditions.checkArgument(outOffset >= 0, "Cannot encode at negative offset: %s", outOffset);
    Preconditions.checkArgument(
        estimateEncodedSize(count) <= out.remaining() - outOffset,
        "Cannot encode %s values to buffer with %s remaining space",
        count,
        out.remaining() - outOffset);

    int bytesWritten = 0;
    int currentOffset = valueOffset;

    while (currentOffset < count) {
      int chunkLength = Math.min(CHUNK_SIZE, count - currentOffset);
      bytesWritten +=
          encodeChunk(values, currentOffset, out, outOffset + bytesWritten, chunkLength);
      currentOffset += chunkLength;
    }

    return bytesWritten;
  }

  /**
   * Decode to produce unsigned byte values.
   *
   * <p>Decodes starting at {@code encoded.position()} and does not modify the input buffer.
   *
   * @param encoded PFOR-encoded ByteBuffer produced by {@link #encode}
   * @param count total number of values to decode
   * @return decoded unsigned byte values
   */
  static int[] decode(ByteBuffer encoded, int count) {
    int[] out = new int[count];
    decode(encoded, 0, out, 0, count);
    return out;
  }

  /**
   * Decode {@code count} unsigned bytes from a buffer into {@code out}.
   *
   * <p>This does not modify the input buffer.
   *
   * @param encoded a buffer containing encoded data
   * @param offset starting offset of encoded values
   * @param out an output value array
   * @param outOffset starting offset in the output array
   * @param count number of values to decode
   * @return the number of bytes read from the encoded buffer
   */
  static int decode(ByteBuffer encoded, int offset, int[] out, int outOffset, int count) {
    Preconditions.checkArgument(offset >= 0, "Cannot decode at negative offset: %s", offset);

    int bytesRead = 0;
    int valuesRead = 0;

    while (valuesRead < count) {
      int chunkSize = Math.min(CHUNK_SIZE, count - valuesRead);
      bytesRead += decodeChunk(encoded, offset + bytesRead, out, outOffset + valuesRead, chunkSize);
      valuesRead += chunkSize;
    }

    return bytesRead;
  }

  /**
   * Encode one chunk into {@code out} starting at absolute position {@code outPos}.
   *
   * @param values array containing source values to encode
   * @param valueOffset starting index of values to encode
   * @param out an output {@link ByteBuffer}
   * @param outOffset starting index for output in the out buffer
   * @param count number of values to encode
   * @return the number of bytes written to the output buffer
   */
  private static int encodeChunk(
      int[] values, int valueOffset, ByteBuffer out, int outOffset, int count) {
    Preconditions.checkArgument(count >= 0, "Invalid value count to encode: %s", count);
    Preconditions.checkArgument(
        valueOffset + count <= values.length,
        "Cannot encode %s values starting at %s from int[%s]: not enough values",
        count,
        valueOffset,
        values.length);

    // find base=min(values) for normalization
    int base = min(values, valueOffset, count);

    // normalize by subtracting base
    int[] normalized = new int[count];
    int setBits = 0;
    int normalizedSetBits = 0;
    for (int i = 0; i < count; i += 1) {
      setBits |= values[valueOffset + i];
      normalized[i] = values[valueOffset + i] - base;
      normalizedSetBits |= normalized[i];
    }

    Preconditions.checkArgument(
        width(setBits) <= 8,
        "Cannot encode values wider than 8 bits: %s bits needed",
        width(setBits));

    // Choose b1 to minimize total encoded data size (excluding 3-byte header)
    int maxWidth = width(normalizedSetBits);
    Pair<Integer, Integer> widthAndExcCount = chooseBitWidth(normalized, count, maxWidth);
    int b1 = widthAndExcCount.first();
    int b2 = maxWidth - b1;
    int excCount = widthAndExcCount.second();

    // check that there is enough space in the buffer for the encoded data
    int requiredSize = encodedSize(count, b1, b2, excCount);
    Preconditions.checkArgument(
        outOffset + requiredSize <= out.remaining(),
        "Cannot decode %s values from buffer with %s remaining bytes",
        requiredSize,
        out.remaining());

    // Special case: b1=8 means store original values as raw bytes with b2, e, and m set to 0.
    if (b1 == 8) {
      writeHeader(out, outOffset, b1, 0 /* b2 */, 0 /* excCount */, 0 /* m */);
      return 3 + BitPacking.packBits(8, values, valueOffset, out, outOffset + 3, count);
    }

    int bytesWritten = writeHeader(out, outOffset, b1, b2, excCount, base);

    // Primary array: low b1 bits of every value
    bytesWritten += BitPacking.packBits(b1, normalized, 0, out, outOffset + bytesWritten, count);

    // b2 is the bit width of exception values: (maxWidth - b1) bits of each exception
    if (excCount > 0) {
      int[] excOffsets = new int[excCount];
      int[] excValues = new int[excCount];

      // Collect exceptions (values that do not fit in b1 bits)
      int excIndex = 0;
      int threshold = 1 << b1;
      for (int i = 0; i < count; i += 1) {
        if (normalized[i] >= threshold) {
          excOffsets[excIndex] = i;
          excValues[excIndex] = normalized[i] >>> b1;
          excIndex += 1;
        }
      }

      // Exception offsets (one byte per exception)
      bytesWritten +=
          BitPacking.packBits(8, excOffsets, 0, out, outOffset + bytesWritten, excCount);

      // Exception values: remaining high b2 bits of each exception
      bytesWritten +=
          BitPacking.packBits(b2, excValues, 0, out, outOffset + bytesWritten, excCount);
    }

    return bytesWritten;
  }

  /**
   * Decode one chunk of encoded data, writing decoded values into an output array.
   *
   * @param data buffer containing source data to decode
   * @param dataOffset starting index in the buffer to decode
   * @param out an output {@link ByteBuffer}
   * @param outOffset starting index for output in the out buffer
   * @param count number of values to decode
   * @return the number of bytes read from {@code data}
   */
  private static int decodeChunk(
      ByteBuffer data, int dataOffset, int[] out, int outOffset, int count) {
    Preconditions.checkArgument(count >= 0, "Invalid value count to decode: %s", count);
    Preconditions.checkArgument(
        outOffset + count <= out.length,
        "Cannot decode %s values starting at %s into int[%s]: not enough space",
        count,
        out.length,
        outOffset);

    int b1 = ByteBuffers.readByte(data, dataOffset) & 0x0F;
    int b2 = (ByteBuffers.readByte(data, dataOffset) >>> 4) & 0x0F;
    int excCount = ByteBuffers.readByte(data, dataOffset + 1);
    int base = ByteBuffers.readByte(data, dataOffset + 2);
    int bytesRead = 3;

    // after reading the header, check that the full chunk is present
    int expectedSize = encodedSize(count, b1, b2, excCount);
    Preconditions.checkArgument(
        dataOffset + expectedSize <= data.remaining(),
        "Cannot decode %s values from buffer with %s remaining bytes",
        expectedSize,
        data.remaining() - dataOffset);

    // Read primary array: low b1 bits of each value
    bytesRead += BitPacking.unpackBits(b1, data, dataOffset + bytesRead, out, outOffset, count);

    // Read exceptions and update output values
    if (excCount > 0) {
      int[] excOffsets = new int[excCount];
      int[] excValues = new int[excCount];
      int excListOffset = dataOffset + bytesRead;
      int excDataOffset = dataOffset + bytesRead + excCount;

      // Read exception indexes
      bytesRead += BitPacking.unpackBits(8, data, excListOffset, excOffsets, 0, excCount);

      // Read exception values and patch the primary values
      bytesRead += BitPacking.unpackBits(b2, data, excDataOffset, excValues, 0, excCount);

      // Update output values
      for (int i = 0; i < excCount; i += 1) {
        out[outOffset + excOffsets[i]] |= excValues[i] << b1;
      }
    }

    // Add back the chunk minimum
    for (int i = 0; i < count; i += 1) {
      out[outOffset + i] += base;
    }

    return bytesRead;
  }

  private static int writeHeader(
      ByteBuffer out, int outOffset, int b1, int b2, int excCount, int base) {
    // Header: b1 in low nibble, b2 in high nibble, then e, then m
    ByteBuffers.writeByte(out, (b2 << 4) | (b1 & 0b1111), outOffset);
    ByteBuffers.writeByte(out, excCount, outOffset + 1);
    ByteBuffers.writeByte(out, base, outOffset + 2);

    return 3;
  }

  /**
   * Choose the primary bit width {@code b1} that minimizes total encoded chunk size.
   *
   * <p>This produces the width that results in the smallest total size and the number of exceptions
   * for that width.
   *
   * <p>Larger width is preferred on ties to reduce the number of exceptions.
   *
   * @param normalized value array to encode, after normalization
   * @param length number of values in the array to encode
   * @param maxWidth the largest bit width of normalized values
   * @return a {@link Pair} of the chosen width and number of exceptions for that width
   */
  private static Pair<Integer, Integer> chooseBitWidth(int[] normalized, int length, int maxWidth) {
    int bestWidth = 0;
    int bestSize = Integer.MAX_VALUE;
    int bestExcCount = 0;

    for (int candidateWidth = 0; candidateWidth <= maxWidth; candidateWidth += 1) {
      int excCount = 0;
      if (candidateWidth < 8) {
        int threshold = 1 << candidateWidth;
        for (int i = 0; i < length; i += 1) {
          if (normalized[i] >= threshold) {
            excCount += 1;
          }
        }
      }

      int b2 = maxWidth - candidateWidth;
      int size = byteWidth(length * candidateWidth) + excCount + byteWidth(excCount * b2);

      if (size <= bestSize) {
        bestSize = size;
        bestWidth = candidateWidth;
        bestExcCount = excCount;
      }
    }

    return Pair.of(bestWidth, bestExcCount);
  }

  /**
   * Return the lowest byte value from the array slice [start, start + length).
   *
   * <p>If length is < 1, the result will be larger than Byte.MAX_VALUE.
   *
   * @param values array of values
   * @param start starting index
   * @param length number of values to check
   * @return the min of the values in the array slice
   */
  private static int min(int[] values, int start, int length) {
    // Use min > Byte.MAX_VALUE to signal no min (length < 1)
    int min = 256;
    for (int i = start; i < start + length; i += 1) {
      if (values[i] < min) {
        min = values[i];
      }
    }

    return min;
  }

  /** Returns the number of bytes required in the worst case to encode {@code valueCount} values. */
  static int estimateEncodedSize(int valueCount) {
    // Worst-case per chunk is b1=8: 3-byte header + 1 byte per value. Any other b1 chosen by the
    // encoder costs <= n bytes of data (otherwise b1=8 would have been selected instead).
    int numChunks = ceilDiv(valueCount, CHUNK_SIZE);
    return 3 * numChunks + valueCount;
  }

  /** Returns the number of bytes required to encode a chunk of values. */
  private static int encodedSize(int count, int b1, int b2, int excCount) {
    return 3 + byteWidth(b1 * count) + excCount + byteWidth(b2 * excCount);
  }

  /** Returns the number of bits required to represent {@code v} (0 for v=0). */
  static int width(int value) {
    return 32 - Integer.numberOfLeadingZeros(value);
  }

  private static int byteWidth(int bits) {
    return ceilDiv(bits, 8);
  }

  private static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }
}
