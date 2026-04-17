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
import org.apache.iceberg.util.Pair;

/**
 * PFOR (Patched Frame of Reference) encoding for arrays of unsigned byte values.
 *
 * <p>Implements the encoding described in Appendix A of the Mumbling bitmap specification. The
 * input array is split into 256-value chunks (the last chunk may be shorter). Each chunk is
 * independently compressed.
 *
 * <p>Each chunk is stored as:
 *
 * <ul>
 *   <li>3-byte header: {@code b1|b2} (low/high nibbles of byte 0), {@code e} (byte 1), {@code m}
 *       (byte 2)
 *   <li>Primary array: {@code ceil(n * b1 / 8)} bytes — the low {@code b1} bits of every value,
 *       packed MSB-first
 *   <li>Exception offsets: {@code e} bytes — the chunk-relative position of each exception value
 *   <li>Exception values: {@code ceil(e * b2 / 8)} bytes — bits {@code [b1, b1+b2)} of each
 *       exception, packed MSB-first
 * </ul>
 *
 * <p>During encoding, the chunk minimum {@code m} is subtracted from every value. During decoding,
 * {@code m} is added back. When {@code b1 = 8}, no exceptions are produced and {@code m} is stored
 * as 0 (original values are written directly). Bit packing and unpacking is delegated to {@link
 * BitPacking}.
 */
class PFOREncoding {
  private static final int CHUNK_SIZE = 256;
  private static final int DENSE_DESCRIPTOR_BIT = 0x20;
  private static final int SPARSE_LENGTH_MASK = 0x1F;
  private static final int DENSE_CONTAINER_BYTES = 32;

  private PFOREncoding() {}

  /**
   * Encodes an array of unsigned byte values (each in {@code [0, 255]}) using PFOR encoding.
   *
   * @param values unsigned byte values to encode
   * @return a newly allocated buffer
   */
  static ByteBuffer encode(int[] values) {
    return encode(values, values.length, null);
  }

  /**
   * Encodes an array of unsigned byte values (each in {@code [0, 255]}) using PFOR encoding.
   *
   * <p>If {@code buffer} has sufficient capacity its backing storage is reused; otherwise a new
   * buffer is allocated. {@code buffer}'s position and limit are never modified. The returned
   * buffer is always a slice with position=0 and limit=encoded length.
   *
   * @param values unsigned byte values to encode
   * @param length number of values to encode
   * @return a slice of {@code buffer} if capacity was sufficient, otherwise a slice of a newly
   *     allocated buffer
   */
  static ByteBuffer encode(int[] values, int length) {
    return encode(values, length, null);
  }

  /**
   * Encodes the first {@code length} unsigned byte values (each in {@code [0, 255]}) from {@code
   * values} using PFOR encoding.
   *
   * <p>If {@code buffer} has sufficient capacity its backing storage is reused; otherwise a new
   * buffer is allocated. {@code buffer}'s position and limit are never modified. The returned
   * buffer is always a slice with position=0 and limit=encoded length.
   *
   * @param values unsigned byte values to encode
   * @param length number of values to encode
   * @param buffer candidate buffer whose storage may be reused
   * @return a slice of {@code buffer} if capacity was sufficient, otherwise a slice of a newly
   *     allocated buffer
   */
  static ByteBuffer encode(int[] values, int length, ByteBuffer buffer) {
    int numChunks = ceilDiv(length, CHUNK_SIZE);
    // Worst-case per chunk is b1=8: 3-byte header + 1 byte per value. Any other b1 chosen by the
    // encoder costs <= n bytes of data (otherwise b1=8 would have been selected instead).
    int maxSize = 3 * numChunks + length;
    ByteBuffer out =
        buffer != null && buffer.capacity() >= maxSize ? buffer : ByteBuffer.allocate(maxSize);
    int pos = 0;
    int offset = 0;
    while (offset < length) {
      int chunkLength = Math.min(CHUNK_SIZE, length - offset);
      pos += encodeChunk(values, offset, chunkLength, out, pos);
      offset += chunkLength;
    }

    return out.slice(0, pos);
  }

  /**
   * Decodes PFOR-encoded bytes back to unsigned byte values. Reads from {@code encoded.position()}
   * using absolute indexing; the buffer's position is not modified.
   *
   * @param encoded PFOR-encoded ByteBuffer produced by {@link #encode}
   * @param count total number of values to decode
   * @return decoded unsigned byte values
   */
  static int[] decode(ByteBuffer encoded, int count) {
    if (count == 0) {
      return new int[0];
    }

    int[] output = new int[count];
    int pos = encoded.position();
    int start = 0;
    while (start < count) {
      int length = Math.min(CHUNK_SIZE, count - start);
      pos += decodeChunk(encoded, pos, output, start, length);
      start += length;
    }

    return output;
  }

  /**
   * Encodes a container offset array as PFOR-encoded container lengths.
   *
   * <p>{@code offsets} must have {@code count + 1} entries. The length of container {@code i} is
   * {@code offsets[i + 1] - offsets[i]} and is used as the descriptor value to encode.
   *
   * @param offsets container offset array with {@code count + 1} entries
   * @param count number of containers
   * @return PFOR-encoded descriptor bytes
   */
  static ByteBuffer encodeOffsets(int[] offsets, int count) {
    int[] lengths = new int[count];
    for (int i = 0; i < count; i += 1) {
      lengths[i] = offsets[i + 1] - offsets[i];
    }

    return encode(lengths, count);
  }

  /**
   * Decodes PFOR-encoded descriptor bytes directly into a container offset array.
   *
   * <p>Reads from {@code encoded.position()} without modifying the buffer's position. Each decoded
   * descriptor value is treated as a container length and accumulated into {@code offsets} as a
   * prefix sum starting at 0. The caller is responsible for adjusting the resulting relative
   * offsets to absolute positions.
   *
   * <p>{@code offsets} must have {@code count + 1} entries. On return, {@code offsets[i]} is the
   * cumulative byte length of containers {@code 0..i-1} and {@code offsets[count]} is the total
   * byte length of all containers.
   *
   * @param encoded PFOR-encoded descriptor bytes
   * @param count number of containers (descriptors) to decode
   * @param offsets array to fill; must have length &gt;= count + 1
   * @return number of bytes consumed from {@code encoded}
   */
  static int decodeOffsets(ByteBuffer encoded, int count, int[] offsets) {
    if (count == 0) {
      offsets[0] = 0;
      return 0;
    }

    int[] chunk = new int[Math.min(CHUNK_SIZE, count)];
    int pos = encoded.position();
    int containerIdx = 0;
    int cumulative = 0;
    while (containerIdx < count) {
      int chunkLen = Math.min(CHUNK_SIZE, count - containerIdx);
      pos += decodeChunk(encoded, pos, chunk, 0, chunkLen);
      for (int i = 0; i < chunkLen; i += 1) {
        offsets[containerIdx + i] = cumulative;
        int descriptor = chunk[i];
        cumulative += (descriptor & DENSE_DESCRIPTOR_BIT) != 0 ? DENSE_CONTAINER_BYTES : (descriptor & SPARSE_LENGTH_MASK);
      }

      containerIdx += chunkLen;
    }
    offsets[count] = cumulative;

    return pos - encoded.position();
  }

  /**
   * Encodes one chunk into {@code out} starting at absolute position {@code outPos}. Returns the
   * number of bytes written.
   */
  private static int encodeChunk(int[] values, int start, int length, ByteBuffer out, int outPos) {
    // Step 1: find base=min(values) for normalization
    int base = min(values, start, length);

    // Step 2: normalize by subtracting base
    int[] normalized = new int[length];
    int setBits = 0;
    for (int i = 0; i < length; i += 1) {
      normalized[i] = values[start + i] - base;
      setBits |= normalized[i];
    }

    // Step 3: find the maximum bit width needed for normalized values
    int maxWidth = width(setBits);

    // Step 4: choose b1 to minimize total encoded data size (excluding 3-byte header)
    Pair<Integer, Integer> widthAndExcCount = chooseBitWidth(normalized, length, maxWidth);
    int b1 = widthAndExcCount.first();
    int b2 = maxWidth - b1;
    int excCount = widthAndExcCount.second();
    int primaryBytes = ceilDiv(length * b1, 8);
    int excValueBytes = ceilDiv(excCount * b2, 8);

    // Special case: b1=8 means store original values as raw bytes with a constant header.
    // b2, e, and m should be 0, so the header is always 0x08 0x00 0x00.
    if (b1 == 8) {
      out.put(outPos, (byte) 0x08);
      out.put(outPos + 1, (byte) 0);
      out.put(outPos + 2, (byte) 0);
      return copyBytes(values, start, length, out, outPos + 3) - outPos;
    }

    // Header: b1 in low nibble, b2 in high nibble, then e, then m

    out.put(outPos, (byte) ((b2 << 4) | b1));
    out.put(outPos + 1, (byte) excCount);
    out.put(outPos + 2, (byte) base);
    int pos = outPos + 3;

    // Primary array: low b1 bits of every value, packed MSB-first
    if (b1 > 0) {
      BitPacking.packBits(normalized, 0, length, out, pos, b1);
      pos += primaryBytes;
    }

    // b2 is the bit width of exception values: bits [b1, b1+b2) of each exception
    if (maxWidth > b1) {
      int[] exceptionOffsets = new int[length];
      int[] exceptionValues = new int[length];

      // Step 5: collect exceptions (values that do not fit in b1 bits)
      int excIndex = 0;
      int threshold = 1 << b1;
      for (int i = 0; i < length; i += 1) {
        if (normalized[i] >= threshold) {
          exceptionOffsets[excIndex] = i;
          exceptionValues[excIndex] = normalized[i] >> b1;
          excIndex += 1;
        }
      }

      // Exception offsets (one byte per exception)
      pos = copyBytes(exceptionOffsets, 0, excCount, out, pos);

      // Exception values: bits [b1, b1+b2) of each exception, packed MSB-first
      if (b2 > 0 && excCount > 0) {
        if (b2 == 8) {
          copyBytes(exceptionValues, 0, excCount, out, pos);
        } else {
          BitPacking.packBits(exceptionValues, 0, excCount, out, pos, b2);
        }
        pos += excValueBytes;
      }
    }

    return pos - outPos;
  }

  /**
   * Chooses the primary bit width {@code b1} that minimizes total encoded chunk size.
   *
   * <p>For each candidate {@code b} from 0 to 8, computes:
   *
   * <ul>
   *   <li>{@code e}: number of values needing more than {@code b} bits
   *   <li>{@code b2 = maxWidth - b}: bits needed for exception remainders
   *   <li>total size = {@code ceil(n * b / 8) + e + ceil(e * b2 / 8)}
   * </ul>
   *
   * Returns the {@code b} with minimum total size, preferring smaller {@code b} on ties.
   */
  private static Pair<Integer, Integer> chooseBitWidth(int[] normalized, int length, int maxWidth) {
    int bestWidth = 0;
    int bestSize = Integer.MAX_VALUE;
    int bestExcCount = 0;

    for (int b = 0; b <= maxWidth; b += 1) {
      int e = 0;
      if (b < 8) {
        int threshold = 1 << b;
        for (int i = 0; i < length; i += 1) {
          if (normalized[i] >= threshold) {
            e += 1;
          }
        }
      }

      int b2 = maxWidth - b;
      int size = ceilDiv(length * b, 8) + e + ceilDiv(e * b2, 8);

      if (size < bestSize) {
        bestSize = size;
        bestWidth = b;
        bestExcCount = e;
      }
    }

    return Pair.of(bestWidth, bestExcCount);
  }

  /**
   * Decodes one chunk of PFOR-encoded data, writing decoded values into {@code output[start,
   * start+length)}.
   *
   * @return the number of bytes read from {@code data}
   */
  private static int decodeChunk(ByteBuffer data, int pos, int[] output, int start, int length) {
    int b1 = data.get(pos) & 0x0F;
    int b2 = (data.get(pos) >> 4) & 0x0F;
    int excCount = data.get(pos + 1) & 0xFF;
    int base = data.get(pos + 2) & 0xFF;
    int cursor = pos + 3;

    // Special case: b1=8 means raw bytes; e is always 0
    if (b1 == 8) {
      for (int i = 0; i < length; i += 1) {
        output[start + i] = (data.get(cursor + i) & 0xFF) + base;
      }
      return cursor + length - pos;
    }

    // Read primary array: low b1 bits of each value
    int[] values = new int[length];
    if (b1 > 0) {
      BitPacking.unpackBits(data, cursor, values, length, b1);
      cursor += ceilDiv(length * b1, 8);
    }

    // Read exception offsets
    int[] offsets = new int[excCount];
    for (int i = 0; i < excCount; i += 1) {
      offsets[i] = data.get(cursor) & 0xFF;
      cursor += 1;
    }

    // Read exception values and patch the primary values
    if (b2 > 0 && excCount > 0) {
      int[] excValues = new int[excCount];
      if (b2 == 8) {
        for (int i = 0; i < excCount; i += 1) {
          excValues[i] = data.get(cursor + i) & 0xFF;
        }
      } else {
        BitPacking.unpackBits(data, cursor, excValues, excCount, b2);
      }
      cursor += ceilDiv(excCount * b2, 8);

      for (int i = 0; i < excCount; i += 1) {
        values[offsets[i]] |= excValues[i] << b1;
      }
    }

    // Add back the chunk minimum
    for (int i = 0; i < length; i += 1) {
      output[start + i] = values[i] + base;
    }

    return cursor - pos;
  }

  // ---------------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------------

  private static int copyBytes(int[] src, int srcStart, int count, ByteBuffer out, int outPos) {
    for (int i = 0; i < count; i += 1) {
      out.put(outPos + i, (byte) src[srcStart + i]);
    }
    return outPos + count;
  }

  private static int min(int[] values, int start, int length) {
    int min = 255;
    for (int i = start; i < start + length; i += 1) {
      if (values[i] < min) {
        min = values[i];
      }
    }

    return min;
  }

  /** Returns the number of bits required to represent {@code v} (0 for v=0). */
  static int width(int value) {
    return 32 - Integer.numberOfLeadingZeros(value);
  }

  static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }
}
