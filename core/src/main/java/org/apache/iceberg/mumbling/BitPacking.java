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

/**
 * MSB-first bit packing and unpacking for values of 1–7 bits.
 *
 * <p>Values are packed in groups of 8: each group of 8 values occupies exactly {@code b} bytes.
 * A trailing partial group of {@code rem = count & 7} values is left-aligned in {@code
 * ceil(rem * b / 8)} bytes, padded with zero bits.
 *
 * <p>Each specialized method (b=1..7) has a compile-time-constant bit width, allowing the JIT to
 * fold shift amounts to immediates and inline the byte sequence, eliminating the inner loop and
 * its loop-carried shift dependency.
 */
class BitPacking {

  private BitPacking() {}

  /**
   * Packs {@code count} values from {@code values[valPos..]} into {@code out} starting at absolute
   * position {@code outPos}, using {@code b} bits per value (1–7).
   */
  static void packBits(
      int[] values, int valPos, int count, ByteBuffer out, int outPos, int b) {
    switch (b) {
      case 1:
        packBits1(values, valPos, count, out, outPos);
        break;
      case 2:
        packBits2(values, valPos, count, out, outPos);
        break;
      case 3:
        packBits3(values, valPos, count, out, outPos);
        break;
      case 4:
        packBits4(values, valPos, count, out, outPos);
        break;
      case 5:
        packBits5(values, valPos, count, out, outPos);
        break;
      case 6:
        packBits6(values, valPos, count, out, outPos);
        break;
      case 7:
        packBits7(values, valPos, count, out, outPos);
        break;
      default:
        throw new IllegalArgumentException("Invalid bit width: " + b);
    }
  }

  /**
   * Unpacks {@code count} values from {@code data} starting at absolute position {@code dataPos}
   * into {@code output}, using {@code b} bits per value (1–7).
   */
  static void unpackBits(ByteBuffer data, int dataPos, int[] output, int count, int b) {
    switch (b) {
      case 1:
        unpackBits1(data, dataPos, output, count);
        break;
      case 2:
        unpackBits2(data, dataPos, output, count);
        break;
      case 3:
        unpackBits3(data, dataPos, output, count);
        break;
      case 4:
        unpackBits4(data, dataPos, output, count);
        break;
      case 5:
        unpackBits5(data, dataPos, output, count);
        break;
      case 6:
        unpackBits6(data, dataPos, output, count);
        break;
      case 7:
        unpackBits7(data, dataPos, output, count);
        break;
      default:
        throw new IllegalArgumentException("Invalid bit width: " + b);
    }
  }

  // ---------------------------------------------------------------------------
  // Specialized pack: b=1..7
  // Each method packs 8 values into exactly b bytes (full groups), plus a
  // partial group of rem < 8 values left-aligned in ceil(rem*b/8) bytes.
  // ---------------------------------------------------------------------------

  /** 1-bit values: 8 values → 1 byte. */
  private static void packBits1(int[] values, int valPos, int count, ByteBuffer out, int outPos) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int vBase = valPos + (g << 3);
      out.put(
          outPos + g,
          (byte)
              (((values[vBase] & 1) << 7)
                  | ((values[vBase + 1] & 1) << 6)
                  | ((values[vBase + 2] & 1) << 5)
                  | ((values[vBase + 3] & 1) << 4)
                  | ((values[vBase + 4] & 1) << 3)
                  | ((values[vBase + 5] & 1) << 2)
                  | ((values[vBase + 6] & 1) << 1)
                  | (values[vBase + 7] & 1)));
    }
    int rem = count & 7;
    if (rem > 0) {
      int vBase = valPos + (fullGroups << 3);
      long word = 0;
      for (int k = 0; k < rem; k += 1) {
        word = (word << 1) | (values[vBase + k] & 1);
      }
      out.put(outPos + fullGroups, (byte) (word << (8 - rem)));
    }
  }

  /** 2-bit values: 8 values → 2 bytes. */
  private static void packBits2(int[] values, int valPos, int count, ByteBuffer out, int outPos) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int vBase = valPos + (g << 3);
      int oBase = outPos + (g << 1);
      int word =
          ((values[vBase] & 3) << 14)
              | ((values[vBase + 1] & 3) << 12)
              | ((values[vBase + 2] & 3) << 10)
              | ((values[vBase + 3] & 3) << 8)
              | ((values[vBase + 4] & 3) << 6)
              | ((values[vBase + 5] & 3) << 4)
              | ((values[vBase + 6] & 3) << 2)
              | (values[vBase + 7] & 3);
      out.put(oBase, (byte) (word >>> 8));
      out.put(oBase + 1, (byte) word);
    }
    packRemainder(values, valPos, count, out, outPos, fullGroups, 2, 3);
  }

  /** 3-bit values: 8 values → 3 bytes. */
  private static void packBits3(int[] values, int valPos, int count, ByteBuffer out, int outPos) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int vBase = valPos + (g << 3);
      int oBase = outPos + g * 3;
      int word =
          ((values[vBase] & 7) << 21)
              | ((values[vBase + 1] & 7) << 18)
              | ((values[vBase + 2] & 7) << 15)
              | ((values[vBase + 3] & 7) << 12)
              | ((values[vBase + 4] & 7) << 9)
              | ((values[vBase + 5] & 7) << 6)
              | ((values[vBase + 6] & 7) << 3)
              | (values[vBase + 7] & 7);
      out.put(oBase, (byte) (word >>> 16));
      out.put(oBase + 1, (byte) (word >>> 8));
      out.put(oBase + 2, (byte) word);
    }
    packRemainder(values, valPos, count, out, outPos, fullGroups, 3, 7);
  }

  /** 4-bit values: 8 values → 4 bytes. */
  private static void packBits4(int[] values, int valPos, int count, ByteBuffer out, int outPos) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int vBase = valPos + (g << 3);
      int oBase = outPos + (g << 2);
      long word =
          ((long) (values[vBase] & 15) << 28)
              | ((long) (values[vBase + 1] & 15) << 24)
              | ((long) (values[vBase + 2] & 15) << 20)
              | ((long) (values[vBase + 3] & 15) << 16)
              | ((long) (values[vBase + 4] & 15) << 12)
              | ((long) (values[vBase + 5] & 15) << 8)
              | ((long) (values[vBase + 6] & 15) << 4)
              | (long) (values[vBase + 7] & 15);
      out.put(oBase, (byte) (word >>> 24));
      out.put(oBase + 1, (byte) (word >>> 16));
      out.put(oBase + 2, (byte) (word >>> 8));
      out.put(oBase + 3, (byte) word);
    }
    packRemainder(values, valPos, count, out, outPos, fullGroups, 4, 15);
  }

  /** 5-bit values: 8 values → 5 bytes. */
  private static void packBits5(int[] values, int valPos, int count, ByteBuffer out, int outPos) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int vBase = valPos + (g << 3);
      int oBase = outPos + g * 5;
      long word =
          ((long) (values[vBase] & 31) << 35)
              | ((long) (values[vBase + 1] & 31) << 30)
              | ((long) (values[vBase + 2] & 31) << 25)
              | ((long) (values[vBase + 3] & 31) << 20)
              | ((long) (values[vBase + 4] & 31) << 15)
              | ((long) (values[vBase + 5] & 31) << 10)
              | ((long) (values[vBase + 6] & 31) << 5)
              | (long) (values[vBase + 7] & 31);
      out.put(oBase, (byte) (word >>> 32));
      out.put(oBase + 1, (byte) (word >>> 24));
      out.put(oBase + 2, (byte) (word >>> 16));
      out.put(oBase + 3, (byte) (word >>> 8));
      out.put(oBase + 4, (byte) word);
    }
    packRemainder(values, valPos, count, out, outPos, fullGroups, 5, 31);
  }

  /** 6-bit values: 8 values → 6 bytes. */
  private static void packBits6(int[] values, int valPos, int count, ByteBuffer out, int outPos) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int vBase = valPos + (g << 3);
      int oBase = outPos + g * 6;
      long word =
          ((long) (values[vBase] & 63) << 42)
              | ((long) (values[vBase + 1] & 63) << 36)
              | ((long) (values[vBase + 2] & 63) << 30)
              | ((long) (values[vBase + 3] & 63) << 24)
              | ((long) (values[vBase + 4] & 63) << 18)
              | ((long) (values[vBase + 5] & 63) << 12)
              | ((long) (values[vBase + 6] & 63) << 6)
              | (long) (values[vBase + 7] & 63);
      out.put(oBase, (byte) (word >>> 40));
      out.put(oBase + 1, (byte) (word >>> 32));
      out.put(oBase + 2, (byte) (word >>> 24));
      out.put(oBase + 3, (byte) (word >>> 16));
      out.put(oBase + 4, (byte) (word >>> 8));
      out.put(oBase + 5, (byte) word);
    }
    packRemainder(values, valPos, count, out, outPos, fullGroups, 6, 63);
  }

  /** 7-bit values: 8 values → 7 bytes. */
  private static void packBits7(int[] values, int valPos, int count, ByteBuffer out, int outPos) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int vBase = valPos + (g << 3);
      int oBase = outPos + g * 7;
      long word =
          ((long) (values[vBase] & 127) << 49)
              | ((long) (values[vBase + 1] & 127) << 42)
              | ((long) (values[vBase + 2] & 127) << 35)
              | ((long) (values[vBase + 3] & 127) << 28)
              | ((long) (values[vBase + 4] & 127) << 21)
              | ((long) (values[vBase + 5] & 127) << 14)
              | ((long) (values[vBase + 6] & 127) << 7)
              | (long) (values[vBase + 7] & 127);
      out.put(oBase, (byte) (word >>> 48));
      out.put(oBase + 1, (byte) (word >>> 40));
      out.put(oBase + 2, (byte) (word >>> 32));
      out.put(oBase + 3, (byte) (word >>> 24));
      out.put(oBase + 4, (byte) (word >>> 16));
      out.put(oBase + 5, (byte) (word >>> 8));
      out.put(oBase + 6, (byte) word);
    }
    packRemainder(values, valPos, count, out, outPos, fullGroups, 7, 127);
  }

  /**
   * Packs the final partial group (rem = count & 7 values) for bit widths 2–7. Values are
   * left-aligned in {@code ceil(rem * bitsPerValue / 8)} bytes.
   */
  private static void packRemainder(
      int[] values,
      int valPos,
      int count,
      ByteBuffer out,
      int outPos,
      int fullGroups,
      int bitsPerValue,
      int mask) {
    int rem = count & 7;
    if (rem > 0) {
      int vBase = valPos + (fullGroups << 3);
      int oBase = outPos + fullGroups * bitsPerValue;
      long word = 0;
      for (int k = 0; k < rem; k += 1) {
        word = (word << bitsPerValue) | (values[vBase + k] & mask);
      }
      int remBits = rem * bitsPerValue;
      int remBytes = (remBits + 7) >>> 3;
      word <<= (remBytes << 3) - remBits;
      for (int k = remBytes - 1; k >= 0; k--) {
        out.put(oBase + k, (byte) word);
        word >>>= 8;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Specialized unpack: b=1..7
  // ---------------------------------------------------------------------------

  /** 1-bit values: 1 byte → 8 values. */
  private static void unpackBits1(ByteBuffer data, int dataPos, int[] output, int count) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int oBase = g << 3;
      int w = data.get(dataPos + g) & 0xFF;
      output[oBase] = (w >>> 7) & 1;
      output[oBase + 1] = (w >>> 6) & 1;
      output[oBase + 2] = (w >>> 5) & 1;
      output[oBase + 3] = (w >>> 4) & 1;
      output[oBase + 4] = (w >>> 3) & 1;
      output[oBase + 5] = (w >>> 2) & 1;
      output[oBase + 6] = (w >>> 1) & 1;
      output[oBase + 7] = w & 1;
    }
    unpackRemainder(data, dataPos, output, count, fullGroups, 1, 1);
  }

  /** 2-bit values: 2 bytes → 8 values. */
  private static void unpackBits2(ByteBuffer data, int dataPos, int[] output, int count) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int oBase = g << 3;
      int dBase = dataPos + (g << 1);
      int w = ((data.get(dBase) & 0xFF) << 8) | (data.get(dBase + 1) & 0xFF);
      output[oBase] = (w >>> 14) & 3;
      output[oBase + 1] = (w >>> 12) & 3;
      output[oBase + 2] = (w >>> 10) & 3;
      output[oBase + 3] = (w >>> 8) & 3;
      output[oBase + 4] = (w >>> 6) & 3;
      output[oBase + 5] = (w >>> 4) & 3;
      output[oBase + 6] = (w >>> 2) & 3;
      output[oBase + 7] = w & 3;
    }
    unpackRemainder(data, dataPos, output, count, fullGroups, 2, 3);
  }

  /** 3-bit values: 3 bytes → 8 values. */
  private static void unpackBits3(ByteBuffer data, int dataPos, int[] output, int count) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int oBase = g << 3;
      int dBase = dataPos + g * 3;
      int w =
          ((data.get(dBase) & 0xFF) << 16)
              | ((data.get(dBase + 1) & 0xFF) << 8)
              | (data.get(dBase + 2) & 0xFF);
      output[oBase] = (w >>> 21) & 7;
      output[oBase + 1] = (w >>> 18) & 7;
      output[oBase + 2] = (w >>> 15) & 7;
      output[oBase + 3] = (w >>> 12) & 7;
      output[oBase + 4] = (w >>> 9) & 7;
      output[oBase + 5] = (w >>> 6) & 7;
      output[oBase + 6] = (w >>> 3) & 7;
      output[oBase + 7] = w & 7;
    }
    unpackRemainder(data, dataPos, output, count, fullGroups, 3, 7);
  }

  /** 4-bit values: 4 bytes → 8 values. */
  private static void unpackBits4(ByteBuffer data, int dataPos, int[] output, int count) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int oBase = g << 3;
      int dBase = dataPos + (g << 2);
      long w =
          ((long) (data.get(dBase) & 0xFF) << 24)
              | ((long) (data.get(dBase + 1) & 0xFF) << 16)
              | ((long) (data.get(dBase + 2) & 0xFF) << 8)
              | (long) (data.get(dBase + 3) & 0xFF);
      output[oBase] = (int) (w >>> 28) & 15;
      output[oBase + 1] = (int) (w >>> 24) & 15;
      output[oBase + 2] = (int) (w >>> 20) & 15;
      output[oBase + 3] = (int) (w >>> 16) & 15;
      output[oBase + 4] = (int) (w >>> 12) & 15;
      output[oBase + 5] = (int) (w >>> 8) & 15;
      output[oBase + 6] = (int) (w >>> 4) & 15;
      output[oBase + 7] = (int) w & 15;
    }
    unpackRemainder(data, dataPos, output, count, fullGroups, 4, 15);
  }

  /** 5-bit values: 5 bytes → 8 values. */
  private static void unpackBits5(ByteBuffer data, int dataPos, int[] output, int count) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int oBase = g << 3;
      int dBase = dataPos + g * 5;
      long w =
          ((long) (data.get(dBase) & 0xFF) << 32)
              | ((long) (data.get(dBase + 1) & 0xFF) << 24)
              | ((long) (data.get(dBase + 2) & 0xFF) << 16)
              | ((long) (data.get(dBase + 3) & 0xFF) << 8)
              | (long) (data.get(dBase + 4) & 0xFF);
      output[oBase] = (int) (w >>> 35) & 31;
      output[oBase + 1] = (int) (w >>> 30) & 31;
      output[oBase + 2] = (int) (w >>> 25) & 31;
      output[oBase + 3] = (int) (w >>> 20) & 31;
      output[oBase + 4] = (int) (w >>> 15) & 31;
      output[oBase + 5] = (int) (w >>> 10) & 31;
      output[oBase + 6] = (int) (w >>> 5) & 31;
      output[oBase + 7] = (int) w & 31;
    }
    unpackRemainder(data, dataPos, output, count, fullGroups, 5, 31);
  }

  /** 6-bit values: 6 bytes → 8 values. */
  private static void unpackBits6(ByteBuffer data, int dataPos, int[] output, int count) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int oBase = g << 3;
      int dBase = dataPos + g * 6;
      long w =
          ((long) (data.get(dBase) & 0xFF) << 40)
              | ((long) (data.get(dBase + 1) & 0xFF) << 32)
              | ((long) (data.get(dBase + 2) & 0xFF) << 24)
              | ((long) (data.get(dBase + 3) & 0xFF) << 16)
              | ((long) (data.get(dBase + 4) & 0xFF) << 8)
              | (long) (data.get(dBase + 5) & 0xFF);
      output[oBase] = (int) (w >>> 42) & 63;
      output[oBase + 1] = (int) (w >>> 36) & 63;
      output[oBase + 2] = (int) (w >>> 30) & 63;
      output[oBase + 3] = (int) (w >>> 24) & 63;
      output[oBase + 4] = (int) (w >>> 18) & 63;
      output[oBase + 5] = (int) (w >>> 12) & 63;
      output[oBase + 6] = (int) (w >>> 6) & 63;
      output[oBase + 7] = (int) w & 63;
    }
    unpackRemainder(data, dataPos, output, count, fullGroups, 6, 63);
  }

  /** 7-bit values: 7 bytes → 8 values. */
  private static void unpackBits7(ByteBuffer data, int dataPos, int[] output, int count) {
    int fullGroups = count >>> 3;
    for (int g = 0; g < fullGroups; g += 1) {
      int oBase = g << 3;
      int dBase = dataPos + g * 7;
      long w =
          ((long) (data.get(dBase) & 0xFF) << 48)
              | ((long) (data.get(dBase + 1) & 0xFF) << 40)
              | ((long) (data.get(dBase + 2) & 0xFF) << 32)
              | ((long) (data.get(dBase + 3) & 0xFF) << 24)
              | ((long) (data.get(dBase + 4) & 0xFF) << 16)
              | ((long) (data.get(dBase + 5) & 0xFF) << 8)
              | (long) (data.get(dBase + 6) & 0xFF);
      output[oBase] = (int) (w >>> 49) & 127;
      output[oBase + 1] = (int) (w >>> 42) & 127;
      output[oBase + 2] = (int) (w >>> 35) & 127;
      output[oBase + 3] = (int) (w >>> 28) & 127;
      output[oBase + 4] = (int) (w >>> 21) & 127;
      output[oBase + 5] = (int) (w >>> 14) & 127;
      output[oBase + 6] = (int) (w >>> 7) & 127;
      output[oBase + 7] = (int) w & 127;
    }
    unpackRemainder(data, dataPos, output, count, fullGroups, 7, 127);
  }

  /**
   * Unpacks the final partial group (rem = count & 7 values) for bit widths 1–7. Reads {@code
   * ceil(rem * bitsPerValue / 8)} bytes and right-aligns before extracting.
   */
  private static void unpackRemainder(
      ByteBuffer data,
      int dataPos,
      int[] output,
      int count,
      int fullGroups,
      int bitsPerValue,
      int mask) {
    int rem = count & 7;
    if (rem > 0) {
      int oBase = fullGroups << 3;
      int dBase = dataPos + fullGroups * bitsPerValue;
      int remBits = rem * bitsPerValue;
      int remBytes = (remBits + 7) >>> 3;
      long word = 0;
      for (int k = 0; k < remBytes; k += 1) {
        word = (word << 8) | (data.get(dBase + k) & 0xFF);
      }
      word >>>= (remBytes << 3) - remBits;
      for (int k = rem - 1; k >= 0; k--) {
        output[oBase + k] = (int) (word & mask);
        word >>>= bitsPerValue;
      }
    }
  }
}
