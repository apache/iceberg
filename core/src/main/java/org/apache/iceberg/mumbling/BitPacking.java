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
import org.apache.iceberg.util.ByteBuffers;

/**
 * Bit packing and unpacking for values of 1–7 bits.
 *
 * <p>The least-significant bits of each value are packed with the first value occupying the most
 * significant bits of the output. Output is padded to the nearest byte with 0s. For example,
 * packing values [0b11, 0b10, 0b01] with width 2 produces 0b11100100.
 */
class BitPacking {

  private BitPacking() {}

  /**
   * Packs {@code width} least-significant bits of {@code count} values into a data buffer.
   *
   * <p>Output is padded to the nearest byte with 0s.
   *
   * <p>Values are written to the buffer's underlying storage, but the buffer's position and limit
   * are not modified.
   *
   * @param width number of bits of each value to pack
   * @param values array containing source values to pack
   * @param valueOffset starting index of values to pack
   * @param data an output {@link ByteBuffer}
   * @param dataOffset starting index for output in the data buffer
   * @param count the number of values to pack
   * @return the number of bytes written to the data buffer
   */
  static int packBits(
      int width, int[] values, int valueOffset, ByteBuffer data, int dataOffset, int count) {
    return switch (width) {
      case 0 -> 0;
      case 1 -> packBits1(values, valueOffset, data, dataOffset, count);
      case 2 -> packBits2(values, valueOffset, data, dataOffset, count);
      case 3 -> packBits3(values, valueOffset, data, dataOffset, count);
      case 4 -> packBits4(values, valueOffset, data, dataOffset, count);
      case 5 -> packBits5(values, valueOffset, data, dataOffset, count);
      case 6 -> packBits6(values, valueOffset, data, dataOffset, count);
      case 7 -> packBits7(values, valueOffset, data, dataOffset, count);
      case 8 -> copyAsBytes(values, valueOffset, data, dataOffset, count);
      default -> throw new IllegalArgumentException("Invalid bit width: " + width);
    };
  }

  /**
   * Unpacks {@code count} values from a data buffer containing {@code width} bits of each value.
   *
   * <p>Unused bits in the last input byte are ignored.
   *
   * <p>The input buffer's position and limit are not modified.
   *
   * @param width number of bits of each value to unpack
   * @param data an input {@link ByteBuffer}
   * @param dataOffset starting index for input in the data buffer
   * @param output array for unpacked output values
   * @param outputOffset starting index to store values in the output array
   * @param count the number of values to unpack
   * @return the number of bytes read from the data buffer
   */
  static int unpackBits(
      int width, ByteBuffer data, int dataOffset, int[] output, int outputOffset, int count) {
    return switch (width) {
      case 0 -> 0;
      case 1 -> unpackBits1(data, dataOffset, output, outputOffset, count);
      case 2 -> unpackBits2(data, dataOffset, output, outputOffset, count);
      case 3 -> unpackBits3(data, dataOffset, output, outputOffset, count);
      case 4 -> unpackBits4(data, dataOffset, output, outputOffset, count);
      case 5 -> unpackBits5(data, dataOffset, output, outputOffset, count);
      case 6 -> unpackBits6(data, dataOffset, output, outputOffset, count);
      case 7 -> unpackBits7(data, dataOffset, output, outputOffset, count);
      case 8 -> copyAsBytes(data, dataOffset, output, outputOffset, count);
      default -> throw new IllegalArgumentException("Invalid bit width: " + width);
    };
  }

  /**
   * Copy byte values from src into a buffer.
   *
   * <p>Values must be bytes stored in an integer array. The 3 most significant bytes of the values
   * are ignored.
   *
   * @param source array of source values to copy
   * @param sourceOffset starting offset of values to copy
   * @param out output buffer values will be copied to
   * @param outOffset starting offset in the output buffer
   * @param count number of values (bytes) to copy
   * @return the number of bytes written to the buffer
   */
  private static int copyAsBytes(
      int[] source, int sourceOffset, ByteBuffer out, int outOffset, int count) {
    for (int i = 0; i < count; i += 1) {
      ByteBuffers.writeByte(out, source[sourceOffset + i], outOffset + i);
    }

    return count;
  }

  /**
   * Copy byte values from a buffer into an int[].
   *
   * @param data buffer of source values to copyß
   * @param dataOffset starting offset in the input buffer
   * @param out output array values will be copied to
   * @param outOffset starting offset in the output buffer
   * @param count number of values (bytes) to copy
   * @return the number of bytes read from the buffer
   */
  private static int copyAsBytes(
      ByteBuffer data, int dataOffset, int[] out, int outOffset, int count) {
    for (int i = 0; i < count; i += 1) {
      out[outOffset + i] = ByteBuffers.readByte(data, dataOffset + i);
    }

    return count;
  }

  // ---------------------------------------------------------------------------
  // Specialized pack: width=1..7
  // Each method packs 8 values into exactly width bytes (full groups), plus a
  // partial group of remaining < 8 values in ceil(remaining*width/8) bytes.
  // ---------------------------------------------------------------------------

  /** 1-bit values: 8 values into 1 byte. */
  private static int packBits1(
      int[] values, int valueOffset, ByteBuffer data, int dataOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = valueOffset + 8 * group;
      int outputOffset = dataOffset + group;
      int word =
          packWord1(
              values[groupOffset],
              values[groupOffset + 1],
              values[groupOffset + 2],
              values[groupOffset + 3],
              values[groupOffset + 4],
              values[groupOffset + 5],
              values[groupOffset + 6],
              values[groupOffset + 7]);
      ByteBuffers.writeByte(data, word, outputOffset);
    }

    int remaining = count % 8;
    if (remaining > 0) {
      int groupOffset = valueOffset + 8 * fullGroups;
      int outputOffset = dataOffset + fullGroups;
      int word =
          packWord1(
              values[groupOffset],
              remaining > 1 ? values[groupOffset + 1] : 0,
              remaining > 2 ? values[groupOffset + 2] : 0,
              remaining > 3 ? values[groupOffset + 3] : 0,
              remaining > 4 ? values[groupOffset + 4] : 0,
              remaining > 5 ? values[groupOffset + 5] : 0,
              remaining > 6 ? values[groupOffset + 6] : 0,
              0);
      ByteBuffers.writeByte(data, word, outputOffset);
    }

    return byteWidth(count);
  }

  private static int packWord1(int a, int b, int c, int d, int e, int f, int g, int h) {
    return ((a & 0b1) << 7)
        | ((b & 0b1) << 6)
        | ((c & 0b1) << 5)
        | ((d & 0b1) << 4)
        | ((e & 0b1) << 3)
        | ((f & 0b1) << 2)
        | ((g & 0b1) << 1)
        | (h & 0b1);
  }

  /** 2-bit values: 8 values into 2 bytes. */
  private static int packBits2(
      int[] values, int valueOffset, ByteBuffer data, int dataOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = valueOffset + 8 * group;
      int outputOffset = dataOffset + 2 * group;
      int word =
          packWord2(
              values[groupOffset],
              values[groupOffset + 1],
              values[groupOffset + 2],
              values[groupOffset + 3],
              values[groupOffset + 4],
              values[groupOffset + 5],
              values[groupOffset + 6],
              values[groupOffset + 7]);
      ByteBuffers.writeByte(data, word >>> 8, outputOffset);
      ByteBuffers.writeByte(data, word, outputOffset + 1);
    }

    int remaining = count % 8;
    if (remaining > 0) {
      int groupOffset = valueOffset + 8 * fullGroups;
      int outputOffset = dataOffset + 2 * fullGroups;
      int word =
          packWord2(
              values[groupOffset],
              remaining > 1 ? values[groupOffset + 1] : 0,
              remaining > 2 ? values[groupOffset + 2] : 0,
              remaining > 3 ? values[groupOffset + 3] : 0,
              remaining > 4 ? values[groupOffset + 4] : 0,
              remaining > 5 ? values[groupOffset + 5] : 0,
              remaining > 6 ? values[groupOffset + 6] : 0,
              0);
      ByteBuffers.writeByte(data, word >>> 8, outputOffset);
      if (remaining > 4) {
        ByteBuffers.writeByte(data, word, outputOffset + 1);
      }
    }

    return byteWidth(2 * count);
  }

  private static int packWord2(int a, int b, int c, int d, int e, int f, int g, int h) {
    return ((a & 0b11) << 14)
        | ((b & 0b11) << 12)
        | ((c & 0b11) << 10)
        | ((d & 0b11) << 8)
        | ((e & 0b11) << 6)
        | ((f & 0b11) << 4)
        | ((g & 0b11) << 2)
        | (h & 0b11);
  }

  /** 3-bit values: 8 values into 3 bytes. */
  private static int packBits3(
      int[] values, int valueOffset, ByteBuffer data, int dataOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = valueOffset + 8 * group;
      int outputOffset = dataOffset + 3 * group;
      int word =
          packWord3(
              values[groupOffset],
              values[groupOffset + 1],
              values[groupOffset + 2],
              values[groupOffset + 3],
              values[groupOffset + 4],
              values[groupOffset + 5],
              values[groupOffset + 6],
              values[groupOffset + 7]);
      ByteBuffers.writeByte(data, word >>> 16, outputOffset);
      ByteBuffers.writeByte(data, word >>> 8, outputOffset + 1);
      ByteBuffers.writeByte(data, word, outputOffset + 2);
    }

    int remaining = count % 8;
    if (remaining > 0) {
      int groupOffset = valueOffset + 8 * fullGroups;
      int outputOffset = dataOffset + 3 * fullGroups;
      int word =
          packWord3(
              values[groupOffset],
              remaining > 1 ? values[groupOffset + 1] : 0,
              remaining > 2 ? values[groupOffset + 2] : 0,
              remaining > 3 ? values[groupOffset + 3] : 0,
              remaining > 4 ? values[groupOffset + 4] : 0,
              remaining > 5 ? values[groupOffset + 5] : 0,
              remaining > 6 ? values[groupOffset + 6] : 0,
              0);
      int byteCount = byteWidth(3 * remaining);
      for (int k = 0; k < byteCount; k += 1) {
        ByteBuffers.writeByte(data, word >>> (16 - 8 * k), outputOffset + k);
      }
    }

    return byteWidth(3 * count);
  }

  private static int packWord3(int a, int b, int c, int d, int e, int f, int g, int h) {
    return ((a & 0b111) << 21)
        | ((b & 0b111) << 18)
        | ((c & 0b111) << 15)
        | ((d & 0b111) << 12)
        | ((e & 0b111) << 9)
        | ((f & 0b111) << 6)
        | ((g & 0b111) << 3)
        | (h & 0b111);
  }

  /** 4-bit values: 8 values into 4 bytes. */
  private static int packBits4(
      int[] values, int valueOffset, ByteBuffer data, int dataOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = valueOffset + 8 * group;
      int outputOffset = dataOffset + 4 * group;
      int word =
          packWord4(
              values[groupOffset],
              values[groupOffset + 1],
              values[groupOffset + 2],
              values[groupOffset + 3],
              values[groupOffset + 4],
              values[groupOffset + 5],
              values[groupOffset + 6],
              values[groupOffset + 7]);
      ByteBuffers.writeByte(data, word >>> 24, outputOffset);
      ByteBuffers.writeByte(data, word >>> 16, outputOffset + 1);
      ByteBuffers.writeByte(data, word >>> 8, outputOffset + 2);
      ByteBuffers.writeByte(data, word, outputOffset + 3);
    }

    int remaining = count % 8;
    if (remaining > 0) {
      int groupOffset = valueOffset + 8 * fullGroups;
      int outputOffset = dataOffset + 4 * fullGroups;
      int word =
          packWord4(
              values[groupOffset],
              remaining > 1 ? values[groupOffset + 1] : 0,
              remaining > 2 ? values[groupOffset + 2] : 0,
              remaining > 3 ? values[groupOffset + 3] : 0,
              remaining > 4 ? values[groupOffset + 4] : 0,
              remaining > 5 ? values[groupOffset + 5] : 0,
              remaining > 6 ? values[groupOffset + 6] : 0,
              0);
      int byteCount = byteWidth(4 * remaining);
      for (int k = 0; k < byteCount; k += 1) {
        ByteBuffers.writeByte(data, word >>> (24 - 8 * k), outputOffset + k);
      }
    }

    return byteWidth(4 * count);
  }

  private static int packWord4(int a, int b, int c, int d, int e, int f, int g, int h) {
    return ((a & 0b1111) << 28)
        | ((b & 0b1111) << 24)
        | ((c & 0b1111) << 20)
        | ((d & 0b1111) << 16)
        | ((e & 0b1111) << 12)
        | ((f & 0b1111) << 8)
        | ((g & 0b1111) << 4)
        | (h & 0b1111);
  }

  /** 5-bit values: 8 values into 5 bytes. */
  private static int packBits5(
      int[] values, int valueOffset, ByteBuffer data, int dataOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = valueOffset + 8 * group;
      int outputOffset = dataOffset + 5 * group;
      long word =
          packWord5(
              values[groupOffset],
              values[groupOffset + 1],
              values[groupOffset + 2],
              values[groupOffset + 3],
              values[groupOffset + 4],
              values[groupOffset + 5],
              values[groupOffset + 6],
              values[groupOffset + 7]);
      ByteBuffers.writeByte(data, (int) (word >>> 32), outputOffset);
      ByteBuffers.writeByte(data, (int) (word >>> 24), outputOffset + 1);
      ByteBuffers.writeByte(data, (int) (word >>> 16), outputOffset + 2);
      ByteBuffers.writeByte(data, (int) (word >>> 8), outputOffset + 3);
      ByteBuffers.writeByte(data, (int) word, outputOffset + 4);
    }

    int remaining = count % 8;
    if (remaining > 0) {
      int groupOffset = valueOffset + 8 * fullGroups;
      int outputOffset = dataOffset + 5 * fullGroups;
      long word =
          packWord5(
              values[groupOffset],
              remaining > 1 ? values[groupOffset + 1] : 0,
              remaining > 2 ? values[groupOffset + 2] : 0,
              remaining > 3 ? values[groupOffset + 3] : 0,
              remaining > 4 ? values[groupOffset + 4] : 0,
              remaining > 5 ? values[groupOffset + 5] : 0,
              remaining > 6 ? values[groupOffset + 6] : 0,
              0);
      int byteCount = byteWidth(5 * remaining);
      for (int k = 0; k < byteCount; k += 1) {
        ByteBuffers.writeByte(data, (int) (word >>> (32 - 8 * k)), outputOffset + k);
      }
    }

    return byteWidth(5 * count);
  }

  private static long packWord5(int a, int b, int c, int d, int e, int f, int g, int h) {
    return ((long) (a & 0b11111) << 35)
        | ((long) (b & 0b11111) << 30)
        | ((long) (c & 0b11111) << 25)
        | ((long) (d & 0b11111) << 20)
        | ((long) (e & 0b11111) << 15)
        | ((long) (f & 0b11111) << 10)
        | ((long) (g & 0b11111) << 5)
        | (long) (h & 0b11111);
  }

  /** 6-bit values: 8 values into 6 bytes. */
  private static int packBits6(
      int[] values, int valueOffset, ByteBuffer data, int dataOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = valueOffset + 8 * group;
      int outputOffset = dataOffset + 6 * group;
      long word =
          packWord6(
              values[groupOffset],
              values[groupOffset + 1],
              values[groupOffset + 2],
              values[groupOffset + 3],
              values[groupOffset + 4],
              values[groupOffset + 5],
              values[groupOffset + 6],
              values[groupOffset + 7]);
      ByteBuffers.writeByte(data, (int) (word >>> 40), outputOffset);
      ByteBuffers.writeByte(data, (int) (word >>> 32), outputOffset + 1);
      ByteBuffers.writeByte(data, (int) (word >>> 24), outputOffset + 2);
      ByteBuffers.writeByte(data, (int) (word >>> 16), outputOffset + 3);
      ByteBuffers.writeByte(data, (int) (word >>> 8), outputOffset + 4);
      ByteBuffers.writeByte(data, (int) word, outputOffset + 5);
    }

    int remaining = count % 8;
    if (remaining > 0) {
      int groupOffset = valueOffset + 8 * fullGroups;
      int outputOffset = dataOffset + 6 * fullGroups;
      long word =
          packWord6(
              values[groupOffset],
              remaining > 1 ? values[groupOffset + 1] : 0,
              remaining > 2 ? values[groupOffset + 2] : 0,
              remaining > 3 ? values[groupOffset + 3] : 0,
              remaining > 4 ? values[groupOffset + 4] : 0,
              remaining > 5 ? values[groupOffset + 5] : 0,
              remaining > 6 ? values[groupOffset + 6] : 0,
              0);
      int byteCount = byteWidth(6 * remaining);
      for (int k = 0; k < byteCount; k += 1) {
        ByteBuffers.writeByte(data, (int) (word >>> (40 - 8 * k)), outputOffset + k);
      }
    }

    return byteWidth(6 * count);
  }

  private static long packWord6(int a, int b, int c, int d, int e, int f, int g, int h) {
    return ((long) (a & 0b111111) << 42)
        | ((long) (b & 0b111111) << 36)
        | ((long) (c & 0b111111) << 30)
        | ((long) (d & 0b111111) << 24)
        | ((long) (e & 0b111111) << 18)
        | ((long) (f & 0b111111) << 12)
        | ((long) (g & 0b111111) << 6)
        | (long) (h & 0b111111);
  }

  /** 7-bit values: 8 values into 7 bytes. */
  private static int packBits7(
      int[] values, int valueOffset, ByteBuffer data, int dataOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = valueOffset + 8 * group;
      int outputOffset = dataOffset + 7 * group;
      long word =
          packWord7(
              values[groupOffset],
              values[groupOffset + 1],
              values[groupOffset + 2],
              values[groupOffset + 3],
              values[groupOffset + 4],
              values[groupOffset + 5],
              values[groupOffset + 6],
              values[groupOffset + 7]);
      ByteBuffers.writeByte(data, (int) (word >>> 48), outputOffset);
      ByteBuffers.writeByte(data, (int) (word >>> 40), outputOffset + 1);
      ByteBuffers.writeByte(data, (int) (word >>> 32), outputOffset + 2);
      ByteBuffers.writeByte(data, (int) (word >>> 24), outputOffset + 3);
      ByteBuffers.writeByte(data, (int) (word >>> 16), outputOffset + 4);
      ByteBuffers.writeByte(data, (int) (word >>> 8), outputOffset + 5);
      ByteBuffers.writeByte(data, (int) word, outputOffset + 6);
    }

    int remaining = count % 8;
    if (remaining > 0) {
      int groupOffset = valueOffset + 8 * fullGroups;
      int outputOffset = dataOffset + 7 * fullGroups;
      long word =
          packWord7(
              values[groupOffset],
              remaining > 1 ? values[groupOffset + 1] : 0,
              remaining > 2 ? values[groupOffset + 2] : 0,
              remaining > 3 ? values[groupOffset + 3] : 0,
              remaining > 4 ? values[groupOffset + 4] : 0,
              remaining > 5 ? values[groupOffset + 5] : 0,
              remaining > 6 ? values[groupOffset + 6] : 0,
              0);
      int byteCount = byteWidth(7 * remaining);
      for (int k = 0; k < byteCount; k += 1) {
        ByteBuffers.writeByte(data, (int) (word >>> (48 - 8 * k)), outputOffset + k);
      }
    }

    return byteWidth(7 * count);
  }

  private static long packWord7(int a, int b, int c, int d, int e, int f, int g, int h) {
    return ((long) (a & 0b1111111) << 49)
        | ((long) (b & 0b1111111) << 42)
        | ((long) (c & 0b1111111) << 35)
        | ((long) (d & 0b1111111) << 28)
        | ((long) (e & 0b1111111) << 21)
        | ((long) (f & 0b1111111) << 14)
        | ((long) (g & 0b1111111) << 7)
        | (long) (h & 0b1111111);
  }

  // ---------------------------------------------------------------------------
  // Specialized unpack: width=1..7
  // ---------------------------------------------------------------------------

  /** 1-bit values: 1 byte into 8 values. */
  private static int unpackBits1(
      ByteBuffer data, int dataOffset, int[] output, int outputOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = outputOffset + 8 * group;
      int word = (int) readWord(data, dataOffset + group, 1);
      output[groupOffset] = (word >>> 7) & 0b1;
      output[groupOffset + 1] = (word >>> 6) & 0b1;
      output[groupOffset + 2] = (word >>> 5) & 0b1;
      output[groupOffset + 3] = (word >>> 4) & 0b1;
      output[groupOffset + 4] = (word >>> 3) & 0b1;
      output[groupOffset + 5] = (word >>> 2) & 0b1;
      output[groupOffset + 6] = (word >>> 1) & 0b1;
      output[groupOffset + 7] = word & 0b1;
    }

    int remaining = count % 8;
    if (remaining > 0) {
      long word = readWord(data, dataOffset + fullGroups, byteWidth(remaining));
      unpackRemainder(1, word, output, outputOffset + 8 * fullGroups, remaining);
    }

    return byteWidth(count);
  }

  /** 2-bit values: 2 bytes into 8 values. */
  private static int unpackBits2(
      ByteBuffer data, int dataOffset, int[] output, int outputOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = outputOffset + 8 * group;
      int word = (int) readWord(data, dataOffset + 2 * group, 2);
      output[groupOffset] = (word >>> 14) & 0b11;
      output[groupOffset + 1] = (word >>> 12) & 0b11;
      output[groupOffset + 2] = (word >>> 10) & 0b11;
      output[groupOffset + 3] = (word >>> 8) & 0b11;
      output[groupOffset + 4] = (word >>> 6) & 0b11;
      output[groupOffset + 5] = (word >>> 4) & 0b11;
      output[groupOffset + 6] = (word >>> 2) & 0b11;
      output[groupOffset + 7] = word & 0b11;
    }

    int remaining = count % 8;
    if (remaining > 0) {
      long word = readWord(data, dataOffset + 2 * fullGroups, byteWidth(2 * remaining));
      unpackRemainder(2, word, output, outputOffset + 8 * fullGroups, remaining);
    }

    return byteWidth(2 * count);
  }

  /** 3-bit values: 3 bytes into 8 values. */
  private static int unpackBits3(
      ByteBuffer data, int dataOffset, int[] output, int outputOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = outputOffset + 8 * group;
      int word = (int) readWord(data, dataOffset + 3 * group, 3);
      output[groupOffset] = (word >>> 21) & 0b111;
      output[groupOffset + 1] = (word >>> 18) & 0b111;
      output[groupOffset + 2] = (word >>> 15) & 0b111;
      output[groupOffset + 3] = (word >>> 12) & 0b111;
      output[groupOffset + 4] = (word >>> 9) & 0b111;
      output[groupOffset + 5] = (word >>> 6) & 0b111;
      output[groupOffset + 6] = (word >>> 3) & 0b111;
      output[groupOffset + 7] = word & 0b111;
    }

    int remaining = count % 8;
    if (remaining > 0) {
      long word = readWord(data, dataOffset + 3 * fullGroups, byteWidth(3 * remaining));
      unpackRemainder(3, word, output, outputOffset + 8 * fullGroups, remaining);
    }

    return byteWidth(3 * count);
  }

  /** 4-bit values: 4 bytes into 8 values. */
  private static int unpackBits4(
      ByteBuffer data, int dataOffset, int[] output, int outputOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = outputOffset + 8 * group;
      long word = readWord(data, dataOffset + 4 * group, 4);
      output[groupOffset] = (int) (word >>> 28) & 0b1111;
      output[groupOffset + 1] = (int) (word >>> 24) & 0b1111;
      output[groupOffset + 2] = (int) (word >>> 20) & 0b1111;
      output[groupOffset + 3] = (int) (word >>> 16) & 0b1111;
      output[groupOffset + 4] = (int) (word >>> 12) & 0b1111;
      output[groupOffset + 5] = (int) (word >>> 8) & 0b1111;
      output[groupOffset + 6] = (int) (word >>> 4) & 0b1111;
      output[groupOffset + 7] = (int) word & 0b1111;
    }

    int remaining = count % 8;
    if (remaining > 0) {
      long word = readWord(data, dataOffset + 4 * fullGroups, byteWidth(4 * remaining));
      unpackRemainder(4, word, output, outputOffset + 8 * fullGroups, remaining);
    }

    return byteWidth(4 * count);
  }

  /** 5-bit values: 5 bytes into 8 values. */
  private static int unpackBits5(
      ByteBuffer data, int dataOffset, int[] output, int outputOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = outputOffset + 8 * group;
      long word = readWord(data, dataOffset + 5 * group, 5);
      output[groupOffset] = (int) (word >>> 35) & 0b11111;
      output[groupOffset + 1] = (int) (word >>> 30) & 0b11111;
      output[groupOffset + 2] = (int) (word >>> 25) & 0b11111;
      output[groupOffset + 3] = (int) (word >>> 20) & 0b11111;
      output[groupOffset + 4] = (int) (word >>> 15) & 0b11111;
      output[groupOffset + 5] = (int) (word >>> 10) & 0b11111;
      output[groupOffset + 6] = (int) (word >>> 5) & 0b11111;
      output[groupOffset + 7] = (int) word & 0b11111;
    }

    int remaining = count % 8;
    if (remaining > 0) {
      long word = readWord(data, dataOffset + 5 * fullGroups, byteWidth(5 * remaining));
      unpackRemainder(5, word, output, outputOffset + 8 * fullGroups, remaining);
    }

    return byteWidth(5 * count);
  }

  /** 6-bit values: 6 bytes into 8 values. */
  private static int unpackBits6(
      ByteBuffer data, int dataOffset, int[] output, int outputOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = outputOffset + 8 * group;
      long word = readWord(data, dataOffset + 6 * group, 6);
      output[groupOffset] = (int) (word >>> 42) & 0b111111;
      output[groupOffset + 1] = (int) (word >>> 36) & 0b111111;
      output[groupOffset + 2] = (int) (word >>> 30) & 0b111111;
      output[groupOffset + 3] = (int) (word >>> 24) & 0b111111;
      output[groupOffset + 4] = (int) (word >>> 18) & 0b111111;
      output[groupOffset + 5] = (int) (word >>> 12) & 0b111111;
      output[groupOffset + 6] = (int) (word >>> 6) & 0b111111;
      output[groupOffset + 7] = (int) word & 0b111111;
    }

    int remaining = count % 8;
    if (remaining > 0) {
      long word = readWord(data, dataOffset + 6 * fullGroups, byteWidth(6 * remaining));
      unpackRemainder(6, word, output, outputOffset + 8 * fullGroups, remaining);
    }

    return byteWidth(6 * count);
  }

  /** 7-bit values: 7 bytes into 8 values. */
  private static int unpackBits7(
      ByteBuffer data, int dataOffset, int[] output, int outputOffset, int count) {
    int fullGroups = count / 8;
    for (int group = 0; group < fullGroups; group += 1) {
      int groupOffset = outputOffset + 8 * group;
      long word = readWord(data, dataOffset + 7 * group, 7);
      output[groupOffset] = (int) (word >>> 49) & 0b1111111;
      output[groupOffset + 1] = (int) (word >>> 42) & 0b1111111;
      output[groupOffset + 2] = (int) (word >>> 35) & 0b1111111;
      output[groupOffset + 3] = (int) (word >>> 28) & 0b1111111;
      output[groupOffset + 4] = (int) (word >>> 21) & 0b1111111;
      output[groupOffset + 5] = (int) (word >>> 14) & 0b1111111;
      output[groupOffset + 6] = (int) (word >>> 7) & 0b1111111;
      output[groupOffset + 7] = (int) word & 0b1111111;
    }

    int remaining = count % 8;
    if (remaining > 0) {
      long word = readWord(data, dataOffset + 7 * fullGroups, byteWidth(7 * remaining));
      unpackRemainder(7, word, output, outputOffset + 8 * fullGroups, remaining);
    }

    return byteWidth(7 * count);
  }

  /**
   * Unpack {@code count < 8} values of {@code width} bits from {@code word}.
   *
   * @param width number of bits stored for each value
   * @param word a long containing the bytes of the remaining values
   * @param output array for unpacked output values
   * @param outputOffset starting index to store values in the output array
   * @param count number of values to unpack from the word
   */
  private static void unpackRemainder(
      int width, long word, int[] output, int outputOffset, int count) {
    int mask = (1 << width) - 1;
    // value bits are stored in the last bytes of the word
    int valueBytes = byteWidth(count * width);
    // the first value is width bits starting with the most-significant bits of the value bytes
    int shift = 8 * valueBytes - width;
    for (int i = 0; i < count; i += 1) {
      output[outputOffset + i] = (int) ((word >>> shift) & mask);
      shift -= width;
    }
  }

  /**
   * Read {@code count} bytes of data into the last {@code count} bytes of {@code word}.
   *
   * <p>The first input byte occupies the most significant bits of the last {@code count} bytes and
   * the last input byte occupies the least significant bits of the word. The remaining most
   * significant bytes of the word are 0.
   */
  private static long readWord(ByteBuffer data, int offset, int count) {
    long word = 0;
    for (int k = 0; k < count; k += 1) {
      word = (word << 8) | ByteBuffers.readByte(data, offset + k);
    }

    return word;
  }

  /** Returns the number of whole bytes needed to hold {@code bits} bits. */
  private static int byteWidth(int bits) {
    return (bits + 7) / 8;
  }
}
