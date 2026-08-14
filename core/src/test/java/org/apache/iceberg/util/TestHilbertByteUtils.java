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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

public class TestHilbertByteUtils {

  private static int toInt(byte[] bytes) {
    int value = 0;
    for (byte b : bytes) {
      value = (value << 8) | (b & 0xFF);
    }
    return value;
  }

  /**
   * Over an 8-bit, 2-dimensional space (a 256x256 grid) the Hilbert index must be a bijection onto
   * 0..65535: every (x, y) maps to a distinct index and every index is produced exactly once.
   */
  @Test
  public void bijectionInTwoDimensions() {
    Set<Integer> seen = Sets.newHashSet();
    for (int x = 0; x < 256; x++) {
      for (int y = 0; y < 256; y++) {
        byte[][] cols = new byte[][] {new byte[] {(byte) x}, new byte[] {(byte) y}};
        int index = toInt(HilbertByteUtils.hilbertIndex(cols, 8));
        assertThat(seen.add(index))
            .as("duplicate hilbert index %s for (%s,%s)", index, x, y)
            .isTrue();
        assertThat(index).isBetween(0, 65535);
      }
    }
    assertThat(seen).hasSize(65536);
  }

  /**
   * The defining Hilbert property: consecutive index values map to grid points that are direct
   * neighbours (Manhattan distance 1). This is what distinguishes it from Z-order.
   */
  @Test
  public void localityInTwoDimensions() {
    int[] xByIndex = new int[65536];
    int[] yByIndex = new int[65536];
    for (int x = 0; x < 256; x++) {
      for (int y = 0; y < 256; y++) {
        byte[][] cols = new byte[][] {new byte[] {(byte) x}, new byte[] {(byte) y}};
        int index = toInt(HilbertByteUtils.hilbertIndex(cols, 8));
        xByIndex[index] = x;
        yByIndex[index] = y;
      }
    }
    for (int index = 1; index < 65536; index++) {
      int manhattan =
          Math.abs(xByIndex[index] - xByIndex[index - 1])
              + Math.abs(yByIndex[index] - yByIndex[index - 1]);
      assertThat(manhattan).as("indices %s and %s are not adjacent", index - 1, index).isEqualTo(1);
    }
  }

  /**
   * The same bijection and adjacency properties over an 8-bit, 3-dimensional space (a 256x256x256
   * cube), which exercises the Gray-encode and adjust steps with more than two axes.
   */
  @Test
  public void bijectionAndLocalityInThreeDimensions() {
    int points = 1 << 24;
    // packs (x, y, z) into the low three bytes of an int, keyed by Hilbert index
    int[] pointByIndex = new int[points];
    BitSet seen = new BitSet(points);
    byte[][] cols = new byte[][] {new byte[1], new byte[1], new byte[1]};
    ByteBuffer reuse = ByteBuffer.allocate(3);

    for (int x = 0; x < 256; x++) {
      for (int y = 0; y < 256; y++) {
        for (int z = 0; z < 256; z++) {
          cols[0][0] = (byte) x;
          cols[1][0] = (byte) y;
          cols[2][0] = (byte) z;
          int index = toInt(HilbertByteUtils.hilbertIndex(cols, 8, reuse));
          // asserting only on failure keeps this 2^24-point sweep fast
          if (index < 0 || index >= points || seen.get(index)) {
            assertThat(index).as("hilbert index for (%s,%s,%s)", x, y, z).isBetween(0, points - 1);
            fail("duplicate hilbert index %s for (%s,%s,%s)", index, x, y, z);
          }
          seen.set(index);
          pointByIndex[index] = (x << 16) | (y << 8) | z;
        }
      }
    }

    assertThat(seen.cardinality()).isEqualTo(points);
    for (int index = 1; index < points; index++) {
      int distance = manhattan(pointByIndex[index - 1], pointByIndex[index], 3);
      if (distance != 1) {
        fail("indices %s and %s are %s apart, expected 1", index - 1, index, distance);
      }
    }
  }

  /**
   * Enumerating an 8-bit, 4-dimensional space in full would require 2^32 points, so this walks the
   * [0, 16)^4 sub-cube instead. Because only the low four bits of each axis are set, those points
   * occupy exactly the first 2^16 indices of the curve, so bijection and adjacency are still exact
   * rather than sampled.
   */
  @Test
  public void bijectionAndLocalityInFourDimensions() {
    int points = 1 << 16; // 16^4, four bits per axis
    int[] pointByIndex = new int[points];
    BitSet seen = new BitSet(points);
    byte[][] cols = new byte[][] {new byte[1], new byte[1], new byte[1], new byte[1]};
    ByteBuffer reuse = ByteBuffer.allocate(4);

    // one flat sweep over the sub-cube, four bits per axis
    for (int point = 0; point < points; point++) {
      int axis0 = (point >> 12) & 0xF;
      int axis1 = (point >> 8) & 0xF;
      int axis2 = (point >> 4) & 0xF;
      int axis3 = point & 0xF;
      cols[0][0] = (byte) axis0;
      cols[1][0] = (byte) axis1;
      cols[2][0] = (byte) axis2;
      cols[3][0] = (byte) axis3;
      int index = toInt(HilbertByteUtils.hilbertIndex(cols, 8, reuse));
      assertThat(index)
          .as("hilbert index for (%s,%s,%s,%s)", axis0, axis1, axis2, axis3)
          .isBetween(0, points - 1);
      assertThat(seen.get(index))
          .as("duplicate hilbert index %s for (%s,%s,%s,%s)", index, axis0, axis1, axis2, axis3)
          .isFalse();
      seen.set(index);
      pointByIndex[index] = (axis0 << 24) | (axis1 << 16) | (axis2 << 8) | axis3;
    }

    assertThat(seen.cardinality()).isEqualTo(points);
    for (int index = 1; index < points; index++) {
      assertThat(manhattan(pointByIndex[index - 1], pointByIndex[index], 4))
          .as("indices %s and %s are not adjacent", index - 1, index)
          .isEqualTo(1);
    }
  }

  /** Manhattan distance between two points packed one axis per byte. */
  private static int manhattan(int left, int right, int dimensions) {
    int distance = 0;
    for (int axis = 0; axis < dimensions; axis++) {
      int shift = axis * 8;
      distance += Math.abs(((left >> shift) & 0xFF) - ((right >> shift) & 0xFF));
    }
    return distance;
  }

  /** A single dimension degenerates to the identity ordering. */
  @Test
  public void singleDimensionIsIdentity() {
    for (int x = 0; x < 256; x++) {
      byte[][] cols = new byte[][] {new byte[] {(byte) x}};
      assertThat(toInt(HilbertByteUtils.hilbertIndex(cols, 8))).isEqualTo(x);
    }
  }

  /** Same input always yields the same output, and the output has the expected length. */
  @Test
  public void deterministicAndSized() {
    byte[][] cols =
        new byte[][] {
          new byte[] {1, 2, 3, 4, 5, 6, 7, 8},
          new byte[] {8, 7, 6, 5, 4, 3, 2, 1},
          new byte[] {0, 0, 0, 0, 0, 0, 0, 9}
        };
    byte[] first = HilbertByteUtils.hilbertIndex(cols, 64);
    byte[] second = HilbertByteUtils.hilbertIndex(cols, 64);
    assertThat(first).isEqualTo(second);
    assertThat(first).hasSize(3 * 8);
  }

  /** Only the high {@code bitsPerColumn} bits of each column participate. */
  @Test
  public void readsLeadingBytesOnly() {
    byte[][] leadingOnly =
        new byte[][] {new byte[] {5, 0, 0, 0, 0, 0, 0, 0}, new byte[] {9, 0, 0, 0, 0, 0, 0, 0}};
    byte[][] withTrailing =
        new byte[][] {new byte[] {5, 77, 0, 0, 0, 0, 0, 0}, new byte[] {9, 13, 0, 0, 0, 0, 0, 0}};
    assertThat(HilbertByteUtils.hilbertIndex(leadingOnly, 8))
        .isEqualTo(HilbertByteUtils.hilbertIndex(withTrailing, 8));
  }

  @Test
  public void bitsPerColumnMustBeAMultipleOfEight() {
    byte[][] cols = new byte[][] {new byte[] {0}, new byte[] {0}};
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(cols, 7))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Hilbert bitsPerColumn must be a positive multiple of 8, was 7");
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(cols, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Hilbert bitsPerColumn must be a positive multiple of 8, was 0");
  }

  @Test
  public void bitsPerColumnMustFitInALong() {
    byte[][] cols = new byte[][] {new byte[] {0}, new byte[] {0}};
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(cols, 72))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Hilbert bitsPerColumn must be no greater than 64, was 72");
  }

  @Test
  public void zeroColumnsIsRejected() {
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(new byte[0][], 8))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot compute a Hilbert index for zero columns");
  }

  @Test
  public void columnShorterThanBitsPerColumnIsRejected() {
    // the second column is one byte short of the two bytes that bitsPerColumn = 16 requires
    byte[][] cols = new byte[][] {new byte[] {0, 0}, new byte[] {0}};
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(cols, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Column 1 contributes 1 bytes but 2 are required");
  }
}
