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
  public void testBijectionTwoDimensions() {
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
  public void testLocalityTwoDimensions() {
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

  /** A single dimension degenerates to the identity ordering. */
  @Test
  public void testSingleDimensionIsIdentity() {
    for (int x = 0; x < 256; x++) {
      byte[][] cols = new byte[][] {new byte[] {(byte) x}};
      assertThat(toInt(HilbertByteUtils.hilbertIndex(cols, 8))).isEqualTo(x);
    }
  }

  /** Same input always yields the same output, and the output has the expected length. */
  @Test
  public void testDeterministicAndSized() {
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
  public void testReadsLeadingBytesOnly() {
    byte[][] leadingOnly =
        new byte[][] {new byte[] {5, 0, 0, 0, 0, 0, 0, 0}, new byte[] {9, 0, 0, 0, 0, 0, 0, 0}};
    byte[][] withTrailing =
        new byte[][] {new byte[] {5, 77, 0, 0, 0, 0, 0, 0}, new byte[] {9, 13, 0, 0, 0, 0, 0, 0}};
    assertThat(HilbertByteUtils.hilbertIndex(leadingOnly, 8))
        .isEqualTo(HilbertByteUtils.hilbertIndex(withTrailing, 8));
  }

  @Test
  public void testInvalidBitsPerColumn() {
    byte[][] cols = new byte[][] {new byte[] {0}, new byte[] {0}};
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(cols, 7))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("multiple of 8");
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(cols, 72))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no greater than 64");
  }

  @Test
  public void testColumnTooShort() {
    byte[][] cols = new byte[][] {new byte[] {0}, new byte[] {0}};
    assertThatThrownBy(() -> HilbertByteUtils.hilbertIndex(cols, 16))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("bytes");
  }
}
