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

import static org.apache.iceberg.mumbling.MumblingTestUtil.bitmap;
import static org.apache.iceberg.mumbling.MumblingTestUtil.build;
import static org.apache.iceberg.mumbling.MumblingTestUtil.dense;
import static org.apache.iceberg.mumbling.MumblingTestUtil.sparse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class TestMumblingBitmap {

  @Test
  void testEmptyBitmap() {
    MumblingBitmap bitmap = bitmap();
    assertThat(bitmap.cardinality()).isEqualTo(0);

    // all positions beyond the bitmap range are false
    assertThat(bitmap.isSet(0)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
    assertThat(bitmap.isSet(256)).isFalse();
  }

  @Test
  void testInvalidPosition() {
    MumblingBitmap bitmap = bitmap();
    assertThat(bitmap.cardinality()).isEqualTo(0);
    assertThat(bitmap.isSet(0)).isFalse();
    assertThatThrownBy(() -> bitmap.isSet(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid bit position: -1 < 0");
  }

  @Test
  void testEmptySparseContainer() {
    MumblingBitmap bitmap = bitmap(sparse());
    assertThat(bitmap.cardinality()).isEqualTo(0);
    assertThat(bitmap.isSet(0)).isFalse();
    assertThat(bitmap.isSet(100)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
  }

  @Test
  void testSparseContainerSetPositions() {
    MumblingBitmap bitmap = bitmap(sparse(0, 5, 100, 255));
    assertThat(bitmap.cardinality()).isEqualTo(4);

    assertThat(bitmap.isSet(0)).isTrue();
    assertThat(bitmap.isSet(5)).isTrue();
    assertThat(bitmap.isSet(100)).isTrue();
    assertThat(bitmap.isSet(255)).isTrue();

    assertThat(bitmap.isSet(1)).isFalse();
    assertThat(bitmap.isSet(4)).isFalse();
    assertThat(bitmap.isSet(6)).isFalse();
    assertThat(bitmap.isSet(99)).isFalse();
    assertThat(bitmap.isSet(101)).isFalse();
    assertThat(bitmap.isSet(254)).isFalse();
    assertThat(bitmap.isSet(256)).isFalse();
  }

  @Test
  void testFullSparseContainer() {
    int[] positions = new int[31];
    for (int i = 0; i < 31; i += 1) {
      positions[i] = i * 8; // 0, 8, 16, ..., 240
    }

    MumblingBitmap bitmap = bitmap(sparse(positions));
    assertThat(bitmap.cardinality()).isEqualTo(31);

    for (int p : positions) {
      assertThat(bitmap.isSet(p)).isTrue();
    }

    assertThat(bitmap.isSet(1)).isFalse();
    assertThat(bitmap.isSet(7)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
  }

  @Test
  void testFullDenseContainer() {
    byte[] container = new byte[32];
    Arrays.fill(container, (byte) 0xFF);

    MumblingBitmap bitmap = bitmap(dense(container));
    assertThat(bitmap.cardinality()).isEqualTo(256);

    for (int i = 0; i < 256; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }

    assertThat(bitmap.isSet(256)).isFalse();
  }

  // Example 1: positions 0-31: `FF FF FF FF 00 ... 00`
  @Test
  void testDenseSpecExample1() {
    byte[] container = new byte[32];
    container[0] = (byte) 0xFF;
    container[1] = (byte) 0xFF;
    container[2] = (byte) 0xFF;
    container[3] = (byte) 0xFF;
    MumblingBitmap bitmap = bitmap(dense(container));
    assertThat(bitmap.cardinality()).isEqualTo(32);

    for (int i = 0; i <= 31; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }

    assertThat(bitmap.isSet(32)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
  }

  // Example 2: positions 0-32: `FF FF FF FF 80 00 ... 00`
  @Test
  void testDenseSpecExample2() {
    byte[] container = new byte[32];
    container[0] = (byte) 0xFF;
    container[1] = (byte) 0xFF;
    container[2] = (byte) 0xFF;
    container[3] = (byte) 0xFF;
    container[4] = (byte) 0x80;

    MumblingBitmap bitmap = bitmap(dense(container));
    assertThat(bitmap.cardinality()).isEqualTo(33);

    for (int i = 0; i <= 32; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }

    assertThat(bitmap.isSet(33)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
  }

  // Example 3: positions 0-15 and 240-255: `FF FF 00 ... 00 FF FF`
  @Test
  void testDenseSpecExample3() {
    byte[] container = new byte[32];
    container[0] = (byte) 0xFF;
    container[1] = (byte) 0xFF;
    container[30] = (byte) 0xFF;
    container[31] = (byte) 0xFF;

    MumblingBitmap bitmap = bitmap(dense(container));
    assertThat(bitmap.cardinality()).isEqualTo(32);

    for (int i = 0; i <= 15; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
    for (int i = 240; i <= 255; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
    assertThat(bitmap.isSet(16)).isFalse();
    assertThat(bitmap.isSet(239)).isFalse();
    assertThat(bitmap.isSet(256)).isFalse();
  }

  // Example 4: even positions 0, 2, 4, ...: `AA AA ... AA AA`
  @Test
  void testDenseSpecExample4() {
    byte[] container = new byte[32];
    Arrays.fill(container, (byte) 0xAA);

    MumblingBitmap bitmap = bitmap(dense(container));
    assertThat(bitmap.cardinality()).isEqualTo(128);

    for (int i = 0; i < 256; i += 1) {
      assertThat(bitmap.isSet(i)).isEqualTo(i % 2 == 0);
    }

    assertThat(bitmap.isSet(256)).isFalse();
  }

  @Test
  void testMultipleContainers() {
    MumblingBitmap bitmap = bitmap(sparse(5), sparse(), sparse(10));
    assertThat(bitmap.cardinality()).isEqualTo(2);

    assertThat(bitmap.isSet(5)).isTrue(); // container 0, pos 5
    assertThat(bitmap.isSet(256)).isFalse(); // container 1
    assertThat(bitmap.isSet(522)).isTrue(); // container 2, pos 10

    assertThat(bitmap.isSet(512)).isFalse();
    assertThat(bitmap.isSet(4)).isFalse();
    assertThat(bitmap.isSet(265)).isFalse();
    assertThat(bitmap.isSet(267)).isFalse();
  }

  @Test
  void testMixedSparseAndDense() {
    byte[] denseContainer = new byte[32];
    denseContainer[0] = (byte) 0xFF;
    denseContainer[1] = (byte) 0xFF;
    denseContainer[2] = (byte) 0xFF;
    denseContainer[3] = (byte) 0xFF;

    MumblingBitmap bitmap = bitmap(dense(denseContainer), sparse(1));
    assertThat(bitmap.cardinality()).isEqualTo(33);

    for (int i = 0; i < 32; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
    assertThat(bitmap.isSet(32)).isFalse();

    assertThat(bitmap.isSet(256)).isFalse();
    assertThat(bitmap.isSet(257)).isTrue(); // container 1, pos 1
    assertThat(bitmap.isSet(258)).isFalse();
  }

  @Test
  void testMixedSparseAndDenseWithPFORException() {
    byte[] denseContainer = new byte[32];
    denseContainer[0] = (byte) 0xFF;
    denseContainer[1] = (byte) 0xFF;
    denseContainer[2] = (byte) 0xFF;
    denseContainer[3] = (byte) 0xFF;

    MumblingBitmap bitmap =
        bitmap(
            sparse(0),
            sparse(1),
            sparse(2),
            sparse(3),
            sparse(4),
            sparse(5),
            sparse(6),
            dense(denseContainer),
            sparse(8),
            sparse(9),
            sparse(10),
            sparse(11),
            sparse(12),
            sparse(13),
            sparse(14));

    assertThat(bitmap.cardinality()).isEqualTo(46);

    for (int i = 0; i < 15; i += 1) {
      if (i != 7) {
        assertThat(bitmap.isSet(256 * i + i)).isTrue();
      }
    }

    for (int i = 0; i < 32; i += 1) {
      assertThat(bitmap.isSet(256 * 7 + i)).isTrue();
    }

    assertThat(bitmap.isSet(256 * 7 - 1)).isFalse();
    assertThat(bitmap.isSet(256 * 7 + 32)).isFalse();
  }

  @Test
  void testBufferWithOffset() {
    // Prepend 4 bytes of garbage before the actual bitmap data
    ByteBuffer buffer = build(sparse(42));
    byte[] rawBytes = new byte[buffer.remaining()];
    buffer.get(rawBytes);

    ByteBuffer padded = ByteBuffer.allocate(4 + rawBytes.length);
    padded.position(4);
    padded.put(rawBytes);
    padded.position(4); // position the buffer at the start of bitmap data

    MumblingBitmap bitmap = new MumblingBitmap(padded);
    assertThat(bitmap.cardinality()).isEqualTo(1);

    assertThat(bitmap.isSet(41)).isFalse();
    assertThat(bitmap.isSet(42)).isTrue();
    assertThat(bitmap.isSet(43)).isFalse();
  }
}
