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
import java.util.Random;
import org.junit.jupiter.api.Test;

class TestMumblingBitmap {
  private static final int HEADER_SIZE = 6;
  private static final byte[] DENSE_CONTAINER = new byte[32];

  static {
    DENSE_CONTAINER[1] = (byte) 0xFF;
    DENSE_CONTAINER[3] = (byte) 0xFF;
    DENSE_CONTAINER[5] = (byte) 0xFF;
    DENSE_CONTAINER[7] = (byte) 0xFF;
  }

  private static final MumblingBitmap MUTABLE_TEST_BASE =
      bitmap(sparse(0), dense(DENSE_CONTAINER), sparse(255));

  @Test
  public void testInvalidVersion() {
    assertThatThrownBy(() -> new MumblingBitmap(ByteBuffer.wrap(new byte[] {0x00})))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported Mumbling bitmap version: 0");

    assertThatThrownBy(() -> new MumblingBitmap(ByteBuffer.wrap(new byte[] {0x02})))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported Mumbling bitmap version: 2");
  }

  @Test
  public void testInvalidCardinality() {
    assertThatThrownBy(
            () ->
                new MumblingBitmap(
                    ByteBuffer.wrap(new byte[] {0x01, 0x01, 0x00, 0x20, 0x00, 0x00})))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid cardinality: 2097153 (must be <= 2,097,152)");
  }

  @Test
  public void testInvalidDescriptor() {
    ByteBuffer descriptor33 =
        ByteBuffer.wrap(
            new byte[] {0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x00, 0b0010_0001});
    assertThatThrownBy(() -> new MumblingBitmap(descriptor33).descriptors())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid descriptor, not sparse or dense: 33");

    ByteBuffer descriptor64 =
        ByteBuffer.wrap(
            new byte[] {0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x00, 0b0100_0000});
    assertThatThrownBy(() -> new MumblingBitmap(descriptor64).descriptors())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid descriptor, not sparse or dense: 64");

    ByteBuffer descriptor128 =
        ByteBuffer.wrap(
            new byte[] {0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x00, (byte) 0b1000_0000});
    assertThatThrownBy(() -> new MumblingBitmap(descriptor128).descriptors())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid descriptor, not sparse or dense: 128");
  }

  @Test
  public void testMaximumCardinalityAndContainers() {
    MumblingBitmap bitmap =
        new MumblingBitmap(
            ByteBuffer.wrap(new byte[] {0x01, (byte) 0xFF, (byte) 0xFF, 0x1F, (byte) 0xFF, 0x1F}));

    assertThat(bitmap.cardinality()).isEqualTo((8192 * 256) - 1);
  }

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

  @Test
  public void testMutableValidatesPositions() {
    MutableBitmap bitmap = MumblingBitmap.newMutableBitmap();

    assertThat(bitmap.set(0)).isTrue();
    assertThatThrownBy(() -> bitmap.set(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set invalid bit position: -1 < 0");

    assertThat(bitmap.set(2_097_151)).isTrue();
    assertThatThrownBy(() -> bitmap.set(2_097_152))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set invalid bit position: 2097152 (must be < 2,097,152)");
  }

  @Test
  public void testMutableOverlayPassthrough() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    assertThat(bitmap.cardinality()).isEqualTo(34);

    assertThat(bitmap.isSet(0)).isTrue();
    assertThat(bitmap.isSet(1)).isFalse();

    assertThat(bitmap.isSet(263)).isFalse();
    assertThat(bitmap.isSet(279)).isFalse();
    assertThat(bitmap.isSet(295)).isFalse();
    assertThat(bitmap.isSet(311)).isFalse();
    for (int i = 0; i < 8; i += 1) {
      assertThat(bitmap.isSet(264 + i)).isTrue();
      assertThat(bitmap.isSet(280 + i)).isTrue();
      assertThat(bitmap.isSet(296 + i)).isTrue();
      assertThat(bitmap.isSet(312 + i)).isTrue();
    }
    assertThat(bitmap.isSet(272)).isFalse();
    assertThat(bitmap.isSet(288)).isFalse();
    assertThat(bitmap.isSet(304)).isFalse();
    assertThat(bitmap.isSet(320)).isFalse();

    assertThat(bitmap.isSet(766)).isFalse();
    assertThat(bitmap.isSet(767)).isTrue();
    assertThat(bitmap.isSet(768)).isFalse();
  }

  @Test
  public void testMutableOverlaySparseUpdate() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    assertThat(bitmap.cardinality()).isEqualTo(34);

    assertThat(bitmap.set(1)).isTrue();
    assertThat(bitmap.set(0)).isFalse();
    assertThat(bitmap.set(1)).isFalse();

    assertThat(bitmap.cardinality()).isEqualTo(35);
    assertThat(bitmap.isSet(0)).isTrue();
    assertThat(bitmap.isSet(1)).isTrue();
    assertThat(bitmap.isSet(2)).isFalse();

    assertThat(bitmap.set(766)).isTrue();
    assertThat(bitmap.set(767)).isFalse();
    assertThat(bitmap.set(766)).isFalse();

    assertThat(bitmap.cardinality()).isEqualTo(36);
    assertThat(bitmap.isSet(765)).isFalse();
    assertThat(bitmap.isSet(766)).isTrue();
    assertThat(bitmap.isSet(767)).isTrue();
    assertThat(bitmap.isSet(768)).isFalse();
  }

  @Test
  public void testMutableOverlayDenseUpdate() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    assertThat(bitmap.cardinality()).isEqualTo(34);

    for (int i = 0; i < 8; i += 1) {
      assertThat(bitmap.set(256 + i)).isTrue();
      assertThat(bitmap.set(256 + i)).isFalse();
      assertThat(bitmap.set(264 + i)).isFalse(); // already set
    }

    assertThat(bitmap.cardinality()).isEqualTo(42);

    assertThat(bitmap.isSet(255)).isFalse();
    for (int i = 0; i < 16; i += 1) {
      assertThat(bitmap.isSet(256 + i)).isTrue();
    }
    assertThat(bitmap.isSet(272)).isFalse();

    assertThat(bitmap.set(511)).isTrue();
    assertThat(bitmap.set(511)).isFalse();
    assertThat(bitmap.isSet(511)).isTrue();

    assertThat(bitmap.cardinality()).isEqualTo(43);
  }

  @Test
  public void testMutableOverlaySparseToDenseUpdate() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    assertThat(bitmap.cardinality()).isEqualTo(34);

    for (int i = 1; i < 33; i += 1) {
      assertThat(bitmap.set(i)).isTrue();
      assertThat(bitmap.set(i)).isFalse();
    }

    assertThat(bitmap.cardinality()).isEqualTo(66);

    for (int i = 0; i < 33; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
    assertThat(bitmap.isSet(33)).isFalse();

    // set positions backward so the sparse list is rewritten
    for (int i = 254; i > 222; i -= 1) {
      assertThat(bitmap.set(i)).isTrue();
      assertThat(bitmap.set(i)).isFalse();
    }

    assertThat(bitmap.cardinality()).isEqualTo(98);
  }

  @Test
  public void testMutableOverlayNewContainer() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    assertThat(bitmap.set(1024)).isTrue(); // container index 4, position 0
    assertThat(bitmap.set(1024)).isFalse();
    assertThat(bitmap.isSet(1024)).isTrue();
    assertThat(bitmap.cardinality()).isEqualTo(35);
  }

  @Test
  public void testMutableEmptySerialization() {
    MutableBitmap empty = MumblingBitmap.newMutableBitmap();

    assertThat(empty.cardinality()).isEqualTo(0);
    assertThat(empty.buffer())
        .isEqualTo(ByteBuffer.wrap(new byte[] {0x01, 0x00, 0x00, 0x00, 0x00, 0x00}));
  }

  @Test
  public void testMutableNoopSerialization() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    assertThat(bitmap.buffer())
        .as("Does not reuse the original buffer")
        .isNotSameAs(MUTABLE_TEST_BASE.buffer());
    assertThat(bitmap.buffer()).isEqualTo(MUTABLE_TEST_BASE.buffer());
  }

  @Test
  public void testMutableNewContainerSerialization() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    assertThat(bitmap.set(1024)).isTrue(); // container 4, position 0
    assertThat(bitmap.cardinality()).isEqualTo(35);
    assertThat(bitmap.set(2048)).isTrue(); // container 8, position 0
    assertThat(bitmap.cardinality()).isEqualTo(36);

    MumblingBitmap result = new MumblingBitmap(bitmap.buffer());
    assertThat(result.cardinality()).isEqualTo(36);
    assertThat(result.descriptors()).containsExactly(1, 32, 1, 0, 1, 0, 0, 0, 1);

    assertThat(result.isSet(0)).isTrue();

    for (int i = 0; i < 8; i += 1) {
      assertThat(result.isSet(264 + i)).isTrue();
      assertThat(result.isSet(280 + i)).isTrue();
      assertThat(result.isSet(296 + i)).isTrue();
      assertThat(result.isSet(312 + i)).isTrue();
    }

    assertThat(result.isSet(767)).isTrue();

    assertThat(result.isSet(1024)).isTrue();
    assertThat(result.isSet(2048)).isTrue();
  }

  @Test
  public void testMutableEmptyWithNewContainerSerialization() {
    MutableBitmap bitmap = MumblingBitmap.newMutableBitmap();

    assertThat(bitmap.set(1024)).isTrue(); // container 4, position 0
    assertThat(bitmap.set(2048)).isTrue(); // container 8, position 0

    assertThat(bitmap.cardinality()).isEqualTo(2);

    MumblingBitmap result = new MumblingBitmap(bitmap.buffer());
    assertThat(result.cardinality()).isEqualTo(2);
    assertThat(result.descriptors()).contains(0, 0, 0, 0, 1, 0, 0, 0, 1);

    assertThat(result.isSet(1024)).isTrue();
    assertThat(result.isSet(2048)).isTrue();
  }

  @Test
  public void testMutableSparseUpdateSerialization() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    assertThat(bitmap.set(1)).isTrue();
    assertThat(bitmap.set(766)).isTrue(); // container index 2, pos 254
    assertThat(bitmap.cardinality()).isEqualTo(36);

    ByteBuffer serialized = bitmap.buffer();
    int[] expectedDescriptors = new int[] {2, 32, 2};

    MumblingBitmap result = new MumblingBitmap(serialized);
    assertThat(result.cardinality()).isEqualTo(36);
    assertThat(result.descriptors()).isEqualTo(expectedDescriptors);

    assertThat(bitmap.isSet(1)).isTrue();
    assertThat(bitmap.isSet(766)).isTrue();

    ByteBuffer descriptors = PFOREncoding.encode(expectedDescriptors, expectedDescriptors.length);
    int descriptorSize = descriptors.remaining();
    int containersOffset = HEADER_SIZE + descriptorSize;

    assertThat(serialized.slice(6, descriptorSize)).isEqualTo(descriptors);

    assertThat(serialized.slice(containersOffset, 2)).isEqualTo(sparse(0, 1).buffer());
    assertThat(serialized.slice(containersOffset + 2, 32))
        .isEqualTo(dense(DENSE_CONTAINER).buffer());
    assertThat(serialized.slice(containersOffset + 34, 2)).isEqualTo(sparse(254, 255).buffer());
  }

  @Test
  public void testMutableDenseUpdateSerialization() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    for (int i = 0; i < 8; i += 1) {
      assertThat(bitmap.set(256 + i)).isTrue();
    }
    assertThat(bitmap.set(511)).isTrue(); // last bit of the dense container

    assertThat(bitmap.cardinality()).isEqualTo(43);

    ByteBuffer serialized = bitmap.buffer();
    int[] expectedDescriptors = new int[] {1, 32, 1};

    MumblingBitmap result = new MumblingBitmap(serialized);
    assertThat(result.cardinality()).isEqualTo(43);
    assertThat(result.descriptors()).isEqualTo(expectedDescriptors);

    ByteBuffer descriptors = PFOREncoding.encode(expectedDescriptors, expectedDescriptors.length);
    int descriptorSize = descriptors.remaining();
    int containersOffset = HEADER_SIZE + descriptorSize;

    assertThat(serialized.slice(6, descriptorSize)).isEqualTo(descriptors);

    assertThat(serialized.slice(containersOffset, 1)).isEqualTo(sparse(0).buffer());

    byte[] denseBytes = Arrays.copyOf(DENSE_CONTAINER, 32);
    denseBytes[0] = (byte) 0xFF;
    denseBytes[31] = (byte) 0x01; // last bit
    assertThat(serialized.slice(containersOffset + 1, 32)).isEqualTo(dense(denseBytes).buffer());

    assertThat(serialized.slice(containersOffset + 33, 1)).isEqualTo(sparse(255).buffer());
  }

  @Test
  public void testMutableSparseToDenseUpdateSerialization() {
    MutableBitmap bitmap = MUTABLE_TEST_BASE.asMutable();

    for (int i = 766; i > 735; i -= 1) {
      assertThat(bitmap.set(i)).isTrue();
    }
    assertThat(bitmap.cardinality()).isEqualTo(65);

    ByteBuffer serialized = bitmap.buffer();
    int[] expectedDescriptors = new int[] {1, 32, 32};

    MumblingBitmap result = new MumblingBitmap(serialized);
    assertThat(result.cardinality()).isEqualTo(65);
    assertThat(result.descriptors()).isEqualTo(expectedDescriptors);

    assertThat(result.isSet(0)).isTrue();

    for (int i = 0; i < 8; i += 1) {
      assertThat(result.isSet(264 + i)).isTrue();
      assertThat(result.isSet(280 + i)).isTrue();
      assertThat(result.isSet(296 + i)).isTrue();
      assertThat(result.isSet(312 + i)).isTrue();
    }

    for (int i = 736; i < 767; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }

    assertThat(result.isSet(767)).isTrue();

    ByteBuffer descriptors = PFOREncoding.encode(expectedDescriptors, expectedDescriptors.length);
    int descriptorSize = descriptors.remaining();
    int containersOffset = HEADER_SIZE + descriptorSize;

    assertThat(serialized.slice(6, descriptorSize)).isEqualTo(descriptors);

    assertThat(serialized.slice(containersOffset, 1)).isEqualTo(sparse(0).buffer());
    assertThat(serialized.slice(containersOffset + 1, 32))
        .isEqualTo(dense(DENSE_CONTAINER).buffer());

    byte[] denseBytes = new byte[32];
    denseBytes[28] = (byte) 0xFF;
    denseBytes[29] = (byte) 0xFF;
    denseBytes[30] = (byte) 0xFF;
    denseBytes[31] = (byte) 0xFF;
    assertThat(serialized.slice(containersOffset + 33, 32)).isEqualTo(dense(denseBytes).buffer());
  }

  @Test
  public void testMutableRandomPositions() {
    Random positions = new Random(39826538713L);
    int count = 0;

    MutableBitmap bitmap = MumblingBitmap.newMutableBitmap();

    for (int i = 0; i < 50_000; i += 1) {
      if (bitmap.set(positions.nextInt(64_000))) {
        count += 1;
      }
    }

    MumblingBitmap result = new MumblingBitmap(bitmap.buffer());

    assertThat(bitmap.cardinality()).isEqualTo(count);
    assertThat(result.cardinality()).isEqualTo(count);

    // check that all positions are set
    Random validation = new Random(39826538713L);
    for (int i = 0; i < 50_000; i += 1) {
      int pos = validation.nextInt(64_000);
      assertThat(bitmap.isSet(pos)).isTrue();
      assertThat(result.isSet(pos)).isTrue();
    }
  }

  @Test
  public void testRelocatedBuffer() {
    MumblingBitmap bitmap = new MumblingBitmap(relocate(MUTABLE_TEST_BASE.buffer()));

    assertThat(bitmap.cardinality()).isEqualTo(34);
    assertThat(bitmap.descriptors()).containsExactly(1, 32, 1);

    assertThat(bitmap.isSet(0)).isTrue();
    assertThat(bitmap.isSet(1)).isFalse();

    assertThat(bitmap.isSet(263)).isFalse();
    for (int i = 0; i < 8; i += 1) {
      assertThat(bitmap.isSet(264 + i)).isTrue();
      assertThat(bitmap.isSet(280 + i)).isTrue();
      assertThat(bitmap.isSet(296 + i)).isTrue();
      assertThat(bitmap.isSet(312 + i)).isTrue();
    }
    assertThat(bitmap.isSet(272)).isFalse();
    assertThat(bitmap.isSet(320)).isFalse();

    assertThat(bitmap.isSet(766)).isFalse();
    assertThat(bitmap.isSet(767)).isTrue();
    assertThat(bitmap.isSet(768)).isFalse();
  }

  @Test
  public void testRelocatedBufferUpdateSerialization() {
    MutableBitmap bitmap = new MumblingBitmap(relocate(MUTABLE_TEST_BASE.buffer())).asMutable();

    // containers 0 and 1 are not modified and must be copied from the relocated buffer
    assertThat(bitmap.set(766)).isTrue(); // container 2, sparse
    assertThat(bitmap.set(1027)).isTrue(); // container 4, new sparse
    assertThat(bitmap.set(1031)).isTrue(); // container 4, new sparse
    assertThat(bitmap.cardinality()).isEqualTo(37);

    ByteBuffer serialized = bitmap.buffer();
    assertThat(serialized.position()).isEqualTo(0);

    int[] expectedDescriptors = new int[] {1, 32, 2, 0, 2};

    MumblingBitmap result = new MumblingBitmap(serialized);
    assertThat(result.cardinality()).isEqualTo(37);
    assertThat(result.descriptors()).isEqualTo(expectedDescriptors);

    assertThat(result.isSet(0)).isTrue();
    assertThat(result.isSet(1)).isFalse();

    assertThat(result.isSet(263)).isFalse();
    for (int i = 0; i < 8; i += 1) {
      assertThat(result.isSet(264 + i)).isTrue();
      assertThat(result.isSet(280 + i)).isTrue();
      assertThat(result.isSet(296 + i)).isTrue();
      assertThat(result.isSet(312 + i)).isTrue();
    }
    assertThat(result.isSet(272)).isFalse();
    assertThat(result.isSet(320)).isFalse();

    assertThat(result.isSet(765)).isFalse();
    assertThat(result.isSet(766)).isTrue();
    assertThat(result.isSet(767)).isTrue();

    assertThat(result.isSet(1024)).isFalse();
    assertThat(result.isSet(1027)).isTrue();
    assertThat(result.isSet(1028)).isFalse();
    assertThat(result.isSet(1031)).isTrue();
    assertThat(result.isSet(1032)).isFalse();

    ByteBuffer descriptors = PFOREncoding.encode(expectedDescriptors, expectedDescriptors.length);
    int descriptorSize = descriptors.remaining();
    int containersOffset = HEADER_SIZE + descriptorSize;

    assertThat(serialized.slice(HEADER_SIZE, descriptorSize)).isEqualTo(descriptors);

    assertThat(serialized.slice(containersOffset, 1)).isEqualTo(sparse(0).buffer());
    assertThat(serialized.slice(containersOffset + 1, 32))
        .isEqualTo(dense(DENSE_CONTAINER).buffer());
    assertThat(serialized.slice(containersOffset + 33, 2)).isEqualTo(sparse(254, 255).buffer());
    assertThat(serialized.slice(containersOffset + 35, 2)).isEqualTo(sparse(3, 7).buffer());
  }

  /** Copy into a buffer with garbage padding outside of position and limit. */
  private static ByteBuffer relocate(ByteBuffer serialized) {
    int prefix = 17;
    int suffix = 13;
    int length = serialized.remaining();

    byte[] bytes = new byte[prefix + length + suffix];
    Arrays.fill(bytes, (byte) 0xA5);
    serialized.duplicate().get(bytes, prefix, length);

    ByteBuffer relocated = ByteBuffer.wrap(bytes);
    relocated.position(prefix);
    relocated.limit(prefix + length);

    return relocated;
  }
}
