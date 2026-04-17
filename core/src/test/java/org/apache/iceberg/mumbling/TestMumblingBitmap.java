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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class TestMumblingBitmap {

  // ---------------------------------------------------------------------------
  // Empty bitmap (0 containers)
  // ---------------------------------------------------------------------------
  @Test
  void testEmptyBitmap() {
    MumblingBitmap bitmap = bitmap(new int[0]);
    assertThat(bitmap.isSet(0)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
    assertThat(bitmap.isSet(256)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Single empty sparse container (descriptor = 0, no container bytes)
  // ---------------------------------------------------------------------------
  @Test
  void testEmptySparseContainer() {
    MumblingBitmap bitmap = bitmap(sparse());
    assertThat(bitmap.isSet(0)).isFalse();
    assertThat(bitmap.isSet(100)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Single sparse container with specific positions
  // ---------------------------------------------------------------------------
  @Test
  void testSparseContainerSetPositions() {
    MumblingBitmap bitmap = bitmap(sparse(0, 5, 100, 255));
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
  }

  // ---------------------------------------------------------------------------
  // Full sparse container (31 positions, the maximum)
  // ---------------------------------------------------------------------------
  @Test
  void testMaxSparseContainer() {
    int[] positions = new int[31];
    for (int i = 0; i < 31; i += 1) {
      positions[i] = i * 8; // 0, 8, 16, ..., 240
    }
    MumblingBitmap bitmap = bitmap(sparse(positions));
    for (int p : positions) {
      assertThat(bitmap.isSet(p)).isTrue();
    }
    assertThat(bitmap.isSet(1)).isFalse();
    assertThat(bitmap.isSet(7)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Dense container: all bits set (32 bytes of 0xFF)
  // ---------------------------------------------------------------------------
  @Test
  void testDenseContainerAllSet() {
    byte[] container = new byte[32];
    Arrays.fill(container, (byte) 0xFF);
    MumblingBitmap bitmap = bitmap(dense(container));
    for (int i = 0; i < 256; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
  }

  // ---------------------------------------------------------------------------
  // Dense container: no bits set (32 bytes of 0x00)
  // ---------------------------------------------------------------------------
  @Test
  void testDenseContainerNoneSet() {
    byte[] container = new byte[32];
    MumblingBitmap bitmap = bitmap(dense(container));
    for (int i = 0; i < 256; i += 1) {
      assertThat(bitmap.isSet(i)).isFalse();
    }
  }

  // ---------------------------------------------------------------------------
  // Dense container spec examples
  // ---------------------------------------------------------------------------

  // `FF FF FF FF 00 ... 00` → positions 0-31
  @Test
  void testDenseSpecExample1() {
    byte[] container = new byte[32];
    container[0] = (byte) 0xFF;
    container[1] = (byte) 0xFF;
    container[2] = (byte) 0xFF;
    container[3] = (byte) 0xFF;
    MumblingBitmap bitmap = bitmap(dense(container));
    for (int i = 0; i <= 31; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
    assertThat(bitmap.isSet(32)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
  }

  // `FF FF FF FF A0 00 ... 00` → positions 0-32
  // 0xA0 = 10100000: MSB (pos 32) is set, pos 33 is not
  @Test
  void testDenseSpecExample2() {
    byte[] container = new byte[32];
    container[0] = (byte) 0xFF;
    container[1] = (byte) 0xFF;
    container[2] = (byte) 0xFF;
    container[3] = (byte) 0xFF;
    container[4] = (byte) 0xA0;
    MumblingBitmap bitmap = bitmap(dense(container));
    for (int i = 0; i <= 32; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
    assertThat(bitmap.isSet(33)).isFalse();
    assertThat(bitmap.isSet(255)).isFalse();
  }

  // `FF FF 00 ... 00 FF FF` → positions 0-15, 240-255
  @Test
  void testDenseSpecExample3() {
    byte[] container = new byte[32];
    container[0] = (byte) 0xFF;
    container[1] = (byte) 0xFF;
    container[30] = (byte) 0xFF;
    container[31] = (byte) 0xFF;
    MumblingBitmap bitmap = bitmap(dense(container));
    for (int i = 0; i <= 15; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
    for (int i = 240; i <= 255; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
    assertThat(bitmap.isSet(16)).isFalse();
    assertThat(bitmap.isSet(239)).isFalse();
  }

  // `AA AA ... AA AA` → even positions: 0, 2, 4, ...
  // 0xAA = 10101010: MSB-first gives positions 0, 2, 4, 6 set per byte
  @Test
  void testDenseSpecExample4() {
    byte[] container = new byte[32];
    Arrays.fill(container, (byte) 0xAA);
    MumblingBitmap bitmap = bitmap(dense(container));
    for (int i = 0; i < 256; i += 1) {
      assertThat(bitmap.isSet(i)).isEqualTo(i % 2 == 0);
    }
  }

  // ---------------------------------------------------------------------------
  // Multiple containers: positions in different containers
  // Container 0 (pos 0–255): sparse {5}
  // Container 1 (pos 256–511): sparse {10} → global position 266
  // Container 2 (pos 512–767): empty sparse
  // ---------------------------------------------------------------------------
  @Test
  void testMultipleContainers() {
    MumblingBitmap bitmap =
        bitmap(
            new ContainerSpec[] {
              sparse(5), sparse(10), sparse()
            });

    assertThat(bitmap.isSet(5)).isTrue(); // container 0, pos 5
    assertThat(bitmap.isSet(266)).isTrue(); // container 1, pos 10
    assertThat(bitmap.isSet(512)).isFalse(); // container 2, empty
    assertThat(bitmap.isSet(4)).isFalse();
    assertThat(bitmap.isSet(265)).isFalse();
    assertThat(bitmap.isSet(267)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Mixed sparse and dense containers
  // Container 0: dense with byte 0 = 0xFF (positions 0–7 set)
  // Container 1: sparse {0} (global position 256)
  // ---------------------------------------------------------------------------
  @Test
  void testMixedSparseAndDense() {
    byte[] denseContainer = new byte[32];
    denseContainer[0] = (byte) 0xFF;
    MumblingBitmap bitmap =
        bitmap(
            new ContainerSpec[] {
              dense(denseContainer), sparse(0)
            });

    for (int i = 0; i < 8; i += 1) {
      assertThat(bitmap.isSet(i)).isTrue();
    }
    assertThat(bitmap.isSet(8)).isFalse();
    assertThat(bitmap.isSet(256)).isTrue(); // container 1, pos 0
    assertThat(bitmap.isSet(257)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Position beyond container count is always unset
  // ---------------------------------------------------------------------------
  @Test
  void testPositionBeyondContainerCount() {
    MumblingBitmap bitmap = bitmap(sparse(5));
    // Only container 0 exists; container 1 (pos 256+) does not
    assertThat(bitmap.isSet(256)).isFalse();
    assertThat(bitmap.isSet(511)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Buffer with non-zero initial position
  // ---------------------------------------------------------------------------
  @Test
  void testBufferWithOffset() {
    // Prepend 4 bytes of garbage before the actual bitmap data
    ByteBuffer raw = build(new ContainerSpec[] {sparse(42)});
    byte[] rawBytes = new byte[raw.remaining()];
    raw.get(rawBytes);

    ByteBuffer padded = ByteBuffer.allocate(4 + rawBytes.length);
    padded.position(4);
    padded.put(rawBytes);
    padded.position(4); // position the buffer at the start of bitmap data

    MumblingBitmap bitmap = new MumblingBitmap(padded);
    assertThat(bitmap.isSet(42)).isTrue();
    assertThat(bitmap.isSet(43)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Lazy init: calling isSet multiple times produces consistent results
  // ---------------------------------------------------------------------------
  @Test
  void testLazyInitConsistency() {
    MumblingBitmap bitmap = bitmap(sparse(1, 2, 3));
    for (int trial = 0; trial < 3; trial += 1) {
      assertThat(bitmap.isSet(1)).isTrue();
      assertThat(bitmap.isSet(2)).isTrue();
      assertThat(bitmap.isSet(3)).isTrue();
      assertThat(bitmap.isSet(0)).isFalse();
      assertThat(bitmap.isSet(4)).isFalse();
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Descriptor + bytes for a sparse container. */
  private static ContainerSpec sparse(int... positions) {
    byte[] bytes = new byte[positions.length];
    for (int i = 0; i < positions.length; i += 1) {
      bytes[i] = (byte) positions[i];
    }
    return new ContainerSpec(positions.length, bytes);
  }

  /** Descriptor + bytes for a dense container. */
  private static ContainerSpec dense(byte[] container) {
    if (container.length != 32) {
      throw new IllegalArgumentException("Dense container must be 32 bytes");
    }
    return new ContainerSpec(32, container);
  }

  private static class ContainerSpec {
    final int descriptor;
    final byte[] bytes;

    ContainerSpec(int descriptor, byte[] bytes) {
      this.descriptor = descriptor;
      this.bytes = bytes;
    }
  }

  /** Builds a bitmap with a single container. */
  private static MumblingBitmap bitmap(ContainerSpec spec) {
    return new MumblingBitmap(build(new ContainerSpec[] {spec}));
  }

  /** Builds a bitmap with no containers. */
  private static MumblingBitmap bitmap(int[] ignored) {
    return new MumblingBitmap(build(new ContainerSpec[0]));
  }

  /** Builds a bitmap with multiple containers. */
  private static MumblingBitmap bitmap(ContainerSpec[] specs) {
    return new MumblingBitmap(build(specs));
  }

  private static ByteBuffer build(ContainerSpec[] specs) {
    int[] descriptors = new int[specs.length];
    for (int i = 0; i < specs.length; i += 1) {
      descriptors[i] = specs[i].descriptor;
    }

    ByteBuffer encodedDescriptors =
        specs.length > 0 ? PFOREncoding.encode(descriptors, specs.length) : ByteBuffer.allocate(0);

    int totalContainerBytes = 0;
    for (ContainerSpec spec : specs) {
      totalContainerBytes += spec.bytes.length;
    }

    int totalSize = 6 + encodedDescriptors.remaining() + totalContainerBytes;
    ByteBuffer buf = ByteBuffer.allocate(totalSize);

    // Header: version (1 byte), cardinality (3 bytes LE), container count (2 bytes LE)
    buf.put((byte) 1);
    buf.put((byte) 0);
    buf.put((byte) 0);
    buf.put((byte) 0);
    buf.put((byte) (specs.length & 0xFF));
    buf.put((byte) ((specs.length >>> 8) & 0xFF));

    buf.put(encodedDescriptors);
    for (ContainerSpec spec : specs) {
      buf.put(spec.bytes);
    }

    buf.flip();
    return buf;
  }
}
