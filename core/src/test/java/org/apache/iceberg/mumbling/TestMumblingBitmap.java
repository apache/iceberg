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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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

  private static Container sparse(int... positions) {
    byte[] bytes = new byte[positions.length];
    for (int i = 0; i < positions.length; i += 1) {
      if (i > 0) {
        Preconditions.checkArgument(
            positions[i] < 256, "Invalid position in container: %s", positions[i]);
        Preconditions.checkArgument(
            positions[i] > positions[i - 1],
            "Invalid sparse container: pos %s=%s >= pos %s=%s",
            i - 1,
            positions[i - 1],
            i,
            positions[i]);
      }

      bytes[i] = (byte) positions[i];
    }

    return new Container(bytes);
  }

  /** Descriptor + bytes for a dense container. */
  private static Container dense(byte[] container) {
    Preconditions.checkArgument(container.length == 32, "Dense container must be 32 bytes");
    return new Container(container);
  }

  private static class Container {
    private final byte[] bytes;
    private final int descriptor;
    private final int cardinality;

    Container(byte[] bytes) {
      this.bytes = bytes;
      this.descriptor = bytes.length;
      this.cardinality = cardinality(bytes);
    }

    private static int cardinality(byte[] bytes) {
      if (bytes.length < 32) {
        return bytes.length;
      } else if (bytes.length == 32) {
        int setBits = 0;
        for (byte b : bytes) {
          setBits += Integer.bitCount(b & 0xFF);
        }

        Preconditions.checkArgument(
            setBits > 31, "Invalid dense container: %s values should be sparse", setBits);

        return setBits;
      } else {
        throw new IllegalArgumentException("Invalid container: longer than 32 bytes");
      }
    }
  }

  private static MumblingBitmap bitmap(Container... containers) {
    return new MumblingBitmap(build(containers));
  }

  private static ByteBuffer build(Container... containers) {
    Preconditions.checkArgument(
        containers.length <= 8192, "Invalid container count (max 8192): %s", containers.length);

    int[] descriptors = new int[containers.length];
    int cardinality = 0;
    int sizeEstimate = 6;
    for (int i = 0; i < containers.length; i += 1) {
      descriptors[i] = containers[i].descriptor;
      cardinality += containers[i].cardinality;
      sizeEstimate += containers[i].bytes.length;
    }

    Preconditions.checkArgument(
        cardinality <= 2_097_152, "Invalid cardinality (max 2,097,152): %s", cardinality);

    sizeEstimate += PFOREncoding.estimateEncodedSize(containers.length);
    ByteBuffer buf = ByteBuffer.allocate(sizeEstimate);

    // header: version (1 byte), cardinality (3 bytes LE), container count (2 bytes LE)
    buf.put(0, (byte) 1);
    buf.put(1, (byte) (cardinality & 0xFF));
    buf.put(2, (byte) ((cardinality >>> 8) & 0xFF));
    buf.put(3, (byte) ((cardinality >>> 16) & 0xFF));
    buf.put(4, (byte) (containers.length & 0xFF));
    buf.put(5, (byte) ((containers.length >>> 8) & 0xFF));

    // write encoded descriptors
    int descriptorArraySize =
        PFOREncoding.encode(descriptors, 0, buf, buf.position() + 6, descriptors.length);

    // copy container bytes into the array
    int containerOffset = 6 + descriptorArraySize;
    for (Container spec : containers) {
      buf.put(containerOffset, spec.bytes);
      containerOffset += spec.bytes.length;
    }

    // the offset after the last container is the length
    buf.limit(containerOffset);

    return buf;
  }
}
