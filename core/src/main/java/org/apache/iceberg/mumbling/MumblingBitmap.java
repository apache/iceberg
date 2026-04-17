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
 * Read-only view of a Mumbling compressed bitmap stored in a {@link ByteBuffer}.
 *
 * <p>The bitmap is lazy: no decoding is done at construction time. On the first call to {@link
 * #isSet}, the PFOR-encoded descriptor array is decoded and used to build an offsets array that
 * maps each container index to its absolute byte position in the buffer. This offsets array is the
 * only derived state kept by this class.
 *
 * <p>Format (all integers unsigned, little-endian):
 *
 * <ul>
 *   <li>Header (6 bytes): version (1), cardinality (3), container count (2)
 *   <li>Descriptor array: PFOR-encoded, one byte per container
 *   <li>Containers: concatenated sparse (0–31 bytes) or dense (32 bytes) containers
 * </ul>
 */
class MumblingBitmap {
  private static final int VERSION = 1;
  private static final int HEADER_SIZE = 6;
  private static final int DENSE_CONTAINER_BYTES = 32;

  private final ByteBuffer data;
  private int[] offsets;

  MumblingBitmap(ByteBuffer data) {
    int version = data.get(data.position()) & 0xFF;
    if (version != VERSION) {
      throw new IllegalArgumentException("Unsupported Mumbling bitmap version: " + version);
    }
    this.data = data;
  }

  /**
   * Returns {@code true} if the bit at {@code pos} is set in the bitmap.
   *
   * <p>Positions beyond the range of any container are always unset.
   */
  boolean isSet(int pos) {
    int containerIndex = pos >>> 8;
    int posInContainer = pos & 0xFF;

    int[] offs = ensureOffsets();
    if (containerIndex >= offs.length - 1) {
      return false;
    }

    int containerStart = offs[containerIndex];
    int containerLength = offs[containerIndex + 1] - containerStart;

    if (containerLength == DENSE_CONTAINER_BYTES) {
      // Dense: 32-byte bitset, MSB of byte 0 is position 0
      int byteIdx = posInContainer >>> 3;
      int bitIdx = 7 - (posInContainer & 7);
      return ((data.get(containerStart + byteIdx) >>> bitIdx) & 1) == 1;
    } else {
      // Sparse: sorted list of set positions; scan until found or exceeded
      for (int i = 0; i < containerLength; i += 1) {
        int stored = data.get(containerStart + i) & 0xFF;
        if (stored == posInContainer) {
          return true;
        }
        if (stored > posInContainer) {
          return false;
        }
      }
      return false;
    }
  }

  private int[] ensureOffsets() {
    if (offsets == null) {
      offsets = buildOffsets();
    }
    return offsets;
  }

  private int[] buildOffsets() {
    int base = data.position();

    // Container count: bytes 4–5, little-endian
    int containerCount = (data.get(base + 4) & 0xFF) | ((data.get(base + 5) & 0xFF) << 8);

    // Decode the PFOR descriptor array directly into a relative offset array, tracking bytes
    // consumed so we know where the containers section starts
    int[] offsets = new int[containerCount + 1];
    ByteBuffer descriptorBuffer = data.duplicate();
    descriptorBuffer.position(base + HEADER_SIZE);
    int descriptorBytes = PFOREncoding.decodeOffsets(descriptorBuffer, containerCount, offsets);

    // Adjust relative offsets to absolute positions in the buffer
    int containersStart = base + HEADER_SIZE + descriptorBytes;
    for (int i = 0; i <= containerCount; i += 1) {
      offsets[i] += containersStart;
    }

    return offsets;
  }
}
