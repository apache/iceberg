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
import org.apache.curator.shaded.com.google.common.base.Preconditions;

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
  private static final int DENSE_CONTAINER_BIT = 0b0010_0000;

  private final ByteBuffer data;
  private final int cardinality;
  private final int containerCount;
  private int[] descriptors = null;
  private int[] offsets = null;

  MumblingBitmap(ByteBuffer data) {
    int version = data.get(data.position()) & 0xFF;
    if (version != VERSION) {
      throw new UnsupportedOperationException("Unsupported Mumbling bitmap version: " + version);
    }

    this.data = data;
    this.cardinality =
        (data.get(data.position() + 1) & 0xFF)
            | ((data.get(data.position() + 2) & 0xFF) << 8)
            | ((data.get(data.position() + 3) & 0xFF) << 16);
    this.containerCount =
        (data.get(data.position() + 4) & 0xFF) | ((data.get(data.position() + 5) & 0xFF) << 8);
  }

  /** Returns the number of bits set in the bitmap. */
  public int cardinality() {
    return cardinality;
  }

  /**
   * Returns {@code true} if the bit at {@code pos} is set in the bitmap.
   *
   * <p>Positions beyond the range of any container are always unset.
   */
  public boolean isSet(int pos) {
    Preconditions.checkArgument(pos >= 0, "Invalid bit position: %s < 0", pos);
    int containerIndex = pos >>> 8;
    int posInContainer = pos & 0xFF;

    if (containerIndex >= containerCount) {
      return false;
    }

    int containerStart = offset(containerIndex);
    int descriptor = descriptor(containerIndex);

    if (isDense(descriptor)) {
      // Dense: 32-byte bitset, MSB of byte 0 is position 0
      int byteIndex = posInContainer >>> 3;
      int bitShift = 7 - (posInContainer & 0b111);
      return ((data.get(containerStart + byteIndex) >>> bitShift) & 0b1) == 0b1;

    } else {
      // Sparse: sorted list of set positions; scan until found or exceeded
      for (int i = 0; i < descriptor; i += 1) {
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

  private int descriptor(int containerIndex) {
    if (null == descriptors) {
      decodeDescriptors();
    }

    return descriptors[containerIndex];
  }

  private int offset(int containerIndex) {
    if (null == offsets) {
      decodeDescriptors();
    }

    return offsets[containerIndex];
  }

  /**
   * Decode the descriptor array and produce an array of absolute container offsets in the buffer.
   */
  private void decodeDescriptors() {
    this.descriptors = new int[containerCount];
    int bytesRead =
        PFOREncoding.decode(data, data.position() + HEADER_SIZE, descriptors, 0, containerCount);

    this.offsets = new int[containerCount + 1];
    int firstContainerOffset = data.position() + HEADER_SIZE + bytesRead;
    descriptorsToOffsets(firstContainerOffset, descriptors, offsets);
  }

  private static boolean isDense(int descriptor) {
    return (descriptor & DENSE_CONTAINER_BIT) == DENSE_CONTAINER_BIT;
  }

  /**
   * Convert an array of lengths into an array of offsets starting at 0.
   *
   * <p>For example, descriptorsToOffsets([1, 1, 2]) produces [0, 1, 2, 4].
   *
   * @param baseOffset initial offset of the first container
   * @param descriptors an array of descriptor bytes
   * @param offsets output array of offsets
   */
  private static void descriptorsToOffsets(int baseOffset, int[] descriptors, int[] offsets) {
    Preconditions.checkArgument(
        offsets.length > descriptors.length,
        "Cannot decode %s lengths into %s offsets (not enough space)",
        descriptors.length,
        offsets.length);

    offsets[0] = baseOffset;
    for (int i = 0; i < descriptors.length; i += 1) {
      if (isDense(descriptors[i])) {
        offsets[i + 1] = offsets[i] + 32;
      } else {
        offsets[i + 1] = offsets[i] + descriptors[i];
      }
    }
  }
}
