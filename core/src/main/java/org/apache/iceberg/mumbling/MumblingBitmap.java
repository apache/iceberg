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
import java.util.Map;
import java.util.NavigableMap;
import org.apache.iceberg.ManifestBitmap;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;

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
class MumblingBitmap implements ManifestBitmap {
  private static final ByteBuffer EMPTY_BITMAP =
      ByteBuffer.wrap(new byte[] {0x01, 0x00, 0x00, 0x00, 0x00, 0x00});
  private static final int VERSION = 1;
  private static final int HEADER_SIZE = 6;
  private static final int DENSE_CONTAINER_BIT = 0b0010_0000;
  private static final int DENSE_CONTAINER_SIZE = 32;
  private static final int MAX_SPARSE_CARDINALITY = 31;
  private static final int MAX_CONTAINER_COUNT = 8_192;
  private static final int MAX_CARDINALITY = 2_097_152;

  static MutableBitmap newMutableBitmap() {
    return new MumblingBitmap(EMPTY_BITMAP).asMutable();
  }

  private final ByteBuffer data;
  private final int cardinality;
  private final int containerCount;
  private volatile int[] descriptors = null;
  private volatile int[] offsets = null;

  MumblingBitmap(ByteBuffer data) {
    int version = ByteBuffers.readByte(data, 0);
    if (version != VERSION) {
      throw new UnsupportedOperationException("Unsupported Mumbling bitmap version: " + version);
    }

    this.data = data;
    this.cardinality = ByteBuffers.readLittleEndianUnsigned(data, 1, 3);
    this.containerCount = ByteBuffers.readLittleEndianUnsigned(data, 4, 2);

    Preconditions.checkState(
        containerCount <= MAX_CONTAINER_COUNT,
        "Invalid container count: %s (must be <= 8,192)",
        containerCount);
    Preconditions.checkState(
        cardinality <= MAX_CARDINALITY,
        "Invalid cardinality: %s (must be <= 2,097,152)",
        cardinality);
  }

  @Override
  public ByteBuffer buffer() {
    return data;
  }

  /** Returns the number of bits set in the bitmap. */
  @Override
  public int cardinality() {
    return cardinality;
  }

  /**
   * Returns {@code true} if the bit at {@code pos} is set in the bitmap.
   *
   * <p>Positions beyond the range of any container are always unset.
   */
  @Override
  public boolean isSet(int pos) {
    Preconditions.checkArgument(pos >= 0, "Invalid bit position: %s < 0", pos);
    int containerIndex = containerIndex(pos);
    int posInContainer = posInContainer(pos);

    if (containerIndex >= containerCount) {
      return false;
    }

    int containerStart = offset(containerIndex);
    int descriptor = descriptor(containerIndex);

    if (isDense(descriptor)) {
      // Dense: 32-byte bitset, MSB of byte 0 is position 0
      int byteIndex = containerStart + denseByteIndex(posInContainer);
      int bitShift = denseBitShift(posInContainer);
      return ((ByteBuffers.readByte(data, byteIndex) >>> bitShift) & 0b1) == 0b1;

    } else {
      // Sparse: sorted list of set positions; scan until found or exceeded
      for (int i = 0; i < descriptor; i += 1) {
        int stored = ByteBuffers.readByte(data, containerStart + i);
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

  @VisibleForTesting
  int[] descriptors() {
    if (null == descriptors) {
      decodeDescriptors();
    }

    return descriptors;
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

  public MutableBitmap asMutable() {
    return new Mutable(this);
  }

  /**
   * A copy-on-write {@link MutableBitmap} implementation for Mumbling.
   *
   * <p>When a new bit position is set, an overlay is created for the position's container.
   *
   * <p>The initial buffer is not modified.
   */
  private static class Mutable implements MutableBitmap {
    private final NavigableMap<Integer, Container> overlays = Maps.newTreeMap();
    private final MumblingBitmap base;
    private final int baseContainerCount;
    private int cardinality = 0;

    private Mutable(MumblingBitmap base) {
      this.base = base;
      this.baseContainerCount = base.descriptors().length;
      this.cardinality = base.cardinality();
    }

    @Override
    public int cardinality() {
      return cardinality;
    }

    @Override
    public boolean isSet(int pos) {
      Container overlay = overlays.get(containerIndex(pos));
      if (overlay != null) {
        return overlay.isSet(posInContainer(pos));
      }

      return base.isSet(pos);
    }

    @Override
    public boolean set(int pos) {
      Preconditions.checkArgument(pos >= 0, "Cannot set invalid bit position: %s < 0", pos);
      Preconditions.checkArgument(
          pos < MAX_CARDINALITY, "Cannot set invalid bit position: %s (must be < 2,097,152)", pos);

      int containerIndex = containerIndex(pos);
      Container overlay = overlays.get(containerIndex);
      if (overlay == null) {
        if (containerIndex < baseContainerCount) {
          ByteBuffer src = base.buffer();
          int offset = src.position() + base.offset(containerIndex);
          overlay = new Container(src, offset, base.descriptor(containerIndex));
        } else {
          overlay = new Container();
        }
        overlays.put(containerIndex, overlay);
      }

      if (overlay.set(posInContainer(pos))) {
        this.cardinality += 1;
        return true;
      }

      return false;
    }

    @Override
    public ByteBuffer buffer() {
      int[] descriptorArray = descriptorArray();
      int containersSize = sizeOfContainers(descriptorArray, 0, descriptorArray.length);
      ByteBuffer descBuffer = PFOREncoding.encode(descriptorArray, descriptorArray.length);
      int descriptorsSize = descBuffer.remaining();

      ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + descriptorsSize + containersSize);
      int currentPos = writeHeader(buffer, cardinality, descriptorArray.length);

      buffer.put(HEADER_SIZE, descBuffer, descBuffer.position(), descriptorsSize);
      currentPos += descriptorsSize;

      int nextContainerIndex = 0;
      for (Map.Entry<Integer, Container> entry : overlays.entrySet()) {
        int containerIndex = entry.getKey();
        currentPos +=
            copyOriginalContainers(
                descriptorArray, nextContainerIndex, containerIndex, buffer, currentPos);
        currentPos += entry.getValue().writeTo(buffer, currentPos);
        nextContainerIndex = containerIndex + 1;
      }

      copyOriginalContainers(
          descriptorArray, nextContainerIndex, descriptorArray.length, buffer, currentPos);

      return buffer;
    }

    private int[] descriptorArray() {
      int[] baseDescriptors = base.descriptors();
      int lastOverlayIndex = overlays.isEmpty() ? -1 : overlays.lastKey();
      int numContainers = Math.max(lastOverlayIndex + 1, baseDescriptors.length);
      int[] descriptors = new int[numContainers]; // filled with 0
      System.arraycopy(baseDescriptors, 0, descriptors, 0, baseDescriptors.length);

      for (Map.Entry<Integer, Container> entry : overlays.entrySet()) {
        int containerIndex = entry.getKey();
        int containerCardinality = entry.getValue().cardinality();
        descriptors[containerIndex] = Math.min(containerCardinality, DENSE_CONTAINER_SIZE);
      }

      return descriptors;
    }

    private int sizeOfContainers(int[] descriptorArray, int startIndex, int endIndexExclusive) {
      int size = 0;
      for (int i = startIndex; i < endIndexExclusive; i += 1) {
        size += descriptorArray[i];
      }

      return size;
    }

    private static int writeHeader(ByteBuffer buffer, int cardinality, int descriptorCount) {
      ByteBuffers.writeByte(buffer, VERSION, 0);
      ByteBuffers.writeLittleEndianUnsigned(buffer, cardinality, 1, 3);
      ByteBuffers.writeLittleEndianUnsigned(buffer, descriptorCount, 4, 2);

      return HEADER_SIZE;
    }

    private int copyOriginalContainers(
        int[] descriptorArray,
        int startIndex,
        int endIndexExclusive,
        ByteBuffer buffer,
        int currentPos) {
      if (startIndex < baseContainerCount && startIndex < endIndexExclusive) {
        int offset = base.offset(startIndex);
        int sizeToCopy = sizeOfContainers(descriptorArray, startIndex, endIndexExclusive);
        if (sizeToCopy > 0) {
          ByteBuffer src = base.buffer();
          buffer.put(currentPos, src, src.position() + offset, sizeToCopy);
        }

        return sizeToCopy;
      }

      return 0;
    }
  }

  private static class Container {
    private byte[] dense = null;
    private byte[] sparse = null;
    private int cardinality = 0;

    private Container() {
      this.sparse = new byte[DENSE_CONTAINER_SIZE];
    }

    private Container(ByteBuffer buffer, int offset, int length) {
      byte[] bytes = new byte[DENSE_CONTAINER_SIZE];
      buffer.get(offset, bytes, 0, length);

      if (isDense(length)) {
        this.dense = bytes;
        this.cardinality = denseCardinality(dense);
      } else {
        this.sparse = bytes;
        this.cardinality = length;
      }
    }

    int cardinality() {
      return cardinality;
    }

    private boolean isSet(int posInContainer) {
      if (cardinality < 32) {
        // sparse
        for (int i = 0; i < cardinality; i += 1) {
          if ((sparse[i] & 0xFF) == posInContainer) {
            return true;
          }
        }

        return false;
      } else {
        // dense
        int bitShift = denseBitShift(posInContainer);
        return ((dense[denseByteIndex(posInContainer)] >>> bitShift) & 0b1) == 0b1;
      }
    }

    private boolean set(int posInContainer) {
      if (setInternal(posInContainer)) {
        this.cardinality += 1;
        return true;
      }

      return false;
    }

    int writeTo(ByteBuffer buffer, int offset) {
      if (cardinality < DENSE_CONTAINER_SIZE) {
        buffer.put(offset, sparse, 0, cardinality);
        return cardinality;
      } else {
        buffer.put(offset, dense, 0, dense.length);
        return dense.length;
      }
    }

    private boolean setInternal(int posInContainer) {
      if (cardinality < MAX_SPARSE_CARDINALITY) {
        return sparseSet(sparse, cardinality, posInContainer);
      } else if (cardinality > MAX_SPARSE_CARDINALITY) {
        return denseSet(dense, posInContainer);
      } else {
        // convert to dense, reuse converted buffer if it is present
        if (dense == null) {
          this.dense = convertToDense(sparse, cardinality);
        }

        if (denseSet(dense, posInContainer)) {
          this.sparse = null;
          return true;
        }

        // leave the converted dense array for next set attempt
        return false;
      }
    }

    private static boolean sparseSet(byte[] bytes, int length, int posInContainer) {
      byte nextInsert = (byte) posInContainer;
      for (int i = 0; i < length; i += 1) {
        if (nextInsert == bytes[i]) {
          return false; // already set
        } else if ((nextInsert & 0xFF) < (bytes[i] & 0xFF)) {
          byte replaced = bytes[i];
          bytes[i] = nextInsert;
          nextInsert = replaced;
        }
      }

      bytes[length] = nextInsert;
      return true;
    }

    private static byte[] convertToDense(byte[] sparse, int cardinality) {
      byte[] dense = new byte[DENSE_CONTAINER_SIZE];
      for (int i = 0; i < cardinality; i += 1) {
        denseSet(dense, sparse[i] & 0xFF);
      }

      return dense;
    }

    private static boolean denseSet(byte[] bytes, int posInContainer) {
      int byteIndex = denseByteIndex(posInContainer);
      byte bit = (byte) (0b1 << denseBitShift(posInContainer));
      if ((bytes[byteIndex] & bit) != bit) {
        bytes[byteIndex] |= bit;
        return true;
      }

      return false;
    }

    private static int denseCardinality(byte[] bytes) {
      int cardinality = 0;
      for (int i = 0; i < bytes.length; i += 1) {
        cardinality += Integer.bitCount(bytes[i] & 0xFF);
      }

      return cardinality;
    }
  }

  /**
   * Decode the descriptor array and produce an array of absolute container offsets in the buffer.
   */
  private void decodeDescriptors() {
    int[] descriptorArray = new int[containerCount];
    int bytesRead = PFOREncoding.decode(data, HEADER_SIZE, descriptorArray, 0, containerCount);

    int[] offsetArray = new int[containerCount + 1];
    int firstContainerOffset = HEADER_SIZE + bytesRead;
    descriptorsToOffsets(firstContainerOffset, descriptorArray, offsetArray);

    // update the references last so that only valid values are available
    this.descriptors = descriptorArray;
    this.offsets = offsetArray;
  }

  private static boolean isDense(int descriptor) {
    return (descriptor & 0xFF) == DENSE_CONTAINER_BIT;
  }

  private static boolean isSparse(int descriptor) {
    return descriptor < DENSE_CONTAINER_SIZE;
  }

  private static int containerIndex(int pos) {
    return pos >>> 8;
  }

  private static int posInContainer(int pos) {
    return pos & 0xFF;
  }

  private static int denseByteIndex(int posInContainer) {
    return posInContainer >>> 3;
  }

  private static int denseBitShift(int posInContainer) {
    return 7 - (posInContainer & 0b111);
  }

  /**
   * Convert an array of lengths into an array of offsets starting at the given base.
   *
   * <p>For example, descriptorsToOffsets(0, [1, 1, 2]) produces [0, 1, 2, 4].
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
        offsets[i + 1] = offsets[i] + DENSE_CONTAINER_SIZE;
      } else if (isSparse(descriptors[i])) {
        offsets[i + 1] = offsets[i] + descriptors[i];
      } else {
        throw new IllegalStateException(
            "Invalid descriptor, not sparse or dense: " + descriptors[i]);
      }
    }
  }
}
