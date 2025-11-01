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
package org.apache.iceberg.geospatial;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Represents a geospatial bounding box composed of minimum and maximum bounds.
 *
 * <p>A bounding box (also called a Minimum Bounding Rectangle or MBR) is defined by two points: the
 * minimum and maximum coordinates that define the box's corners. This provides a simple
 * approximation of a more complex geometry for efficient filtering and data skipping.
 */
public class BoundingBox {
  /**
   * Create a {@link BoundingBox} object from buffers containing min and max bounds
   *
   * @param min the serialized minimum bound
   * @param max the serialized maximum bound
   * @return a BoundingBox instance
   */
  public static BoundingBox fromByteBuffers(ByteBuffer min, ByteBuffer max) {
    return new BoundingBox(
        GeospatialBound.fromByteBuffer(min), GeospatialBound.fromByteBuffer(max));
  }

  /**
   * Deserialize a byte buffer as a {@link BoundingBox} object
   *
   * @param buffer the serialized bounding box
   * @return a BoundingBox instance
   */
  public static BoundingBox fromByteBuffer(ByteBuffer buffer) {
    ByteBuffer tmp = buffer.duplicate();
    tmp.order(ByteOrder.LITTLE_ENDIAN);

    int minLen = tmp.getInt();
    Preconditions.checkArgument(
        minLen == 2 * Double.BYTES || minLen == 3 * Double.BYTES || minLen == 4 * Double.BYTES,
        "Invalid geo spatial lower bound buffer size for: %s. Valid sizes are 16, 24, or 32 bytes.",
        minLen);
    ByteBuffer min = tmp.slice().order(ByteOrder.LITTLE_ENDIAN);
    min.limit(minLen);
    tmp.position(tmp.position() + minLen);

    int maxLen = tmp.getInt();
    Preconditions.checkArgument(
        maxLen == 2 * Double.BYTES || maxLen == 3 * Double.BYTES || maxLen == 4 * Double.BYTES,
        "Invalid geo spatial upper bound buffer size: %s. Valid sizes are 16, 24, or 32 bytes.",
        maxLen);
    ByteBuffer max = tmp.slice().order(ByteOrder.LITTLE_ENDIAN);
    max.limit(maxLen);

    return fromByteBuffers(min, max);
  }

  /**
   * Serializes this bounding box to a byte buffer. The serialized byte buffer could be deserialized
   * using {@link #fromByteBuffer(ByteBuffer)}.
   *
   * @return a byte buffer containing the serialized bounding box
   */
  public ByteBuffer toByteBuffer() {
    ByteBuffer minBuffer = min.toByteBuffer();
    ByteBuffer maxBuffer = max.toByteBuffer();

    int totalSize = Integer.BYTES + minBuffer.remaining() + Integer.BYTES + maxBuffer.remaining();
    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);

    buffer.putInt(minBuffer.remaining());
    buffer.put(minBuffer);
    buffer.putInt(maxBuffer.remaining());
    buffer.put(maxBuffer);
    buffer.flip();
    return buffer;
  }

  public BoundingBox(GeospatialBound min, GeospatialBound max) {
    this.min = min;
    this.max = max;
  }

  private final GeospatialBound min;
  private final GeospatialBound max;

  /**
   * Get the minimum corner of the bounding box.
   *
   * @return the minimum bound
   */
  public GeospatialBound min() {
    return min;
  }

  /**
   * Get the maximum corner of the bounding box.
   *
   * @return the maximum bound
   */
  public GeospatialBound max() {
    return max;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof BoundingBox)) {
      return false;
    }

    BoundingBox that = (BoundingBox) other;
    return Objects.equals(min, that.min) && Objects.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(min, max);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(BoundingBox.class).add("min", min).add("max", max).toString();
  }
}
