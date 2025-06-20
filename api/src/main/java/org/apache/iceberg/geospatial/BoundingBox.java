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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents a geospatial bounding box composed of minimum and maximum bounds.
 *
 * <p>A bounding box (also called a Minimum Bounding Rectangle or MBR) is defined by two points: the
 * minimum and maximum coordinates that define the box's corners. This provides a simple
 * approximation of a more complex geometry for efficient filtering and data skipping.
 */
public class BoundingBox implements Serializable, Comparable<BoundingBox> {
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
   * Create an empty bounding box
   *
   * @return an empty bounding box
   */
  public static BoundingBox empty() {
    return new BoundingBox(
        GeospatialBound.createXY(Double.NaN, Double.NaN),
        GeospatialBound.createXY(Double.NaN, Double.NaN));
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
    return "BoundingBox{min=" + min.simpleString() + ", max=" + max.simpleString() + '}';
  }

  @Override
  public int compareTo(BoundingBox other) {
    int minComparison = min.compareTo(other.min);
    if (minComparison != 0) {
      return minComparison;
    }

    return max.compareTo(other.max);
  }
}
