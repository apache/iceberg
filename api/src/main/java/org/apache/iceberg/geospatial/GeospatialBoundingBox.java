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
public class GeospatialBoundingBox implements Serializable, Comparable<GeospatialBoundingBox> {
  private final GeospatialBound min;
  private final GeospatialBound max;

  public static final GeospatialBoundingBox SANITIZED =
      new GeospatialBoundingBox(
          GeospatialBound.createXY(Double.NaN, Double.NaN),
          GeospatialBound.createXY(Double.NaN, Double.NaN));

  /**
   * Create an appropriate GeospatialBoundingBox according to the geospatial type.
   *
   * @param min the serialized minimum bound
   * @param max the serialized maximum bound
   * @return a GeospatialBoundingBox instance
   */
  public static GeospatialBoundingBox create(ByteBuffer min, ByteBuffer max) {
    return new GeospatialBoundingBox(
        GeospatialBound.fromByteBuffer(min), GeospatialBound.fromByteBuffer(max));
  }

  public GeospatialBoundingBox(GeospatialBound min, GeospatialBound max) {
    this.min = min;
    this.max = max;
  }

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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GeospatialBoundingBox that = (GeospatialBoundingBox) o;
    return Objects.equals(min, that.min) && Objects.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(min, max);
  }

  @Override
  public String toString() {
    if (SANITIZED.equals(this)) {
      return "BoundingBox{sanitized}";
    }
    return "BoundingBox{min=" + min.simpleString() + ", max=" + max.simpleString() + '}';
  }

  @Override
  public int compareTo(GeospatialBoundingBox o) {
    int minComparison = min.compareTo(o.min);
    if (minComparison != 0) {
      return minComparison;
    }
    return max.compareTo(o.max);
  }
}
