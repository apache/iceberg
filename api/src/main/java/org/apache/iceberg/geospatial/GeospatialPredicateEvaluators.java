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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class GeospatialPredicateEvaluators {
  private GeospatialPredicateEvaluators() {}

  public interface GeospatialPredicateEvaluator {
    /**
     * Determines whether the two bounding boxes intersect.
     *
     * @param bbox1 the first bounding box
     * @param bbox2 the second bounding box
     * @return true if this box intersects the other box
     */
    boolean intersects(BoundingBox bbox1, BoundingBox bbox2);
  }

  /**
   * Create an evaluator for evaluating bounding box relationship for the given geospatial type.
   *
   * @param type the geospatial type, should be one of Type.TypeID.GEOMETRY or Type.TypeID.GEOGRAPHY
   * @return the evaluator
   */
  public static GeospatialPredicateEvaluator create(Type type) {
    switch (type.typeId()) {
      case GEOMETRY:
        return create((Types.GeometryType) type);
      case GEOGRAPHY:
        return create((Types.GeographyType) type);
      default:
        throw new UnsupportedOperationException("Unsupported type for BoundingBox: " + type);
    }
  }

  /**
   * Create an evaluator for evaluating bounding box relationship for planar geometries
   *
   * @return the evaluator
   */
  public static GeometryEvaluator create(Types.GeometryType type) {
    return new GeometryEvaluator();
  }

  /**
   * Create an evaluator for evaluating bounding box relationship for geographies
   *
   * @return the evaluator
   */
  public static GeographyEvaluator create(Types.GeographyType type) {
    return new GeographyEvaluator();
  }

  public static class GeometryEvaluator implements GeospatialPredicateEvaluator {

    /**
     * Check if two bounding boxes intersect
     *
     * @param bbox1 the first bounding box
     * @param bbox2 the second bounding box
     * @return true if the bounding boxes intersect
     */
    @Override
    public boolean intersects(BoundingBox bbox1, BoundingBox bbox2) {
      if (!intersectsYZM(bbox1, bbox2)) {
        return false;
      }

      // Check X dimension (longitude/easting) - no wrap-around
      return rangeIntersects(bbox1.min().x(), bbox1.max().x(), bbox2.min().x(), bbox2.max().x());
    }

    static boolean intersectsYZM(BoundingBox bbox1, BoundingBox bbox2) {
      // Check Z dimension (elevation) if both boxes have Z coordinates - no wrap-around
      if (bbox1.min().hasZ() && bbox1.max().hasZ() && bbox2.min().hasZ() && bbox2.max().hasZ()) {
        if (!rangeIntersects(bbox1.min().z(), bbox1.max().z(), bbox2.min().z(), bbox2.max().z())) {
          return false;
        }
      }

      // Check M dimension (measure) if both boxes have M coordinates - no wrap-around
      if (bbox1.min().hasM() && bbox1.max().hasM() && bbox2.min().hasM() && bbox2.max().hasM()) {
        if (!rangeIntersects(bbox1.min().m(), bbox1.max().m(), bbox2.min().m(), bbox2.max().m())) {
          return false;
        }
      }

      // Check Y dimension (latitude/northing) - no wrap-around
      if (!rangeIntersects(bbox1.min().y(), bbox1.max().y(), bbox2.min().y(), bbox2.max().y())) {
        return false;
      }

      return true;
    }

    /**
     * Check if two intervals intersect using regular interval logic. Two intervals [min1, max1] and
     * [min2, max2] intersect if min1 <= max2 AND max1 >= min2.
     *
     * @param min1 minimum of first interval
     * @param max1 maximum of first interval
     * @param min2 minimum of second interval
     * @param max2 maximum of second interval
     * @return true if the intervals intersect
     */
    static boolean rangeIntersects(double min1, double max1, double min2, double max2) {
      return min1 <= max2 && max1 >= min2;
    }
  }

  public static class GeographyEvaluator implements GeospatialPredicateEvaluator {
    /**
     * Check if two bounding boxes intersect, taking wrap-around into account.
     *
     * <p>Wraparound (or antimeridian crossing) occurs when a geography crosses the 180°/-180°
     * longitude line on a map. In these cases, the minimum X value is greater than the maximum X
     * value (xmin > xmax). This represents a bounding box that wraps around the globe.
     *
     * <p>For example, a bounding box with xmin=170° and xmax=-170° represents an area that spans
     * from 170° east to 190° east (or equivalently, -170° west). This is important for geometries
     * that cross the antimeridian, like a path from Japan to Alaska.
     *
     * <p>When xmin > xmax, a point matches if its X coordinate is either X ≥ xmin OR X ≤ xmax,
     * rather than the usual X ≥ xmin AND X ≤ xmax. In geographic terms, if the westernmost
     * longitude is greater than the easternmost longitude, this indicates an antimeridian crossing.
     *
     * @param bbox1 the first bounding box
     * @param bbox2 the second bounding box
     * @return true if the bounding boxes intersect
     */
    @Override
    public boolean intersects(BoundingBox bbox1, BoundingBox bbox2) {
      validateBoundingBox(bbox1);
      validateBoundingBox(bbox2);

      if (!GeometryEvaluator.intersectsYZM(bbox1, bbox2)) {
        return false;
      }

      // Check X dimension (longitude/easting) - with wrap-around
      return rangeIntersectsWithWrapAround(
          bbox1.min().x(), bbox1.max().x(), bbox2.min().x(), bbox2.max().x());
    }

    /**
     * For geography types, coordinates are restricted to the canonical ranges of [-180°, 180°] for
     * longitude (X) and [-90°, 90°] for latitude (Y).
     *
     * @param bbox the bounding box to validate
     * @throws IllegalArgumentException if the bounding box is invalid
     */
    private void validateBoundingBox(BoundingBox bbox) {
      Preconditions.checkArgument(
          bbox.min().y() >= -90.0d && bbox.max().y() <= 90.0d, "Latitude out of range: %s", bbox);
      Preconditions.checkArgument(
          bbox.min().x() >= -180.0d
              && bbox.min().x() <= 180.0d
              && bbox.max().x() >= -180.0d
              && bbox.max().x() <= 180.0d,
          "Longitude out of range: %s",
          bbox);
    }

    /**
     * Check if two intervals intersect with wrap-around support for longitude/X dimension. Handles
     * antimeridian crossing where min > max indicates wrapping around the globe.
     *
     * @param min1 minimum of first interval (may be > max1 if wrapping)
     * @param max1 maximum of first interval (may be < min1 if wrapping)
     * @param min2 minimum of second interval (may be > max2 if wrapping)
     * @param max2 maximum of second interval (may be < min2 if wrapping)
     * @return true if the intervals intersect
     */
    private static boolean rangeIntersectsWithWrapAround(
        double min1, double max1, double min2, double max2) {
      boolean interval1WrapsAround = min1 > max1;
      boolean interval2WrapsAround = min2 > max2;

      if (!interval1WrapsAround && !interval2WrapsAround) {
        // No wrap-around in either interval - use regular intersection
        return GeometryEvaluator.rangeIntersects(min1, max1, min2, max2);
      } else if (interval1WrapsAround && interval2WrapsAround) {
        // Both intervals wrap around - they must intersect somewhere
        return true;
      } else if (interval1WrapsAround) {
        // interval1 wraps around, interval2 does not
        return min1 <= max2 || max1 >= min2;
      } else {
        // interval2 wraps around, interval1 does not
        return min2 <= max1 || max2 >= min1;
      }
    }
  }
}
