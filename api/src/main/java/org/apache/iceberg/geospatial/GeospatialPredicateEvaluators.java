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

public class GeospatialPredicateEvaluators {
  private GeospatialPredicateEvaluators() {}

  public interface GeospatialPredicateEvaluator {
    /**
     * Test whether this bounding box intersects with another.
     *
     * @param bbox1 the first bounding box
     * @param bbox2 the second bounding box
     * @return true if this box intersects the other box
     */
    boolean intersects(BoundingBox bbox1, BoundingBox bbox2);
  }

  public static GeospatialPredicateEvaluator create(Type type) {
    switch (type.typeId()) {
      case GEOMETRY:
        return new GeometryEvaluator();
      case GEOGRAPHY:
        return new GeographyEvaluator();
      default:
        throw new UnsupportedOperationException("Unsupported type for BoundingBox: " + type);
    }
  }

  static class GeometryEvaluator implements GeospatialPredicateEvaluator {
    @Override
    public boolean intersects(BoundingBox bbox1, BoundingBox bbox2) {
      return intersectsWithWrapAround(bbox1, bbox2);
    }

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
     * <p>The Iceberg specification does not explicitly rule out the use of wrap-around in bounding
     * boxes for geometry types, so we handle wrap-around for both geography and geometry bounding
     * boxes.
     *
     * @param bbox1 the first bounding box
     * @param bbox2 the second bounding box
     * @return true if the bounding boxes intersect
     */
    static boolean intersectsWithWrapAround(BoundingBox bbox1, BoundingBox bbox2) {
      // Let's check y first, and if y does not intersect, we can return false
      if (bbox1.min().y() > bbox2.max().y() || bbox1.max().y() < bbox2.min().y()) {
        return false;
      }

      // Now check x, need to take wrap-around into account
      if (bbox1.min().x() <= bbox1.max().x() && bbox2.min().x() <= bbox2.max().x()) {
        // No wrap-around
        return bbox1.min().x() <= bbox2.max().x() && bbox1.max().x() >= bbox2.min().x();
      } else if (bbox1.min().x() > bbox1.max().x() && bbox2.min().x() <= bbox2.max().x()) {
        // bbox1 wraps around the antimeridian, bbox2 does not
        return bbox1.min().x() <= bbox2.max().x() || bbox1.max().x() >= bbox2.min().x();
      } else if (bbox1.min().x() <= bbox1.max().x() && bbox2.min().x() > bbox2.max().x()) {
        // bbox2 wraps around the antimeridian, bbox1 does not
        return intersectsWithWrapAround(bbox2, bbox1);
      } else {
        // Both wrap around the antimeridian, they must intersect
        return true;
      }
    }
  }

  static class GeographyEvaluator implements GeospatialPredicateEvaluator {
    @Override
    public boolean intersects(BoundingBox bbox1, BoundingBox bbox2) {
      validateBoundingBox(bbox1);
      validateBoundingBox(bbox2);
      return GeometryEvaluator.intersectsWithWrapAround(bbox1, bbox2);
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
          bbox.min().y() >= -90 && bbox.max().y() <= 90, "Latitude out of range: %s", bbox);
      Preconditions.checkArgument(
          bbox.min().x() >= -180
              && bbox.min().x() <= 180
              && bbox.max().x() >= -180
              && bbox.max().x() <= 180,
          "Longitude out of range: %s",
          bbox);
    }
  }
}
