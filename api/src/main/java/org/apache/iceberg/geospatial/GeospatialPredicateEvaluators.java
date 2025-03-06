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

import org.apache.iceberg.types.Type;

public class GeospatialPredicateEvaluators {

  public interface GeospatialPredicateEvaluator {
    /**
     * Test whether this bounding box intersects with another.
     *
     * @param a the first bounding box
     * @param b the second bounding box
     * @return true if this box intersects the other box
     */
    boolean intersects(GeospatialBoundingBox a, GeospatialBoundingBox b);
  }

  public static GeospatialPredicateEvaluator create(Type type) {
    switch (type.typeId()) {
      case GEOMETRY:
        return new GeometryEvaluator();
      case GEOGRAPHY:
        return new GeographyEvaluator();
      default:
        throw new UnsupportedOperationException(
            "Unsupported type for GeospatialBoundingBox: " + type);
    }
  }

  static class GeometryEvaluator implements GeospatialPredicateEvaluator {
    @Override
    public boolean intersects(GeospatialBoundingBox a, GeospatialBoundingBox b) {
      return intersectsWithWrapAround(a, b);
    }

    static boolean intersectsWithWrapAround(GeospatialBoundingBox a, GeospatialBoundingBox b) {
      // Let's check y first, and if y does not intersect, we can return false
      if (a.min().y() > b.max().y() || a.max().y() < b.min().y()) {
        return false;
      }

      // Now check x, need to take wrap-around into account
      if (a.min().x() <= a.max().x() && b.min().x() <= b.max().x()) {
        // No wrap-around
        return a.min().x() <= b.max().x() && a.max().x() >= b.min().x();
      } else if (a.min().x() > a.max().x() && b.min().x() <= b.max().x()) {
        // a wraps around the antimeridian, b does not
        return a.min().x() <= b.max().x() || a.max().x() >= b.min().x();
      } else if (a.min().x() <= a.max().x() && b.min().x() > b.max().x()) {
        // b wraps around the antimeridian, a does not
        return intersectsWithWrapAround(b, a);
      } else {
        // Both wrap around the antimeridian, they must intersect
        return true;
      }
    }
  }

  static class GeographyEvaluator implements GeospatialPredicateEvaluator {
    @Override
    public boolean intersects(GeospatialBoundingBox a, GeospatialBoundingBox b) {
      validateBoundingBox(a);
      validateBoundingBox(b);
      return GeometryEvaluator.intersectsWithWrapAround(a, b);
    }

    private void validateBoundingBox(GeospatialBoundingBox box) {
      if (box.min().y() < -90 || box.max().y() > 90) {
        throw new IllegalArgumentException("Latitude out of range: " + box);
      }
      if (box.min().x() < -180
          || box.min().x() > 180
          || box.max().x() < -180
          || box.max().x() > 180) {
        throw new IllegalArgumentException("Longitude out of range: " + box);
      }
    }
  }
}
