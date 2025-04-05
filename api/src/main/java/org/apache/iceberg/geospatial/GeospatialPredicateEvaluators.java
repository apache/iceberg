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
    boolean intersects(GeospatialBoundingBox bbox1, GeospatialBoundingBox bbox2);
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
    public boolean intersects(GeospatialBoundingBox bbox1, GeospatialBoundingBox bbox2) {
      return intersectsWithWrapAround(bbox1, bbox2);
    }

    static boolean intersectsWithWrapAround(
        GeospatialBoundingBox bbox1, GeospatialBoundingBox bbox2) {
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
    public boolean intersects(GeospatialBoundingBox bbox1, GeospatialBoundingBox bbox2) {
      validateBoundingBox(bbox1);
      validateBoundingBox(bbox2);
      return GeometryEvaluator.intersectsWithWrapAround(bbox1, bbox2);
    }

    private void validateBoundingBox(GeospatialBoundingBox bbox) {
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
