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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestGeospatialPredicateEvaluators {

  @Test
  public void testGeometryType() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    assertThat(evaluator).isInstanceOf(GeospatialPredicateEvaluators.GeometryEvaluator.class);
  }

  @Test
  public void testOverlappingBoxesIntersect() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(5.0, 5.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(3.0, 3.0);
    GeospatialBound max2 = GeospatialBound.createXY(8.0, 8.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testNonOverlappingBoxesDontIntersect() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(2.0, 2.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(3.0, 3.0);
    GeospatialBound max2 = GeospatialBound.createXY(5.0, 5.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isFalse();
    assertThat(evaluator.intersects(box2, box1)).isFalse();
  }

  @Test
  public void testBoxesTouchingAtCornerIntersect() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(2.0, 2.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(2.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXY(4.0, 4.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testBoxesTouchingAtEdgeIntersect() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(2.0, 2.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(2.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(4.0, 2.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testBoxContainedWithinAnotherIntersects() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(10.0, 10.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(2.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXY(5.0, 5.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testBoxesWithZCoordinate() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // Two boxes with Z coordinates that overlap in X and Y but not in Z
    // Note: The current implementation only checks X and Y coordinates
    GeospatialBound min1 = GeospatialBound.createXYZ(0.0, 0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXYZ(2.0, 2.0, 1.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXYZ(1.0, 1.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXYZ(3.0, 3.0, 3.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    // They should intersect because the current implementation only checks X and Y
    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testBoxesWithMCoordinate() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // Two boxes with M coordinates that overlap in X and Y but not in M
    // Note: The current implementation only checks X and Y coordinates
    GeospatialBound min1 = GeospatialBound.createXYM(0.0, 0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXYM(2.0, 2.0, 1.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXYM(1.0, 1.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXYM(3.0, 3.0, 3.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    // They should intersect because the current implementation only checks X and Y
    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testGeometryWrapAroundOnA() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // First box wraps around antimeridian (min.x > max.x), second doesn't
    GeospatialBound min1 = GeospatialBound.createXY(170.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(-170.0, 10.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    // Box that overlaps with the part after the wrap around
    GeospatialBound min2 = GeospatialBound.createXY(-175.0, 5.0);
    GeospatialBound max2 = GeospatialBound.createXY(-160.0, 15.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();

    // Box that overlaps with the part before the wrap around
    GeospatialBound min3 = GeospatialBound.createXY(160.0, 5.0);
    GeospatialBound max3 = GeospatialBound.createXY(175.0, 15.0);
    GeospatialBoundingBox box3 = new GeospatialBoundingBox(min3, max3);

    assertThat(evaluator.intersects(box1, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box1)).isTrue();

    // Box that doesn't overlap with either part
    GeospatialBound min4 = GeospatialBound.createXY(-150.0, 20.0);
    GeospatialBound max4 = GeospatialBound.createXY(-140.0, 30.0);
    GeospatialBoundingBox box4 = new GeospatialBoundingBox(min4, max4);

    assertThat(evaluator.intersects(box1, box4)).isFalse();
    assertThat(evaluator.intersects(box4, box1)).isFalse();
  }

  @Test
  public void testGeometryWrapAroundOnB() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // First box doesn't wrap around, second does (min.x > max.x)
    GeospatialBound min1 = GeospatialBound.createXY(-175.0, 5.0);
    GeospatialBound max1 = GeospatialBound.createXY(-160.0, 15.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(170.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(-170.0, 10.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testBothGeometryWrappingAround() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // Both boxes wrap around (min.x > max.x)
    GeospatialBound min1 = GeospatialBound.createXY(170.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(-170.0, 10.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(160.0, 5.0);
    GeospatialBound max2 = GeospatialBound.createXY(-160.0, 15.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    // When both wrap around, they must intersect
    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testBasicGeographyCases() {
    Type geographyType = Types.GeographyType.of("srid:4326", EdgeAlgorithm.SPHERICAL);
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geographyType);

    // Two overlapping boxes
    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(10.0, 10.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(5.0, 5.0);
    GeospatialBound max2 = GeospatialBound.createXY(15.0, 15.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();

    // Non-overlapping boxes
    GeospatialBound min3 = GeospatialBound.createXY(20.0, 20.0);
    GeospatialBound max3 = GeospatialBound.createXY(30.0, 30.0);
    GeospatialBoundingBox box3 = new GeospatialBoundingBox(min3, max3);

    assertThat(evaluator.intersects(box1, box3)).isFalse();
    assertThat(evaluator.intersects(box3, box1)).isFalse();

    // Boxes at extreme valid latitudes
    GeospatialBound min4 = GeospatialBound.createXY(-10.0, -90.0);
    GeospatialBound max4 = GeospatialBound.createXY(10.0, -80.0);
    GeospatialBoundingBox box4 = new GeospatialBoundingBox(min4, max4);

    GeospatialBound min5 = GeospatialBound.createXY(-5.0, 80.0);
    GeospatialBound max5 = GeospatialBound.createXY(15.0, 90.0);
    GeospatialBoundingBox box5 = new GeospatialBoundingBox(min5, max5);

    assertThat(evaluator.intersects(box4, box5)).isFalse();
    assertThat(evaluator.intersects(box5, box4)).isFalse();
  }

  @Test
  public void testGeographyWrapAround() {
    Type geographyType = Types.GeographyType.of("srid:4326", EdgeAlgorithm.SPHERICAL);
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geographyType);

    // Box that wraps around the antimeridian
    GeospatialBound min1 = GeospatialBound.createXY(170.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(-170.0, 10.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    // Box that overlaps with the part after the wrap around
    GeospatialBound min2 = GeospatialBound.createXY(-175.0, 5.0);
    GeospatialBound max2 = GeospatialBound.createXY(-160.0, 15.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testInvalidGeographyLatitude() {
    Type geographyType = Types.GeographyType.of("srid:4326", EdgeAlgorithm.SPHERICAL);
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geographyType);

    // Box with latitude below -90
    GeospatialBound min1 = GeospatialBound.createXY(0.0, -91.0);
    GeospatialBound max1 = GeospatialBound.createXY(10.0, 0.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    // Box with latitude above 90
    GeospatialBound min2 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(10.0, 91.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    GeospatialBound validMin = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound validMax = GeospatialBound.createXY(10.0, 10.0);
    GeospatialBoundingBox validBox = new GeospatialBoundingBox(validMin, validMax);

    assertThatThrownBy(() -> evaluator.intersects(box1, validBox))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Latitude out of range");

    assertThatThrownBy(() -> evaluator.intersects(validBox, box1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Latitude out of range");

    assertThatThrownBy(() -> evaluator.intersects(box2, validBox))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Latitude out of range");

    assertThatThrownBy(() -> evaluator.intersects(validBox, box2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Latitude out of range");
  }

  @Test
  public void testInvalidGeographyLongitude() {
    Type geographyType = Types.GeographyType.of("srid:4326", EdgeAlgorithm.SPHERICAL);
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geographyType);

    // Box with longitude below -180
    GeospatialBound min1 = GeospatialBound.createXY(-181.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(0.0, 10.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    // Box with longitude above 180
    GeospatialBound min2 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(181.0, 10.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    GeospatialBound validMin = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound validMax = GeospatialBound.createXY(10.0, 10.0);
    GeospatialBoundingBox validBox = new GeospatialBoundingBox(validMin, validMax);

    assertThatThrownBy(() -> evaluator.intersects(box1, validBox))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Longitude out of range");

    assertThatThrownBy(() -> evaluator.intersects(validBox, box1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Longitude out of range");

    assertThatThrownBy(() -> evaluator.intersects(box2, validBox))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Longitude out of range");

    assertThatThrownBy(() -> evaluator.intersects(validBox, box2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Longitude out of range");
  }

  @Test
  public void testExtremeLongitudeBoundaries() {
    // Tests valid boxes at the extreme boundaries of longitude
    Type geographyType = Types.GeographyType.of("srid:4326", EdgeAlgorithm.SPHERICAL);
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geographyType);

    // Box at -180 longitude
    GeospatialBound min1 = GeospatialBound.createXY(-180.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(-170.0, 10.0);
    GeospatialBoundingBox box1 = new GeospatialBoundingBox(min1, max1);

    // Box at 180 longitude
    GeospatialBound min2 = GeospatialBound.createXY(170.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(180.0, 10.0);
    GeospatialBoundingBox box2 = new GeospatialBoundingBox(min2, max2);

    // These boxes should not intersect
    assertThat(evaluator.intersects(box1, box2)).isFalse();
    assertThat(evaluator.intersects(box2, box1)).isFalse();

    // Box that wraps around the antimeridian, touching -180 and 180
    GeospatialBound min3 = GeospatialBound.createXY(180.0, 0.0);
    GeospatialBound max3 = GeospatialBound.createXY(-180.0, 10.0);
    GeospatialBoundingBox box3 = new GeospatialBoundingBox(min3, max3);

    // This should intersect with both boxes at the extreme edges
    assertThat(evaluator.intersects(box1, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box1)).isTrue();
    assertThat(evaluator.intersects(box2, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box2)).isTrue();
  }

  @Test
  public void testSphericalGeographyType() {
    Type geographyType = Types.GeographyType.of("srid:4326", EdgeAlgorithm.SPHERICAL);
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geographyType);

    assertThat(evaluator).isInstanceOf(GeospatialPredicateEvaluators.GeographyEvaluator.class);
  }

  @Test
  public void testUnsupportedType() {
    Type stringType = Types.StringType.get();

    assertThatThrownBy(() -> GeospatialPredicateEvaluators.create(stringType))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Unsupported type for GeospatialBoundingBox");
  }
}
