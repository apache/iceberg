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
        .hasMessageContaining("Unsupported type for BoundingBox");
  }

  @Test
  public void testOverlappingGeometryBoxesIntersect() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(5.0, 5.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(3.0, 3.0);
    GeospatialBound max2 = GeospatialBound.createXY(8.0, 8.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testNonOverlappingGeometryBoxesDontIntersect() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(2.0, 2.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(3.0, 3.0);
    GeospatialBound max2 = GeospatialBound.createXY(5.0, 5.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isFalse();
    assertThat(evaluator.intersects(box2, box1)).isFalse();
  }

  @Test
  public void testGeometryBoxesTouchingAtCornerIntersect() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(2.0, 2.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(2.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXY(4.0, 4.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testGeometryBoxesTouchingAtEdgeIntersect() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(2.0, 2.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(2.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(4.0, 2.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testGeometryBoxContainedWithinAnotherIntersects() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(10.0, 10.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(2.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXY(5.0, 5.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testGeometryBoxesWithZCoordinate() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // Two boxes with Z coordinates that overlap in X and Y but not in Z
    GeospatialBound min1 = GeospatialBound.createXYZ(0.0, 0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXYZ(2.0, 2.0, 1.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXYZ(1.0, 1.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXYZ(3.0, 3.0, 3.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    GeospatialBound min3 = GeospatialBound.createXYZ(1.0, 1.0, 1.0);
    GeospatialBound max3 = GeospatialBound.createXYZ(3.0, 3.0, 3.0);
    BoundingBox box3 = new BoundingBox(min3, max3);

    assertThat(evaluator.intersects(box1, box2)).isFalse();
    assertThat(evaluator.intersects(box2, box1)).isFalse();
    assertThat(evaluator.intersects(box1, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box1)).isTrue();
    assertThat(evaluator.intersects(box2, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box2)).isTrue();
  }

  @Test
  public void testGeometryBoxesWithMCoordinate() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // Two boxes with M coordinates that overlap in X and Y but not in M
    GeospatialBound min1 = GeospatialBound.createXYM(0.0, 0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXYM(2.0, 2.0, 1.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXYM(1.0, 1.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXYM(3.0, 3.0, 3.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    GeospatialBound min3 = GeospatialBound.createXYM(1.0, 1.0, 1.0);
    GeospatialBound max3 = GeospatialBound.createXYM(3.0, 3.0, 3.0);
    BoundingBox box3 = new BoundingBox(min3, max3);

    assertThat(evaluator.intersects(box1, box2)).isFalse();
    assertThat(evaluator.intersects(box2, box1)).isFalse();
    assertThat(evaluator.intersects(box1, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box1)).isTrue();
    assertThat(evaluator.intersects(box2, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box2)).isTrue();
  }

  @Test
  public void testGeometryWrapAroundOnA() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // First box wraps around antimeridian (min.x > max.x), second doesn't
    GeospatialBound min1 = GeospatialBound.createXY(170.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(-170.0, 10.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    // Box that overlaps with the part after the wrap around
    GeospatialBound min2 = GeospatialBound.createXY(-175.0, 5.0);
    GeospatialBound max2 = GeospatialBound.createXY(-160.0, 15.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();

    // Box that overlaps with the part before the wrap around
    GeospatialBound min3 = GeospatialBound.createXY(160.0, 5.0);
    GeospatialBound max3 = GeospatialBound.createXY(175.0, 15.0);
    BoundingBox box3 = new BoundingBox(min3, max3);

    assertThat(evaluator.intersects(box1, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box1)).isTrue();

    // Box that doesn't overlap with either part
    GeospatialBound min4 = GeospatialBound.createXY(-150.0, 20.0);
    GeospatialBound max4 = GeospatialBound.createXY(-140.0, 30.0);
    BoundingBox box4 = new BoundingBox(min4, max4);

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
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(170.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(-170.0, 10.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testBothGeometriesWrappingAround() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // Both boxes wrap around (min.x > max.x)
    GeospatialBound min1 = GeospatialBound.createXY(170.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(-170.0, 10.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(160.0, 5.0);
    GeospatialBound max2 = GeospatialBound.createXY(-160.0, 15.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

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
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXY(5.0, 5.0);
    GeospatialBound max2 = GeospatialBound.createXY(15.0, 15.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();

    // Non-overlapping boxes
    GeospatialBound min3 = GeospatialBound.createXY(20.0, 20.0);
    GeospatialBound max3 = GeospatialBound.createXY(30.0, 30.0);
    BoundingBox box3 = new BoundingBox(min3, max3);

    assertThat(evaluator.intersects(box1, box3)).isFalse();
    assertThat(evaluator.intersects(box3, box1)).isFalse();

    // Boxes at extreme valid latitudes
    GeospatialBound min4 = GeospatialBound.createXY(-10.0, -90.0);
    GeospatialBound max4 = GeospatialBound.createXY(10.0, -80.0);
    BoundingBox box4 = new BoundingBox(min4, max4);

    GeospatialBound min5 = GeospatialBound.createXY(-5.0, 80.0);
    GeospatialBound max5 = GeospatialBound.createXY(15.0, 90.0);
    BoundingBox box5 = new BoundingBox(min5, max5);

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
    BoundingBox box1 = new BoundingBox(min1, max1);

    // Box that overlaps with the part after the wrap around
    GeospatialBound min2 = GeospatialBound.createXY(-175.0, 5.0);
    GeospatialBound max2 = GeospatialBound.createXY(-160.0, 15.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

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
    BoundingBox box1 = new BoundingBox(min1, max1);

    // Box with latitude above 90
    GeospatialBound min2 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(10.0, 91.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    GeospatialBound validMin = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound validMax = GeospatialBound.createXY(10.0, 10.0);
    BoundingBox validBox = new BoundingBox(validMin, validMax);

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
    BoundingBox box1 = new BoundingBox(min1, max1);

    // Box with longitude above 180
    GeospatialBound min2 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(181.0, 10.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    GeospatialBound validMin = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound validMax = GeospatialBound.createXY(10.0, 10.0);
    BoundingBox validBox = new BoundingBox(validMin, validMax);

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
  public void testExtremeGeographyLongitudeBoundaries() {
    // Tests valid boxes at the extreme boundaries of longitude
    Type geographyType = Types.GeographyType.of("srid:4326", EdgeAlgorithm.SPHERICAL);
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geographyType);

    // Box at -180 longitude
    GeospatialBound min1 = GeospatialBound.createXY(-180.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(-170.0, 10.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    // Box at 180 longitude
    GeospatialBound min2 = GeospatialBound.createXY(170.0, 0.0);
    GeospatialBound max2 = GeospatialBound.createXY(180.0, 10.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    // These boxes should not intersect
    assertThat(evaluator.intersects(box1, box2)).isFalse();
    assertThat(evaluator.intersects(box2, box1)).isFalse();

    // Box that wraps around the antimeridian, touching -180 and 180
    GeospatialBound min3 = GeospatialBound.createXY(180.0, 0.0);
    GeospatialBound max3 = GeospatialBound.createXY(-180.0, 10.0);
    BoundingBox box3 = new BoundingBox(min3, max3);

    // This should intersect with both boxes at the extreme edges
    assertThat(evaluator.intersects(box1, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box1)).isTrue();
    assertThat(evaluator.intersects(box2, box3)).isTrue();
    assertThat(evaluator.intersects(box3, box2)).isTrue();
  }

  @Test
  public void testBoxesWithXYZMCoordinates() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // Two boxes with all XYZM coordinates that overlap in X, Y, Z but not in M
    GeospatialBound min1 = GeospatialBound.createXYZM(0.0, 0.0, 0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXYZM(2.0, 2.0, 2.0, 1.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXYZM(1.0, 1.0, 1.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXYZM(3.0, 3.0, 3.0, 3.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    // They should NOT intersect because M dimensions don't overlap
    assertThat(evaluator.intersects(box1, box2)).isFalse();
    assertThat(evaluator.intersects(box2, box1)).isFalse();
  }

  @Test
  public void testBoxesWithXYZMCoordinatesIntersecting() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // Two boxes with all XYZM coordinates that overlap in all dimensions
    GeospatialBound min1 = GeospatialBound.createXYZM(0.0, 0.0, 0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXYZM(2.0, 2.0, 2.0, 2.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXYZM(1.0, 1.0, 1.0, 1.0);
    GeospatialBound max2 = GeospatialBound.createXYZM(3.0, 3.0, 3.0, 3.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    // They should intersect because all dimensions overlap
    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testMixedDimensionsXYvsXYZ() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // One box with XY coordinates, another with XYZ coordinates
    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(2.0, 2.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXYZ(1.0, 1.0, 100.0);
    GeospatialBound max2 = GeospatialBound.createXYZ(3.0, 3.0, 200.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    // They should intersect because Z dimension is ignored when not present in both
    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testMixedDimensionsXYvsXYM() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // One box with XY coordinates, another with XYM coordinates
    GeospatialBound min1 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXY(2.0, 2.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXYM(1.0, 1.0, 100.0);
    GeospatialBound max2 = GeospatialBound.createXYM(3.0, 3.0, 200.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    // They should intersect because M dimension is ignored when not present in both
    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }

  @Test
  public void testMixedDimensionsXYZvsXYM() {
    Type geometryType = Types.GeometryType.crs84();
    GeospatialPredicateEvaluators.GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(geometryType);

    // One box with XYZ coordinates, another with XYM coordinates
    GeospatialBound min1 = GeospatialBound.createXYZ(0.0, 0.0, 0.0);
    GeospatialBound max1 = GeospatialBound.createXYZ(2.0, 2.0, 2.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    GeospatialBound min2 = GeospatialBound.createXYM(1.0, 1.0, 100.0);
    GeospatialBound max2 = GeospatialBound.createXYM(3.0, 3.0, 200.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    // They should intersect because both Z and M dimensions are ignored when not present in both
    assertThat(evaluator.intersects(box1, box2)).isTrue();
    assertThat(evaluator.intersects(box2, box1)).isTrue();
  }
}
