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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class TestGeometryUtil {
  private static final GeometryFactory FACTORY = new GeometryFactory();

  @Test
  public void testToWKB() {
    Geometry geometry = FACTORY.createPoint(new Coordinate(1.0, 2.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYToWKB() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXY(1.0, 2.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYZToWKB() {
    Geometry geometry = FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isEqualTo(3.0);
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  @Disabled("https://github.com/locationtech/jts/issues/733")
  public void testXYMToWKB() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXYM(1.0, 2.0, 3.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isEqualTo(3.0);
  }

  @Test
  public void testXYZMToWKB() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isEqualTo(3.0);
    assertThat(coordinate.getM()).isEqualTo(4.0);
  }

  @Test
  public void testToWKT() {
    Geometry geometry = FACTORY.createPoint(new Coordinate(1.0, 2.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYToWKT() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXY(1.0, 2.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYZToWKT() {
    Geometry geometry = FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isEqualTo(3.0);
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYMToWKT() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXYM(1.0, 2.0, 3.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isEqualTo(3.0);
  }

  @Test
  public void testXYZMToWKT() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = readGeometry.getCoordinate();
    assertThat(coordinate.getZ()).isEqualTo(3.0);
    assertThat(coordinate.getM()).isEqualTo(4.0);
  }

  @Test
  public void testBoundMayIntersects() {
    GeometryFactory factory = new GeometryFactory();

    // Test regular case (not crossing anti-meridian)
    Point lowerBound = factory.createPoint(new Coordinate(0, 0));
    Point upperBound = factory.createPoint(new Coordinate(10, 10));

    // Envelope completely inside bound
    Geometry geom = factory.toGeometry(new Envelope(2, 8, 2, 8));
    assertThat(GeometryUtil.boundMayIntersects(lowerBound, upperBound, geom)).isTrue();

    // Envelope partially overlapping bound
    geom = factory.toGeometry(new Envelope(5, 15, 5, 15));
    assertThat(GeometryUtil.boundMayIntersects(lowerBound, upperBound, geom)).isTrue();

    // Envelope completely outside bound
    geom = factory.toGeometry(new Envelope(15, 20, 15, 20));
    assertThat(GeometryUtil.boundMayIntersects(lowerBound, upperBound, geom)).isFalse();

    // Test anti-meridian crossing case
    lowerBound = factory.createPoint(new Coordinate(170, 0));
    upperBound = factory.createPoint(new Coordinate(-170, 10));

    // Envelope in the western part of the bound
    geom = factory.toGeometry(new Envelope(172, 178, 2, 8));
    assertThat(GeometryUtil.boundMayIntersects(lowerBound, upperBound, geom)).isTrue();

    // Envelope in the eastern part of the bound
    geom = factory.toGeometry(new Envelope(-178, -172, 2, 8));
    assertThat(GeometryUtil.boundMayIntersects(lowerBound, upperBound, geom)).isTrue();

    // Envelope crossing the anti-meridian within the bound
    geom = factory.toGeometry(new Envelope(175, -175, 2, 8));
    assertThat(GeometryUtil.boundMayIntersects(lowerBound, upperBound, geom)).isTrue();

    // Envelope outside the bound (latitude)
    geom = factory.toGeometry(new Envelope(172, 178, 12, 15));
    assertThat(GeometryUtil.boundMayIntersects(lowerBound, upperBound, geom)).isFalse();

    // Envelope outside the bound (longitude)
    geom = factory.toGeometry(new Envelope(160, 165, 2, 8));
    assertThat(GeometryUtil.boundMayIntersects(lowerBound, upperBound, geom)).isFalse();
  }

  @Test
  public void testBoundMayCovers() {
    GeometryFactory factory = new GeometryFactory();

    // Test regular case (not crossing anti-meridian)
    Point lowerBound = factory.createPoint(new Coordinate(0, 0));
    Point upperBound = factory.createPoint(new Coordinate(10, 10));

    // Envelope completely inside bound
    Geometry geom = factory.toGeometry(new Envelope(2, 8, 2, 8));
    assertThat(GeometryUtil.boundMayCovers(lowerBound, upperBound, geom)).isTrue();

    // Envelope partially inside bound
    geom = factory.toGeometry(new Envelope(5, 15, 5, 15));
    assertThat(GeometryUtil.boundMayCovers(lowerBound, upperBound, geom)).isFalse();

    // Envelope completely outside bound
    geom = factory.toGeometry(new Envelope(15, 20, 15, 20));
    assertThat(GeometryUtil.boundMayCovers(lowerBound, upperBound, geom)).isFalse();

    // Test anti-meridian crossing case
    lowerBound = factory.createPoint(new Coordinate(170, 0));
    upperBound = factory.createPoint(new Coordinate(-170, 10));

    // Envelope in the western part of the bound
    geom = factory.toGeometry(new Envelope(172, 178, 2, 8));
    assertThat(GeometryUtil.boundMayCovers(lowerBound, upperBound, geom)).isTrue();

    // Envelope in the eastern part of the bound
    geom = factory.toGeometry(new Envelope(-178, -172, 2, 8));
    assertThat(GeometryUtil.boundMayCovers(lowerBound, upperBound, geom)).isTrue();

    // Envelope partially outside the bound (latitude)
    geom = factory.toGeometry(new Envelope(172, 178, -2, 12));
    assertThat(GeometryUtil.boundMayCovers(lowerBound, upperBound, geom)).isFalse();

    // Envelope outside the bound (longitude)
    geom = factory.toGeometry(new Envelope(160, 165, 2, 8));
    assertThat(GeometryUtil.boundMayCovers(lowerBound, upperBound, geom)).isFalse();
  }

  @Test
  public void testBoundMustBeCoveredBy() {
    GeometryFactory factory = new GeometryFactory();

    // Test regular case (not crossing anti-meridian)
    Point lowerBound = factory.createPoint(new Coordinate(2, 2));
    Point upperBound = factory.createPoint(new Coordinate(8, 8));

    // Envelope completely covering the bound
    Geometry geom = factory.toGeometry(new Envelope(0, 10, 0, 10));
    assertThat(GeometryUtil.boundMustBeCoveredBy(lowerBound, upperBound, geom)).isTrue();

    // Envelope partially covering the bound
    geom = factory.toGeometry(new Envelope(3, 10, 0, 10));
    assertThat(GeometryUtil.boundMustBeCoveredBy(lowerBound, upperBound, geom)).isFalse();

    // Envelope not covering the bound
    geom = factory.toGeometry(new Envelope(0, 5, 0, 5));
    assertThat(GeometryUtil.boundMustBeCoveredBy(lowerBound, upperBound, geom)).isFalse();

    // Test anti-meridian crossing case - should always return false
    lowerBound = factory.createPoint(new Coordinate(170, 0));
    upperBound = factory.createPoint(new Coordinate(-170, 10));

    // Large envelope covering the entire region
    geom = factory.toGeometry(new Envelope(160, -160, -10, 20));
    assertThat(GeometryUtil.boundMustBeCoveredBy(lowerBound, upperBound, geom)).isFalse();

    // Envelope exactly matching the bound coordinates
    geom = factory.toGeometry(new Envelope(170, -170, 0, 10));
    assertThat(GeometryUtil.boundMustBeCoveredBy(lowerBound, upperBound, geom)).isFalse();
  }
}
