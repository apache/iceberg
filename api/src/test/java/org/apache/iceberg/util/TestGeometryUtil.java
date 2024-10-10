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

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class TestGeometryUtil {
  private static final GeometryFactory FACTORY = new GeometryFactory();

  @Test
  public void testToWKB() {
    Geometry geometry = FACTORY.createPoint(new Coordinate(1.0, 2.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYToWKB() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXY(1.0, 2.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYZToWKB() {
    Geometry geometry = FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isEqualTo(3.0);
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYMToWKB() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXYM(1.0, 2.0, 3.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isEqualTo(3.0);
  }

  @Test
  public void testXYZMToWKB() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0));
    byte[] wkb = GeometryUtil.toWKB(geometry);
    Geometry readGeometry = GeometryUtil.fromWKB(wkb);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isEqualTo(3.0);
    assertThat(coordinate.getM()).isEqualTo(4.0);
  }

  @Test
  public void testToWKT() {
    Geometry geometry = FACTORY.createPoint(new Coordinate(1.0, 2.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYToWKT() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXY(1.0, 2.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYZToWKT() {
    Geometry geometry = FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isEqualTo(3.0);
    assertThat(coordinate.getM()).isNaN();
  }

  @Test
  public void testXYMToWKT() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXYM(1.0, 2.0, 3.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isNaN();
    assertThat(coordinate.getM()).isEqualTo(3.0);
  }

  @Test
  public void testXYZMToWKT() {
    Geometry geometry = FACTORY.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0));
    String wkt = GeometryUtil.toWKT(geometry);
    Geometry readGeometry = GeometryUtil.fromWKT(wkt);
    assertThat(geometry).isEqualTo(readGeometry);
    Coordinate coordinate = geometry.getCoordinate();
    assertThat(coordinate.getZ()).isEqualTo(3.0);
    assertThat(coordinate.getM()).isEqualTo(4.0);
  }
}
