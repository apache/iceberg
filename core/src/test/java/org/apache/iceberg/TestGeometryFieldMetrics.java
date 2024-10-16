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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

class TestGeometryFieldMetrics {
  private static final GeometryFactory FACTORY = new GeometryFactory();

  @Test
  public void testEmpty() {
    GeometryFieldMetrics.Builder builder = new GeometryFieldMetrics.Builder(1);
    GeometryFieldMetrics metrics = builder.build();
    assertThat(metrics.lowerBound()).isNull();
    assertThat(metrics.upperBound()).isNull();
  }

  @Test
  public void test2DGeometryMetrics() {
    GeometryFieldMetrics.Builder builder = new GeometryFieldMetrics.Builder(1);
    builder.add(FACTORY.createPoint(new Coordinate(1.0, 2.0)));
    builder.add(FACTORY.createPoint(new Coordinate(3.0, 4.0)));
    builder.add(FACTORY.createPoint(new Coordinate(-1.0, 3.0)));
    GeometryFieldMetrics metrics = builder.build();
    assertThat(metrics.lowerBound()).isInstanceOf(Point.class);
    assertThat(metrics.upperBound()).isInstanceOf(Point.class);
    Coordinate lowerLeft = metrics.lowerBound().getCoordinate();
    Coordinate upperRight = metrics.upperBound().getCoordinate();
    assertThat(lowerLeft.getX()).isEqualTo(-1.0);
    assertThat(lowerLeft.getY()).isEqualTo(2.0);
    assertThat(upperRight.getX()).isEqualTo(3.0);
    assertThat(upperRight.getY()).isEqualTo(4.0);
    assertThat(lowerLeft.getZ()).isGreaterThan(upperRight.getZ());
    assertThat(lowerLeft.getM()).isGreaterThan(upperRight.getM());
  }

  @Test
  public void testXYZGeometryMetrics() {
    GeometryFieldMetrics.Builder builder = new GeometryFieldMetrics.Builder(1);
    builder.add(FACTORY.createPoint(new Coordinate(1.0, 2.0, 3.0)));
    builder.add(FACTORY.createPoint(new Coordinate(3.0, 4.0, 5.0)));
    builder.add(FACTORY.createPoint(new Coordinate(-1.0, 3.0, 4.0)));
    GeometryFieldMetrics metrics = builder.build();
    assertThat(metrics.lowerBound()).isInstanceOf(Point.class);
    assertThat(metrics.upperBound()).isInstanceOf(Point.class);
    Coordinate lowerLeft = metrics.lowerBound().getCoordinate();
    Coordinate upperRight = metrics.upperBound().getCoordinate();
    assertThat(lowerLeft.getX()).isEqualTo(-1.0);
    assertThat(lowerLeft.getY()).isEqualTo(2.0);
    assertThat(lowerLeft.getZ()).isEqualTo(3.0);
    assertThat(upperRight.getX()).isEqualTo(3.0);
    assertThat(upperRight.getY()).isEqualTo(4.0);
    assertThat(upperRight.getZ()).isEqualTo(5.0);
    assertThat(lowerLeft.getM()).isGreaterThan(upperRight.getM());
  }

  @Test
  public void testXYMGeometryMetrics() {
    GeometryFieldMetrics.Builder builder = new GeometryFieldMetrics.Builder(1);
    builder.add(FACTORY.createPoint(new CoordinateXYM(1.0, 2.0, 4.0)));
    builder.add(FACTORY.createPoint(new CoordinateXYM(3.0, 4.0, 6.0)));
    builder.add(FACTORY.createPoint(new CoordinateXYM(-1.0, 3.0, 5.0)));
    GeometryFieldMetrics metrics = builder.build();
    assertThat(metrics.lowerBound()).isInstanceOf(Point.class);
    assertThat(metrics.upperBound()).isInstanceOf(Point.class);
    Coordinate lowerLeft = metrics.lowerBound().getCoordinate();
    Coordinate upperRight = metrics.upperBound().getCoordinate();
    assertThat(lowerLeft.getX()).isEqualTo(-1.0);
    assertThat(lowerLeft.getY()).isEqualTo(2.0);
    assertThat(lowerLeft.getM()).isEqualTo(4.0);
    assertThat(upperRight.getX()).isEqualTo(3.0);
    assertThat(upperRight.getY()).isEqualTo(4.0);
    assertThat(upperRight.getM()).isEqualTo(6.0);
    assertThat(lowerLeft.getZ()).isGreaterThan(upperRight.getZ());
  }

  @Test
  public void testXYZMGeometryMetrics() {
    GeometryFieldMetrics.Builder builder = new GeometryFieldMetrics.Builder(1);
    builder.add(FACTORY.createPoint(new CoordinateXYZM(1.0, 2.0, 3.0, 4.0)));
    builder.add(FACTORY.createPoint(new CoordinateXYZM(3.0, 4.0, 5.0, 6.0)));
    builder.add(FACTORY.createPoint(new CoordinateXYZM(-1.0, 3.0, 4.0, 5.0)));
    GeometryFieldMetrics metrics = builder.build();
    assertThat(metrics.lowerBound()).isInstanceOf(Point.class);
    assertThat(metrics.upperBound()).isInstanceOf(Point.class);
    Coordinate lowerLeft = metrics.lowerBound().getCoordinate();
    Coordinate upperRight = metrics.upperBound().getCoordinate();
    assertThat(lowerLeft.getX()).isEqualTo(-1.0);
    assertThat(lowerLeft.getY()).isEqualTo(2.0);
    assertThat(lowerLeft.getZ()).isEqualTo(3.0);
    assertThat(lowerLeft.getM()).isEqualTo(4.0);
    assertThat(upperRight.getX()).isEqualTo(3.0);
    assertThat(upperRight.getY()).isEqualTo(4.0);
    assertThat(upperRight.getZ()).isEqualTo(5.0);
    assertThat(upperRight.getM()).isEqualTo(6.0);
  }

  @Test
  public void testGeometryMetricsOfPolygon() {
    GeometryFieldMetrics.Builder builder = new GeometryFieldMetrics.Builder(1);
    builder.add(
        FACTORY.createPolygon(
            new Coordinate[] {
              new Coordinate(1.0, 2.0),
              new Coordinate(3.0, 4.0),
              new Coordinate(-1.0, 3.0),
              new Coordinate(1.0, 2.0)
            }));
    builder.add(
        FACTORY.createPolygon(
            new Coordinate[] {
              new Coordinate(3.0, 4.0),
              new Coordinate(5.0, 6.0),
              new Coordinate(1.0, 5.0),
              new Coordinate(3.0, 4.0)
            }));
    builder.add(
        FACTORY.createPolygon(
            new Coordinate[] {
              new Coordinate(-1.0, 3.0),
              new Coordinate(1.0, 5.0),
              new Coordinate(-3.0, 4.0),
              new Coordinate(-1.0, 3.0)
            }));
    GeometryFieldMetrics metrics = builder.build();
    assertThat(metrics.lowerBound()).isInstanceOf(Point.class);
    assertThat(metrics.upperBound()).isInstanceOf(Point.class);
    Coordinate lowerLeft = metrics.lowerBound().getCoordinate();
    Coordinate upperRight = metrics.upperBound().getCoordinate();
    assertThat(lowerLeft.getX()).isEqualTo(-3.0);
    assertThat(lowerLeft.getY()).isEqualTo(2.0);
    assertThat(upperRight.getX()).isEqualTo(5.0);
    assertThat(upperRight.getY()).isEqualTo(6.0);
    assertThat(lowerLeft.getZ()).isGreaterThan(upperRight.getZ());
    assertThat(lowerLeft.getM()).isGreaterThan(upperRight.getM());
  }
}
