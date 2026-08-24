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

import java.util.Random;
import java.util.stream.Stream;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestSphericalGeographyBounds {
  private static final double LONGITUDE_TOLERANCE = 1e-9;

  @Test
  void capturesInteriorNorthernLatitudeExtremum() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addEdge(0.0, 60.0, 90.0, 60.0);

    BoundingBox box = bounds.build();
    assertThat(box.min()).isEqualTo(GeospatialBound.createXY(0.0, 60.0));
    assertThat(box.max().x()).isEqualTo(90.0);
    assertThat(box.max().y()).isGreaterThan(67.79234427).isLessThan(67.793);
  }

  @Test
  void capturesInteriorSouthernLatitudeExtremum() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addEdge(0.0, -60.0, 90.0, -60.0);

    BoundingBox box = bounds.build();
    assertThat(box.min().x()).isEqualTo(0.0);
    assertThat(box.min().y()).isLessThan(-67.79234427).isGreaterThan(-67.793);
    assertThat(box.max()).isEqualTo(GeospatialBound.createXY(90.0, -60.0));
  }

  @Test
  void doesNotExpandEndpointLatitude() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addEdge(20.0, 10.0, 20.0, 80.0);

    BoundingBox box = bounds.build();
    assertThat(box.min()).isEqualTo(GeospatialBound.createXY(20.0, 10.0));
    assertThat(box.max()).isEqualTo(GeospatialBound.createXY(20.0, 80.0));
  }

  @Test
  void representsAntimeridianCrossingAsWrappedInterval() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addEdge(170.0, 5.0, -170.0, 8.0);

    BoundingBox box = bounds.build();
    assertThat(box.min().x()).isEqualTo(170.0);
    assertThat(box.max().x()).isEqualTo(-170.0);
    assertThat(box.min().x()).isGreaterThan(box.max().x());
  }

  @Test
  void mergesIntervalsRatherThanOnlyEndpoints() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addEdge(100.0, 0.0, -100.0, 0.0);
    bounds.addEdge(-10.0, 0.0, 10.0, 0.0);

    BoundingBox box = bounds.build();
    assertThat(longitudeIsContained(180.0, box)).isTrue();
    assertThat(longitudeIsContained(0.0, box)).isTrue();
    assertThat(longitudeIsContained(50.0, box)).isTrue();
    assertThat(longitudeIsContained(-50.0, box)).isFalse();
  }

  @Test
  void ignoresPoleLongitudeWhenFiniteLongitudeExists() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addPoint(-120.0, 90.0);
    bounds.addPoint(40.0, 10.0);

    BoundingBox box = bounds.build();
    assertThat(box.min()).isEqualTo(GeospatialBound.createXY(40.0, 10.0));
    assertThat(box.max()).isEqualTo(GeospatialBound.createXY(40.0, 90.0));
  }

  @Test
  void usesFullLongitudeRangeForPoleOnlyBounds() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addPoint(10.0, 90.0);
    bounds.addPoint(-50.0, 90.0);

    assertThat(bounds.build())
        .isEqualTo(
            new BoundingBox(
                GeospatialBound.createXY(-180.0, 90.0), GeospatialBound.createXY(180.0, 90.0)));
  }

  @Test
  void usesFullWorldForOppositePoles() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addEdge(0.0, -90.0, 30.0, 90.0);

    assertThat(bounds.build())
        .isEqualTo(
            new BoundingBox(
                GeospatialBound.createXY(-180.0, -90.0), GeospatialBound.createXY(180.0, 90.0)));
  }

  @Test
  void usesFullWorldForAntipodalEndpoints() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addEdge(0.0, 0.0, 180.0, 0.0);

    assertThat(bounds.build())
        .isEqualTo(
            new BoundingBox(
                GeospatialBound.createXY(-180.0, -90.0), GeospatialBound.createXY(180.0, 90.0)));
  }

  @Test
  void treatsCoincidentEndpointsAsAPoint() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addEdge(12.0, 34.0, 12.0, 34.0);

    assertThat(bounds.build())
        .isEqualTo(
            new BoundingBox(
                GeospatialBound.createXY(12.0, 34.0), GeospatialBound.createXY(12.0, 34.0)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidCoordinates")
  void invalidCoordinateSuppressesBounds(String description, double longitude, double latitude) {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();
    bounds.addPoint(0.0, 0.0);
    bounds.addPoint(longitude, latitude);

    assertThat(bounds.isValid()).isFalse();
    assertThat(bounds.hasBounds()).isFalse();
    assertThat(bounds.build()).isNull();
  }

  @Test
  void emptyCollectorHasNoBounds() {
    SphericalGeographyBounds bounds = new SphericalGeographyBounds();

    assertThat(bounds.isValid()).isTrue();
    assertThat(bounds.hasBounds()).isFalse();
    assertThat(bounds.build()).isNull();
  }

  @Test
  void coversSampledPointsAlongRandomMinorArcs() {
    Random random = new Random(42L);
    for (int edge = 0; edge < 2_000; edge += 1) {
      double longitude1 = random.nextDouble() * 360.0 - 180.0;
      double latitude1 = random.nextDouble() * 140.0 - 70.0;
      double longitude2 = normalizeLongitude(longitude1 + random.nextDouble() * 240.0 - 120.0);
      double latitude2 = random.nextDouble() * 140.0 - 70.0;

      double[] point1 = toUnitVector(longitude1, latitude1);
      double[] point2 = toUnitVector(longitude2, latitude2);
      double centralAngle = Math.acos(clamp(dot(point1, point2), -1.0, 1.0));
      if (centralAngle > Math.toRadians(179.0)) {
        continue;
      }

      SphericalGeographyBounds bounds = new SphericalGeographyBounds();
      bounds.addEdge(longitude1, latitude1, longitude2, latitude2);
      BoundingBox box = bounds.build();

      for (int sample = 0; sample <= 100; sample += 1) {
        double[] point = slerp(point1, point2, sample / 100.0, centralAngle);
        double latitude = Math.toDegrees(Math.asin(clamp(point[2], -1.0, 1.0)));
        double longitude = Math.toDegrees(Math.atan2(point[1], point[0]));

        assertThat(latitude)
            .as("latitude for edge %s sample %s", edge, sample)
            .isBetween(box.min().y() - 1e-8, box.max().y() + 1e-8);
        assertThat(longitudeIsContained(longitude, box))
            .as("longitude for edge %s sample %s", edge, sample)
            .isTrue();
      }
    }
  }

  private static Stream<Arguments> invalidCoordinates() {
    return Stream.of(
        Arguments.of("longitude above range", 180.1, 0.0),
        Arguments.of("longitude below range", -180.1, 0.0),
        Arguments.of("latitude above range", 0.0, 90.1),
        Arguments.of("latitude below range", 0.0, -90.1),
        Arguments.of("NaN longitude", Double.NaN, 0.0),
        Arguments.of("NaN latitude", 0.0, Double.NaN),
        Arguments.of("infinite longitude", Double.POSITIVE_INFINITY, 0.0),
        Arguments.of("infinite latitude", 0.0, Double.NEGATIVE_INFINITY));
  }

  private static boolean longitudeIsContained(double longitude, BoundingBox box) {
    double west = box.min().x();
    double east = box.max().x();
    if (west <= east) {
      return longitude >= west - LONGITUDE_TOLERANCE && longitude <= east + LONGITUDE_TOLERANCE;
    }

    return longitude >= west - LONGITUDE_TOLERANCE || longitude <= east + LONGITUDE_TOLERANCE;
  }

  private static double[] slerp(
      double[] point1, double[] point2, double fraction, double centralAngle) {
    if (centralAngle < 1e-12) {
      return point1;
    }

    double sinAngle = Math.sin(centralAngle);
    double scale1 = Math.sin((1.0 - fraction) * centralAngle) / sinAngle;
    double scale2 = Math.sin(fraction * centralAngle) / sinAngle;
    double[] result =
        new double[] {
          scale1 * point1[0] + scale2 * point2[0],
          scale1 * point1[1] + scale2 * point2[1],
          scale1 * point1[2] + scale2 * point2[2]
        };
    double length = Math.sqrt(dot(result, result));
    return new double[] {result[0] / length, result[1] / length, result[2] / length};
  }

  private static double[] toUnitVector(double longitudeDegrees, double latitudeDegrees) {
    double longitude = Math.toRadians(longitudeDegrees);
    double latitude = Math.toRadians(latitudeDegrees);
    double cosLatitude = Math.cos(latitude);
    return new double[] {
      cosLatitude * Math.cos(longitude), cosLatitude * Math.sin(longitude), Math.sin(latitude)
    };
  }

  private static double dot(double[] vector1, double[] vector2) {
    return vector1[0] * vector2[0] + vector1[1] * vector2[1] + vector1[2] * vector2[2];
  }

  private static double normalizeLongitude(double longitude) {
    if (longitude > 180.0) {
      return longitude - 360.0;
    } else if (longitude < -180.0) {
      return longitude + 360.0;
    }

    return longitude;
  }

  private static double clamp(double value, double min, double max) {
    return Math.max(min, Math.min(max, value));
  }
}
