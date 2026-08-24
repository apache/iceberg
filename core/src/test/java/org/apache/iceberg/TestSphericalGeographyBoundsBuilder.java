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

class TestSphericalGeographyBoundsBuilder {
  private static final double LATITUDE_TOLERANCE = 1e-9;
  private static final double LONGITUDE_TOLERANCE = 1e-9;

  @Test
  void capturesInteriorNorthernLatitudeExtremum() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addEdge(0.0, 60.0, 90.0, 60.0);

    BoundingBox box = bounds.build();
    assertThat(box.min()).isEqualTo(GeospatialBound.createXY(0.0, 60.0));
    assertThat(box.max().x()).isEqualTo(90.0);
    assertThat(box.max().y()).isGreaterThan(67.79234427).isLessThan(67.793);
  }

  @Test
  void capturesInteriorSouthernLatitudeExtremum() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addEdge(0.0, -60.0, 90.0, -60.0);

    BoundingBox box = bounds.build();
    assertThat(box.min().x()).isEqualTo(0.0);
    assertThat(box.min().y()).isLessThan(-67.79234427).isGreaterThan(-67.793);
    assertThat(box.max()).isEqualTo(GeospatialBound.createXY(90.0, -60.0));
  }

  @Test
  void doesNotExpandEndpointLatitude() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addEdge(20.0, 10.0, 20.0, 80.0);

    BoundingBox box = bounds.build();
    assertThat(box.min()).isEqualTo(GeospatialBound.createXY(20.0, 10.0));
    assertThat(box.max()).isEqualTo(GeospatialBound.createXY(20.0, 80.0));
  }

  @Test
  void representsAntimeridianCrossingAsWrappedInterval() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addEdge(170.0, 5.0, -170.0, 8.0);

    BoundingBox box = bounds.build();
    assertThat(box.min().x()).isEqualTo(170.0);
    assertThat(box.max().x()).isEqualTo(-170.0);
    assertThat(box.min().x()).isGreaterThan(box.max().x());
  }

  @Test
  void mergesIntervalsRatherThanOnlyEndpoints() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
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
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addPoint(-120.0, 90.0);
    bounds.addPoint(40.0, 10.0);

    BoundingBox box = bounds.build();
    assertThat(box.min()).isEqualTo(GeospatialBound.createXY(40.0, 10.0));
    assertThat(box.max()).isEqualTo(GeospatialBound.createXY(40.0, 90.0));
  }

  @Test
  void usesFullLongitudeRangeForPoleOnlyBounds() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addPoint(10.0, 90.0);
    bounds.addPoint(-50.0, 90.0);

    assertThat(bounds.build())
        .isEqualTo(
            new BoundingBox(
                GeospatialBound.createXY(-180.0, 90.0), GeospatialBound.createXY(180.0, 90.0)));
  }

  @Test
  void usesFullWorldForOppositePoles() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addEdge(0.0, -90.0, 30.0, 90.0);

    assertThat(bounds.build())
        .isEqualTo(
            new BoundingBox(
                GeospatialBound.createXY(-180.0, -90.0), GeospatialBound.createXY(180.0, 90.0)));
  }

  @Test
  void usesFullWorldForAntipodalEndpoints() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addEdge(0.0, 0.0, 180.0, 0.0);

    assertThat(bounds.build())
        .isEqualTo(
            new BoundingBox(
                GeospatialBound.createXY(-180.0, -90.0), GeospatialBound.createXY(180.0, 90.0)));
  }

  @Test
  void treatsCoincidentEndpointsAsAPoint() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addEdge(12.0, 34.0, 12.0, 34.0);

    assertThat(bounds.build())
        .isEqualTo(
            new BoundingBox(
                GeospatialBound.createXY(12.0, 34.0), GeospatialBound.createXY(12.0, 34.0)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidCoordinates")
  void invalidCoordinateSuppressesBounds(String description, double longitude, double latitude) {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    bounds.addPoint(0.0, 0.0);
    bounds.addPoint(longitude, latitude);

    assertThat(bounds.build()).isNull();
  }

  @Test
  void emptyBuilderHasNoBounds() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();

    assertThat(bounds.build()).isNull();
  }

  @ParameterizedTest(name = "({0}, {1}) to ({2}, {3})")
  @MethodSource("edgeBoundsCases")
  void buildsBoundsForSphericalEdges(
      double longitude1,
      double latitude1,
      double longitude2,
      double latitude2,
      BoundingBox expected) {
    SphericalGeographyBoundsBuilder forward = new SphericalGeographyBoundsBuilder();
    forward.addEdge(longitude1, latitude1, longitude2, latitude2);
    assertBoundsCloseTo(forward.build(), expected);

    SphericalGeographyBoundsBuilder reverse = new SphericalGeographyBoundsBuilder();
    reverse.addEdge(longitude2, latitude2, longitude1, latitude1);
    assertBoundsCloseTo(reverse.build(), expected);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("lineBoundsCases")
  void buildsBoundsForSphericalLines(String description, double[][] points, BoundingBox expected) {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    addLine(bounds, points);

    assertBoundsCloseTo(bounds.build(), expected);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("pointBoundsCases")
  void buildsBoundsForPointSets(String description, double[][] points, BoundingBox expected) {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    for (double[] point : points) {
      bounds.addPoint(point[0], point[1]);
    }

    assertBoundsCloseTo(bounds.build(), expected);
  }

  @Test
  void mergesBoundsAcrossDisconnectedLines() {
    SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
    addLine(bounds, new double[][] {{180.0, 10.0}, {170.0, 10.0}});
    addLine(bounds, new double[][] {{20.0, 10.0}, {-150.0, 10.0}, {-180.0, 10.0}});
    addLine(bounds, new double[][] {{160.0, 0.0}, {40.0, 20.0}});

    assertBoundsCloseTo(bounds.build(), box(40.0, 0.0, 20.0, 63.69752002440885));
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

      SphericalGeographyBoundsBuilder bounds = new SphericalGeographyBoundsBuilder();
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

  private static Stream<Arguments> lineBoundsCases() {
    return Stream.of(
        Arguments.of(
            "ordinary polyline",
            new double[][] {{1.0, 2.0}, {5.0, 6.0}, {-10.0, -7.0}},
            box(-10.0, -7.0, 5.0, 6.0)),
        Arguments.of(
            "approaches positive antimeridian",
            new double[][] {{180.0, 0.0}, {170.0, 0.0}},
            box(170.0, 0.0, 180.0, 0.0)),
        Arguments.of(
            "crosses negative antimeridian",
            new double[][] {{-180.0, 0.0}, {170.0, 0.0}},
            box(170.0, 0.0, -180.0, 0.0)),
        Arguments.of(
            "latitude bulge near antimeridian",
            new double[][] {{180.0, 10.0}, {170.0, 10.0}},
            box(170.0, 10.0, 180.0, 10.037424049653)),
        Arguments.of(
            "narrow antimeridian crossing",
            new double[][] {{-179.0, 10.0}, {179.0, 10.0}},
            box(179.0, 10.0, -179.0, 10.001493527133333)),
        Arguments.of(
            "edges cover every longitude",
            new double[][] {
              {180.0, 10.0},
              {170.0, 10.0},
              {20.0, 10.0},
              {-20.0, 10.0},
              {-160.0, 10.0},
              {-180.0, 10.0}
            },
            box(-180.0, 10.0, 180.0, 34.265634937025254)),
        Arguments.of(
            "wide non-wrapping line",
            new double[][] {
              {179.0, 10.0},
              {170.0, 10.0},
              {20.0, 10.0},
              {-20.0, 10.0},
              {-160.0, 10.0},
              {-179.0, 10.0}
            },
            box(-179.0, 10.0, 179.0, 34.265634937025254)),
        Arguments.of(
            "line touches north pole",
            new double[][] {{10.0, 20.0}, {10.0, 90.0}, {30.0, 20.0}},
            box(10.0, 20.0, 30.0, 90.0)),
        Arguments.of(
            "line stays on north pole",
            new double[][] {{20.0, 90.0}, {10.0, 90.0}, {30.0, 90.0}},
            box(-180.0, 90.0, 180.0, 90.0)),
        Arguments.of(
            "line stays on south pole",
            new double[][] {{20.0, -90.0}, {10.0, -90.0}, {30.0, -90.0}},
            box(-180.0, -90.0, 180.0, -90.0)),
        Arguments.of(
            "edge connects opposite poles",
            new double[][] {{30.0, 90.0}, {10.0, -90.0}},
            box(-180.0, -90.0, 180.0, 90.0)),
        Arguments.of(
            "line visits both poles",
            new double[][] {{10.0, 90.0}, {10.0, 0.0}, {10.0, -90.0}, {20.0, 0.0}, {20.0, 90.0}},
            box(10.0, -90.0, 20.0, 90.0)));
  }

  private static Stream<Arguments> edgeBoundsCases() {
    return Stream.concat(antimeridianEdgeBoundsCases(), latitudeEdgeBoundsCases());
  }

  private static Stream<Arguments> antimeridianEdgeBoundsCases() {
    return Stream.of(
        edgeCase(5.0, 10.0, 15.0, 10.0, 5.0, 10.0, 15.0, 10.03742404965304),
        edgeCase(5.0, -10.0, 15.0, -10.0, 5.0, -10.037424049653, 15.0, -10.0),
        edgeCase(5.0, 10.0, -179.0, 10.0, 5.0, 10.0, -179.0, 78.80444354002829),
        edgeCase(5.0, 10.0, -175.1, 10.0, 5.0, 10.0, -175.1, 89.716447231812),
        edgeCase(5.0, 10.0, 105.0, -10.0, 5.0, -10.0, 105.0, 10.0),
        edgeCase(5.0, 10.0, 25.0, 10.0, 5.0, 10.0, 25.0, 10.15108272615629),
        edgeCase(5.0, -10.0, 25.0, -10.0, 5.0, -10.15108272615629, 25.0, -10.0),
        edgeCase(-170.0, 10.0, 160.0, 10.0, 160.0, 10.0, -170.0, 10.34527108067699),
        edgeCase(-170.0, -10.0, 160.0, -10.0, 160.0, -10.34527108067699, -170.0, -10.0),
        edgeCase(-180.0, 10.0, -170.0, 10.0, -180.0, 10.0, -170.0, 10.03742404965304),
        edgeCase(180.0, 10.0, 170.0, 10.0, 170.0, 10.0, 180.0, 10.037424049653),
        edgeCase(180.0, 10.0, 180.0, 5.0, -180.0, 5.0, -180.0, 10.0),
        edgeCase(-180.0, 10.0, -180.0, 5.0, -180.0, 5.0, -180.0, 10.0),
        edgeCase(10.0, 90.0, 20.0, 90.0, -180.0, 90.0, 180.0, 90.0),
        edgeCase(10.0, -90.0, 20.0, -90.0, -180.0, -90.0, 180.0, -90.0));
  }

  private static Stream<Arguments> latitudeEdgeBoundsCases() {
    return Stream.of(
        edgeCase(10.0, 90.0, 20.0, -90.0, -180.0, -90.0, 180.0, 90.0),
        edgeCase(10.0, 90.0, 10.0, -90.0, -180.0, -90.0, 180.0, 90.0),
        edgeCase(10.0, -0.1, 100.0, 1.0, 10.0, -0.1, 100.0, 1.0),
        edgeCase(10.0, -1.0, 100.0, 1.0, 10.0, -1.0, 100.0, 1.0),
        edgeCase(10.0, 0.0, 120.0, 0.0, 10.0, 0.0, 120.0, 0.0),
        edgeCase(10.0, 0.0, 120.0, 1.0, 10.0, 0.0, 120.0, 1.06416356550489),
        edgeCase(10.0, 10.0, 20.0, 20.0, 10.0, 10.0, 20.0, 20.0),
        edgeCase(10.0, 60.0, 70.0, 70.0, 10.0, 60.0, 70.0, 70.20558550568438),
        edgeCase(10.0, 10.0, 80.0, 20.0, 10.0, 10.0, 80.0, 20.21005666515848),
        edgeCase(5.0, 10.0, 105.0, 10.0, 5.0, 10.0, 105.0, 15.33981603316944),
        edgeCase(5.0, 10.0, 175.0, 10.0, 5.0, 10.0, 175.0, 63.69752002440885),
        edgeCase(10.0, 20.0, 10.0, 90.0, 10.0, 20.0, 10.0, 90.0),
        edgeCase(10.0, 20.0, 10.0, -90.0, 10.0, -90.0, 10.0, 20.0),
        edgeCase(10.0, -20.0, -10.0, 90.0, 10.0, -20.0, 10.0, 90.0),
        edgeCase(10.0, -20.0, -10.0, -90.0, 10.0, -90.0, 10.0, -20.0));
  }

  private static Stream<Arguments> pointBoundsCases() {
    return Stream.of(
        Arguments.of(
            "ordinary points",
            new double[][] {{1.0, 2.0}, {5.0, 6.0}, {-10.0, -7.0}},
            box(-10.0, -7.0, 5.0, 6.0)),
        Arguments.of(
            "points approach positive antimeridian",
            new double[][] {{180.0, 0.0}, {170.0, 0.0}},
            box(170.0, 0.0, 180.0, 0.0)),
        Arguments.of(
            "points cross negative antimeridian",
            new double[][] {{-180.0, 0.0}, {170.0, 0.0}},
            box(170.0, 0.0, -180.0, 0.0)),
        Arguments.of(
            "point set wraps around antimeridian",
            new double[][] {
              {180.0, 10.0}, {170.0, 10.0}, {20.0, 10.0}, {-160.0, 10.0}, {-180.0, 10.0}
            },
            box(20.0, 10.0, -160.0, 10.0)),
        Arguments.of(
            "point set excludes its largest circular gap",
            new double[][] {
              {179.0, 10.0},
              {170.0, 10.0},
              {20.0, 10.0},
              {-20.0, 10.0},
              {-160.0, 10.0},
              {-179.0, 10.0}
            },
            box(170.0, 10.0, 20.0, 10.0)),
        Arguments.of(
            "points stay on north pole",
            new double[][] {{20.0, 90.0}, {10.0, 90.0}, {30.0, 90.0}},
            box(-180.0, 90.0, 180.0, 90.0)),
        Arguments.of(
            "points include both poles",
            new double[][] {{10.0, 90.0}, {30.0, -90.0}},
            box(-180.0, -90.0, 180.0, 90.0)),
        Arguments.of(
            "pole longitudes do not widen finite points",
            new double[][] {{10.0, 90.0}, {10.0, 0.0}, {10.0, -90.0}, {20.0, 0.0}, {20.0, 90.0}},
            box(10.0, -90.0, 20.0, 90.0)));
  }

  private static void addLine(SphericalGeographyBoundsBuilder bounds, double[][] points) {
    if (points.length == 1) {
      bounds.addPoint(points[0][0], points[0][1]);
      return;
    }

    for (int index = 1; index < points.length; index += 1) {
      double[] start = points[index - 1];
      double[] end = points[index];
      bounds.addEdge(start[0], start[1], end[0], end[1]);
    }
  }

  private static void assertBoundsCloseTo(BoundingBox actual, BoundingBox expected) {
    assertThat(actual).isNotNull();
    assertThat(actual.min().x()).isEqualTo(expected.min().x());
    assertThat(actual.max().x()).isEqualTo(expected.max().x());
    assertThat(Math.abs(actual.min().y() - expected.min().y()))
        .isLessThanOrEqualTo(LATITUDE_TOLERANCE);
    assertThat(Math.abs(actual.max().y() - expected.max().y()))
        .isLessThanOrEqualTo(LATITUDE_TOLERANCE);
  }

  private static Arguments edgeCase(
      double longitude1,
      double latitude1,
      double longitude2,
      double latitude2,
      double west,
      double south,
      double east,
      double north) {
    return Arguments.of(
        longitude1, latitude1, longitude2, latitude2, box(west, south, east, north));
  }

  private static BoundingBox box(double west, double south, double east, double north) {
    return new BoundingBox(
        GeospatialBound.createXY(west, south), GeospatialBound.createXY(east, north));
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
