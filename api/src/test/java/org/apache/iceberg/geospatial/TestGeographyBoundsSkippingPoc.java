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

import java.util.List;
import org.apache.iceberg.geospatial.GeospatialPredicateEvaluators.GeospatialPredicateEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Proof of concept for a spherical (great-circle) geography bounding box, and that it drives file
 * skipping.
 *
 * <p>A geography bounding box cannot be the planar min/max of the vertices: an edge is a
 * great-circle arc that can reach a higher latitude than either endpoint, and longitude is periodic
 * so a tight box may cross the antimeridian (its western bound numerically greater than its eastern
 * bound, per the spec). This PoC implements a minimal spherical bbox kernel and shows that
 *
 * <ol>
 *   <li>it captures the poleward bulge of an edge (latitude), and
 *   <li>it produces an antimeridian-crossing (wraparound) longitude interval that prunes correctly
 *       through the existing {@link GeospatialPredicateEvaluators} (which already understands
 *       wraparound boxes).
 * </ol>
 *
 * <p>This is a demonstration harness, not the production writer/pruning path. It computes a box for
 * a set of great-circle edges in-test; wiring a producer into the Parquet metrics path and a
 * spatial predicate into scan planning is separate work.
 */
public class TestGeographyBoundsSkippingPoc {

  private static final double EPS = 1e-9;

  @Test
  public void testLatitudeBulgeCapturedByBoundingBox() {
    // An edge from (0, 60) to (90, 60): both endpoints are at 60N, but the great-circle arc
    // between them bulges north to ~67.79N. A planar min/max of the vertices would top out at 60N
    // and fail to contain the arc.
    BoundingBox planar =
        new BoundingBox(GeospatialBound.createXY(0, 60), GeospatialBound.createXY(90, 60));
    BoundingBox spherical = sphericalBounds(ImmutableList.of(edge(0, 60, 90, 60)));

    assertThat(planar.max().y()).as("planar box tops out at the endpoint latitude").isEqualTo(60.0);
    assertThat(spherical.max().y())
        .as("spherical box captures the poleward bulge of the arc")
        .isCloseTo(67.7923, org.assertj.core.data.Offset.offset(1e-3));
    assertThat(spherical.max().y())
        .as("spherical box must cover more than the planar box (else it under-covers)")
        .isGreaterThan(planar.max().y());
  }

  @Test
  public void testAntimeridianWraparoundDrivesSkipping() {
    // A geography clustered near the date line: longitudes 170 and -170. The tight covering
    // interval is the 20-degree band across +/-180, encoded as xmin=170 > xmax=-170.
    BoundingBox fileNearDateLine = sphericalBounds(ImmutableList.of(edge(170, 5, -170, 8)));

    assertThat(fileNearDateLine.min().x())
        .as("western bound is numerically greater than eastern bound (antimeridian wraparound)")
        .isGreaterThan(fileNearDateLine.max().x());
    assertThat(fileNearDateLine.min().x()).isCloseTo(170, offset());
    assertThat(fileNearDateLine.max().x()).isCloseTo(-170, offset());

    GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(Types.GeographyType.crs84());

    // A query window near the date line (179..180) overlaps the file and must be scanned.
    BoundingBox nearDateLineQuery =
        new BoundingBox(GeospatialBound.createXY(179, 0), GeospatialBound.createXY(180, 10));
    assertThat(evaluator.intersects(nearDateLineQuery, fileNearDateLine))
        .as("query on the date line overlaps the wraparound file and must be scanned")
        .isTrue();

    // A query window on the prime meridian (0..10) is disjoint and the file can be skipped -
    // this is the pruning that a naive planar box [-170, 170] (a 340-degree band) would miss.
    BoundingBox primeMeridianQuery =
        new BoundingBox(GeospatialBound.createXY(0, 0), GeospatialBound.createXY(10, 10));
    assertThat(evaluator.intersects(primeMeridianQuery, fileNearDateLine))
        .as("query on the prime meridian is disjoint from the date-line file and can be skipped")
        .isFalse();
  }

  // ---- minimal spherical bounding-box kernel (PoC) ----

  private static double[] edge(double lon1, double lat1, double lon2, double lat2) {
    return new double[] {lon1, lat1, lon2, lat2};
  }

  /** Computes a 2D geography bounding box (spherical edges) covering the given edges. */
  private static BoundingBox sphericalBounds(List<double[]> edges) {
    double minLat = Double.POSITIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;
    // Longitudes contributing to the circular interval, in the order visited.
    List<Double> lons = Lists.newArrayList();

    for (double[] e : edges) {
      double lon1 = e[0];
      double lat1 = e[1];
      double lon2 = e[2];
      double lat2 = e[3];

      // Latitude: start with the endpoints, then add the great-circle vertex if it lies inside
      // this edge (S2 RectBounder vertex test).
      double eMin = Math.min(lat1, lat2);
      double eMax = Math.max(lat1, lat2);
      double[] pointA = toUnit(lon1, lat1);
      double[] pointB = toUnit(lon2, lat2);
      double[] normal = crossProduct(pointA, pointB);
      double[] meridianNormal = crossProduct(normal, new double[] {0, 0, 1});
      double projA = dot(meridianNormal, pointA);
      double projB = dot(meridianNormal, pointB);
      if (projA * projB < 0) {
        // interior extremum: max latitude is 90 - lat(N), min is its reflection
        double extremum =
            Math.toDegrees(Math.atan2(Math.hypot(normal[0], normal[1]), Math.abs(normal[2])));
        eMax = Math.max(eMax, extremum);
        eMin = Math.min(eMin, -extremum);
      }
      minLat = Math.min(minLat, eMin);
      maxLat = Math.max(maxLat, eMax);

      lons.add(lon1);
      lons.add(lon2);
    }

    double[] lonRange = coveringLongitudeInterval(lons);
    return new BoundingBox(
        GeospatialBound.createXY(lonRange[0], minLat),
        GeospatialBound.createXY(lonRange[1], maxLat));
  }

  /**
   * Smallest circular interval on [-180, 180] covering the longitudes, found by the largest-gap
   * method. Returns {west, east}; west may be greater than east (antimeridian wraparound).
   */
  private static double[] coveringLongitudeInterval(List<Double> lons) {
    List<Double> sorted = Lists.newArrayList(lons);
    java.util.Collections.sort(sorted);

    // Largest gap between consecutive longitudes (including the wrap gap from max back to min).
    double largestGap = -1;
    double gapStart = 0;
    double gapEnd = 0;
    for (int i = 0; i < sorted.size(); i++) {
      double lo = sorted.get(i);
      double hi = i + 1 < sorted.size() ? sorted.get(i + 1) : sorted.get(0) + 360;
      double gap = hi - lo;
      if (gap > largestGap) {
        largestGap = gap;
        gapStart = lo;
        gapEnd = hi;
      }
    }
    // The covering interval is the complement of the largest gap: [gapEnd, gapStart] (wrapping).
    double west = normalizeLon(gapEnd);
    double east = normalizeLon(gapStart);
    return new double[] {west, east};
  }

  private static double normalizeLon(double lon) {
    double result = lon;
    while (result > 180) {
      result -= 360;
    }
    while (result < -180) {
      result += 360;
    }
    return result;
  }

  private static double[] toUnit(double lonDeg, double latDeg) {
    double lon = Math.toRadians(lonDeg);
    double lat = Math.toRadians(latDeg);
    return new double[] {
      Math.cos(lat) * Math.cos(lon), Math.cos(lat) * Math.sin(lon), Math.sin(lat)
    };
  }

  private static double[] crossProduct(double[] vec1, double[] vec2) {
    return new double[] {
      vec1[1] * vec2[2] - vec1[2] * vec2[1],
      vec1[2] * vec2[0] - vec1[0] * vec2[2],
      vec1[0] * vec2[1] - vec1[1] * vec2[0]
    };
  }

  private static double dot(double[] vec1, double[] vec2) {
    return vec1[0] * vec2[0] + vec1[1] * vec2[1] + vec1[2] * vec2[2];
  }

  private static org.assertj.core.data.Offset<Double> offset() {
    return org.assertj.core.data.Offset.offset(EPS);
  }
}
