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
import java.util.Random;
import org.apache.iceberg.geospatial.GeospatialPredicateEvaluators.GeospatialPredicateEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Proof of concept for a spherical (great-circle) geography bounding box that is <b>guaranteed to
 * cover</b> the geography, and that it drives file skipping.
 *
 * <p>A geography bounding box cannot be the planar min/max of the vertices:
 *
 * <ol>
 *   <li><b>Latitude</b> — an edge is a great-circle arc that can reach a higher (or lower) latitude
 *       than either endpoint, so the box must consider the arc's interior extremum, not just the
 *       vertices.
 *   <li><b>Longitude</b> — longitude is periodic, so the tightest covering interval may cross the
 *       antimeridian (its western bound numerically greater than its eastern bound, per the spec).
 *   <li><b>Coverage under rounding</b> — the interior latitude extremum is computed with inverse
 *       trigonometric functions, so the box expands that computed value outward by a small margin
 *       to guarantee it still contains the arc despite floating-point error. Under-coverage would
 *       let a consumer wrongly prune a matching file; looseness only costs I/O.
 * </ol>
 *
 * <p>This is a demonstration harness, not the production writer/pruning path. It computes a box for
 * a set of great-circle edges in-test; wiring a producer into the Parquet metrics path and a
 * spatial predicate into scan planning is separate work. The outward margin here is a conservative
 * placeholder — a rigorous error bound for the trig kernel is a follow-up.
 */
public class TestGeographyBoundsSkippingPoc {

  private static final double EPS = 1e-9;

  /**
   * Outward margin (degrees) applied to a <em>computed</em> latitude extremum so the box is
   * guaranteed to cover the arc despite floating-point error in the trig. Absolute (not
   * multiplicative) so it stays covering near the equator. Chosen far above the ~1 ULP error of
   * {@link Math#atan2}; a rigorous bound is follow-up.
   */
  private static final double LAT_MARGIN_DEG = 1e-7;

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
    // The southern side has no interior extremum on this arc, so ymin stays at the endpoint.
    assertThat(spherical.min().y())
        .as("no southern bulge on this arc: ymin stays at the endpoint latitude")
        .isCloseTo(60.0, offset());
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

  @Test
  public void testBoundingBoxCoversEveryPointAlongTheArc() {
    // The core correctness property: for many random edges, every point sampled along the actual
    // great-circle arc must fall inside the computed box. This is what guarantees a consumer never
    // wrongly prunes a matching file. Endpoints are kept away from the poles and not near-antipodal
    // so the minor arc is well defined and longitude stays monotonic along it.
    Random rnd = new Random(42);
    for (int i = 0; i < 2000; i++) {
      double lon1 = rnd.nextDouble() * 360 - 180;
      double lat1 = rnd.nextDouble() * 140 - 70;
      double lon2 = normalizeLon(lon1 + (rnd.nextDouble() * 240 - 120));
      double lat2 = rnd.nextDouble() * 140 - 70;

      BoundingBox box = sphericalBounds(ImmutableList.of(edge(lon1, lat1, lon2, lat2)));

      double[] pointA = toUnit(lon1, lat1);
      double[] pointB = toUnit(lon2, lat2);
      double omega = Math.acos(clamp(dot(pointA, pointB), -1, 1));
      if (omega > Math.toRadians(179)) {
        continue; // near-antipodal: minor arc ambiguous, skip
      }

      for (int k = 0; k <= 100; k++) {
        double frac = k / 100.0;
        double[] point = slerp(pointA, pointB, frac, omega);
        double lat = Math.toDegrees(Math.asin(clamp(point[2], -1, 1)));
        double lon = Math.toDegrees(Math.atan2(point[1], point[0]));

        assertThat(lat)
            .as("arc point latitude must be within the box [%s, %s]", box.min().y(), box.max().y())
            .isBetween(box.min().y() - 1e-6, box.max().y() + 1e-6);
        assertThat(lonContained(lon, box.min().x(), box.max().x()))
            .as(
                "arc point longitude %s must be within the covering interval [%s, %s]",
                lon, box.min().x(), box.max().x())
            .isTrue();
      }
    }
  }

  @Test
  public void testConservativeMarginKeepsComputedExtremumCovering() {
    // The interior extremum is a computed value; the box top must be strictly above it so rounding
    // in the trig can never make the box fall short of the true arc peak.
    BoundingBox box = sphericalBounds(ImmutableList.of(edge(0, 60, 90, 60)));
    double arcPeak = 67.79234427; // asin(sqrt(0.375)/sqrt(0.4375)) in degrees
    assertThat(box.max().y())
        .as("box top is expanded outward past the computed arc peak")
        .isGreaterThan(arcPeak);
    assertThat(box.max().y() - arcPeak)
        .as("outward expansion is small (tight but covering)")
        .isLessThan(1e-3);
  }

  @Test
  public void testPoleEndpointYieldsFiniteLongitude() {
    // An edge touching the north pole: the pole contributes no longitude (its longitude is
    // undefined), so the covering interval is determined by the non-pole endpoint and stays finite.
    BoundingBox box = sphericalBounds(ImmutableList.of(edge(0, 90, 40, 10)));

    assertThat(box.max().y()).as("box reaches the pole latitude").isCloseTo(90, offset());
    assertThat(Double.isFinite(box.min().x()))
        .as("pole longitude is excluded; western bound stays finite")
        .isTrue();
    assertThat(Double.isFinite(box.max().x()))
        .as("pole longitude is excluded; eastern bound stays finite")
        .isTrue();
    assertThat(box.max().x())
        .as("longitude comes from the non-pole endpoint")
        .isCloseTo(40, offset());
  }

  // ---- minimal spherical bounding-box kernel (PoC) ----

  private static double[] edge(double lon1, double lat1, double lon2, double lat2) {
    return new double[] {lon1, lat1, lon2, lat2};
  }

  /** Computes a 2D geography bounding box (spherical edges) guaranteed to cover the given edges. */
  private static BoundingBox sphericalBounds(List<double[]> edges) {
    double minLat = Double.POSITIVE_INFINITY;
    double maxLat = Double.NEGATIVE_INFINITY;
    // Longitudes contributing to the circular interval (pole endpoints are excluded).
    List<Double> lons = Lists.newArrayList();

    for (double[] e : edges) {
      double lon1 = e[0];
      double lat1 = e[1];
      double lon2 = e[2];
      double lat2 = e[3];

      // Latitude: start with the endpoints (exact), then extend by the great-circle interior
      // extremum on whichever side actually lies within the segment.
      double eMin = Math.min(lat1, lat2);
      double eMax = Math.max(lat1, lat2);

      double[] pointA = toUnit(lon1, lat1);
      double[] pointB = toUnit(lon2, lat2);
      double[] normal = crossProduct(pointA, pointB);
      double normalLen = norm(normal);
      if (normalLen > 1e-12) {
        // Max latitude reached anywhere on the full great circle (its northern "vertex").
        double vertexLatDeg =
            Math.toDegrees(Math.asin(clamp(Math.hypot(normal[0], normal[1]) / normalLen, 0, 1)));
        // The two extreme-latitude points are antipodal: north at +vertexLat, south at -vertexLat.
        double[] northVertex = maxLatitudePoint(normal, normalLen);
        double[] southVertex = negate(northVertex);
        // Only the extremum that lies on the minor arc, and only if it beats the endpoints,
        // contributes - and because it is computed, it is expanded outward to stay covering.
        if (onMinorArc(northVertex, pointA, pointB, normal) && vertexLatDeg > eMax) {
          eMax = expandUp(vertexLatDeg);
        }
        if (onMinorArc(southVertex, pointA, pointB, normal) && -vertexLatDeg < eMin) {
          eMin = expandDown(-vertexLatDeg);
        }
      }

      minLat = Math.min(minLat, eMin);
      maxLat = Math.max(maxLat, eMax);

      if (Math.abs(lat1) != 90) {
        lons.add(lon1);
      }
      if (Math.abs(lat2) != 90) {
        lons.add(lon2);
      }
    }

    double[] lonRange = coveringLongitudeInterval(lons);
    return new BoundingBox(
        GeospatialBound.createXY(lonRange[0], minLat),
        GeospatialBound.createXY(lonRange[1], maxLat));
  }

  /** The point of maximum latitude on the great circle with the given (unnormalized) normal. */
  private static double[] maxLatitudePoint(double[] normal, double normalLen) {
    // Project the north pole onto the great-circle plane: p = z_hat - (n_hat . z_hat) n_hat.
    double nz = normal[2] / normalLen;
    double[] point =
        new double[] {
          -nz * normal[0] / normalLen, -nz * normal[1] / normalLen, 1 - nz * (normal[2] / normalLen)
        };
    double len = norm(point);
    return new double[] {point[0] / len, point[1] / len, point[2] / len};
  }

  /** True if unit point {@code pt} (on the great circle) lies on the minor arc from A to B. */
  private static boolean onMinorArc(
      double[] pt, double[] pointA, double[] pointB, double[] normal) {
    return dot(crossProduct(pointA, pt), normal) >= 0 && dot(crossProduct(pt, pointB), normal) >= 0;
  }

  private static double expandUp(double latDeg) {
    return Math.min(latDeg + LAT_MARGIN_DEG, 90);
  }

  private static double expandDown(double latDeg) {
    return Math.max(latDeg - LAT_MARGIN_DEG, -90);
  }

  /**
   * Smallest circular interval on [-180, 180] covering the longitudes, found by the largest-gap
   * method. Returns {west, east}; west may be greater than east (antimeridian wraparound). If no
   * longitudes contribute (e.g. a pole-only segment), the whole longitude range is returned.
   */
  private static double[] coveringLongitudeInterval(List<Double> lons) {
    if (lons.isEmpty()) {
      return new double[] {-180, 180};
    }
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

  private static boolean lonContained(double lon, double west, double east) {
    double tol = 1e-6;
    if (west <= east) {
      return lon >= west - tol && lon <= east + tol;
    }
    // wraparound interval: match if east of west OR west of east
    return lon >= west - tol || lon <= east + tol;
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

  private static double[] slerp(double[] pointA, double[] pointB, double frac, double omega) {
    if (omega < 1e-9) {
      return pointA;
    }
    double sinOmega = Math.sin(omega);
    double s1 = Math.sin((1 - frac) * omega) / sinOmega;
    double s2 = Math.sin(frac * omega) / sinOmega;
    double[] result =
        new double[] {
          s1 * pointA[0] + s2 * pointB[0],
          s1 * pointA[1] + s2 * pointB[1],
          s1 * pointA[2] + s2 * pointB[2]
        };
    double len = norm(result);
    return new double[] {result[0] / len, result[1] / len, result[2] / len};
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

  private static double norm(double[] vec) {
    return Math.sqrt(dot(vec, vec));
  }

  private static double[] negate(double[] vec) {
    return new double[] {-vec[0], -vec[1], -vec[2]};
  }

  private static double clamp(double value, double lo, double hi) {
    return Math.max(lo, Math.min(hi, value));
  }

  private static org.assertj.core.data.Offset<Double> offset() {
    return org.assertj.core.data.Offset.offset(EPS);
  }
}
