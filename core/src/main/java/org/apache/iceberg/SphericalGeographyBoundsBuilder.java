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

import java.util.List;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Builds an XY bounding box from geography points and minor great-circle edges on a sphere.
 *
 * <p>Values are supplied already decoded, as points ({@link #addPoint}) and minor great-circle
 * edges ({@link #addEdge}); this class does not parse WKB.
 *
 * <p>The contract a caller must reason about:
 *
 * <ul>
 *   <li>{@link #build()} returns {@code null} for empty input, or when either dimension never
 *       received a non-{@code NaN} ordinate.
 *   <li>A {@code NaN} ordinate is skipped per dimension, matching empty WKB values such as {@code
 *       POINT EMPTY}; it does not turn bounds off for the rest of the file.
 *   <li>A single out-of-range or infinite coordinate turns bounds off permanently, so {@code
 *       build()} then returns {@code null} for the whole file: a coordinate off the sphere has no
 *       meaning.
 *   <li>An ambiguous antipodal edge, whose great-circle plane is undetermined, yields world bounds
 *       ({@code [-180, 180]} x {@code [-90, 90]}) rather than a tighter box.
 *   <li>The longitude interval may wrap across the antimeridian, so the box can have {@code west >
 *       east}; a point then matches when its longitude is {@code >= west} OR {@code <= east}.
 *   <li>A pole vertex still contributes its stored longitude. The spec's geography X predicate is
 *       numeric, so dropping that X would under-cover a value present in the file.
 *   <li>Latitude extrema are deliberately widened by a small margin, so a bound is guaranteed to
 *       cover its edge but is not tight.
 * </ul>
 */
class SphericalGeographyBoundsBuilder {
  private static final double MIN_LONGITUDE = -180.0;
  private static final double MAX_LONGITUDE = 180.0;
  private static final double MIN_LATITUDE = -90.0;
  private static final double MAX_LATITUDE = 90.0;
  private static final double LONGITUDE_SPAN = MAX_LONGITUDE - MIN_LONGITUDE;
  private static final int MAX_LONGITUDE_INTERVALS = 64;

  // For unit endpoints, |point1 x point2| = sin(central angle). A tiny normal means
  // the endpoints are nearly coincident or antipodal, so normalizing the great-circle
  // plane is numerically unstable.
  private static final double MIN_NORMAL_LENGTH = 1e-12;

  // A point lies on the oriented minor arc when both exact side tests are nonnegative:
  //
  //   point1 -------- point -------- point2
  //          (point1 x point) . normal >= 0
  //          (point x point2) . normal >= 0
  //
  // Permit a small negative result introduced by floating-point rounding.
  private static final double ARC_CONTAINMENT_TOLERANCE = 1e-12;

  // A minor great-circle arc can extend beyond both endpoint latitudes:
  //
  //             conservative north bound
  //   ---------------------------------------------
  //                         ^ margin
  //                         * arc vertex
  //                      .-' '-.
  //            endpoint *       * endpoint
  //
  // The factor adds a relative 1e-7 margin away from the equator to avoid an
  // under-covering bound; the result is then clamped to [-90, 90].
  private static final double LATITUDE_SCALING_FACTOR = 1.0000001;

  // Disjoint non-wrapping segments, merged and compacted as they accumulate so memory use does
  // not scale with a file's vertex count.
  private final List<LongitudeInterval> longitudeIntervals = Lists.newArrayList();
  private boolean fullLongitude = false;
  private double minLatitude = Double.POSITIVE_INFINITY;
  private double maxLatitude = Double.NEGATIVE_INFINITY;
  private State state = State.EMPTY;

  void addPoint(double longitude, double latitude) {
    includeCoordinate(longitude, latitude);
  }

  void addEdge(double longitude1, double latitude1, double longitude2, double latitude2) {
    boolean firstIsComplete = includeCoordinate(longitude1, latitude1);
    boolean secondIsComplete = includeCoordinate(longitude2, latitude2);
    if (!firstIsComplete || !secondIsComplete || state != State.ACTIVE) {
      return;
    }

    if (addEdgeWithPole(latitude1, latitude2)) {
      return;
    }

    addLongitudeInterval(minimumLongitudeInterval(longitude1, longitude2));
    addInteriorLatitudeExtrema(longitude1, latitude1, longitude2, latitude2);
  }

  private boolean addEdgeWithPole(double latitude1, double latitude2) {
    boolean firstIsPole = isPole(latitude1);
    boolean secondIsPole = isPole(latitude2);
    if (firstIsPole && secondIsPole) {
      if (latitude1 != latitude2) {
        includeFullWorld();
      }

      return true;
    }

    return firstIsPole || secondIsPole;
  }

  private void addInteriorLatitudeExtrema(
      double longitude1, double latitude1, double longitude2, double latitude2) {
    Vector3 point1 = toUnitVector(longitude1, latitude1);
    Vector3 point2 = toUnitVector(longitude2, latitude2);
    Vector3 normal = point1.crossProduct(point2);
    double normalLength = normal.length();
    if (normalLength <= MIN_NORMAL_LENGTH) {
      if (point1.dotProduct(point2) < 0) {
        includeFullWorld();
      }

      return;
    }

    Vector3 unitNormal = normal.scale(1.0 / normalLength);
    double horizontalNormalLength = Math.hypot(unitNormal.xComponent, unitNormal.yComponent);
    if (horizontalNormalLength == 0) {
      return;
    }

    double vertexLatitude = Math.toDegrees(Math.asin(clamp(horizontalNormalLength, 0.0, 1.0)));
    Vector3 northVertex =
        new Vector3(
            -unitNormal.zComponent * unitNormal.xComponent / horizontalNormalLength,
            -unitNormal.zComponent * unitNormal.yComponent / horizontalNormalLength,
            horizontalNormalLength);

    double endpointMaxLatitude = Math.max(latitude1, latitude2);
    if (vertexLatitude > endpointMaxLatitude
        && isOnMinorArc(northVertex, point1, point2, unitNormal)) {
      // Expand a computed extremum so rounding cannot produce an under-covering bound.
      maxLatitude =
          Math.max(maxLatitude, Math.min(LATITUDE_SCALING_FACTOR * vertexLatitude, MAX_LATITUDE));
    }

    double endpointMinLatitude = Math.min(latitude1, latitude2);
    Vector3 southVertex = northVertex.scale(-1.0);
    if (-vertexLatitude < endpointMinLatitude
        && isOnMinorArc(southVertex, point1, point2, unitNormal)) {
      // Expand a computed extremum so rounding cannot produce an under-covering bound.
      minLatitude =
          Math.min(minLatitude, Math.max(-LATITUDE_SCALING_FACTOR * vertexLatitude, MIN_LATITUDE));
    }
  }

  BoundingBox build() {
    if (state == State.EMPTY || state == State.INVALID) {
      return null;
    }

    if (state == State.FULL_WORLD) {
      return worldBounds();
    }

    if (!hasLatitude() || !hasLongitude()) {
      return null;
    }

    LongitudeInterval longitudeBounds = longitudeBounds();
    return new BoundingBox(
        GeospatialBound.createXY(longitudeBounds.west, minLatitude),
        GeospatialBound.createXY(longitudeBounds.east, maxLatitude));
  }

  private void includeLatitude(double latitude) {
    minLatitude = Math.min(minLatitude, latitude);
    maxLatitude = Math.max(maxLatitude, latitude);
  }

  private boolean hasLatitude() {
    return minLatitude <= maxLatitude;
  }

  private boolean hasLongitude() {
    return fullLongitude || !longitudeIntervals.isEmpty();
  }

  private void includeFullWorld() {
    minLatitude = MIN_LATITUDE;
    maxLatitude = MAX_LATITUDE;
    longitudeIntervals.clear();
    fullLongitude = true;
    state = State.FULL_WORLD;
  }

  private static BoundingBox worldBounds() {
    return new BoundingBox(
        GeospatialBound.createXY(MIN_LONGITUDE, MIN_LATITUDE),
        GeospatialBound.createXY(MAX_LONGITUDE, MAX_LATITUDE));
  }

  private boolean includeCoordinate(double longitude, double latitude) {
    if (state == State.INVALID) {
      return false;
    }

    boolean longitudeNaN = Double.isNaN(longitude);
    boolean latitudeNaN = Double.isNaN(latitude);
    if (!longitudeNaN && !isInLongitudeRange(longitude)) {
      state = State.INVALID;
      return false;
    }

    if (!latitudeNaN && !isInLatitudeRange(latitude)) {
      state = State.INVALID;
      return false;
    }

    if (state == State.FULL_WORLD) {
      return false;
    }

    if (!latitudeNaN) {
      includeLatitude(latitude);
      state = State.ACTIVE;
    }

    if (!longitudeNaN) {
      addLongitudeInterval(new LongitudeInterval(longitude, longitude));
      state = State.ACTIVE;
    }

    return !longitudeNaN && !latitudeNaN;
  }

  private void addLongitudeInterval(LongitudeInterval interval) {
    if (fullLongitude) {
      return;
    }

    if (interval.west <= interval.east) {
      insertSegment(interval.west, interval.east);
    } else {
      insertSegment(interval.west, MAX_LONGITUDE);
      insertSegment(MIN_LONGITUDE, interval.east);
    }
  }

  private void insertSegment(double west, double east) {
    if (fullLongitude) {
      return;
    }

    int index = 0;
    while (index < longitudeIntervals.size() && longitudeIntervals.get(index).east < west) {
      index += 1;
    }

    double mergedWest = west;
    double mergedEast = east;
    int mergeFrom = index;
    while (index < longitudeIntervals.size() && longitudeIntervals.get(index).west <= mergedEast) {
      LongitudeInterval existing = longitudeIntervals.get(index);
      mergedWest = Math.min(mergedWest, existing.west);
      mergedEast = Math.max(mergedEast, existing.east);
      index += 1;
    }

    longitudeIntervals.subList(mergeFrom, index).clear();
    if (mergedWest <= MIN_LONGITUDE && mergedEast >= MAX_LONGITUDE) {
      fullLongitude = true;
      longitudeIntervals.clear();
      return;
    }

    longitudeIntervals.add(mergeFrom, new LongitudeInterval(mergedWest, mergedEast));
    compactLongitudeIntervals();
  }

  private void compactLongitudeIntervals() {
    while (longitudeIntervals.size() > MAX_LONGITUDE_INTERVALS) {
      int mergeIndex = 1;
      double smallestGap = Double.POSITIVE_INFINITY;
      for (int index = 1; index < longitudeIntervals.size(); index += 1) {
        double gap = longitudeIntervals.get(index).west - longitudeIntervals.get(index - 1).east;
        if (gap < smallestGap) {
          mergeIndex = index;
          smallestGap = gap;
        }
      }

      LongitudeInterval previous = longitudeIntervals.get(mergeIndex - 1);
      LongitudeInterval next = longitudeIntervals.remove(mergeIndex);
      longitudeIntervals.set(mergeIndex - 1, new LongitudeInterval(previous.west, next.east));
    }
  }

  private LongitudeInterval longitudeBounds() {
    if (fullLongitude) {
      return new LongitudeInterval(MIN_LONGITUDE, MAX_LONGITUDE);
    }

    // The minimum covering circular interval is the complement of the largest uncovered gap.
    LongitudeInterval first = longitudeIntervals.get(0);
    LongitudeInterval last = longitudeIntervals.get(longitudeIntervals.size() - 1);
    double largestGap = LONGITUDE_SPAN + first.west - last.east;
    LongitudeInterval bounds = new LongitudeInterval(first.west, last.east);
    for (int index = 1; index < longitudeIntervals.size(); index += 1) {
      LongitudeInterval previous = longitudeIntervals.get(index - 1);
      LongitudeInterval next = longitudeIntervals.get(index);
      double gap = next.west - previous.east;
      if (gap > largestGap) {
        largestGap = gap;
        bounds = new LongitudeInterval(next.west, previous.east);
      }
    }

    return bounds;
  }

  private static LongitudeInterval minimumLongitudeInterval(double longitude1, double longitude2) {
    // A coordinate on the antimeridian is kept as given: +180 and -180 both name that meridian
    // and are preserved rather than folded onto one sign, so the same coordinate yields the same
    // interval whether it arrives as a point or a degenerate edge. The interval spans the shorter
    // of the two arcs between the endpoints, wrapping past the antimeridian (west > east) when
    // that arc is the shorter one.
    double west = Math.min(longitude1, longitude2);
    double east = Math.max(longitude1, longitude2);
    double directGap = east - west;
    double antimeridianGap = LONGITUDE_SPAN - directGap;
    return antimeridianGap >= directGap
        ? new LongitudeInterval(west, east)
        : new LongitudeInterval(east, west);
  }

  private static boolean isInLongitudeRange(double longitude) {
    return Double.isFinite(longitude) && longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE;
  }

  private static boolean isInLatitudeRange(double latitude) {
    return Double.isFinite(latitude) && latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE;
  }

  private static boolean isPole(double latitude) {
    return Math.abs(latitude) == MAX_LATITUDE;
  }

  private static boolean isOnMinorArc(
      Vector3 point, Vector3 point1, Vector3 point2, Vector3 unitNormal) {
    return point1.crossProduct(point).dotProduct(unitNormal) >= -ARC_CONTAINMENT_TOLERANCE
        && point.crossProduct(point2).dotProduct(unitNormal) >= -ARC_CONTAINMENT_TOLERANCE;
  }

  private static Vector3 toUnitVector(double longitudeDegrees, double latitudeDegrees) {
    double longitude = Math.toRadians(longitudeDegrees);
    double latitude = Math.toRadians(latitudeDegrees);
    double cosLatitude = Math.cos(latitude);
    return new Vector3(
        cosLatitude * Math.cos(longitude), cosLatitude * Math.sin(longitude), Math.sin(latitude));
  }

  private static double clamp(double value, double min, double max) {
    return Math.max(min, Math.min(max, value));
  }

  private enum State {
    EMPTY,
    ACTIVE,
    FULL_WORLD,
    INVALID
  }

  private static class Vector3 {
    private final double xComponent;
    private final double yComponent;
    private final double zComponent;

    private Vector3(double xComponent, double yComponent, double zComponent) {
      this.xComponent = xComponent;
      this.yComponent = yComponent;
      this.zComponent = zComponent;
    }

    private Vector3 crossProduct(Vector3 other) {
      return new Vector3(
          yComponent * other.zComponent - zComponent * other.yComponent,
          zComponent * other.xComponent - xComponent * other.zComponent,
          xComponent * other.yComponent - yComponent * other.xComponent);
    }

    private double dotProduct(Vector3 other) {
      return xComponent * other.xComponent
          + yComponent * other.yComponent
          + zComponent * other.zComponent;
    }

    private double length() {
      return Math.sqrt(dotProduct(this));
    }

    private Vector3 scale(double factor) {
      return new Vector3(factor * xComponent, factor * yComponent, factor * zComponent);
    }
  }

  private static class LongitudeInterval {
    private final double west;
    private final double east;

    private LongitudeInterval(double west, double east) {
      this.west = west;
      this.east = east;
    }
  }
}
