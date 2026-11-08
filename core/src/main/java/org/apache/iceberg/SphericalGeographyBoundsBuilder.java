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

import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;

/**
 * Builds an XY bounding box from geography points and minor great-circle edges on a sphere.
 *
 * <p>Bounds may cross the antimeridian, with west greater than east. Pole vertices retain their
 * stored longitude. Computed latitude extrema are widened to avoid undercoverage.
 *
 * <p>NaN ordinates do not contribute to their dimension. Empty input or a missing dimension
 * produces no bounds. Out-of-range or infinite coordinates permanently suppress bounds. Numerically
 * unresolved near-antipodal edges produce world bounds.
 *
 * <p>Callers must bound polygon interiors separately from their edges.
 */
class SphericalGeographyBoundsBuilder {
  private static final double MIN_LONGITUDE = -180.0;
  private static final double MAX_LONGITUDE = 180.0;
  private static final double MIN_LATITUDE = -90.0;
  private static final double MAX_LATITUDE = 90.0;
  private static final double LONGITUDE_SPAN = MAX_LONGITUDE - MIN_LONGITUDE;

  // A small cross product amplifies rounding errors when normalizing the great-circle plane.
  private static final double MIN_NORMAL_LENGTH = 1e-6;

  private static final double ARC_CONTAINMENT_TOLERANCE = 1e-12;
  // Relative padding alone vanishes for extrema near the equator.
  private static final double LATITUDE_MARGIN = 1e-6;
  private static final double LATITUDE_SCALING_FACTOR = 1.0000001;

  private final LongitudeBoundsBuilder longitudes = new LongitudeBoundsBuilder();
  private double minLatitude = Double.POSITIVE_INFINITY;
  private double maxLatitude = Double.NEGATIVE_INFINITY;
  private State state = State.EMPTY;

  void addPoint(double longitude, double latitude) {
    includeCoordinate(longitude, latitude);
  }

  void addWorld() {
    if (state != State.INVALID) {
      includeFullWorld();
    }
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

    addMinimumLongitudeInterval(longitude1, longitude2);
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
    if (latitude1 == latitude2
        && (longitude1 == longitude2 || Math.abs(longitude1 - longitude2) == LONGITUDE_SPAN)) {
      return;
    }

    Vector3 point1 = toUnitVector(longitude1, latitude1);
    Vector3 point2 = toUnitVector(longitude2, latitude2);
    Vector3 normal = point1.crossProduct(point2);
    double normalLength = normal.length();
    if (normalLength <= MIN_NORMAL_LENGTH) {
      if (point1.dotProduct(point2) < 0) {
        includeFullWorld();
      } else {
        addShortArcLatitudeBounds(latitude1, latitude2, normalLength);
      }

      return;
    }

    Vector3 unitNormal = normal.scale(1.0 / normalLength);
    double horizontalNormalLength = Math.hypot(unitNormal.xComponent, unitNormal.yComponent);
    if (horizontalNormalLength == 0) {
      return;
    }

    double vertexLatitude = Math.toDegrees(Math.asin(clamp(horizontalNormalLength, 0.0, 1.0)));
    double conservativeVertexLatitude =
        Math.min(
            Math.max(LATITUDE_SCALING_FACTOR * vertexLatitude, vertexLatitude + LATITUDE_MARGIN),
            MAX_LATITUDE);
    Vector3 northVertex =
        new Vector3(
            -unitNormal.zComponent * unitNormal.xComponent / horizontalNormalLength,
            -unitNormal.zComponent * unitNormal.yComponent / horizontalNormalLength,
            horizontalNormalLength);

    if (isOnMinorArc(northVertex, point1, point2, unitNormal)) {
      maxLatitude = Math.max(maxLatitude, conservativeVertexLatitude);
      if (conservativeVertexLatitude == MAX_LATITUDE) {
        longitudes.add(MIN_LONGITUDE, MAX_LONGITUDE);
      }
    }

    Vector3 southVertex = northVertex.scale(-1.0);
    if (isOnMinorArc(southVertex, point1, point2, unitNormal)) {
      minLatitude = Math.min(minLatitude, -conservativeVertexLatitude);
      if (conservativeVertexLatitude == MAX_LATITUDE) {
        longitudes.add(MIN_LONGITUDE, MAX_LONGITUDE);
      }
    }
  }

  private void addShortArcLatitudeBounds(double latitude1, double latitude2, double normalLength) {
    // Latitude cannot change by more than the central angle along a minor arc.
    double latitudePadding = Math.toDegrees(Math.asin(normalLength)) + LATITUDE_MARGIN;
    double south = Math.max(MIN_LATITUDE, Math.min(latitude1, latitude2) - latitudePadding);
    double north = Math.min(MAX_LATITUDE, Math.max(latitude1, latitude2) + latitudePadding);
    includeLatitude(south);
    includeLatitude(north);
    if (south == MIN_LATITUDE || north == MAX_LATITUDE) {
      longitudes.add(MIN_LONGITUDE, MAX_LONGITUDE);
    }
  }

  BoundingBox build() {
    if (state == State.EMPTY || state == State.INVALID) {
      return null;
    }

    if (state == State.FULL_WORLD) {
      return worldBounds();
    }

    LongitudeBoundsBuilder.Interval longitudeBounds = longitudes.build();
    if (!hasLatitude() || longitudeBounds == null) {
      return null;
    }

    return new BoundingBox(
        GeospatialBound.createXY(longitudeBounds.west(), minLatitude),
        GeospatialBound.createXY(longitudeBounds.east(), maxLatitude));
  }

  private void includeLatitude(double latitude) {
    minLatitude = Math.min(minLatitude, latitude);
    maxLatitude = Math.max(maxLatitude, latitude);
  }

  private boolean hasLatitude() {
    return minLatitude <= maxLatitude;
  }

  private void includeFullWorld() {
    minLatitude = MIN_LATITUDE;
    maxLatitude = MAX_LATITUDE;
    longitudes.add(MIN_LONGITUDE, MAX_LONGITUDE);
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
      longitudes.add(longitude, longitude);
      state = State.ACTIVE;
    }

    return !longitudeNaN && !latitudeNaN;
  }

  private void addMinimumLongitudeInterval(double longitude1, double longitude2) {
    // A coordinate on the antimeridian is kept as given: +180 and -180 both name that meridian
    // and are preserved rather than folded onto one sign, so the same coordinate yields the same
    // interval whether it arrives as a point or a degenerate edge. The interval spans the shorter
    // of the two arcs between the endpoints, wrapping past the antimeridian (west > east) when
    // that arc is the shorter one.
    double west = Math.min(longitude1, longitude2);
    double east = Math.max(longitude1, longitude2);
    double directGap = east - west;
    double antimeridianGap = LONGITUDE_SPAN - directGap;
    if (antimeridianGap >= directGap) {
      longitudes.add(west, east);
    } else {
      longitudes.add(east, west);
    }
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
}
