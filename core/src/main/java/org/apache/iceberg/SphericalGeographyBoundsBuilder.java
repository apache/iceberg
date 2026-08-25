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

import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Builds an XY bounding box from geography points and minor great-circle edges on a sphere. */
class SphericalGeographyBoundsBuilder {
  private static final double MIN_NORMAL_LENGTH = 1e-12;
  private static final double ARC_CONTAINMENT_TOLERANCE = 1e-12;
  private static final double LATITUDE_SCALING_FACTOR = 1.0000001;

  private final List<LongitudeInterval> longitudeIntervals = Lists.newArrayList();
  private double minLatitude = Double.POSITIVE_INFINITY;
  private double maxLatitude = Double.NEGATIVE_INFINITY;
  private boolean hasCoordinates = false;
  private boolean incomplete = false;
  private boolean fullLongitudeRange = false;

  void addPoint(double longitude, double latitude) {
    if (!coordinatesAreValid(longitude, latitude)) {
      incomplete = true;
      return;
    }

    hasCoordinates = true;
    if (fullLongitudeRange) {
      return;
    }

    includeLatitude(latitude);
    if (!isPole(latitude)) {
      longitudeIntervals.add(new LongitudeInterval(longitude, longitude));
    }
  }

  void addEdge(double longitude1, double latitude1, double longitude2, double latitude2) {
    if (!coordinatesAreValid(longitude1, latitude1)
        || !coordinatesAreValid(longitude2, latitude2)) {
      incomplete = true;
      return;
    }

    hasCoordinates = true;
    if (fullLongitudeRange) {
      return;
    }

    includeLatitude(latitude1);
    includeLatitude(latitude2);

    if (addEdgeWithPole(longitude1, latitude1, longitude2, latitude2)) {
      return;
    }

    longitudeIntervals.add(minimumLongitudeInterval(longitude1, longitude2));
    addInteriorLatitudeExtrema(longitude1, latitude1, longitude2, latitude2);
  }

  private boolean addEdgeWithPole(
      double longitude1, double latitude1, double longitude2, double latitude2) {
    boolean firstIsPole = isPole(latitude1);
    boolean secondIsPole = isPole(latitude2);
    if (firstIsPole && secondIsPole) {
      if (latitude1 != latitude2) {
        includeFullWorld();
      }

      return true;
    } else if (firstIsPole) {
      longitudeIntervals.add(new LongitudeInterval(longitude2, longitude2));
      return true;
    } else if (secondIsPole) {
      longitudeIntervals.add(new LongitudeInterval(longitude1, longitude1));
      return true;
    }

    return false;
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
    double horizontalNormalLength = Math.hypot(unitNormal.x, unitNormal.y);
    if (horizontalNormalLength == 0) {
      return;
    }

    double vertexLatitude = Math.toDegrees(Math.asin(clamp(horizontalNormalLength, 0.0, 1.0)));
    Vector3 northVertex =
        new Vector3(
            -unitNormal.z * unitNormal.x / horizontalNormalLength,
            -unitNormal.z * unitNormal.y / horizontalNormalLength,
            horizontalNormalLength);

    double endpointMaxLatitude = Math.max(latitude1, latitude2);
    if (vertexLatitude > endpointMaxLatitude
        && isOnMinorArc(northVertex, point1, point2, unitNormal)) {
      // Expand a computed extremum so rounding cannot produce an under-covering bound.
      maxLatitude = Math.max(maxLatitude, Math.min(LATITUDE_SCALING_FACTOR * vertexLatitude, 90.0));
    }

    double endpointMinLatitude = Math.min(latitude1, latitude2);
    Vector3 southVertex = northVertex.scale(-1.0);
    if (-vertexLatitude < endpointMinLatitude
        && isOnMinorArc(southVertex, point1, point2, unitNormal)) {
      // Expand a computed extremum so rounding cannot produce an under-covering bound.
      minLatitude =
          Math.min(minLatitude, Math.max(-LATITUDE_SCALING_FACTOR * vertexLatitude, -90.0));
    }
  }

  BoundingBox build() {
    if (incomplete || !hasCoordinates) {
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

  private void includeFullWorld() {
    minLatitude = -90.0;
    maxLatitude = 90.0;
    longitudeIntervals.clear();
    fullLongitudeRange = true;
  }

  private LongitudeInterval longitudeBounds() {
    if (fullLongitudeRange || longitudeIntervals.isEmpty()) {
      return new LongitudeInterval(-180.0, 180.0);
    }

    // The minimum covering circular interval is the complement of the largest uncovered gap.
    List<LongitudeEvent> events = Lists.newArrayListWithExpectedSize(2 * longitudeIntervals.size());
    for (LongitudeInterval interval : longitudeIntervals) {
      if (interval.west > interval.east) {
        events.add(new LongitudeEvent(-180.0, true));
        events.add(new LongitudeEvent(interval.east, false));
        events.add(new LongitudeEvent(interval.west, true));
        events.add(new LongitudeEvent(180.0, false));
      } else {
        events.add(new LongitudeEvent(interval.west, true));
        events.add(new LongitudeEvent(interval.east, false));
      }
    }

    events.sort(
        Comparator.comparingDouble((LongitudeEvent event) -> event.longitude)
            .thenComparing(event -> !event.start));

    double largestGapStart = 0.0;
    double largestGapEnd = -1.0;
    int overlapCount = 0;
    for (int i = 0; i < events.size(); i += 1) {
      LongitudeEvent event = events.get(i);
      if (event.start) {
        if (overlapCount == 0 && i > 0) {
          double gapStart = events.get(i - 1).longitude;
          if (event.longitude - gapStart > largestGapEnd - largestGapStart) {
            largestGapStart = gapStart;
            largestGapEnd = event.longitude;
          }
        }

        overlapCount += 1;
      } else {
        overlapCount -= 1;
      }
    }

    double firstLongitude = events.get(0).longitude;
    double lastLongitude = events.get(events.size() - 1).longitude;
    double antimeridianGap = 360.0 + firstLongitude - lastLongitude;
    if (antimeridianGap >= largestGapEnd - largestGapStart) {
      return new LongitudeInterval(firstLongitude, lastLongitude);
    }

    return new LongitudeInterval(largestGapEnd, largestGapStart);
  }

  private static LongitudeInterval minimumLongitudeInterval(double longitude1, double longitude2) {
    if (Math.abs(longitude1) == 180.0 && Math.abs(longitude2) == 180.0) {
      return new LongitudeInterval(-180.0, -180.0);
    }

    double west = Math.min(longitude1, longitude2);
    double east = Math.max(longitude1, longitude2);
    double directGap = east - west;
    double antimeridianGap = 360.0 - directGap;
    return antimeridianGap >= directGap
        ? new LongitudeInterval(west, east)
        : new LongitudeInterval(east, west);
  }

  private static boolean coordinatesAreValid(double longitude, double latitude) {
    return Double.isFinite(longitude)
        && Double.isFinite(latitude)
        && longitude >= -180.0
        && longitude <= 180.0
        && latitude >= -90.0
        && latitude <= 90.0;
  }

  private static boolean isPole(double latitude) {
    return Math.abs(latitude) == 90.0;
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

  private static class Vector3 {
    private final double x;
    private final double y;
    private final double z;

    private Vector3(double x, double y, double z) {
      this.x = x;
      this.y = y;
      this.z = z;
    }

    private Vector3 crossProduct(Vector3 other) {
      return new Vector3(
          y * other.z - z * other.y, z * other.x - x * other.z, x * other.y - y * other.x);
    }

    private double dotProduct(Vector3 other) {
      return x * other.x + y * other.y + z * other.z;
    }

    private double length() {
      return Math.sqrt(dotProduct(this));
    }

    private Vector3 scale(double factor) {
      return new Vector3(factor * x, factor * y, factor * z);
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

  private static class LongitudeEvent {
    private final double longitude;
    private final boolean start;

    private LongitudeEvent(double longitude, boolean start) {
      this.longitude = longitude;
      this.start = start;
    }
  }
}
