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

import org.apache.iceberg.Geography;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

public class GeometryUtil {

  private GeometryUtil() {}

  private static final GeometryFactory FACTORY = new GeometryFactory();

  public static byte[] toWKB(Geometry geom) {
    WKBWriter wkbWriter = new WKBWriter(getOutputDimension(geom), false);
    return wkbWriter.write(geom);
  }

  public static String toWKT(Geometry geom) {
    WKTWriter wktWriter = new WKTWriter(getOutputDimension(geom));
    return wktWriter.write(geom);
  }

  public static Geometry fromWKB(byte[] wkb) {
    WKBReader reader = new WKBReader();
    try {
      return reader.read(wkb);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse WKB", e);
    }
  }

  public static Geometry fromWKT(String wkt) {
    WKTReader reader = new WKTReader();
    try {
      return reader.read(wkt);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse WKT", e);
    }
  }

  public static int getOutputDimension(Geometry geom) {
    int dimension = 2;
    Coordinate coordinate = geom.getCoordinate();

    // We need to set outputDimension = 4 for XYM geometries to make JTS WKTWriter or WKBWriter work
    // correctly.
    // The WKB/WKT writers will ignore Z ordinate for XYM geometries.
    if (!Double.isNaN(coordinate.getZ())) {
      dimension = 3;
    }
    if (!Double.isNaN(coordinate.getM())) {
      dimension = 4;
    }
    return dimension;
  }

  /**
   * Check if the geometry may intersect with the given bound. The bound represents a rectangle
   * crossing the anti-meridian when the x of lower bound is greater than the x of upper bound.
   *
   * @param lowerBound The lower-left point of the bound
   * @param upperBound The upper-right point of the bound
   * @param geom The geometry to check
   * @return true if the geometry may intersect with the bound; false if the geometry definitely
   *     does not intersect with the bound
   */
  public static boolean boundMayIntersects(
      Geometry lowerBound, Geometry upperBound, Geometry geom) {
    Preconditions.checkArgument(lowerBound instanceof Point, "Lower bound must be a point");
    Preconditions.checkArgument(upperBound instanceof Point, "Upper bound must be a point");

    Coordinate lowerCoordinate = lowerBound.getCoordinate();
    Coordinate upperCoordinate = upperBound.getCoordinate();
    if (lowerCoordinate.x <= upperCoordinate.x) {
      // Not crossing the anti-meridian
      Envelope envelope = new Envelope(lowerBound.getCoordinate(), upperBound.getCoordinate());
      return geom.intersects(FACTORY.toGeometry(envelope));
    } else {
      // Crossing the anti-meridian. Use the envelope of geom to evaluate the intersection with
      // false positives
      Envelope envelope = geom.getEnvelopeInternal();
      if (envelope.getMinY() > upperCoordinate.y || envelope.getMaxY() < lowerCoordinate.y) {
        return false;
      }
      return (envelope.getMinX() <= upperCoordinate.x || envelope.getMaxX() >= lowerCoordinate.x);
    }
  }

  /**
   * Check if the geography may intersect with the given bound. The bound represents a rectangle
   * crossing the anti-meridian when the x of lower bound is greater than the x of upper bound.
   *
   * @param lowerBound The lower-left point of the bound
   * @param upperBound The upper-right point of the bound
   * @param geog The geography to check
   * @return true if the geography may intersect with the bound; false if the geography definitely
   *     does not intersect with the bound
   */
  public static boolean boundMayIntersects(
      Geography lowerBound, Geography upperBound, Geography geog) {
    // TODO: implement a correct spherical intersection algorithm
    return boundMayIntersects(lowerBound.geometry(), upperBound.geometry(), geog.geometry());
  }

  /**
   * Check if the bound may cover the geometry. The bound represents a rectangle crossing the
   * anti-meridian when the x of lower bound is greater than the x of upper bound.
   *
   * @param lowerBound The lower-left point of the bound
   * @param upperBound The upper-right point of the bound
   * @param geom The geometry to check
   * @return true if the bound may cover the geometry; false if the bound definitely does not cover
   *     the geometry
   */
  public static boolean boundMayCovers(Geometry lowerBound, Geometry upperBound, Geometry geom) {
    Preconditions.checkArgument(lowerBound instanceof Point, "Lower bound must be a point");
    Preconditions.checkArgument(upperBound instanceof Point, "Upper bound must be a point");

    Coordinate lowerCoordinate = lowerBound.getCoordinate();
    Coordinate upperCoordinate = upperBound.getCoordinate();
    if (lowerCoordinate.x <= upperCoordinate.x) {
      // Not crossing the anti-meridian
      Envelope envelope = new Envelope(lowerBound.getCoordinate(), upperBound.getCoordinate());
      return FACTORY.toGeometry(envelope).covers(geom);
    } else {
      // Crossing the anti-meridian. Use the envelope of geom to evaluate the covers with false
      // positives
      Envelope envelope = geom.getEnvelopeInternal();
      if (envelope.getMinY() < lowerCoordinate.y || envelope.getMaxY() > upperCoordinate.y) {
        return false;
      }
      return (envelope.getMaxX() <= upperCoordinate.x || envelope.getMinX() >= lowerCoordinate.x);
    }
  }

  /**
   * Check if the bound may cover the geography. The bound represents a rectangle crossing the
   * anti-meridian when the x of lower bound is greater than the x of upper bound.
   *
   * @param lowerBound The lower-left point of the bound
   * @param upperBound The upper-right point of the bound
   * @param geog The geography to check
   * @return true if the bound may cover the geography; false if the bound definitely does not cover
   *     the geography
   */
  public static boolean boundMayCovers(Geography lowerBound, Geography upperBound, Geography geog) {
    // TODO: implement a correct spherical covers algorithm
    return boundMayCovers(lowerBound.geometry(), upperBound.geometry(), geog.geometry());
  }

  /**
   * Check if we are sure that the bound must be covered by the geometry. The bound represents a
   * rectangle crossing the anti-meridian when the x of lower bound is greater than the x of upper
   * bound.
   *
   * @param lowerBound The lower-left point of the bound
   * @param upperBound The upper-right point of the bound
   * @param geom The geometry to check
   * @return true if the bound is definitely covered by the geometry; false if the bound may or may
   *     not cover the geometry
   */
  public static boolean boundMustBeCoveredBy(
      Geometry lowerBound, Geometry upperBound, Geometry geom) {
    Preconditions.checkArgument(lowerBound instanceof Point, "Lower bound must be a point");
    Preconditions.checkArgument(upperBound instanceof Point, "Upper bound must be a point");

    Coordinate lowerCoordinate = lowerBound.getCoordinate();
    Coordinate upperCoordinate = upperBound.getCoordinate();
    if (lowerCoordinate.x <= upperCoordinate.x) {
      // Not crossing the anti-meridian
      Envelope envelope = new Envelope(lowerBound.getCoordinate(), upperBound.getCoordinate());
      return FACTORY.toGeometry(envelope).coveredBy(geom);
    } else {
      // Crossing the anti-meridian. This case can be tricky so we always return false to be safe.
      return false;
    }
  }

  /**
   * Check if we are sure that the bound must be covered by the geography. The bound represents a
   * rectangle crossing the anti-meridian when the x of lower bound is greater than the x of upper
   * bound.
   *
   * @param lowerBound The lower-left point of the bound
   * @param upperBound The upper-right point of the bound
   * @param geog The geography to check
   * @return true if the bound is definitely covered by the geography; false if the bound may or may
   *     not cover the geography
   */
  public static boolean boundMustBeCoveredBy(
      Geography lowerBound, Geography upperBound, Geography geog) {
    // TODO: implement a correct spherical covered-by algorithm
    return boundMustBeCoveredBy(lowerBound.geometry(), upperBound.geometry(), geog.geometry());
  }
}
