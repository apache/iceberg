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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Accumulates geometry bounds from values encoded as Well-Known Binary (WKB).
 *
 * <p>The seven OGC geometry types are supported: point, line string, polygon, multi point, multi
 * line string, multi polygon, and geometry collection.
 *
 * <p>Coordinates are tracked independently for the X and Y dimensions. A {@code NaN} ordinate marks
 * an empty value and does not contribute to its dimension; an infinite ordinate is a real position
 * and is kept as a bound, since the spec forbids only NaN as a lower or upper bound. No bounds are
 * produced unless both dimensions are present.
 *
 * <p>These bounds apply to {@code geometry} columns, whose edges are always interpolated linearly,
 * so a box that contains every vertex contains the whole geometry. They are not valid for {@code
 * geography} columns: geodesic edges can reach beyond their endpoints, longitude is periodic, and a
 * geography box may cross the antimeridian.
 *
 * <p>Only the X and Y dimensions contribute to the box. Z and M ordinates are valid in the ISO WKB
 * serializations that Iceberg accepts, so they are read past and ignored rather than rejected.
 *
 * <p>Every ring of a polygon contributes, so the box covers the whole polygon even when an interior
 * ring extends past the shell. This matches the envelope a library such as JTS derives from all of
 * a geometry's coordinates.
 */
class GeometryBoundsBuilder {

  private static final int TYPE_POINT = 1;
  private static final int TYPE_LINE_STRING = 2;
  private static final int TYPE_POLYGON = 3;
  private static final int TYPE_MULTI_POINT = 4;
  private static final int TYPE_MULTI_LINE_STRING = 5;
  private static final int TYPE_MULTI_POLYGON = 6;
  private static final int TYPE_GEOMETRY_COLLECTION = 7;
  private static final int ANY_GEOMETRY = 0;

  // ISO WKB encodes the dimensions of a geometry in the thousands digit of its type code
  private static final int XY_GROUP = 0;
  private static final int XYZ_GROUP = 1;
  private static final int XYM_GROUP = 2;
  private static final int XYZM_GROUP = 3;

  private final DimensionBounds xBounds = new DimensionBounds();
  private final DimensionBounds yBounds = new DimensionBounds();

  /**
   * Adds one WKB geometry value to these bounds.
   *
   * <p>The input is read through a duplicate, so its position and limit are left unchanged.
   *
   * <p>If this throws, the builder's state is undefined: coordinates parsed before the failure may
   * already be folded in, so a caller that continues after a rejected value must discard this
   * builder.
   *
   * @param wkb a buffer containing exactly one WKB geometry
   * @throws IllegalArgumentException if the WKB is malformed
   */
  public void addValue(ByteBuffer wkb) {
    Preconditions.checkArgument(wkb != null, "Invalid WKB buffer: null");
    ByteBuffer buffer = wkb.duplicate();
    parseGeometry(buffer, ANY_GEOMETRY);
  }

  /**
   * Builds the bounding box covering every geometry added, or {@code null} if either the X or Y
   * dimension has no value.
   */
  public BoundingBox build() {
    if (!xBounds.hasValue() || !yBounds.hasValue()) {
      return null;
    }

    GeospatialBound min = GeospatialBound.createXY(xBounds.lower(), yBounds.lower());
    GeospatialBound max = GeospatialBound.createXY(xBounds.upper(), yBounds.upper());
    return new BoundingBox(min, max);
  }

  private void parseGeometry(ByteBuffer buffer, int expectedType) {
    checkRemaining(buffer, 5);

    // each geometry sets its own byte order; restore the caller's order before returning so a
    // sibling read after a nested geometry is not misread with the wrong endianness
    ByteOrder callerOrder = buffer.order();
    byte order = buffer.get();
    if (order == 0) {
      buffer.order(ByteOrder.BIG_ENDIAN);
    } else if (order == 1) {
      buffer.order(ByteOrder.LITTLE_ENDIAN);
    } else {
      throw new IllegalArgumentException("Invalid WKB byte order: " + order);
    }

    try {
      parseGeometryBody(buffer, expectedType);
    } finally {
      buffer.order(callerOrder);
    }
  }

  private void parseGeometryBody(ByteBuffer buffer, int expectedType) {
    long typeCode = buffer.getInt() & 0xFFFFFFFFL;
    long dimensionGroup = typeCode / 1000;
    int geometryType = (int) (typeCode % 1000);
    Preconditions.checkArgument(
        geometryType >= TYPE_POINT
            && geometryType <= TYPE_GEOMETRY_COLLECTION
            && dimensionGroup <= XYZM_GROUP,
        "Invalid or unsupported WKB geometry type: %s",
        typeCode);
    // built with an if/throw rather than checkArgument so typeName() is not evaluated for every
    // value on the write path, only when the type is actually wrong
    if (expectedType != ANY_GEOMETRY && geometryType != expectedType) {
      throw new IllegalArgumentException(
          "Invalid WKB: expected geometry type "
              + typeName(expectedType)
              + " but found "
              + typeName(geometryType));
    }

    int numDimensions = numDimensions(dimensionGroup);

    switch (geometryType) {
      case TYPE_POINT:
        readCoordinate(buffer, numDimensions);
        break;
      case TYPE_LINE_STRING:
        readCoordinateSequence(buffer, numDimensions);
        break;
      case TYPE_POLYGON:
        readPolygon(buffer, numDimensions);
        break;
      case TYPE_MULTI_POINT:
        readCollection(buffer, TYPE_POINT);
        break;
      case TYPE_MULTI_LINE_STRING:
        readCollection(buffer, TYPE_LINE_STRING);
        break;
      case TYPE_MULTI_POLYGON:
        readCollection(buffer, TYPE_POLYGON);
        break;
      case TYPE_GEOMETRY_COLLECTION:
        readCollection(buffer, ANY_GEOMETRY);
        break;
      default:
        throw new IllegalArgumentException("Invalid or unsupported WKB geometry type: " + typeCode);
    }
  }

  private static String typeName(int geometryType) {
    switch (geometryType) {
      case TYPE_POINT:
        return "Point";
      case TYPE_LINE_STRING:
        return "LineString";
      case TYPE_POLYGON:
        return "Polygon";
      case TYPE_MULTI_POINT:
        return "MultiPoint";
      case TYPE_MULTI_LINE_STRING:
        return "MultiLineString";
      case TYPE_MULTI_POLYGON:
        return "MultiPolygon";
      case TYPE_GEOMETRY_COLLECTION:
        return "GeometryCollection";
      default:
        return String.valueOf(geometryType);
    }
  }

  private static int numDimensions(long dimensionGroup) {
    switch ((int) dimensionGroup) {
      case XY_GROUP:
        return 2;
      case XYZ_GROUP:
      case XYM_GROUP:
        return 3;
      default: // XYZM_GROUP, the only remaining group the caller accepts
        return 4;
    }
  }

  private void readPolygon(ByteBuffer buffer, int numDimensions) {
    // every ring contributes, including interior rings: a hole extending past the shell would
    // otherwise under-cover the polygon, pruning a file from a query it should match
    int numRings = readCount(buffer);
    for (int i = 0; i < numRings; i += 1) {
      readCoordinateSequence(buffer, numDimensions);
    }
  }

  private void readCollection(ByteBuffer buffer, int expectedChildType) {
    int numElements = readCount(buffer);
    for (int i = 0; i < numElements; i += 1) {
      // each child carries its own byte order, type code, and dimensions
      parseGeometry(buffer, expectedChildType);
    }
  }

  private void readCoordinateSequence(ByteBuffer buffer, int numDimensions) {
    int numPoints = readCount(buffer);
    long numBytes = (long) numPoints * numDimensions * Double.BYTES;
    checkRemaining(buffer, numBytes);
    for (int i = 0; i < numPoints; i += 1) {
      readCoordinate(buffer, numDimensions);
    }
  }

  private void readCoordinate(ByteBuffer buffer, int numDimensions) {
    checkRemaining(buffer, (long) numDimensions * Double.BYTES);
    double xCoord = buffer.getDouble();
    double yCoord = buffer.getDouble();
    // only X and Y contribute to the box; skip any Z and M ordinates
    for (int i = 2; i < numDimensions; i += 1) {
      buffer.getDouble();
    }

    xBounds.add(xCoord);
    yBounds.add(yCoord);
  }

  private static int readCount(ByteBuffer buffer) {
    checkRemaining(buffer, Integer.BYTES);
    long count = buffer.getInt() & 0xFFFFFFFFL;
    // every element or point occupies at least one more byte, so a count larger than the bytes left
    // cannot be valid; catch it here with a precise message instead of looping until the buffer
    // ends
    Preconditions.checkArgument(
        count <= buffer.remaining(),
        "Invalid WKB element count: %s exceeds %s remaining bytes",
        count,
        buffer.remaining());
    return (int) count;
  }

  private static void checkRemaining(ByteBuffer buffer, long bytes) {
    Preconditions.checkArgument(
        buffer.remaining() >= bytes, "Invalid WKB: unexpected end of buffer");
  }

  private static class DimensionBounds {
    private double lower;
    private double upper;
    private boolean hasValue = false;

    private void add(double value) {
      // NaN marks an empty ordinate and is skipped per the spec; an infinite value is a real
      // position and is kept, since the spec forbids only NaN as a lower or upper bound
      if (Double.isNaN(value)) {
        return;
      }

      if (hasValue) {
        lower = Math.min(lower, value);
        upper = Math.max(upper, value);
      } else {
        lower = value;
        upper = value;
        hasValue = true;
      }
    }

    private boolean hasValue() {
      return hasValue;
    }

    private double lower() {
      return lower;
    }

    private double upper() {
      return upper;
    }
  }
}
