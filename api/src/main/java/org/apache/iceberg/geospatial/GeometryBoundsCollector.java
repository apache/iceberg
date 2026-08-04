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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Accumulates geometry bounds from values encoded as Well-Known Binary (WKB).
 *
 * <p>The seven OGC geometry types are supported: point, line string, polygon, multi point, multi
 * line string, multi polygon, and geometry collection. WKB values carrying Z or M dimensions are
 * rejected.
 *
 * <p>Coordinates are tracked independently for the X and Y dimensions. {@code NaN} values do not
 * contribute to a dimension, and no bounds are produced unless both dimensions are present.
 */
public final class GeometryBoundsCollector {

  private static final int TYPE_POINT = 1;
  private static final int TYPE_LINE_STRING = 2;
  private static final int TYPE_POLYGON = 3;
  private static final int TYPE_MULTI_POINT = 4;
  private static final int TYPE_MULTI_LINE_STRING = 5;
  private static final int TYPE_MULTI_POLYGON = 6;
  private static final int TYPE_GEOMETRY_COLLECTION = 7;
  private static final int ANY_GEOMETRY = 0;

  private static final int MAX_DEPTH = 100;

  private final DimensionBounds xBounds = new DimensionBounds();
  private final DimensionBounds yBounds = new DimensionBounds();

  // reusable copies of the accumulated bounds, used to undo a partially parsed value
  private final DimensionBounds xSaved = new DimensionBounds();
  private final DimensionBounds ySaved = new DimensionBounds();

  /**
   * Adds the coordinates from one WKB geometry to these bounds.
   *
   * <p>The input is read through a duplicate, so its position and limit are left unchanged.
   *
   * @param wkb a buffer containing exactly one WKB geometry
   * @throws IllegalArgumentException if the WKB is invalid or unsupported
   */
  public void add(ByteBuffer wkb) {
    Preconditions.checkArgument(wkb != null, "Invalid WKB buffer: null");
    xSaved.copyFrom(xBounds);
    ySaved.copyFrom(yBounds);
    ByteBuffer buffer = wkb.duplicate();
    try {
      parseGeometry(buffer, 0, ANY_GEOMETRY);
      Preconditions.checkArgument(!buffer.hasRemaining(), "Invalid WKB: trailing data");
    } catch (RuntimeException e) {
      xBounds.copyFrom(xSaved);
      yBounds.copyFrom(ySaved);
      throw e;
    }
  }

  /** Returns the accumulated bounding box, or {@code null} if either X or Y has no value. */
  public BoundingBox boundingBox() {
    if (!xBounds.hasValue() || !yBounds.hasValue()) {
      return null;
    }

    GeospatialBound min = GeospatialBound.createXY(xBounds.lower(), yBounds.lower());
    GeospatialBound max = GeospatialBound.createXY(xBounds.upper(), yBounds.upper());
    return new BoundingBox(min, max);
  }

  private void parseGeometry(ByteBuffer buffer, int depth, int expectedType) {
    Preconditions.checkArgument(depth <= MAX_DEPTH, "Invalid WKB: nesting too deep");
    checkRemaining(buffer, 5);

    byte order = buffer.get();
    if (order == 0) {
      buffer.order(ByteOrder.BIG_ENDIAN);
    } else if (order == 1) {
      buffer.order(ByteOrder.LITTLE_ENDIAN);
    } else {
      throw new IllegalArgumentException("Invalid WKB byte order: " + order);
    }

    long typeCode = buffer.getInt() & 0xFFFFFFFFL;
    int geometryType = (int) (typeCode % 1000);
    Preconditions.checkArgument(
        typeCode / 1000 == 0, "Unsupported WKB: only 2D geometries are supported");
    Preconditions.checkArgument(
        expectedType == ANY_GEOMETRY || geometryType == expectedType,
        "Invalid WKB: expected geometry type %s but found %s",
        expectedType,
        geometryType);

    switch (geometryType) {
      case TYPE_POINT:
        readCoordinate(buffer);
        break;
      case TYPE_LINE_STRING:
        readCoordinateSequence(buffer, true);
        break;
      case TYPE_POLYGON:
        readPolygon(buffer);
        break;
      case TYPE_MULTI_POINT:
        readCollection(buffer, depth, TYPE_POINT);
        break;
      case TYPE_MULTI_LINE_STRING:
        readCollection(buffer, depth, TYPE_LINE_STRING);
        break;
      case TYPE_MULTI_POLYGON:
        readCollection(buffer, depth, TYPE_POLYGON);
        break;
      case TYPE_GEOMETRY_COLLECTION:
        readCollection(buffer, depth, ANY_GEOMETRY);
        break;
      default:
        throw new IllegalArgumentException("Invalid or unsupported WKB geometry type: " + typeCode);
    }
  }

  private void readPolygon(ByteBuffer buffer) {
    int numRings = readCount(buffer);
    if (numRings > 0) {
      readCoordinateSequence(buffer, true);
    }

    // interior rings are contained by the exterior ring and cannot widen the bounds
    for (int i = 1; i < numRings; i += 1) {
      readCoordinateSequence(buffer, false);
    }
  }

  private void readCollection(ByteBuffer buffer, int depth, int expectedChildType) {
    int numElements = readCount(buffer);
    for (int i = 0; i < numElements; i += 1) {
      parseGeometry(buffer, depth + 1, expectedChildType);
    }
  }

  private void readCoordinateSequence(ByteBuffer buffer, boolean updateBounds) {
    int numPoints = readCount(buffer);
    long numBytes = (long) numPoints * 2 * Double.BYTES;
    checkRemaining(buffer, numBytes);
    if (!updateBounds) {
      buffer.position(buffer.position() + (int) numBytes);
      return;
    }

    for (int i = 0; i < numPoints; i += 1) {
      readCoordinate(buffer);
    }
  }

  private void readCoordinate(ByteBuffer buffer) {
    checkRemaining(buffer, 2 * Double.BYTES);
    double xCoord = buffer.getDouble();
    double yCoord = buffer.getDouble();
    xBounds.add(xCoord);
    yBounds.add(yCoord);
  }

  private static int readCount(ByteBuffer buffer) {
    checkRemaining(buffer, Integer.BYTES);
    long count = buffer.getInt() & 0xFFFFFFFFL;
    Preconditions.checkArgument(count <= Integer.MAX_VALUE, "Invalid WKB element count: %s", count);
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

    private void copyFrom(DimensionBounds other) {
      this.lower = other.lower;
      this.upper = other.upper;
      this.hasValue = other.hasValue;
    }
  }
}
