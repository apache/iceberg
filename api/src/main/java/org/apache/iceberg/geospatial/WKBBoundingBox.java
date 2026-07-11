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
 * Computes a two-dimensional (XY) bounding box from geometry values encoded as Well-Known Binary
 * (WKB).
 *
 * <p>The bounding box is the minimum axis-aligned rectangle that contains every coordinate of the
 * geometries. Only the X and Y dimensions are considered; any Z or M values present in the WKB are
 * read past and ignored. Coordinates whose X or Y is {@code NaN} are skipped, matching the spec
 * rule that null or NaN values in a coordinate dimension do not contribute to bounds (an empty
 * geometry such as {@code POINT EMPTY} therefore contributes nothing).
 *
 * <p>Parsing follows the OGC Simple Feature Access WKB layout and validates the input defensively:
 * a malformed or truncated buffer results in an {@link IllegalArgumentException} rather than an
 * out-of-bounds read.
 */
public class WKBBoundingBox {

  // OGC WKB base geometry type codes (after stripping the dimension offset).
  private static final int TYPE_POINT = 1;
  private static final int TYPE_LINE_STRING = 2;
  private static final int TYPE_POLYGON = 3;
  private static final int TYPE_MULTI_POINT = 4;
  private static final int TYPE_MULTI_LINE_STRING = 5;
  private static final int TYPE_MULTI_POLYGON = 6;
  private static final int TYPE_GEOMETRY_COLLECTION = 7;

  // Bounds the recursion depth for nested collections to reject pathological or malicious input.
  private static final int MAX_DEPTH = 100;

  private WKBBoundingBox() {}

  /**
   * Parses one WKB geometry and folds all of its XY coordinates into the given accumulator.
   *
   * <p>The buffer is read via a duplicate, so the caller's position and limit are left unchanged.
   *
   * @param wkb a buffer containing a single WKB geometry
   * @param accumulator the accumulator to update with the geometry's coordinates
   * @throws IllegalArgumentException if the buffer is not a well-formed WKB geometry
   */
  public static void accumulate(ByteBuffer wkb, XYAccumulator accumulator) {
    Preconditions.checkArgument(wkb != null, "Invalid WKB buffer: null");
    Preconditions.checkArgument(accumulator != null, "Invalid accumulator: null");
    ByteBuffer buffer = wkb.duplicate();
    parseGeometry(buffer, accumulator, 0);
  }

  private static void parseGeometry(ByteBuffer buffer, XYAccumulator accumulator, int depth) {
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
    int dimensionGroup = (int) (typeCode / 1000);
    int numDimensions = numDimensions(dimensionGroup, typeCode);

    switch (geometryType) {
      case TYPE_POINT:
        readCoordinate(buffer, accumulator, numDimensions);
        break;
      case TYPE_LINE_STRING:
        readCoordinateSequence(buffer, accumulator, numDimensions);
        break;
      case TYPE_POLYGON:
        int numRings = readCount(buffer);
        for (int i = 0; i < numRings; i += 1) {
          readCoordinateSequence(buffer, accumulator, numDimensions);
        }
        break;
      case TYPE_MULTI_POINT:
      case TYPE_MULTI_LINE_STRING:
      case TYPE_MULTI_POLYGON:
      case TYPE_GEOMETRY_COLLECTION:
        int numElements = readCount(buffer);
        for (int i = 0; i < numElements; i += 1) {
          // Each child carries its own byte-order and type header.
          parseGeometry(buffer, accumulator, depth + 1);
        }
        break;
      default:
        throw new IllegalArgumentException("Invalid or unsupported WKB geometry type: " + typeCode);
    }
  }

  private static int numDimensions(int dimensionGroup, long typeCode) {
    switch (dimensionGroup) {
      case 0: // XY
        return 2;
      case 1: // XYZ
      case 2: // XYM
        return 3;
      case 3: // XYZM
        return 4;
      default:
        throw new IllegalArgumentException("Invalid or unsupported WKB geometry type: " + typeCode);
    }
  }

  private static void readCoordinateSequence(
      ByteBuffer buffer, XYAccumulator accumulator, int numDimensions) {
    int numPoints = readCount(buffer);
    // Validate the full extent up front so a bogus point count cannot drive a long read loop, but
    // never pre-allocate from the count itself.
    checkRemaining(buffer, (long) numPoints * numDimensions * Double.BYTES);
    for (int i = 0; i < numPoints; i += 1) {
      readCoordinate(buffer, accumulator, numDimensions);
    }
  }

  private static void readCoordinate(
      ByteBuffer buffer, XYAccumulator accumulator, int numDimensions) {
    checkRemaining(buffer, (long) numDimensions * Double.BYTES);
    double xCoord = buffer.getDouble();
    double yCoord = buffer.getDouble();
    // Skip any Z and/or M values; only X and Y contribute to the box.
    for (int i = 2; i < numDimensions; i += 1) {
      buffer.getDouble();
    }
    accumulator.addXY(xCoord, yCoord);
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

  /**
   * A mutable accumulator of the minimum and maximum X and Y coordinates seen so far.
   *
   * <p>Coordinates whose X or Y is {@code NaN} are ignored. An accumulator that has seen no non-NaN
   * coordinate reports {@link #hasBounds()} as {@code false} and returns {@code null} bounds.
   */
  public static class XYAccumulator {
    private double minX = Double.POSITIVE_INFINITY;
    private double minY = Double.POSITIVE_INFINITY;
    private double maxX = Double.NEGATIVE_INFINITY;
    private double maxY = Double.NEGATIVE_INFINITY;
    private boolean hasBounds = false;

    /**
     * Folds a single coordinate into the box, ignoring it if either dimension is {@code NaN}.
     *
     * @param xCoord the X coordinate
     * @param yCoord the Y coordinate
     */
    public void addXY(double xCoord, double yCoord) {
      if (Double.isNaN(xCoord) || Double.isNaN(yCoord)) {
        return;
      }
      minX = Math.min(minX, xCoord);
      minY = Math.min(minY, yCoord);
      maxX = Math.max(maxX, xCoord);
      maxY = Math.max(maxY, yCoord);
      hasBounds = true;
    }

    /** Returns whether any non-NaN coordinate has been accumulated. */
    public boolean hasBounds() {
      return hasBounds;
    }

    /**
     * Returns the lower corner (minimum X and Y) of the box, or {@code null} if no coordinate has
     * been accumulated.
     */
    public GeospatialBound minBound() {
      return hasBounds ? GeospatialBound.createXY(minX, minY) : null;
    }

    /**
     * Returns the upper corner (maximum X and Y) of the box, or {@code null} if no coordinate has
     * been accumulated.
     */
    public GeospatialBound maxBound() {
      return hasBounds ? GeospatialBound.createXY(maxX, maxY) : null;
    }
  }
}
