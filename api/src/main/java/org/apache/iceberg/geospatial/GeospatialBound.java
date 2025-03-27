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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Objects;

/**
 * Represents a geospatial bound (minimum or maximum) for Iceberg tables.
 *
 * <p>According to the <a href="https://iceberg.apache.org/spec/#bound-serialization">Bound
 * serialization section of Iceberg Table spec</a>, geospatial bounds are serialized differently
 * from the regular WKB representation. Geometry and geography bounds are single point encoded as a
 * concatenation of 8-byte little-endian IEEE 754 coordinate values in the order X, Y, Z (optional),
 * M (optional).
 *
 * <p>The encoding varies based on which coordinates are present:
 *
 * <ul>
 *   <li>x:y (2 doubles) when both z and m are unset
 *   <li>x:y:z (3 doubles) when only m is unset
 *   <li>x:y:NaN:m (4 doubles) when only z is unset
 *   <li>x:y:z:m (4 doubles) when all coordinates are set
 * </ul>
 *
 * <p>This class represents a lower or upper geospatial bound and handles serialization and
 * deserialization of these bounds to/from byte arrays, conforming to the Iceberg specification.
 */
public class GeospatialBound implements Serializable, Comparable<GeospatialBound> {
  /**
   * Parses a geospatial bound from a byte buffer according to Iceberg spec.
   *
   * <p>Based on the buffer size, this method determines which coordinates are present: - 16 bytes
   * (2 doubles): x and y only - 24 bytes (3 doubles): x, y, and z - 32 bytes (4 doubles): x, y, z
   * (might be NaN), and m
   *
   * @param buffer the ByteBuffer containing the serialized geospatial bound
   * @return a GeospatialBound object representing the parsed bound
   * @throws IllegalArgumentException if the buffer has an invalid size
   */
  public static GeospatialBound fromByteBuffer(ByteBuffer buffer) {
    // Create a duplicate to avoid modifying the original buffer's position and byte order
    ByteBuffer tmp = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);

    int size = tmp.remaining();

    if (size == 2 * Double.BYTES) {
      // x:y format (2 doubles)
      double coordX = tmp.getDouble();
      double coordY = tmp.getDouble();
      return createXY(coordX, coordY);
    } else if (size == 3 * Double.BYTES) {
      // x:y:z format (3 doubles)
      double coordX = tmp.getDouble();
      double coordY = tmp.getDouble();
      double coordZ = tmp.getDouble();
      return createXYZ(coordX, coordY, coordZ);
    } else if (size == 4 * Double.BYTES) {
      // x:y:z:m format (4 doubles) - z might be NaN
      double coordX = tmp.getDouble();
      double coordY = tmp.getDouble();
      double coordZ = tmp.getDouble();
      double coordM = tmp.getDouble();
      return new GeospatialBound(coordX, coordY, coordZ, coordM);
    } else {
      throw new IllegalArgumentException(
          "Invalid buffer size for GeospatialBound: expected 16, 24, or 32 bytes, got " + size);
    }
  }

  /**
   * Parses a geospatial bound from a byte array according to Iceberg spec.
   *
   * @param bytes the byte array containing the serialized geospatial bound
   * @return a GeospatialBound object representing the parsed bound
   * @throws IllegalArgumentException if the byte array has an invalid length
   */
  public static GeospatialBound fromByteArray(byte[] bytes) {
    int length = bytes.length;
    if (length != 2 * Double.BYTES && length != 3 * Double.BYTES && length != 4 * Double.BYTES) {
      throw new IllegalArgumentException(
          "Invalid byte array length for GeospatialBound: expected 16, 24, or 32 bytes, got "
              + length);
    }

    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  /**
   * Creates a GeospatialBound with X and Y coordinates only.
   *
   * @param x the X coordinate (longitude/easting)
   * @param y the Y coordinate (latitude/northing)
   * @return a GeospatialBound with XY coordinates
   */
  @SuppressWarnings("ParameterName")
  public static GeospatialBound createXY(double x, double y) {
    return new GeospatialBound(x, y, Double.NaN, Double.NaN);
  }

  /**
   * Creates a GeospatialBound with X, Y, and Z coordinates, with no M value.
   *
   * @param x the X coordinate (longitude/easting)
   * @param y the Y coordinate (latitude/northing)
   * @param z the Z coordinate (elevation)
   * @return a GeospatialBound with XYZ coordinates
   */
  @SuppressWarnings("ParameterName")
  public static GeospatialBound createXYZ(double x, double y, double z) {
    return new GeospatialBound(x, y, z, Double.NaN);
  }

  /**
   * Creates a GeospatialBound with X, Y, Z, and M coordinates.
   *
   * @param x the X coordinate (longitude/easting)
   * @param y the Y coordinate (latitude/northing)
   * @param z the Z coordinate (elevation)
   * @param m the M value (measure)
   * @return a GeospatialBound with XYZM coordinates
   */
  @SuppressWarnings("ParameterName")
  public static GeospatialBound createXYZM(double x, double y, double z, double m) {
    return new GeospatialBound(x, y, z, m);
  }

  /**
   * Creates a GeospatialBound with X, Y, and M values, with no Z coordinate.
   *
   * @param x the X coordinate (longitude/easting)
   * @param y the Y coordinate (latitude/northing)
   * @param m the M value (measure)
   * @return a GeospatialBound with XYM coordinates
   */
  @SuppressWarnings("ParameterName")
  public static GeospatialBound createXYM(double x, double y, double m) {
    return new GeospatialBound(x, y, Double.NaN, m);
  }

  @SuppressWarnings("MemberName")
  private final double x;

  @SuppressWarnings("MemberName")
  private final double y;

  @SuppressWarnings("MemberName")
  private final double z;

  @SuppressWarnings("MemberName")
  private final double m;

  /** Private constructor - use factory methods instead. */
  @SuppressWarnings("ParameterName")
  private GeospatialBound(double x, double y, double z, double m) {
    this.x = x;
    this.y = y;
    this.z = z;
    this.m = m;
  }

  /**
   * Get the X coordinate (longitude/easting).
   *
   * @return X coordinate value
   */
  @SuppressWarnings("MethodName")
  public double x() {
    return x;
  }

  /**
   * Get the Y coordinate (latitude/northing).
   *
   * @return Y coordinate value
   */
  @SuppressWarnings("MethodName")
  public double y() {
    return y;
  }

  /**
   * Get the Z coordinate (typically elevation).
   *
   * @return Z coordinate value or NaN if not set
   */
  @SuppressWarnings("MethodName")
  public double z() {
    return z;
  }

  /**
   * Get the M value (measure).
   *
   * @return M value or NaN if not set
   */
  @SuppressWarnings("MethodName")
  public double m() {
    return m;
  }

  /**
   * Check if this bound has a defined Z coordinate.
   *
   * @return true if Z is not NaN
   */
  public boolean hasZ() {
    return !Double.isNaN(z);
  }

  /**
   * Check if this bound has a defined M value.
   *
   * @return true if M is not NaN
   */
  public boolean hasM() {
    return !Double.isNaN(m);
  }

  /**
   * Serializes this geospatial bound to a byte buffer according to Iceberg spec.
   *
   * <p>Following the Iceberg spec, the bound is serialized based on which coordinates are set: -
   * x:y (2 doubles) when both z and m are unset - x:y:z (3 doubles) when only m is unset -
   * x:y:NaN:m (4 doubles) when only z is unset - x:y:z:m (4 doubles) when all coordinates are set
   *
   * @return A ByteBuffer containing the serialized geospatial bound
   */
  public ByteBuffer toByteBuffer() {
    // Calculate size based on which coordinates are present
    int size;
    if (!hasZ() && !hasM()) {
      // Just x and y
      size = 2 * Double.BYTES;
    } else if (hasZ() && !hasM()) {
      // x, y, and z (no m)
      size = 3 * Double.BYTES;
    } else {
      // x, y, z (or NaN), and m
      size = 4 * Double.BYTES;
    }

    ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putDouble(x);
    buffer.putDouble(y);

    if (hasZ() || hasM()) {
      // If we have z or m or both, we need to include z (could be NaN)
      buffer.putDouble(z);
    }

    if (hasM()) {
      // If we have m, include it
      buffer.putDouble(m);
    }

    buffer.flip();
    return buffer;
  }

  @Override
  public String toString() {
    return "GeospatialBound(" + simpleString() + ")";
  }

  public String simpleString() {
    StringBuilder sb = new StringBuilder();
    sb.append("x=").append(x).append(", y=").append(y);

    if (hasZ()) {
      sb.append(", z=").append(z);
    }

    if (hasM()) {
      sb.append(", m=").append(m);
    }

    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof GeospatialBound)) {
      return false;
    }

    GeospatialBound that = (GeospatialBound) other;
    return Double.compare(that.x, x) == 0
        && Double.compare(that.y, y) == 0
        && Double.compare(that.z, z) == 0
        && Double.compare(that.m, m) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(GeospatialBound.class, x, y, z, m);
  }

  @Override
  public int compareTo(GeospatialBound other) {
    return Comparator.comparingDouble(GeospatialBound::x)
        .thenComparingDouble(GeospatialBound::y)
        .thenComparingDouble(GeospatialBound::z)
        .thenComparingDouble(GeospatialBound::m)
        .compare(this, other);
  }
}
