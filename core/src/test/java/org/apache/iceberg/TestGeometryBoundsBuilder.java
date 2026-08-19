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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestGeometryBoundsBuilder {

  @ParameterizedTest(name = "{0}")
  @MethodSource("boundingBoxCases")
  void boundingBox(String wkt, Geom geom, BoundingBox expected) {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    ByteBuffer wkb = ByteBuffer.wrap(wkb(geom));
    int position = wkb.position();
    int limit = wkb.limit();

    bounds.addValue(wkb);

    assertThat(wkb.position()).as(wkt).isEqualTo(position);
    assertThat(wkb.limit()).as(wkt).isEqualTo(limit);
    assertThat(bounds.build()).as(wkt).isEqualTo(expected);
  }

  @Test
  void boundsFromBufferWithOffset() {
    byte[] padded = new byte[64];
    byte[] wkb = wkb(point(1, 2));
    System.arraycopy(wkb, 0, padded, 11, wkb.length);
    ByteBuffer slice = ByteBuffer.wrap(padded, 11, wkb.length).slice();

    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    bounds.addValue(slice);

    assertThat(slice.position()).isEqualTo(0);
    assertThat(bounds.build()).isEqualTo(box(1, 2, 1, 2));
  }

  @Test
  void noBoundsWhenOneDimensionIsMissing() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    bounds.addValue(ByteBuffer.wrap(wkb(point(1, Double.NaN))));

    assertThat(bounds.build()).as("POINT(1 NaN)").isNull();
  }

  @Test
  void boundsAcrossValuesWithMissingCoordinates() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    bounds.addValue(ByteBuffer.wrap(wkb(point(1, Double.NaN))));
    bounds.addValue(ByteBuffer.wrap(wkb(point(Double.NaN, 2))));

    assertThat(bounds.build()).isEqualTo(box(1, 2, 1, 2));
  }

  @Test
  void infiniteOrdinateIsKeptAsBound() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    // the spec forbids only NaN as a bound, so an infinite ordinate is kept as a real position
    bounds.addValue(ByteBuffer.wrap(wkb(point(Double.POSITIVE_INFINITY, 2))));

    assertThat(bounds.build())
        .as("POINT(Infinity 2)")
        .isEqualTo(box(Double.POSITIVE_INFINITY, 2, Double.POSITIVE_INFINITY, 2));
  }

  @Test
  void infiniteOrdinateWidensBounds() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    bounds.addValue(ByteBuffer.wrap(wkb(point(1, 2))));
    // an infinite coordinate is a real position, so it widens the box toward that infinity
    bounds.addValue(
        ByteBuffer.wrap(wkb(point(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY))));

    assertThat(bounds.build())
        .isEqualTo(box(1, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 2));
  }

  @Test
  void nanIsStillSkippedWhileInfiniteIsKept() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    // NaN X is skipped (empty ordinate) while infinite Y is kept, so only Y produces a bound;
    // with X missing, no box is produced
    bounds.addValue(ByteBuffer.wrap(wkb(point(Double.NaN, Double.POSITIVE_INFINITY))));

    assertThat(bounds.build()).as("POINT(NaN Infinity)").isNull();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("extraDimensionCases")
  void extraDimensionsAreIgnored(String description, Geom geom, BoundingBox expected) {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();

    bounds.addValue(ByteBuffer.wrap(wkb(geom)));

    assertThat(bounds.build()).as(description).isEqualTo(expected);
  }

  @Test
  void boundsAcrossValuesWithDifferentDimensions() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    bounds.addValue(ByteBuffer.wrap(wkb(pointZ(1, 2, 3))));
    bounds.addValue(ByteBuffer.wrap(wkb(pointZM(1, 2, 3, 4))));

    assertThat(bounds.build()).isEqualTo(box(1, 2, 1, 2));
  }

  @Test
  void extraDimensionsNestedInCollectionAreIgnored() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    // a 3D collection holding 3D children (dimensions must match the parent): each child's Z is
    // read past, so only XY bounds the box
    bounds.addValue(ByteBuffer.wrap(wkb(collectionZ(pointZ(1, 2, 9), pointZ(3, 4, 9)))));

    assertThat(bounds.build()).isEqualTo(box(1, 2, 3, 4));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidWkbCases")
  void invalidWkb(String description, byte[] wkb, String expectedMessage) {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();

    assertThatThrownBy(() -> bounds.addValue(ByteBuffer.wrap(wkb)))
        .as(description)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  @Test
  void bigEndianParentWithLittleEndianChild() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    // a big-endian multi point holding a little-endian point, the reverse of the MULTIPOINT case
    bounds.addValue(ByteBuffer.wrap(wkb(multiPointBigEndian(point(1, 2)))));

    assertThat(bounds.build()).isEqualTo(box(1, 2, 1, 2));
  }

  @Test
  void readsFromADirectBuffer() {
    byte[] wkb = wkb(point(1, 2));
    ByteBuffer direct = ByteBuffer.allocateDirect(wkb.length);
    direct.put(wkb).flip();

    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    bounds.addValue(direct);

    assertThat(direct.hasArray()).isFalse();
    assertThat(bounds.build()).isEqualTo(box(1, 2, 1, 2));
  }

  @Test
  void interiorRingContributesToBounds() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    // every ring contributes, so an interior ring extending past the shell widens the box rather
    // than under-covering the polygon (matching the JTS envelope over all coordinates)
    bounds.addValue(
        ByteBuffer.wrap(wkb(polygon(ring(0, 0, 1, 0, 0, 1, 0, 0), ring(0, 0, 9, 0, 0, 9, 0, 0)))));

    assertThat(bounds.build()).isEqualTo(box(0, 0, 9, 9));
  }

  @Test
  void stateIsUndefinedAfterAddValueThrows() {
    GeometryBoundsBuilder bounds = new GeometryBoundsBuilder();
    bounds.addValue(ByteBuffer.wrap(wkb(point(1, 2))));

    // adding a malformed value throws; per the addValue contract the builder must then be
    // discarded,
    // so this only documents that a caller cannot keep using it, not a guaranteed rolled-back state
    assertThatThrownBy(() -> bounds.addValue(ByteBuffer.wrap(truncate(wkb(point(5, 6)), 5))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unexpected end of buffer");
  }

  private static Stream<Arguments> boundingBoxCases() {
    return Stream.of(
        Arguments.of("POINT EMPTY", emptyPoint(), null),
        Arguments.of("POINT(1 2)", point(1, 2), box(1, 2, 1, 2)),
        Arguments.of("POINT(1 2) big endian", pointBigEndian(1, 2), box(1, 2, 1, 2)),
        Arguments.of(
            "LINESTRING(0 1,1 0,2 -1,-1 -2,0 1)",
            lineString(0, 1, 1, 0, 2, -1, -1, -2, 0, 1),
            box(-1, -2, 2, 1)),
        Arguments.of(
            "POLYGON((0 0,10 0,0 10,0 0),(1 1,1 2,2 1,1 1))",
            polygon(ring(0, 0, 10, 0, 0, 10, 0, 0), ring(1, 1, 1, 2, 2, 1, 1, 1)),
            box(0, 0, 10, 10)),
        Arguments.of(
            "MULTIPOINT((1 2),EMPTY,EMPTY,(3 4))",
            // the last child is big-endian, so this also covers a mixed-endian child
            multiPoint(point(1, 2), emptyPoint(), emptyPoint(), pointBigEndian(3, 4)),
            box(1, 2, 3, 4)),
        Arguments.of(
            "MULTILINESTRING((1 2,3 4),(5 6,7 8))",
            multiLineString(lineString(1, 2, 3, 4), lineString(5, 6, 7, 8)),
            box(1, 2, 7, 8)),
        Arguments.of(
            "MULTIPOLYGON(EMPTY,((0 0,10 0,0 10,0 0),(1 1,1 2,2 1,1 1)))",
            multiPolygon(
                emptyPolygon(),
                polygon(ring(0, 0, 10, 0, 0, 10, 0, 0), ring(1, 1, 1, 2, 2, 1, 1, 1))),
            box(0, 0, 10, 10)),
        Arguments.of(
            "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY,POLYGON EMPTY,"
                + "MULTIPOINT EMPTY,MULTILINESTRING EMPTY,MULTIPOLYGON EMPTY,"
                + "GEOMETRYCOLLECTION(POINT EMPTY,LINESTRING EMPTY,POLYGON EMPTY,"
                + "MULTIPOINT EMPTY,MULTILINESTRING EMPTY,MULTIPOLYGON EMPTY))",
            collection(
                point(1, 2),
                emptyLineString(),
                emptyPolygon(),
                emptyMultiPoint(),
                emptyMultiLineString(),
                emptyMultiPolygon(),
                collection(
                    emptyPoint(),
                    emptyLineString(),
                    emptyPolygon(),
                    emptyMultiPoint(),
                    emptyMultiLineString(),
                    emptyMultiPolygon())),
            box(1, 2, 1, 2)));
  }

  private static Stream<Arguments> extraDimensionCases() {
    return Stream.of(
        Arguments.of("POINT Z(1 2 3)", pointZ(1, 2, 3), box(1, 2, 1, 2)),
        Arguments.of("POINT M(1 2 3)", pointM(1, 2, 3), box(1, 2, 1, 2)),
        Arguments.of("POINT ZM(1 2 3 4)", pointZM(1, 2, 3, 4), box(1, 2, 1, 2)),
        Arguments.of(
            "LINESTRING Z(0 1 9,2 -1 9)", lineStringZ(0, 1, 9, 2, -1, 9), box(0, -1, 2, 1)));
  }

  private static Stream<Arguments> invalidWkbCases() {
    return Stream.of(
        // base type 8 is beyond the seven OGC types
        Arguments.of(
            "unknown geometry type", wkb(geom(LE, 8, coordinates(0, 0))), "unsupported WKB"),
        // a type code with the high bits set, e.g. EWKB with an SRID flag
        Arguments.of(
            "type code with SRID flag",
            wkb(geom(LE, 0xFFFFFFFF, coordinates(0, 0))),
            "unsupported WKB"),
        // the leading byte is neither 0 (big endian) nor 1 (little endian)
        Arguments.of("invalid byte order", withByteOrder((byte) 2, wkb(point(0, 0))), "byte order"),
        // a point header with its coordinates cut off
        Arguments.of("truncated point", truncate(wkb(point(1, 2)), 5), "unexpected end of buffer"),
        // a line string that declares far more points than the buffer could hold
        Arguments.of(
            "element count exceeds remaining bytes",
            wkb(
                geom(
                    LE,
                    2,
                    buffer -> {
                      buffer.putInt(1000);
                      buffer.putDouble(0.0);
                      buffer.putDouble(0.0);
                    })),
            "element count"),
        // a multi geometry whose child has the wrong member type
        Arguments.of(
            "multi point with a line string child",
            wkb(multiPoint(lineString(1, 2, 3, 4))),
            "expected geometry type"),
        Arguments.of(
            "multi line string with a point child",
            wkb(multiLineString(point(1, 2))),
            "expected geometry type"),
        Arguments.of(
            "multi polygon with a point child",
            wkb(multiPolygon(point(1, 2))),
            "expected geometry type"),
        // a multi geometry or collection whose child dimension differs from the parent's
        Arguments.of(
            "multi point z with a 2D child",
            wkb(collectionOf(LE, 1004, point(1, 2))),
            "expected dimensions"),
        Arguments.of(
            "geometry collection z with a 2D child",
            wkb(collectionOf(LE, 1007, point(1, 2))),
            "expected dimensions"),
        // a polygon ring that does not close, and one with fewer than four points
        Arguments.of(
            "polygon ring not closed", wkb(polygon(ring(0, 0, 1, 0, 0, 1, 1, 1))), "not closed"),
        Arguments.of(
            "polygon ring with too few points",
            wkb(polygon(ring(0, 0, 1, 0, 0, 0))),
            "fewer than"));
  }

  private static GeospatialBound xy(double xCoord, double yCoord) {
    return GeospatialBound.createXY(xCoord, yCoord);
  }

  private static BoundingBox box(double minX, double minY, double maxX, double maxY) {
    return new BoundingBox(xy(minX, minY), xy(maxX, maxY));
  }

  // ---------------------------------------------------------------------------
  // WKB test data is built from coordinates rather than written as hex, so each
  // case reads as the geometry it represents and cannot silently disagree with it.
  // ---------------------------------------------------------------------------

  private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
  private static final ByteOrder BE = ByteOrder.BIG_ENDIAN;

  /** A WKB geometry that can write itself, including its byte-order and type header. */
  @FunctionalInterface
  private interface Geom {
    void writeTo(WkbBuffer buffer);
  }

  /** Writes the body that follows a geometry's byte-order and type-code header. */
  @FunctionalInterface
  private interface Body {
    void writeTo(WkbBuffer buffer);
  }

  private static byte[] wkb(Geom geom) {
    WkbBuffer buffer = new WkbBuffer();
    geom.writeTo(buffer);
    return buffer.toBytes();
  }

  private static Geom geom(ByteOrder order, int typeCode, Body body) {
    return buffer -> {
      buffer.order(order);
      buffer.putByte(order == BE ? 0 : 1);
      buffer.putInt(typeCode);
      body.writeTo(buffer);
    };
  }

  private static Geom point(double xCoord, double yCoord) {
    return geom(LE, 1, coordinates(xCoord, yCoord));
  }

  private static Geom pointBigEndian(double xCoord, double yCoord) {
    return geom(BE, 1, coordinates(xCoord, yCoord));
  }

  private static Geom emptyPoint() {
    return point(Double.NaN, Double.NaN);
  }

  private static Geom pointZ(double xCoord, double yCoord, double zCoord) {
    return geom(LE, 1001, coordinates(xCoord, yCoord, zCoord));
  }

  private static Geom pointM(double xCoord, double yCoord, double mCoord) {
    return geom(LE, 2001, coordinates(xCoord, yCoord, mCoord));
  }

  private static Geom pointZM(double xCoord, double yCoord, double zCoord, double mCoord) {
    return geom(LE, 3001, coordinates(xCoord, yCoord, zCoord, mCoord));
  }

  private static Geom lineString(double... xy) {
    return geom(LE, 2, sequence(2, xy));
  }

  private static Geom lineStringZ(double... xyz) {
    return geom(LE, 1002, sequence(3, xyz));
  }

  private static Geom emptyLineString() {
    return lineString();
  }

  private static double[] ring(double... xy) {
    return xy;
  }

  private static Geom polygon(double[]... rings) {
    return geom(
        LE,
        3,
        buffer -> {
          buffer.putInt(rings.length);
          for (double[] ring : rings) {
            sequence(2, ring).writeTo(buffer);
          }
        });
  }

  private static Geom emptyPolygon() {
    return polygon();
  }

  private static Geom multiPoint(Geom... children) {
    return collectionOf(LE, 4, children);
  }

  private static Geom multiPointBigEndian(Geom... children) {
    return collectionOf(BE, 4, children);
  }

  private static Geom multiLineString(Geom... children) {
    return collectionOf(LE, 5, children);
  }

  private static Geom multiPolygon(Geom... children) {
    return collectionOf(LE, 6, children);
  }

  private static Geom collection(Geom... children) {
    return collectionOf(LE, 7, children);
  }

  private static Geom collectionZ(Geom... children) {
    return collectionOf(LE, 1007, children);
  }

  private static Geom emptyMultiPoint() {
    return multiPoint();
  }

  private static Geom emptyMultiLineString() {
    return multiLineString();
  }

  private static Geom emptyMultiPolygon() {
    return multiPolygon();
  }

  private static Geom collectionOf(ByteOrder order, int baseType, Geom... children) {
    return geom(
        order,
        baseType,
        buffer -> {
          buffer.putInt(children.length);
          for (Geom child : children) {
            child.writeTo(buffer);
          }
        });
  }

  /** A single coordinate, written with no leading point count. */
  private static Body coordinates(double... ordinates) {
    return buffer -> {
      for (double ordinate : ordinates) {
        buffer.putDouble(ordinate);
      }
    };
  }

  /** A point count followed by that many coordinates of the given dimension. */
  private static Body sequence(int dimensions, double[] flat) {
    return buffer -> {
      buffer.putInt(flat.length / dimensions);
      for (double ordinate : flat) {
        buffer.putDouble(ordinate);
      }
    };
  }

  private static byte[] truncate(byte[] wkb, int length) {
    return Arrays.copyOf(wkb, length);
  }

  private static byte[] withByteOrder(byte order, byte[] wkb) {
    byte[] result = wkb.clone();
    result[0] = order;
    return result;
  }

  /** A growable little/big-endian aware writer for assembling WKB test data. */
  private static final class WkbBuffer {
    private ByteBuffer buffer = ByteBuffer.allocate(256).order(LE);

    private void order(ByteOrder order) {
      buffer.order(order);
    }

    private void putByte(int value) {
      ensure(1).put((byte) value);
    }

    private void putInt(int value) {
      ensure(Integer.BYTES).putInt(value);
    }

    private void putDouble(double value) {
      ensure(Double.BYTES).putDouble(value);
    }

    private ByteBuffer ensure(int bytes) {
      if (buffer.remaining() < bytes) {
        ByteBuffer grown =
            ByteBuffer.allocate(Math.max(buffer.capacity() * 2, buffer.position() + bytes))
                .order(buffer.order());
        grown.put(buffer.duplicate().flip());
        buffer = grown;
      }

      return buffer;
    }

    private byte[] toBytes() {
      return Arrays.copyOf(buffer.array(), buffer.position());
    }
  }
}
