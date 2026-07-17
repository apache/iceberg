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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.iceberg.geospatial.WKBBoundingBox.XYAccumulator;
import org.junit.jupiter.api.Test;

public class TestWKBBoundingBox {

  // OGC WKB geometry type codes; the dimension offset (+1000 Z, +2000 M, +3000 ZM) is added
  // separately by the builders below.
  private static final int POINT = 1;
  private static final int LINE_STRING = 2;
  private static final int POLYGON = 3;
  private static final int MULTI_POINT = 4;
  private static final int MULTI_LINE_STRING = 5;
  private static final int MULTI_POLYGON = 6;
  private static final int GEOMETRY_COLLECTION = 7;

  @Test
  public void testPoint() {
    XYAccumulator acc = accumulate(point(ByteOrder.LITTLE_ENDIAN, 30, 10));
    assertThat(acc.hasBounds()).isTrue();
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(30, 10));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(30, 10));
  }

  @Test
  public void testPointBigEndian() {
    XYAccumulator acc = accumulate(point(ByteOrder.BIG_ENDIAN, -71, 42));
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-71, 42));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(-71, 42));
  }

  @Test
  public void testLineString() {
    XYAccumulator acc =
        accumulate(lineString(ByteOrder.LITTLE_ENDIAN, new double[][] {{0, 0}, {5, 3}, {2, -4}}));
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(0, -4));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(5, 3));
  }

  @Test
  public void testPolygon() {
    // A single ring; the box spans all ring vertices.
    double[][] ring = {{1, 1}, {1, 9}, {8, 9}, {8, 1}, {1, 1}};
    XYAccumulator acc = accumulate(polygon(ByteOrder.LITTLE_ENDIAN, ring));
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(1, 1));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(8, 9));
  }

  @Test
  public void testMultiPoint() {
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            MULTI_POINT,
            point(ByteOrder.LITTLE_ENDIAN, 30, 10),
            point(ByteOrder.LITTLE_ENDIAN, -5, 40));
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-5, 10));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(30, 40));
  }

  @Test
  public void testMultiLineString() {
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            MULTI_LINE_STRING,
            lineString(ByteOrder.LITTLE_ENDIAN, new double[][] {{0, 0}, {1, 1}}),
            lineString(ByteOrder.LITTLE_ENDIAN, new double[][] {{-3, 7}, {2, 2}}));
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-3, 0));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(2, 7));
  }

  @Test
  public void testMultiPolygon() {
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            MULTI_POLYGON,
            polygon(ByteOrder.LITTLE_ENDIAN, new double[][] {{0, 0}, {0, 2}, {2, 2}, {0, 0}}),
            polygon(ByteOrder.LITTLE_ENDIAN, new double[][] {{5, 5}, {5, 9}, {9, 9}, {5, 5}}));
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(0, 0));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(9, 9));
  }

  @Test
  public void testGeometryCollection() {
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            GEOMETRY_COLLECTION,
            point(ByteOrder.LITTLE_ENDIAN, 30, 10),
            lineString(ByteOrder.LITTLE_ENDIAN, new double[][] {{-8, 3}, {12, 25}}));
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-8, 3));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(30, 25));
  }

  @Test
  public void testMixedEndianChildren() {
    // A collection whose children use different byte orders; each child re-reads its own order.
    byte[] wkb =
        collection(
            ByteOrder.BIG_ENDIAN,
            MULTI_POINT,
            point(ByteOrder.LITTLE_ENDIAN, 30, 10),
            point(ByteOrder.BIG_ENDIAN, -5, 40));
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-5, 10));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(30, 40));
  }

  @Test
  public void testNestedGeometryCollection() {
    byte[] inner =
        collection(
            ByteOrder.LITTLE_ENDIAN, GEOMETRY_COLLECTION, point(ByteOrder.LITTLE_ENDIAN, 100, 50));
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            GEOMETRY_COLLECTION,
            point(ByteOrder.LITTLE_ENDIAN, 1, 2),
            inner);
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(1, 2));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(100, 50));
  }

  @Test
  public void testXYZSkipsZ() {
    // A 3D point (type code 1001). Only X and Y should contribute; Z is read past.
    byte[] wkb = pointWithDims(ByteOrder.LITTLE_ENDIAN, 1000, 30, 10, 999.0);
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(30, 10));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(30, 10));
  }

  @Test
  public void testXYMSkipsM() {
    // A measured point (type code 2001).
    byte[] wkb = pointWithDims(ByteOrder.LITTLE_ENDIAN, 2000, -5, 40, 7.5);
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-5, 40));
  }

  @Test
  public void testXYZMSkipsZAndM() {
    // A 4D point (type code 3001).
    byte[] wkb = pointWithDims(ByteOrder.LITTLE_ENDIAN, 3000, 12, 34, 5.0, 6.0);
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(12, 34));
  }

  @Test
  public void testEmptyPointContributesNothing() {
    // POINT EMPTY is encoded as a point with NaN coordinates.
    XYAccumulator acc = accumulate(point(ByteOrder.LITTLE_ENDIAN, Double.NaN, Double.NaN));
    assertThat(acc.hasBounds()).isFalse();
    assertThat(acc.minBound()).isNull();
    assertThat(acc.maxBound()).isNull();
  }

  @Test
  public void testEmptyLineStringContributesNothing() {
    XYAccumulator acc = accumulate(lineString(ByteOrder.LITTLE_ENDIAN, new double[0][]));
    assertThat(acc.hasBounds()).isFalse();
  }

  @Test
  public void testNaNCoordinateSkipped() {
    // The valid point still sets the box; the NaN point contributes nothing.
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            MULTI_POINT,
            point(ByteOrder.LITTLE_ENDIAN, 30, 10),
            point(ByteOrder.LITTLE_ENDIAN, Double.NaN, Double.NaN));
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(30, 10));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(30, 10));
  }

  @Test
  void nanCoordinateComponentsAreSkippedIndependently() {
    XYAccumulator acc = new XYAccumulator();
    WKBBoundingBox.accumulate(ByteBuffer.wrap(point(ByteOrder.LITTLE_ENDIAN, 1, Double.NaN)), acc);

    assertThat(acc.hasBounds()).isFalse();
    assertThat(acc.minBound()).isNull();
    assertThat(acc.maxBound()).isNull();

    WKBBoundingBox.accumulate(ByteBuffer.wrap(point(ByteOrder.LITTLE_ENDIAN, Double.NaN, 20)), acc);

    assertThat(acc.hasBounds()).isTrue();
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(1, 20));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(1, 20));
  }

  @Test
  public void testInfinityWidensBounds() {
    // Only NaN is skipped; +/-Infinity is a real value that widens the box (per spec).
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            MULTI_POINT,
            point(ByteOrder.LITTLE_ENDIAN, 0, 0),
            point(ByteOrder.LITTLE_ENDIAN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(0, Double.NEGATIVE_INFINITY));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(Double.POSITIVE_INFINITY, 0));
  }

  @Test
  public void testCallerBufferPositionUnchanged() {
    ByteBuffer buffer = ByteBuffer.wrap(point(ByteOrder.LITTLE_ENDIAN, 30, 10));
    int position = buffer.position();
    int limit = buffer.limit();
    XYAccumulator acc = new XYAccumulator();
    WKBBoundingBox.accumulate(buffer, acc);
    assertThat(buffer.position()).isEqualTo(position);
    assertThat(buffer.limit()).isEqualTo(limit);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(30, 10));
  }

  @Test
  public void testAccumulateAcrossMultipleGeometries() {
    XYAccumulator acc = new XYAccumulator();
    WKBBoundingBox.accumulate(ByteBuffer.wrap(point(ByteOrder.LITTLE_ENDIAN, 30, 10)), acc);
    WKBBoundingBox.accumulate(ByteBuffer.wrap(point(ByteOrder.LITTLE_ENDIAN, -5, 40)), acc);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-5, 10));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(30, 40));
  }

  @Test
  public void testRejectsBadByteOrder() {
    byte[] wkb = point(ByteOrder.LITTLE_ENDIAN, 30, 10);
    wkb[0] = 2; // neither 0 (BE) nor 1 (LE)
    assertThatThrownBy(() -> accumulate(wkb))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("byte order");
  }

  @Test
  public void testRejectsUnknownGeometryType() {
    byte[] wkb =
        ByteBuffer.allocate(21)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put((byte) 1)
            .putInt(99) // not a valid geometry type
            .putDouble(0)
            .putDouble(0)
            .array();
    assertThatThrownBy(() -> accumulate(wkb))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("geometry type");
  }

  @Test
  public void testRejectsUnknownDimension() {
    // Dimension group 4 (type code 4001) is not a valid OGC dimension offset.
    byte[] wkb =
        ByteBuffer.allocate(21)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put((byte) 1)
            .putInt(4001)
            .putDouble(0)
            .putDouble(0)
            .array();
    assertThatThrownBy(() -> accumulate(wkb))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("geometry type");
  }

  @Test
  public void testRejectsTruncatedHeader() {
    byte[] wkb = Arrays.copyOf(point(ByteOrder.LITTLE_ENDIAN, 30, 10), 3);
    assertThatThrownBy(() -> accumulate(wkb))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("end of buffer");
  }

  @Test
  public void testRejectsTruncatedCoordinates() {
    // A point header that promises two doubles but only supplies one.
    byte[] wkb =
        ByteBuffer.allocate(13)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put((byte) 1)
            .putInt(POINT)
            .putDouble(30)
            .array();
    assertThatThrownBy(() -> accumulate(wkb))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("end of buffer");
  }

  @Test
  public void testRejectsHugeElementCount() {
    // A multipoint that claims 0xFFFFFFFF elements but supplies none.
    byte[] wkb =
        ByteBuffer.allocate(9)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put((byte) 1)
            .putInt(MULTI_POINT)
            .putInt(0xFFFFFFFF) // -1 as signed, ~4 billion unsigned
            .array();
    assertThatThrownBy(() -> accumulate(wkb))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("WKB");
  }

  @Test
  public void testRejectsHugePointCount() {
    // A linestring that claims a massive point count but supplies no coordinates.
    byte[] wkb =
        ByteBuffer.allocate(9)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put((byte) 1)
            .putInt(LINE_STRING)
            .putInt(Integer.MAX_VALUE)
            .array();
    assertThatThrownBy(() -> accumulate(wkb))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("end of buffer");
  }

  @Test
  public void testRejectsDeeplyNestedCollection() {
    // Build 200 nested geometry collections, each containing the next.
    byte[] wkb = point(ByteOrder.LITTLE_ENDIAN, 0, 0);
    for (int i = 0; i < 200; i += 1) {
      wkb = collection(ByteOrder.LITTLE_ENDIAN, GEOMETRY_COLLECTION, wkb);
    }
    byte[] deep = wkb;
    assertThatThrownBy(() -> accumulate(deep))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nesting too deep");
  }

  private static XYAccumulator accumulate(byte[] wkb) {
    XYAccumulator acc = new XYAccumulator();
    WKBBoundingBox.accumulate(ByteBuffer.wrap(wkb), acc);
    return acc;
  }

  private static byte[] point(ByteOrder order, double xCoord, double yCoord) {
    return ByteBuffer.allocate(21)
        .order(order)
        .put(orderFlag(order))
        .putInt(POINT)
        .putDouble(xCoord)
        .putDouble(yCoord)
        .array();
  }

  private static byte[] pointWithDims(
      ByteOrder order, int dimOffset, double xCoord, double yCoord, double... extra) {
    ByteBuffer buffer =
        ByteBuffer.allocate(5 + (2 + extra.length) * Double.BYTES)
            .order(order)
            .put(orderFlag(order))
            .putInt(POINT + dimOffset)
            .putDouble(xCoord)
            .putDouble(yCoord);
    for (double value : extra) {
      buffer.putDouble(value);
    }
    return buffer.array();
  }

  private static byte[] lineString(ByteOrder order, double[][] coords) {
    ByteBuffer buffer =
        ByteBuffer.allocate(5 + Integer.BYTES + coords.length * 2 * Double.BYTES)
            .order(order)
            .put(orderFlag(order))
            .putInt(LINE_STRING)
            .putInt(coords.length);
    for (double[] coord : coords) {
      buffer.putDouble(coord[0]).putDouble(coord[1]);
    }
    return buffer.array();
  }

  private static byte[] polygon(ByteOrder order, double[]... ring) {
    ByteBuffer buffer =
        ByteBuffer.allocate(5 + Integer.BYTES + Integer.BYTES + ring.length * 2 * Double.BYTES)
            .order(order)
            .put(orderFlag(order))
            .putInt(POLYGON)
            .putInt(1) // one ring
            .putInt(ring.length);
    for (double[] coord : ring) {
      buffer.putDouble(coord[0]).putDouble(coord[1]);
    }
    return buffer.array();
  }

  private static byte[] collection(ByteOrder order, int type, byte[]... children) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] header =
        ByteBuffer.allocate(5 + Integer.BYTES)
            .order(order)
            .put(orderFlag(order))
            .putInt(type)
            .putInt(children.length)
            .array();
    out.writeBytes(header);
    for (byte[] child : children) {
      out.writeBytes(child);
    }
    return out.toByteArray();
  }

  private static byte orderFlag(ByteOrder order) {
    return (byte) (order == ByteOrder.LITTLE_ENDIAN ? 1 : 0);
  }
}
