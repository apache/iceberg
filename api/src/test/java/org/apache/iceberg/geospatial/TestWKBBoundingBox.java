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
import java.util.stream.Stream;
import org.apache.iceberg.geospatial.WKBBoundingBox.XYAccumulator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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

  @ParameterizedTest
  @ValueSource(ints = {1000, 2000, 3000})
  void dimensionedLineStringsSkipExtraCoordinates(int dimensionOffset) {
    double[][] coordinates = {
      coordinateWithDims(dimensionOffset, -3, 7), coordinateWithDims(dimensionOffset, 8, -4)
    };

    XYAccumulator acc =
        accumulate(lineStringWithDims(ByteOrder.LITTLE_ENDIAN, dimensionOffset, coordinates));

    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-3, -4));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(8, 7));
  }

  @ParameterizedTest
  @ValueSource(ints = {1000, 2000, 3000})
  void dimensionedPolygonsSkipExtraCoordinates(int dimensionOffset) {
    double[][] ring = {
      coordinateWithDims(dimensionOffset, 2, 1),
      coordinateWithDims(dimensionOffset, 9, 1),
      coordinateWithDims(dimensionOffset, 2, 6),
      coordinateWithDims(dimensionOffset, 2, 1)
    };

    XYAccumulator acc = accumulate(polygonWithDims(ByteOrder.LITTLE_ENDIAN, dimensionOffset, ring));

    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(2, 1));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(9, 6));
  }

  @ParameterizedTest
  @ValueSource(ints = {1000, 2000, 3000})
  void dimensionedCollectionsSkipExtraCoordinates(int dimensionOffset) {
    byte[] point =
        pointWithDims(
            ByteOrder.LITTLE_ENDIAN, dimensionOffset, 1, 2, extraDimensions(dimensionOffset));
    byte[] line =
        lineStringWithDims(
            ByteOrder.LITTLE_ENDIAN,
            dimensionOffset,
            new double[][] {
              coordinateWithDims(dimensionOffset, -3, 4), coordinateWithDims(dimensionOffset, 5, -6)
            });
    byte[] polygon =
        polygonWithDims(
            ByteOrder.LITTLE_ENDIAN,
            dimensionOffset,
            new double[][] {
              coordinateWithDims(dimensionOffset, 7, 8),
              coordinateWithDims(dimensionOffset, 10, 8),
              coordinateWithDims(dimensionOffset, 7, 12),
              coordinateWithDims(dimensionOffset, 7, 8)
            });
    byte[] multiPoint =
        collectionWithDims(ByteOrder.LITTLE_ENDIAN, MULTI_POINT, dimensionOffset, point);
    byte[] multiLineString =
        collectionWithDims(ByteOrder.LITTLE_ENDIAN, MULTI_LINE_STRING, dimensionOffset, line);
    byte[] multiPolygon =
        collectionWithDims(ByteOrder.LITTLE_ENDIAN, MULTI_POLYGON, dimensionOffset, polygon);
    byte[] geometryCollection =
        collectionWithDims(
            ByteOrder.LITTLE_ENDIAN,
            GEOMETRY_COLLECTION,
            dimensionOffset,
            multiPoint,
            multiLineString,
            multiPolygon);

    XYAccumulator acc = accumulate(geometryCollection);

    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-3, -6));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(10, 12));
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
  void emptyPolygonContributesNothing() {
    XYAccumulator acc = accumulate(polygon(ByteOrder.LITTLE_ENDIAN));
    assertThat(acc.hasBounds()).isFalse();
    assertThat(acc.minBound()).isNull();
    assertThat(acc.maxBound()).isNull();
  }

  @Test
  void emptyCollectionsContributeNothing() {
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            GEOMETRY_COLLECTION,
            collection(ByteOrder.LITTLE_ENDIAN, MULTI_POINT),
            collection(ByteOrder.LITTLE_ENDIAN, MULTI_LINE_STRING),
            collection(ByteOrder.LITTLE_ENDIAN, MULTI_POLYGON),
            collection(ByteOrder.LITTLE_ENDIAN, GEOMETRY_COLLECTION));

    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.hasBounds()).isFalse();
    assertThat(acc.minBound()).isNull();
    assertThat(acc.maxBound()).isNull();
  }

  @Test
  void emptyChildrenDoNotHideNonEmptyBounds() {
    byte[] multiPoint =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            MULTI_POINT,
            point(ByteOrder.LITTLE_ENDIAN, Double.NaN, Double.NaN),
            point(ByteOrder.LITTLE_ENDIAN, 3, 4));
    byte[] multiLineString =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            MULTI_LINE_STRING,
            lineString(ByteOrder.LITTLE_ENDIAN, new double[0][]),
            lineString(ByteOrder.LITTLE_ENDIAN, new double[][] {{-5, 6}, {7, -8}}));
    byte[] multiPolygon =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            MULTI_POLYGON,
            polygon(ByteOrder.LITTLE_ENDIAN),
            polygon(
                ByteOrder.LITTLE_ENDIAN, new double[][] {{10, 10}, {12, 10}, {10, 13}, {10, 10}}));
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            GEOMETRY_COLLECTION,
            multiPoint,
            multiLineString,
            multiPolygon);

    XYAccumulator acc = accumulate(wkb);

    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(-5, -8));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(12, 13));
  }

  @Test
  void polygonWithInteriorRing() {
    double[][] outerRing = {{0, 0}, {10, 0}, {0, 10}, {0, 0}};
    double[][] innerRing = {{1, 1}, {1, 2}, {2, 1}, {1, 1}};
    byte[] polygon = polygon(ByteOrder.LITTLE_ENDIAN, outerRing, innerRing);
    byte[] wkb =
        collection(
            ByteOrder.LITTLE_ENDIAN,
            GEOMETRY_COLLECTION,
            polygon,
            point(ByteOrder.LITTLE_ENDIAN, 20, -5));

    XYAccumulator acc = accumulate(wkb);
    assertThat(acc.minBound()).isEqualTo(GeospatialBound.createXY(0, -5));
    assertThat(acc.maxBound()).isEqualTo(GeospatialBound.createXY(20, 10));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("degeneratePolygonCases")
  void degeneratePolygonBounds(
      String description,
      double[][] ring,
      GeospatialBound expectedMin,
      GeospatialBound expectedMax) {
    XYAccumulator acc = accumulate(polygon(ByteOrder.LITTLE_ENDIAN, ring));

    assertThat(acc.minBound()).isEqualTo(expectedMin);
    assertThat(acc.maxBound()).isEqualTo(expectedMax);
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

  private static Stream<Arguments> degeneratePolygonCases() {
    return Stream.of(
        Arguments.of(
            "point-degenerate ring",
            new double[][] {{4, -2}, {4, -2}, {4, -2}, {4, -2}},
            GeospatialBound.createXY(4, -2),
            GeospatialBound.createXY(4, -2)),
        Arguments.of(
            "vertical ring",
            new double[][] {{3, -2}, {3, 7}, {3, 1}, {3, -2}},
            GeospatialBound.createXY(3, -2),
            GeospatialBound.createXY(3, 7)),
        Arguments.of(
            "horizontal ring",
            new double[][] {{-9, -4}, {8, -4}, {2, -4}, {-9, -4}},
            GeospatialBound.createXY(-9, -4),
            GeospatialBound.createXY(8, -4)),
        Arguments.of(
            "clockwise ring",
            new double[][] {{0, 0}, {0, 11}, {13, 0}, {0, 0}},
            GeospatialBound.createXY(0, 0),
            GeospatialBound.createXY(13, 11)),
        Arguments.of(
            "counterclockwise ring",
            new double[][] {{0, 0}, {13, 0}, {0, 11}, {0, 0}},
            GeospatialBound.createXY(0, 0),
            GeospatialBound.createXY(13, 11)),
        Arguments.of(
            "repeated vertices",
            new double[][] {{0, 0}, {0, 0}, {6, 0}, {4, 5}, {0, 0}, {0, 0}},
            GeospatialBound.createXY(0, 0),
            GeospatialBound.createXY(6, 5)));
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
    return lineStringWithDims(order, 0, coords);
  }

  private static byte[] lineStringWithDims(
      ByteOrder order, int dimensionOffset, double[][] coords) {
    int dimensions = dimensions(dimensionOffset);
    ByteBuffer buffer =
        ByteBuffer.allocate(5 + Integer.BYTES + coords.length * dimensions * Double.BYTES)
            .order(order)
            .put(orderFlag(order))
            .putInt(LINE_STRING + dimensionOffset)
            .putInt(coords.length);
    for (double[] coord : coords) {
      for (int i = 0; i < dimensions; i += 1) {
        buffer.putDouble(coord[i]);
      }
    }

    return buffer.array();
  }

  private static byte[] polygon(ByteOrder order, double[][]... rings) {
    return polygonWithDims(order, 0, rings);
  }

  private static byte[] polygonWithDims(ByteOrder order, int dimensionOffset, double[][]... rings) {
    int dimensions = dimensions(dimensionOffset);
    int size = 5 + Integer.BYTES;
    for (double[][] ring : rings) {
      size += Integer.BYTES + ring.length * dimensions * Double.BYTES;
    }

    ByteBuffer buffer =
        ByteBuffer.allocate(size)
            .order(order)
            .put(orderFlag(order))
            .putInt(POLYGON + dimensionOffset)
            .putInt(rings.length);
    for (double[][] ring : rings) {
      buffer.putInt(ring.length);
      for (double[] coord : ring) {
        for (int i = 0; i < dimensions; i += 1) {
          buffer.putDouble(coord[i]);
        }
      }
    }

    return buffer.array();
  }

  private static byte[] collection(ByteOrder order, int type, byte[]... children) {
    return collectionWithDims(order, type, 0, children);
  }

  private static byte[] collectionWithDims(
      ByteOrder order, int type, int dimensionOffset, byte[]... children) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] header =
        ByteBuffer.allocate(5 + Integer.BYTES)
            .order(order)
            .put(orderFlag(order))
            .putInt(type + dimensionOffset)
            .putInt(children.length)
            .array();
    out.writeBytes(header);
    for (byte[] child : children) {
      out.writeBytes(child);
    }
    return out.toByteArray();
  }

  private static double[] coordinateWithDims(int dimensionOffset, double xCoord, double yCoord) {
    double[] extraDimensions = extraDimensions(dimensionOffset);
    double[] coordinate = new double[2 + extraDimensions.length];
    coordinate[0] = xCoord;
    coordinate[1] = yCoord;
    System.arraycopy(extraDimensions, 0, coordinate, 2, extraDimensions.length);
    return coordinate;
  }

  private static double[] extraDimensions(int dimensionOffset) {
    switch (dimensionOffset) {
      case 0:
        return new double[0];
      case 1000:
        return new double[] {17};
      case 2000:
        return new double[] {23};
      case 3000:
        return new double[] {17, 23};
      default:
        throw new IllegalArgumentException("Invalid dimension offset: " + dimensionOffset);
    }
  }

  private static int dimensions(int dimensionOffset) {
    return 2 + extraDimensions(dimensionOffset).length;
  }

  private static byte orderFlag(ByteOrder order) {
    return (byte) (order == ByteOrder.LITTLE_ENDIAN ? 1 : 0);
  }
}
