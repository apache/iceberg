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
import java.util.stream.Stream;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestGeometryBoundsCollector {

  @ParameterizedTest(name = "{0}")
  @MethodSource("boundingBoxCases")
  void boundingBox(String wkt, String hexWkb, BoundingBox expected) {
    GeometryBoundsCollector bounds = new GeometryBoundsCollector();
    ByteBuffer wkb = decode(hexWkb);
    int position = wkb.position();
    int limit = wkb.limit();

    bounds.add(wkb);

    assertThat(wkb.position()).as(wkt).isEqualTo(position);
    assertThat(wkb.limit()).as(wkt).isEqualTo(limit);
    assertThat(bounds.boundingBox()).as(wkt).isEqualTo(expected);
  }

  @Test
  void boundsFromBufferWithOffset() {
    byte[] padded = new byte[64];
    byte[] wkb = bytes("0101000000000000000000f03f0000000000000040");
    System.arraycopy(wkb, 0, padded, 11, wkb.length);
    ByteBuffer slice = ByteBuffer.wrap(padded, 11, wkb.length).slice();

    GeometryBoundsCollector bounds = new GeometryBoundsCollector();
    bounds.add(slice);

    assertThat(slice.position()).isEqualTo(0);
    assertThat(bounds.boundingBox()).isEqualTo(box(1, 2, 1, 2));
  }

  @Test
  void noBoundsWhenOneDimensionIsMissing() {
    GeometryBoundsCollector bounds = new GeometryBoundsCollector();
    bounds.add(decode("0101000000000000000000f03f000000000000f87f"));

    assertThat(bounds.boundingBox()).as("POINT(1 NaN)").isNull();
  }

  @Test
  void boundsAcrossValuesWithMissingCoordinates() {
    GeometryBoundsCollector bounds = new GeometryBoundsCollector();
    bounds.add(decode("0101000000000000000000f03f000000000000f87f"));
    bounds.add(decode("0101000000000000000000f87f0000000000000040"));

    assertThat(bounds.boundingBox()).isEqualTo(box(1, 2, 1, 2));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("extraDimensionCases")
  void extraDimensionsAreIgnored(String description, String hexWkb, BoundingBox expected) {
    GeometryBoundsCollector bounds = new GeometryBoundsCollector();

    bounds.add(decode(hexWkb));

    assertThat(bounds.boundingBox()).as(description).isEqualTo(expected);
  }

  @Test
  void boundsAcrossValuesWithDifferentDimensions() {
    GeometryBoundsCollector bounds = new GeometryBoundsCollector();
    // POINT Z(1 2 3) then POINT ZM(1 2 3 4), which share the same XY
    bounds.add(decode("01e9030000000000000000f03f00000000000000400000000000000840"));
    bounds.add(
        decode("01b90b0000000000000000f03f000000000000004000000000000008400000000000001040"));

    assertThat(bounds.boundingBox()).isEqualTo(box(1, 2, 1, 2));
  }

  @Test
  void extraDimensionsNestedInCollectionAreIgnored() {
    GeometryBoundsCollector bounds = new GeometryBoundsCollector();

    // GEOMETRYCOLLECTION(POINT(1 2), POINT Z(3 4 5))
    bounds.add(
        decode(
            "0107000000020000000101000000000000000000f03f00000000000000400"
                + "1e9030000000000000000084000000000000010400000000000001440"));

    assertThat(bounds.boundingBox()).isEqualTo(box(1, 2, 3, 4));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalidWkbCases")
  void invalidWkb(String description, String hexWkb, String expectedMessage) {
    GeometryBoundsCollector bounds = new GeometryBoundsCollector();

    assertThatThrownBy(() -> bounds.add(decode(hexWkb)))
        .as(description)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  @Test
  void deeplyNestedWkbIsRejected() {
    GeometryBoundsCollector bounds = new GeometryBoundsCollector();

    assertThatThrownBy(() -> bounds.add(nestedCollections(200)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nesting too deep");
  }

  /** Returns WKB for a chain of geometry collections, each holding the next, around POINT(1 2). */
  private static ByteBuffer nestedCollections(int depth) {
    ByteBuffer buffer = ByteBuffer.allocate(depth * 9 + 21).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < depth; i += 1) {
      buffer.put((byte) 1);
      buffer.putInt(7);
      buffer.putInt(1);
    }

    buffer.put((byte) 1);
    buffer.putInt(1);
    buffer.putDouble(1.0);
    buffer.putDouble(2.0);
    buffer.flip();
    return buffer;
  }

  private static Stream<Arguments> boundingBoxCases() {
    return Stream.of(
        Arguments.of("POINT EMPTY", "0101000000000000000000f87f000000000000f87f", null),
        Arguments.of("POINT(1 2)", "0101000000000000000000f03f0000000000000040", box(1, 2, 1, 2)),
        Arguments.of(
            "POINT(1 2) big endian", "00000000013ff00000000000004000000000000000", box(1, 2, 1, 2)),
        Arguments.of(
            "LINESTRING(0 1,1 0,2 -1,-1 -2,0 1)",
            "0102000000050000000000000000000000000000000000f03f000000000000f03f"
                + "00000000000000000000000000000040000000000000f0bf000000000000f0bf"
                + "00000000000000c00000000000000000000000000000f03f",
            box(-1, -2, 2, 1)),
        Arguments.of(
            "POLYGON((0 0,10 0,0 10,0 0),(1 1,1 2,2 1,1 1))",
            "010300000002000000040000000000000000000000000000000000000000000000"
                + "000024400000000000000000000000000000000000000000000024400000000000"
                + "000000000000000000000004000000000000000000f03f000000000000f03f0000"
                + "00000000f03f00000000000000400000000000000040000000000000f03f000000"
                + "000000f03f000000000000f03f",
            box(0, 0, 10, 10)),
        Arguments.of(
            "MULTIPOINT((1 2),EMPTY,EMPTY,(3 4))",
            "0104000000040000000101000000000000000000f03f000000000000004001010000"
                + "00000000000000f87f000000000000f87f0101000000000000000000f87f00000000"
                + "0000f87f000000000140080000000000004010000000000000",
            box(1, 2, 3, 4)),
        Arguments.of(
            "MULTILINESTRING((1 2,3 4),(5 6,7 8))",
            "010500000002000000010200000002000000000000000000f03f0000000000000040"
                + "0000000000000840000000000000104001020000000200000000000000000014400000"
                + "0000000018400000000000001c400000000000002040",
            box(1, 2, 7, 8)),
        Arguments.of(
            "MULTIPOLYGON(EMPTY,((0 0,10 0,0 10,0 0),(1 1,1 2,2 1,1 1)))",
            "01060000000200000001030000000000000001030000000200000004000000000000000000000000000"
                + "00000000000000000000000244000000000000000000000000000000000000000000000244000000000"
                + "00000000000000000000000004000000000000000000f03f000000000000f03f000000000000f03f000"
                + "00000000000400000000000000040000000000000f03f000000000000f03f000000000000f03f",
            box(0, 0, 10, 10)),
        Arguments.of(
            "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING EMPTY,POLYGON EMPTY,"
                + "MULTIPOINT EMPTY,MULTILINESTRING EMPTY,MULTIPOLYGON EMPTY,"
                + "GEOMETRYCOLLECTION(POINT EMPTY,LINESTRING EMPTY,POLYGON EMPTY,"
                + "MULTIPOINT EMPTY,MULTILINESTRING EMPTY,MULTIPOLYGON EMPTY))",
            "0107000000070000000101000000000000000000f03f000000000000004001020000"
                + "00000000000103000000000000000104000000000000000105000000000000000106"
                + "000000000000000107000000060000000101000000000000000000f87f0000000000"
                + "00f87f01020000000000000001030000000000000001040000000000000001050000"
                + "0000000000010600000000000000",
            box(1, 2, 1, 2)));
  }

  private static Stream<Arguments> extraDimensionCases() {
    return Stream.of(
        Arguments.of(
            "POINT Z(1 2 3)",
            "01e9030000000000000000f03f00000000000000400000000000000840",
            box(1, 2, 1, 2)),
        Arguments.of(
            "POINT M(1 2 3)",
            "01d1070000000000000000f03f00000000000000400000000000000840",
            box(1, 2, 1, 2)),
        Arguments.of(
            "POINT ZM(1 2 3 4)",
            "01b90b0000000000000000f03f000000000000004000000000000008400000000000001040",
            box(1, 2, 1, 2)),
        Arguments.of(
            "LINESTRING Z(0 1 9,2 -1 9)",
            "01ea030000020000000000000000000000000000000000f03f000000000000224000"
                + "00000000000040000000000000f0bf0000000000002240",
            box(0, -1, 2, 1)));
  }

  private static Stream<Arguments> invalidWkbCases() {
    return Stream.of(
        Arguments.of(
            "trailing data", "01010000000000000000000840000000000000104000", "trailing data"),
        Arguments.of(
            "invalid multi-point child",
            "010400000001000000010200000000000000",
            "expected geometry type"),
        Arguments.of(
            "unknown geometry type",
            "010800000000000000000000000000000000000000",
            "unsupported WKB"),
        Arguments.of(
            "type code with SRID flag",
            "01ffffffff0000000000000000000000000000000000",
            "unsupported WKB"),
        Arguments.of(
            "invalid byte order", "0201000000000000000000000000000000000000", "byte order"),
        Arguments.of("truncated point", "0101000000", "unexpected end of buffer"));
  }

  private static GeospatialBound xy(double xCoord, double yCoord) {
    return GeospatialBound.createXY(xCoord, yCoord);
  }

  private static BoundingBox box(double minX, double minY, double maxX, double maxY) {
    return new BoundingBox(xy(minX, minY), xy(maxX, maxY));
  }

  private static ByteBuffer decode(String hex) {
    return ByteBuffer.wrap(bytes(hex));
  }

  private static byte[] bytes(String hex) {
    byte[] bytes = new byte[hex.length() / 2];
    for (int i = 0; i < bytes.length; i += 1) {
      int offset = i * 2;
      bytes[i] = (byte) Integer.parseInt(hex.substring(offset, offset + 2), 16);
    }

    return bytes;
  }
}
