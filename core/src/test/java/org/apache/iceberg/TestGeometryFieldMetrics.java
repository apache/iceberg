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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestGeometryFieldMetrics {

  private static final Types.GeometryType GEOMETRY = Types.GeometryType.crs84();

  @Test
  public void testBoundsAcrossMultiplePoints() {
    GeometryFieldMetrics.Builder builder = new GeometryFieldMetrics.Builder(2, GEOMETRY);
    builder.addValue(wkbPoint(30, 10));
    builder.addValue(wkbPoint(-5, 40));

    FieldMetrics<GeospatialBound> metrics = builder.build();

    assertThat(metrics.id()).isEqualTo(2);
    assertThat(metrics.valueCount()).isEqualTo(2L);
    // null accounting is reconciled by the optional-field writer, not here
    assertThat(metrics.nullValueCount()).isEqualTo(0L);
    // geometry has no NaN metric; -1 suppresses the entry downstream
    assertThat(metrics.nanValueCount()).isEqualTo(-1L);
    assertThat(metrics.originalType()).isEqualTo(GEOMETRY);
    assertThat(metrics.lowerBound()).isEqualTo(GeospatialBound.createXY(-5, 10));
    assertThat(metrics.upperBound()).isEqualTo(GeospatialBound.createXY(30, 40));
  }

  @Test
  public void testSinglePointBoundsAreThatPoint() {
    GeometryFieldMetrics.Builder builder = new GeometryFieldMetrics.Builder(2, GEOMETRY);
    builder.addValue(wkbPoint(12, 34));

    FieldMetrics<GeospatialBound> metrics = builder.build();

    assertThat(metrics.valueCount()).isEqualTo(1L);
    assertThat(metrics.lowerBound()).isEqualTo(GeospatialBound.createXY(12, 34));
    assertThat(metrics.upperBound()).isEqualTo(GeospatialBound.createXY(12, 34));
  }

  @Test
  public void testEmptyGeometryProducesNoBounds() {
    // POINT EMPTY (NaN coordinates) still counts as a value but contributes no coordinate.
    GeometryFieldMetrics.Builder builder = new GeometryFieldMetrics.Builder(2, GEOMETRY);
    builder.addValue(wkbPoint(Double.NaN, Double.NaN));

    FieldMetrics<GeospatialBound> metrics = builder.build();

    assertThat(metrics.valueCount()).isEqualTo(1L);
    assertThat(metrics.lowerBound()).isNull();
    assertThat(metrics.upperBound()).isNull();
  }

  @Test
  public void testNoValuesProducesNoBounds() {
    FieldMetrics<GeospatialBound> metrics = new GeometryFieldMetrics.Builder(2, GEOMETRY).build();

    assertThat(metrics.valueCount()).isEqualTo(0L);
    assertThat(metrics.lowerBound()).isNull();
    assertThat(metrics.upperBound()).isNull();
  }

  private static ByteBuffer wkbPoint(double xCoord, double yCoord) {
    return ByteBuffer.allocate(21)
        .order(ByteOrder.LITTLE_ENDIAN)
        .put((byte) 1) // little-endian
        .putInt(1) // WKB geometry type: Point
        .putDouble(xCoord)
        .putDouble(yCoord)
        .flip();
  }
}
