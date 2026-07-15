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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.STUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.BinaryView;
import org.junit.jupiter.api.Test;

public class TestStructInternalRowGeospatial {

  @Test
  public void convertsIcebergWkbToSparkPhysicalValues() {
    Types.GeometryType geometryType = Types.GeometryType.of("EPSG:3857");
    Types.GeographyType geographyType = Types.GeographyType.crs84();
    Types.StructType structType =
        Types.StructType.of(
            Types.NestedField.required(1, "geom", geometryType),
            Types.NestedField.required(2, "geog", geographyType));
    byte[] geomWkb = pointWkb(30.0, 10.0);
    byte[] geogWkb = pointWkb(-71.0, 42.0);
    GenericRecord record = GenericRecord.create(structType);
    record.set(0, ByteBuffer.wrap(geomWkb));
    record.set(1, ByteBuffer.wrap(geogWkb));

    InternalRow row = new StructInternalRow(structType).setStruct(record);
    assertGeometry(row.getBinaryView(0), 3857, geomWkb);
    assertGeography(row.getBinaryView(1), 4326, geogWkb);

    DataType sparkGeometryType = SparkSchemaUtil.convert(geometryType);
    DataType sparkGeographyType = SparkSchemaUtil.convert(geographyType);
    assertGeometry((BinaryView) row.get(0, sparkGeometryType), 3857, geomWkb);
    assertGeography((BinaryView) row.get(1, sparkGeographyType), 4326, geogWkb);
  }

  @Test
  public void convertsNestedIcebergWkbToSparkPhysicalValues() {
    Types.GeometryType geometryType = Types.GeometryType.of("EPSG:3857");
    Types.ListType listType = Types.ListType.ofRequired(2, geometryType);
    Types.StructType structType =
        Types.StructType.of(Types.NestedField.required(1, "geometries", listType));
    byte[] firstWkb = pointWkb(30.0, 10.0);
    byte[] secondWkb = pointWkb(40.0, 20.0);
    GenericRecord record = GenericRecord.create(structType);
    record.set(0, Arrays.asList(ByteBuffer.wrap(firstWkb), ByteBuffer.wrap(secondWkb)));

    InternalRow row = new StructInternalRow(structType).setStruct(record);
    ArrayData geometries = row.getArray(0);
    DataType sparkGeometryType = SparkSchemaUtil.convert(geometryType);
    assertGeometry((BinaryView) geometries.get(0, sparkGeometryType), 3857, firstWkb);
    assertGeometry((BinaryView) geometries.get(1, sparkGeometryType), 3857, secondWkb);
  }

  private static void assertGeometry(BinaryView geometry, int srid, byte[] wkb) {
    assertThat(STUtils.stGeomSrid(geometry)).isEqualTo(srid);
    assertThat(STUtils.stGeomAsBinary(geometry)).isEqualTo(wkb);
  }

  private static void assertGeography(BinaryView geography, int srid, byte[] wkb) {
    assertThat(STUtils.stGeogSrid(geography)).isEqualTo(srid);
    assertThat(STUtils.stGeogAsBinary(geography)).isEqualTo(wkb);
  }

  private static byte[] pointWkb(double xCoordinate, double yCoordinate) {
    return ByteBuffer.allocate(21)
        .order(ByteOrder.LITTLE_ENDIAN)
        .put((byte) 1)
        .putInt(1)
        .putDouble(xCoordinate)
        .putDouble(yCoordinate)
        .array();
  }
}
