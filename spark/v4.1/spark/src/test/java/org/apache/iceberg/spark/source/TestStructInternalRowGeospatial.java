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
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.catalyst.util.STUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.GeographyVal;
import org.apache.spark.unsafe.types.GeometryVal;
import org.junit.jupiter.api.Test;

public class TestStructInternalRowGeospatial {

  @Test
  public void convertsGeospatialArrays() {
    Types.GeometryType geometryType = Types.GeometryType.of("EPSG:3857");
    Types.GeographyType geographyType = Types.GeographyType.crs84();
    Types.StructType structType =
        Types.StructType.of(
            Types.NestedField.required(1, "geometries", Types.ListType.ofOptional(2, geometryType)),
            Types.NestedField.required(
                3, "geographies", Types.ListType.ofOptional(4, geographyType)));

    byte[] point = pointWkb(30.0, 10.0);
    byte[] lineString = lineStringWkb(30.0, 10.0, 40.0, 20.0);
    GenericRecord record = GenericRecord.create(structType);
    record.set(0, Arrays.asList(ByteBuffer.wrap(point), lineString, null));
    record.set(1, Arrays.asList(lineString, ByteBuffer.wrap(point), null));

    InternalRow row = new StructInternalRow(structType).setStruct(record);
    DataType sparkGeometryType = SparkSchemaUtil.convert(geometryType);
    DataType sparkGeographyType = SparkSchemaUtil.convert(geographyType);

    ArrayData geometries = row.getArray(0);
    assertGeometry((GeometryVal) geometries.get(0, sparkGeometryType), 3857, point);
    assertGeometry((GeometryVal) geometries.get(1, sparkGeometryType), 3857, lineString);
    assertThat(geometries.isNullAt(2)).isTrue();

    ArrayData geographies = row.getArray(1);
    assertGeography((GeographyVal) geographies.get(0, sparkGeographyType), lineString);
    assertGeography((GeographyVal) geographies.get(1, sparkGeographyType), point);
    assertThat(geographies.isNullAt(2)).isTrue();
  }

  @Test
  public void convertsGeospatialMapKeysAndValues() {
    Types.GeometryType geometryType = Types.GeometryType.of("EPSG:3857");
    Types.GeographyType geographyType = Types.GeographyType.crs84();
    Types.MapType mapType = Types.MapType.ofOptional(2, 3, geometryType, geographyType);
    Types.StructType structType =
        Types.StructType.of(Types.NestedField.required(1, "locations", mapType));

    byte[] firstGeometry = pointWkb(30.0, 10.0);
    byte[] secondGeometry = lineStringWkb(30.0, 10.0, 40.0, 20.0);
    byte[] geography = pointWkb(-71.0, 42.0);
    Map<Object, Object> locations = new LinkedHashMap<>();
    locations.put(ByteBuffer.wrap(firstGeometry), geography);
    locations.put(secondGeometry, null);

    GenericRecord record = GenericRecord.create(structType);
    record.set(0, locations);

    MapData map = new StructInternalRow(structType).setStruct(record).getMap(0);
    DataType sparkGeometryType = SparkSchemaUtil.convert(geometryType);
    DataType sparkGeographyType = SparkSchemaUtil.convert(geographyType);
    assertGeometry((GeometryVal) map.keyArray().get(0, sparkGeometryType), 3857, firstGeometry);
    assertGeometry((GeometryVal) map.keyArray().get(1, sparkGeometryType), 3857, secondGeometry);
    assertGeography((GeographyVal) map.valueArray().get(0, sparkGeographyType), geography);
    assertThat(map.valueArray().isNullAt(1)).isTrue();
  }

  @Test
  public void convertsGeospatialValuesInStructList() {
    Types.GeometryType geometryType = Types.GeometryType.of("EPSG:3857");
    Types.GeographyType geographyType = Types.GeographyType.crs84();
    Types.StructType locationType =
        Types.StructType.of(
            Types.NestedField.required(3, "geometry", geometryType),
            Types.NestedField.required(4, "geography", geographyType));
    Types.StructType structType =
        Types.StructType.of(
            Types.NestedField.required(1, "locations", Types.ListType.ofOptional(2, locationType)));

    byte[] geometry = lineStringWkb(30.0, 10.0, 40.0, 20.0);
    byte[] geography = pointWkb(-71.0, 42.0);
    GenericRecord location = GenericRecord.create(locationType);
    location.set(0, ByteBuffer.wrap(geometry));
    location.set(1, geography);
    GenericRecord record = GenericRecord.create(structType);
    record.set(0, Arrays.asList(location, null));

    ArrayData locations = new StructInternalRow(structType).setStruct(record).getArray(0);
    InternalRow converted = locations.getStruct(0, locationType.fields().size());
    assertGeometry(converted.getGeometry(0), 3857, geometry);
    assertGeography(converted.getGeography(1), geography);
    assertThat(locations.isNullAt(1)).isTrue();
  }

  private static void assertGeometry(GeometryVal geometry, int srid, byte[] wkb) {
    assertThat(STUtils.stSrid(geometry)).isEqualTo(srid);
    assertThat(STUtils.stAsBinary(geometry)).isEqualTo(wkb);
  }

  private static void assertGeography(GeographyVal geography, byte[] wkb) {
    assertThat(STUtils.stSrid(geography)).isEqualTo(4326);
    assertThat(STUtils.stAsBinary(geography)).isEqualTo(wkb);
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

  private static byte[] lineStringWkb(
      double firstX, double firstY, double secondX, double secondY) {
    return ByteBuffer.allocate(41)
        .order(ByteOrder.LITTLE_ENDIAN)
        .put((byte) 1)
        .putInt(2)
        .putInt(2)
        .putDouble(firstX)
        .putDouble(firstY)
        .putDouble(secondX)
        .putDouble(secondY)
        .array();
  }
}
