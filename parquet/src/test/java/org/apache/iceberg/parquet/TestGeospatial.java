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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.InternalTestHelpers;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

public class TestGeospatial {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "geom", Types.GeometryType.crs84()),
          Types.NestedField.optional(3, "geog", Types.GeographyType.crs84()));

  private static final Schema SCHEMA_NON_CRS84 =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "geom_3857", Types.GeometryType.of("srid:3857")),
          Types.NestedField.optional(
              3, "geog_4269_karney", Types.GeographyType.of("srid:4269", EdgeAlgorithm.KARNEY)));

  private static final GenericRecord RECORD = GenericRecord.create(SCHEMA);
  private static final GenericRecord RECORD_NON_CRS_84 = GenericRecord.create(SCHEMA_NON_CRS84);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final WKBWriter WKB_WRITER = new WKBWriter();

  private static ByteBuffer createPointWKB(double coordX, double coordY) {
    return ByteBuffer.wrap(
        WKB_WRITER.write(GEOMETRY_FACTORY.createPoint(new Coordinate(coordX, coordY))));
  }

  private static ByteBuffer createEmptyLineStringWKB() {
    return ByteBuffer.wrap(WKB_WRITER.write(GEOMETRY_FACTORY.createLineString()));
  }

  // WKB for POINT (1 1)
  private static final ByteBuffer POINT_1_1_WKB = createPointWKB(1, 1);
  // WKB for POINT (2 3)
  private static final ByteBuffer POINT_2_3_WKB = createPointWKB(2, 3);
  // WKB for LINESTRING EMPTY
  private static final ByteBuffer LINESTRING_EMPTY_WKB = createEmptyLineStringWKB();

  @TempDir private Path temp;

  @Test
  public void testGeospatialTypes() throws IOException {
    Record record =
        RECORD.copy(
            "id", 1,
            "geom", POINT_1_1_WKB.slice(), // use slice to ensure independent buffer positions
            "geog", POINT_2_3_WKB.slice());

    Record actual = writeAndRead(SCHEMA, record);
    InternalTestHelpers.assertEquals(SCHEMA.asStruct(), record, actual);
  }

  @Test
  public void testNullGeospatialTypes() throws IOException {
    Record record =
        RECORD.copy(
            "id", 2,
            "geom", null,
            "geog", null);

    Record actual = writeAndRead(SCHEMA, record);
    InternalTestHelpers.assertEquals(SCHEMA.asStruct(), record, actual);
  }

  @Test
  public void testEmptyGeospatialTypes() throws IOException {
    Record record =
        RECORD.copy(
            "id", 3,
            "geom", LINESTRING_EMPTY_WKB.slice(),
            "geog", LINESTRING_EMPTY_WKB.slice());

    Record actual = writeAndRead(SCHEMA, record);
    InternalTestHelpers.assertEquals(SCHEMA.asStruct(), record, actual);
  }

  @Test
  public void testNonCrs84GeospatialTypes() throws IOException {
    Record record =
        RECORD_NON_CRS_84.copy(
            "id", 4,
            "geom_3857", POINT_1_1_WKB.slice(),
            "geog_4269_karney", POINT_2_3_WKB.slice());

    Record actual = writeAndRead(SCHEMA_NON_CRS84, record);
    InternalTestHelpers.assertEquals(SCHEMA_NON_CRS84.asStruct(), record, actual);
  }

  @Test
  public void testGeometryMetrics() throws IOException {
    Record record0 =
        RECORD.copy(
            "id", 1,
            "geom", POINT_1_1_WKB.slice(), // use slice to ensure independent buffer positions
            "geog", POINT_1_1_WKB.slice());
    Record record1 =
        RECORD.copy(
            "id", 1,
            "geom", POINT_2_3_WKB.slice(), // use slice to ensure independent buffer positions
            "geog", POINT_2_3_WKB.slice());
    Metrics metrics = writeAndRetrieveMetrics(SCHEMA, ImmutableList.of(record0, record1));
    ByteBuffer lowerBound = metrics.lowerBounds().get(2);
    ByteBuffer upperBound = metrics.upperBounds().get(2);
    GeospatialBound geoLowerBound = GeospatialBound.fromByteBuffer(lowerBound);
    GeospatialBound geoUpperBound = GeospatialBound.fromByteBuffer(upperBound);
    assertThat(geoLowerBound.x()).isEqualTo(1);
    assertThat(geoLowerBound.y()).isEqualTo(1);
    assertThat(geoUpperBound.x()).isEqualTo(2);
    assertThat(geoUpperBound.y()).isEqualTo(3);
  }

  @Test
  public void testEmptyGeometryMetrics() throws IOException {
    Record record =
        RECORD.copy(
            "id", 3,
            "geom", LINESTRING_EMPTY_WKB.slice(),
            "geog", LINESTRING_EMPTY_WKB.slice());
    Metrics metrics = writeAndRetrieveMetrics(SCHEMA, ImmutableList.of(record));
    ByteBuffer lowerBound = metrics.lowerBounds().get(2);
    ByteBuffer upperBound = metrics.upperBounds().get(2);
    assertThat(lowerBound).isNull();
    assertThat(upperBound).isNull();
  }

  @Test
  public void testNullGeometryMetrics() throws IOException {
    Record record =
        RECORD.copy(
            "id", 1,
            "geom", null,
            "geog", null);
    Metrics metrics = writeAndRetrieveMetrics(SCHEMA, ImmutableList.of(record));
    ByteBuffer lowerBound = metrics.lowerBounds().get(2);
    ByteBuffer upperBound = metrics.upperBounds().get(2);
    assertThat(lowerBound).isNull();
    assertThat(upperBound).isNull();
  }

  private Record writeAndRead(Schema schema, Record record) throws IOException {
    return Iterables.getOnlyElement(writeAndRead(schema, ImmutableList.of(record)));
  }

  private List<Record> writeAndRead(Schema schema, List<Record> records) throws IOException {
    OutputFile outputFile = Files.localOutput(temp.resolve("geospatial-test.parquet").toFile());

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(schema)
            .createWriterFunc(fileSchema -> InternalWriter.create(schema.asStruct(), fileSchema))
            .build()) {
      for (Record r : records) {
        writer.add(r);
      }
    }

    InputFile inputFile = outputFile.toInputFile();
    try (CloseableIterable<Record> reader =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> InternalReader.create(schema, fileSchema))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  private Metrics writeAndRetrieveMetrics(Schema schema, List<Record> records) throws IOException {
    OutputFile outputFile = Files.localOutput(temp.resolve("geospatial-test.parquet").toFile());

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(schema)
            .createWriterFunc(fileSchema -> InternalWriter.create(schema.asStruct(), fileSchema))
            .build()) {
      for (Record r : records) {
        writer.add(r);
      }
      writer.close();
      return writer.metrics();
    }
  }
}
