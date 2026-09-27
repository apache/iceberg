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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.STUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSparkParquetWriter {
  @TempDir private Path temp;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "id_long", Types.LongType.get()));

  private static final Schema COMPLEX_SCHEMA =
      new Schema(
          required(1, "roots", Types.LongType.get()),
          optional(3, "lime", Types.ListType.ofRequired(4, Types.DoubleType.get())),
          required(
              5,
              "strict",
              Types.StructType.of(
                  required(9, "tangerine", Types.StringType.get()),
                  optional(
                      6,
                      "hopeful",
                      Types.StructType.of(
                          required(7, "steel", Types.FloatType.get()),
                          required(8, "lantern", Types.DateType.get()))),
                  optional(10, "vehement", Types.LongType.get()))),
          optional(
              11,
              "metamorphosis",
              Types.MapType.ofRequired(
                  12, 13, Types.StringType.get(), Types.TimestampType.withZone())),
          required(
              14,
              "winter",
              Types.ListType.ofOptional(
                  15,
                  Types.StructType.of(
                      optional(16, "beet", Types.DoubleType.get()),
                      required(17, "stamp", Types.FloatType.get()),
                      optional(18, "wheeze", Types.StringType.get())))),
          optional(
              19,
              "renovate",
              Types.MapType.ofRequired(
                  20,
                  21,
                  Types.StringType.get(),
                  Types.StructType.of(
                      optional(22, "jumpy", Types.DoubleType.get()),
                      required(23, "koala", Types.UUIDType.get()),
                      required(24, "couch rope", Types.IntegerType.get())))),
          optional(2, "slide", Types.StringType.get()));

  @Test
  public void testCorrectness() throws IOException {
    int numRows = 50_000;
    Iterable<InternalRow> records = RandomData.generateSpark(COMPLEX_SCHEMA, numRows, 19981);

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(testFile))
            .schema(COMPLEX_SCHEMA)
            .createWriterFunc(
                msgType ->
                    SparkParquetWriters.buildWriter(
                        SparkSchemaUtil.convert(COMPLEX_SCHEMA), msgType))
            .build()) {
      writer.addAll(records);
    }

    try (CloseableIterable<InternalRow> reader =
        Parquet.read(Files.localInput(testFile))
            .project(COMPLEX_SCHEMA)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(COMPLEX_SCHEMA, type))
            .build()) {
      Iterator<InternalRow> expected = records.iterator();
      Iterator<InternalRow> rows = reader.iterator();
      for (int i = 0; i < numRows; i += 1) {
        assertThat(rows).as("Should have expected number of rows").hasNext();
        TestHelpers.assertEquals(COMPLEX_SCHEMA, expected.next(), rows.next());
      }
      assertThat(rows).as("Should not have extra rows").isExhausted();
    }
  }

  @Test
  public void geospatialWriterStoresPureWkb() throws IOException {
    Schema geoSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "geom", Types.GeometryType.crs84()),
            optional(3, "geog", Types.GeographyType.crs84()));

    byte[] geomWkb = geometryCollectionWkb();
    byte[] geogWkb = pointWkb(ByteOrder.BIG_ENDIAN, -71.0, 42.0);
    InternalRow row = new GenericInternalRow(3);
    row.update(0, 1L);
    row.update(1, STUtils.stGeomFromWKB(geomWkb, 4326));
    row.update(2, STUtils.stGeogFromWKB(geogWkb, 4326));
    // second row leaves the geo columns null
    InternalRow nulls = new GenericInternalRow(3);
    nulls.update(0, 2L);

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(testFile))
            .schema(geoSchema)
            .createWriterFunc(
                msgType ->
                    SparkParquetWriters.buildWriter(SparkSchemaUtil.convert(geoSchema), msgType))
            .build()) {
      writer.add(row);
      writer.add(nulls);
    }

    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(testFile))
            .project(geoSchema)
            .createReaderFunc(type -> GenericParquetReaders.buildReader(geoSchema, type))
            .build()) {
      List<Record> rows = Lists.newArrayList(reader);
      assertThat(rows).hasSize(2);
      assertThat(bufferBytes(rows.get(0).get(1, ByteBuffer.class))).isEqualTo(geomWkb);
      assertThat(bufferBytes(rows.get(0).get(2, ByteBuffer.class))).isEqualTo(geogWkb);
      assertThat(rows.get(1).get(1)).isNull();
      assertThat(rows.get(1).get(2)).isNull();
    }
  }

  private static byte[] pointWkb(double xCoordinate, double yCoordinate) {
    return pointWkb(ByteOrder.LITTLE_ENDIAN, xCoordinate, yCoordinate);
  }

  private static byte[] pointWkb(ByteOrder byteOrder, double xCoordinate, double yCoordinate) {
    return ByteBuffer.allocate(21)
        .order(byteOrder)
        .put(byteOrder == ByteOrder.LITTLE_ENDIAN ? (byte) 1 : (byte) 0)
        .putInt(1)
        .putDouble(xCoordinate)
        .putDouble(yCoordinate)
        .array();
  }

  private static byte[] geometryCollectionWkb() {
    return ByteBuffer.allocate(51)
        .order(ByteOrder.BIG_ENDIAN)
        .put((byte) 0)
        .putInt(7)
        .putInt(2)
        .put(pointWkb(ByteOrder.BIG_ENDIAN, 30.0, 10.0))
        .put(pointWkb(ByteOrder.LITTLE_ENDIAN, 40.0, 20.0))
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

  private static byte[] bufferBytes(ByteBuffer buffer) {
    ByteBuffer copy = buffer.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  @Test
  public void testGeospatialAvgValueSizeMetrics() throws IOException {
    Schema geoSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "geom", Types.GeometryType.crs84()),
            optional(3, "geog", Types.GeographyType.crs84()));

    // WKB payloads of 21 and 41 bytes for geometry (avg 31), 21 bytes for geography.
    byte[] geomWkbSmall = pointWkb(30.0, 10.0);
    byte[] geomWkbLarge = lineStringWkb(30.0, 10.0, 40.0, 20.0);
    byte[] geogWkb = pointWkb(-71.0, 42.0);

    InternalRow first = new GenericInternalRow(3);
    first.update(0, 1L);
    // Spark's GeometryVal/GeographyVal wrap [SRID | WKB]; build them from the pure WKB.
    first.update(1, STUtils.stGeomFromWKB(geomWkbSmall));
    first.update(2, STUtils.stGeogFromWKB(geogWkb));
    InternalRow second = new GenericInternalRow(3);
    second.update(0, 2L);
    second.update(1, STUtils.stGeomFromWKB(geomWkbLarge));
    // geography left null on the second row, so it must not affect the average.

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    MessageType parquetSchema = ParquetSchemaUtil.convert(geoSchema, "table");
    ParquetValueWriter<InternalRow> writer =
        SparkParquetWriters.buildWriter(SparkSchemaUtil.convert(geoSchema), parquetSchema);

    ColumnWriteStore columnStore = mock(ColumnWriteStore.class);
    when(columnStore.getColumnWriter(any())).thenReturn(mock(ColumnWriter.class));
    writer.setColumnStore(columnStore);
    writer.write(0, first);
    writer.write(0, second);

    Map<Integer, FieldMetrics<?>> metricsById =
        writer.metrics().collect(Collectors.toMap(FieldMetrics::id, Function.identity()));

    int geomId = fieldId(parquetSchema, "geom");
    int geogId = fieldId(parquetSchema, "geog");

    FieldMetrics<?> geomMetrics = metricsById.get(geomId);
    assertThat(geomMetrics.valueCount()).isEqualTo(2);
    assertThat(geomMetrics.nullValueCount()).isZero();
    assertThat(geomMetrics.avgValueSizeInBytes()).isEqualTo(31);

    FieldMetrics<?> geogMetrics = metricsById.get(geogId);
    assertThat(geogMetrics.valueCount()).isEqualTo(2);
    assertThat(geogMetrics.nullValueCount()).isEqualTo(1);
    assertThat(geogMetrics.avgValueSizeInBytes()).isEqualTo(21);
  }

  private static int fieldId(MessageType parquetSchema, String column) {
    return parquetSchema
        .getColumnDescription(new String[] {column})
        .getPrimitiveType()
        .getId()
        .intValue();
  }

  @Test
  public void testFpp() throws IOException, NoSuchFieldException, IllegalAccessException {
    File testFile = File.createTempFile("junit", null, temp.toFile());
    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(testFile))
            .schema(SCHEMA)
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id", "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX + "id", "0.05")
            .createWriterFunc(
                msgType ->
                    SparkParquetWriters.buildWriter(SparkSchemaUtil.convert(SCHEMA), msgType))
            .build()) {
      // Using reflection to access the private 'props' field in ParquetWriter
      Field propsField = writer.getClass().getDeclaredField("props");
      propsField.setAccessible(true);
      ParquetProperties props = (ParquetProperties) propsField.get(writer);
      MessageType parquetSchema = ParquetSchemaUtil.convert(SCHEMA, "test");
      ColumnDescriptor descriptor = parquetSchema.getColumnDescription(new String[] {"id"});
      double fpp = props.getBloomFilterFPP(descriptor).getAsDouble();
      assertThat(fpp).isEqualTo(0.05);
    }
  }

  @Test
  public void testNdv() throws IOException, NoSuchFieldException, IllegalAccessException {
    final long expectedNdv = 1000;
    final String col = "id";
    File testFile = File.createTempFile("junit", null, temp.toFile());
    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(testFile))
            .schema(SCHEMA)
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + col, "true")
            .set(PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX + col, Long.toString(expectedNdv))
            .createWriterFunc(
                msgType ->
                    SparkParquetWriters.buildWriter(SparkSchemaUtil.convert(SCHEMA), msgType))
            .build()) {
      // Using reflection to access the private 'props' field in ParquetWriter
      Field propsField = writer.getClass().getDeclaredField("props");
      propsField.setAccessible(true);
      ParquetProperties props = (ParquetProperties) propsField.get(writer);
      MessageType parquetSchema = ParquetSchemaUtil.convert(SCHEMA, "test");
      ColumnDescriptor descriptor = parquetSchema.getColumnDescription(new String[] {col});
      OptionalLong bloomFilterNDV = props.getBloomFilterNDV(descriptor);
      assertThat(bloomFilterNDV).isPresent();
      assertThat(bloomFilterNDV.getAsLong()).isEqualTo(expectedNdv);
    }
  }
}
