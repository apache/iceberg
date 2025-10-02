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
package org.apache.iceberg.data.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

public class TestGeospatialMetricsRowGroupFilter {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "geom", Types.GeometryType.crs84()));

  @TempDir private File tempDir;

  private static class ParquetFileMetadata {
    BlockMetaData blockMetaData;
    MessageType schema;
  }

  private ParquetFileMetadata nonEmptyBlockMetadata;
  private ParquetFileMetadata emptyBlockMetadata;
  private ParquetFileMetadata nullBlockMetadata;

  @BeforeEach
  public void createTestFiles() throws IOException {
    File nonEmptyFile = new File(tempDir, "test_file_non_empty.parquet");
    File emptyFile = new File(tempDir, "test_file_empty.parquet");
    File nullFile = new File(tempDir, "test_file_null.parquet");

    GeometryFactory factory = new GeometryFactory();
    WKBWriter wkbWriter = new WKBWriter();

    // Create test files with different geometries
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", 1);
    Geometry polygon = factory.toGeometry(new Envelope(1, 2, 3, 4));
    byte[] polygonWkb = wkbWriter.write(polygon);

    record.setField("geom", ByteBuffer.wrap(polygonWkb));
    nonEmptyBlockMetadata = createFileWithRecord(nonEmptyFile, record);

    byte[] emptyLineString = wkbWriter.write(factory.createLineString());
    record.setField("geom", ByteBuffer.wrap(emptyLineString));
    emptyBlockMetadata = createFileWithRecord(emptyFile, record);

    record.setField("geom", null);
    nullBlockMetadata = createFileWithRecord(nullFile, record);
  }

  private ParquetFileMetadata createFileWithRecord(File file, GenericRecord record)
      throws IOException {
    OutputFile outputFile = Files.localOutput(file);
    try (FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(fileSchema -> InternalWriter.create(SCHEMA.asStruct(), fileSchema))
            .build()) {
      appender.add(record);
    }

    LocalInputFile inFile = new LocalInputFile(file.toPath());
    try (ParquetFileReader reader = ParquetFileReader.open(inFile)) {
      assertThat(reader.getRowGroups()).as("Should create only one row group").hasSize(1);
      ParquetFileMetadata metadata = new ParquetFileMetadata();
      metadata.schema = reader.getFileMetaData().getSchema();
      metadata.blockMetaData = reader.getRowGroups().get(0);
      return metadata;
    }
  }

  @Test
  public void testHitNonEmptyFile() {
    boolean shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(1, 3, 2, 4)), nonEmptyBlockMetadata);
    assertThat(shouldRead).isTrue();

    shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(1, 2, 3, 4)), nonEmptyBlockMetadata);
    assertThat(shouldRead).isTrue();

    shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(0, 0, 5, 5)), nonEmptyBlockMetadata);
    assertThat(shouldRead).isTrue();

    shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(1, 3, 1, 3)), nonEmptyBlockMetadata);
    assertThat(shouldRead).isTrue();

    shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(2, 4, 2, 4)), nonEmptyBlockMetadata);
    assertThat(shouldRead).isTrue();
  }

  @Test
  public void testNotHitNonEmptyFile() {
    boolean shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(0, 0, 1, 1)), nonEmptyBlockMetadata);
    assertThat(shouldRead).isFalse();

    shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(4, 4, 5, 5)), nonEmptyBlockMetadata);
    assertThat(shouldRead).isFalse();
  }

  @Test
  public void testHitEmptyFile() {
    // We cannot skip row groups without geospatial bounding box.
    boolean shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(1, 3, 2, 4)), emptyBlockMetadata);
    assertThat(shouldRead).isTrue();

    shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(1, 2, 3, 4)), emptyBlockMetadata);
    assertThat(shouldRead).isTrue();
  }

  @Test
  public void testNotHitNullFile() {
    boolean shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(1, 3, 2, 4)), nullBlockMetadata);
    assertThat(shouldRead).isFalse();

    shouldRead =
        shouldReadParquet(
            Expressions.stIntersects("geom", createBoundingBox(1, 2, 3, 4)), nullBlockMetadata);
    assertThat(shouldRead).isFalse();
  }

  private boolean shouldReadParquet(Expression expression, ParquetFileMetadata metadata) {
    return new ParquetMetricsRowGroupFilter(SCHEMA, expression, true)
        .shouldRead(metadata.schema, metadata.blockMetaData);
  }

  private static BoundingBox createBoundingBox(double minX, double minY, double maxX, double maxY) {
    return new BoundingBox(
        GeospatialBound.createXY(minX, minY), GeospatialBound.createXY(maxX, maxY));
  }
}
