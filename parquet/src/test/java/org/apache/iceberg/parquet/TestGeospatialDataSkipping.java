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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.geospatial.GeospatialPredicateEvaluators;
import org.apache.iceberg.geospatial.GeospatialPredicateEvaluators.GeospatialPredicateEvaluator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proof of concept: the per-file geometry bounding box written into {@code lower_bounds} / {@code
 * upper_bounds} is sufficient to drive file-level data skipping.
 *
 * <p>This is a demonstration/WIP harness, not the production skipping path. It shows that a query
 * bounding box can be intersected against each file's stored geometry bbox (via the existing {@link
 * GeospatialPredicateEvaluators}) to decide whether the file can be skipped. What is still missing
 * for real pruning is (a) a spatial predicate in the {@code Expression} API to carry the query
 * bbox, and (b) wiring that intersection into {@code InclusiveMetricsEvaluator} / {@code
 * ManifestReader}. See the PR description.
 */
public class TestGeospatialDataSkipping {

  private static final int GEOM_FIELD_ID = 2;
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(GEOM_FIELD_ID, "geom", Types.GeometryType.crs84()));

  @TempDir private Path temp;

  @Test
  public void testGeometryBoundsDriveFileSkipping() throws IOException {
    // Two files with disjoint geometry clusters:
    //   file A: points around (10, 10) .. (20, 20)
    //   file B: points around (110, 60) .. (120, 70)
    DataFile fileA = writeGeometryFile(point(10, 10), point(20, 20), point(15, 12));
    DataFile fileB = writeGeometryFile(point(110, 60), point(120, 70), point(115, 65));

    BoundingBox bboxA = geometryBounds(fileA);
    BoundingBox bboxB = geometryBounds(fileB);

    // The per-file bbox reflects the data cluster (min/max of the written points).
    assertThat(bboxA.min()).isEqualTo(GeospatialBound.createXY(10, 10));
    assertThat(bboxA.max()).isEqualTo(GeospatialBound.createXY(20, 20));
    assertThat(bboxB.min()).isEqualTo(GeospatialBound.createXY(110, 60));
    assertThat(bboxB.max()).isEqualTo(GeospatialBound.createXY(120, 70));

    GeospatialPredicateEvaluator evaluator =
        GeospatialPredicateEvaluators.create(Types.GeometryType.crs84());

    // A query window over (5,5)-(25,25) overlaps file A but not file B.
    BoundingBox query = box(5, 5, 25, 25);
    assertThat(evaluator.intersects(query, bboxA))
        .as("File A overlaps the query window and must be scanned")
        .isTrue();
    assertThat(evaluator.intersects(query, bboxB))
        .as("File B is disjoint from the query window and can be skipped")
        .isFalse();

    // A query window far to the northeast overlaps file B but not file A.
    BoundingBox otherQuery = box(100, 55, 130, 75);
    assertThat(evaluator.intersects(otherQuery, bboxA)).isFalse();
    assertThat(evaluator.intersects(otherQuery, bboxB)).isTrue();
  }

  private DataFile writeGeometryFile(ByteBuffer... points) throws IOException {
    GenericRecord record = GenericRecord.create(SCHEMA);
    ImmutableList.Builder<Record> records = ImmutableList.builder();
    long id = 0;
    for (ByteBuffer wkb : points) {
      records.add(record.copy(ImmutableMap.of("id", id++, "geom", wkb)));
    }

    OutputFile file =
        Files.localOutput(java.io.File.createTempFile("geo", ".parquet", temp.toFile()));
    DataWriter<Record> dataWriter =
        Parquet.writeData(file)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try (dataWriter) {
      for (Record geoRecord : records.build()) {
        dataWriter.write(geoRecord);
      }
    }
    return dataWriter.toDataFile();
  }

  private static BoundingBox geometryBounds(DataFile file) {
    Type geomType = Types.GeometryType.crs84();
    GeospatialBound min =
        (GeospatialBound)
            Conversions.fromByteBuffer(geomType, file.lowerBounds().get(GEOM_FIELD_ID));
    GeospatialBound max =
        (GeospatialBound)
            Conversions.fromByteBuffer(geomType, file.upperBounds().get(GEOM_FIELD_ID));
    return new BoundingBox(min, max);
  }

  private static ByteBuffer point(double xCoord, double yCoord) {
    return ByteBuffer.wrap(RandomUtil.wkbPoint(xCoord, yCoord));
  }

  private static BoundingBox box(double minX, double minY, double maxX, double maxY) {
    return new BoundingBox(
        GeospatialBound.createXY(minX, minY), GeospatialBound.createXY(maxX, maxY));
  }
}
