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
package org.apache.iceberg.expressions;

import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Demonstrates end-to-end file pruning driven by a spatial predicate: an {@code ST_INTERSECTS}
 * {@link Expression} is bound and evaluated by {@link InclusiveMetricsEvaluator} against a file's
 * stored geometry bounds, skipping files whose bounding box is disjoint from the query window.
 */
public class TestSpatialMetricsEvaluator {

  private static final int GEOM_ID = 2;
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(GEOM_ID, "geom", Types.GeometryType.crs84()));

  // File A holds geometries within (10,10)-(20,20); file B within (110,60)-(120,70).
  private static final DataFile FILE_A = geoFile("a.parquet", 10, 10, 20, 20);
  private static final DataFile FILE_B = geoFile("b.parquet", 110, 60, 120, 70);

  @Test
  public void testSpatialPredicatePrunesDisjointFile() {
    // A query window over (5,5)-(25,25) overlaps file A but is disjoint from file B.
    Expression query = Expressions.stIntersects("geom", box(5, 5, 25, 25));

    assertThat(new InclusiveMetricsEvaluator(SCHEMA, query).eval(FILE_A))
        .as("file A overlaps the query window and must be scanned")
        .isTrue();
    assertThat(new InclusiveMetricsEvaluator(SCHEMA, query).eval(FILE_B))
        .as("file B is disjoint from the query window and can be skipped")
        .isFalse();
  }

  @Test
  public void testSpatialPredicateKeepsOverlappingFiles() {
    // A query window over the northeast cluster overlaps file B but not file A.
    Expression query = Expressions.stIntersects("geom", box(100, 55, 130, 75));

    assertThat(new InclusiveMetricsEvaluator(SCHEMA, query).eval(FILE_A)).isFalse();
    assertThat(new InclusiveMetricsEvaluator(SCHEMA, query).eval(FILE_B)).isTrue();
  }

  private static DataFile geoFile(String path, double minX, double minY, double maxX, double maxY) {
    Types.GeometryType geom = Types.GeometryType.crs84();
    Map<Integer, ByteBuffer> lower =
        ImmutableMap.of(GEOM_ID, toByteBuffer(geom, GeospatialBound.createXY(minX, minY)));
    Map<Integer, ByteBuffer> upper =
        ImmutableMap.of(GEOM_ID, toByteBuffer(geom, GeospatialBound.createXY(maxX, maxY)));
    return new TestDataFile(
        path,
        Row.of(),
        50,
        ImmutableMap.of(GEOM_ID, 50L), // value counts
        ImmutableMap.of(GEOM_ID, 0L), // null value counts
        null, // nan value counts
        lower,
        upper);
  }

  private static BoundingBox box(double minX, double minY, double maxX, double maxY) {
    return new BoundingBox(
        GeospatialBound.createXY(minX, minY), GeospatialBound.createXY(maxX, maxY));
  }
}
