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

import java.nio.ByteBuffer;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.geospatial.WKBBoundingBox;
import org.apache.iceberg.types.Type;

/**
 * Field-level metrics for a geometry column, tracked by the Parquet writer.
 *
 * <p>The Parquet footer's lexicographic min/max over the WKB bytes is not meaningful for spatial
 * values, so the writer instead scans each value's coordinates and accumulates a two-dimensional
 * bounding box. The resulting {@link FieldMetrics} carries the box's lower and upper corners as
 * {@link GeospatialBound} points, which serialize through the existing geometry conversion path.
 * The null value count is left at zero here; it is reconciled by the optional-field writer that
 * owns null accounting.
 */
public class GeometryFieldMetrics extends FieldMetrics<GeospatialBound> {

  private GeometryFieldMetrics(
      int id, long valueCount, GeospatialBound lowerBound, GeospatialBound upperBound, Type type) {
    super(id, valueCount, 0L, lowerBound, upperBound, type);
  }

  public static class Builder {
    private final int id;
    private final Type geometryType;
    private final WKBBoundingBox.XYAccumulator accumulator = new WKBBoundingBox.XYAccumulator();
    private long valueCount = 0;

    public Builder(int id, Type geometryType) {
      this.id = id;
      this.geometryType = geometryType;
    }

    public void addValue(ByteBuffer wkb) {
      this.valueCount += 1;
      WKBBoundingBox.accumulate(wkb, accumulator);
    }

    public GeometryFieldMetrics build() {
      return new GeometryFieldMetrics(
          id, valueCount, accumulator.minBound(), accumulator.maxBound(), geometryType);
    }
  }
}
