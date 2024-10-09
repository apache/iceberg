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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Iceberg internally tracked field level metrics, used by Parquet and ORC writers only.
 *
 * <p>Bounding box of geometry fields were tracked and recorded in both manifest file and geoparquet
 * footer, which can help us prunning data files disjoint with the query window.
 */
public class GeometryFieldMetrics extends FieldMetrics<Geometry> {

  private static final GeometryFactory FACTORY = new GeometryFactory();

  private GeometryFieldMetrics(int id, long valueCount, Geometry min, Geometry max) {
    super(id, valueCount, 0L, 0L, min, max);
  }

  /**
   * Generic builder for tracking bounding box of geometries of any acceptable types, including byte
   * buffers. and jts geometry objects
   */
  public static class GenericBuilder {
    private final int id;
    private long valueCount = 0;
    private long nonEmptyValueCount = 0;
    private double xMin = Double.POSITIVE_INFINITY;
    private double xMax = Double.NEGATIVE_INFINITY;
    private double yMin = Double.POSITIVE_INFINITY;
    private double yMax = Double.NEGATIVE_INFINITY;

    public GenericBuilder(int id) {
      this.id = id;
    }

    protected void addEnvelope(double minX, double minY, double maxX, double maxY) {
      nonEmptyValueCount++;
      this.xMin = Math.min(minX, this.xMin);
      this.yMin = Math.min(minY, this.yMin);
      this.xMax = Math.max(maxX, this.xMax);
      this.yMax = Math.max(maxY, this.yMax);
    }

    public GeometryFieldMetrics build() {
      boolean hasBound = nonEmptyValueCount > 0;
      return new GeometryFieldMetrics(
          id,
          valueCount,
          hasBound ? FACTORY.createPoint(new Coordinate(xMin, yMin)) : null,
          hasBound ? FACTORY.createPoint(new Coordinate(xMax, yMax)) : null);
    }

    public void add(Geometry geom) {
      this.valueCount++;
      Envelope env = geom.getEnvelopeInternal();
      if (!env.isNull()) {
        addEnvelope(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
      }
    }

    public void add(Envelope env) {
      this.valueCount++;
      if (!env.isNull()) {
        addEnvelope(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
      }
    }
  }
}
