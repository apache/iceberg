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
import org.locationtech.jts.geom.CoordinateFilter;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Iceberg internally tracked field level metrics, used by Parquet and ORC writers only.
 *
 * <p>Bounding box of geometry fields were tracked and recorded in both manifest file and parquet
 * statistics, which can help us to prune data files disjoint with the query window.
 */
public class GeometryFieldMetrics extends FieldMetrics<Geometry> {

  private static final GeometryFactory FACTORY = new GeometryFactory();

  private GeometryFieldMetrics(int id, long valueCount, Geometry min, Geometry max) {
    super(id, valueCount, 0L, 0L, min, max);
  }

  /** Builder for tracking bounding box of geometries */
  public static class Builder {
    private final int id;
    private long valueCount = 0;
    private long nonEmptyValueCount = 0;
    private double xMin = Double.POSITIVE_INFINITY;
    private double xMax = Double.NEGATIVE_INFINITY;
    private double yMin = Double.POSITIVE_INFINITY;
    private double yMax = Double.NEGATIVE_INFINITY;
    private double zMin = Double.POSITIVE_INFINITY;
    private double zMax = Double.NEGATIVE_INFINITY;
    private double mMin = Double.POSITIVE_INFINITY;
    private double mMax = Double.NEGATIVE_INFINITY;

    public Builder(int id) {
      this.id = id;
    }

    public void add(Geometry geom) {
      this.valueCount++;
      GeometryBoundExtractor boundExtractor = new GeometryBoundExtractor();
      geom.apply(boundExtractor);
      if (boundExtractor.isValid()) {
        addEnvelope(
            boundExtractor.xMin,
            boundExtractor.yMin,
            boundExtractor.xMax,
            boundExtractor.yMax,
            boundExtractor.zMin,
            boundExtractor.zMax,
            boundExtractor.mMin,
            boundExtractor.mMax);
      }
    }

    protected void addEnvelope(
        double minX,
        double minY,
        double maxX,
        double maxY,
        double minZ,
        double maxZ,
        double minM,
        double maxM) {
      nonEmptyValueCount++;
      this.xMin = Math.min(minX, this.xMin);
      this.yMin = Math.min(minY, this.yMin);
      this.xMax = Math.max(maxX, this.xMax);
      this.yMax = Math.max(maxY, this.yMax);
      if (minZ <= maxZ) {
        this.zMin = Math.min(minZ, this.zMin);
        this.zMax = Math.max(maxZ, this.zMax);
      }
      if (minM <= maxM) {
        this.mMin = Math.min(minM, this.mMin);
        this.mMax = Math.max(maxM, this.mMax);
      }
    }

    public GeometryFieldMetrics build() {
      boolean hasBound = nonEmptyValueCount > 0;
      return new GeometryFieldMetrics(
          id,
          valueCount,
          hasBound ? FACTORY.createPoint(new CoordinateXYZM(xMin, yMin, zMin, mMin)) : null,
          hasBound ? FACTORY.createPoint(new CoordinateXYZM(xMax, yMax, zMax, mMax)) : null);
    }
  }

  private static class GeometryBoundExtractor implements CoordinateFilter {
    private double xMin = Double.POSITIVE_INFINITY;
    private double xMax = Double.NEGATIVE_INFINITY;
    private double yMin = Double.POSITIVE_INFINITY;
    private double yMax = Double.NEGATIVE_INFINITY;
    private double zMin = Double.POSITIVE_INFINITY;
    private double zMax = Double.NEGATIVE_INFINITY;
    private double mMin = Double.POSITIVE_INFINITY;
    private double mMax = Double.NEGATIVE_INFINITY;

    public boolean isValid() {
      return xMin <= xMax && yMin <= yMax;
    }

    @Override
    @SuppressWarnings("LocalVariableName")
    public void filter(Coordinate coord) {
      double x = coord.getX();
      double y = coord.getY();
      double z = coord.getZ();
      double m = coord.getM();
      xMin = Math.min(x, xMin);
      yMin = Math.min(y, yMin);
      xMax = Math.max(x, xMax);
      yMax = Math.max(y, yMax);
      if (!Double.isNaN(z)) {
        zMin = Math.min(z, zMin);
        zMax = Math.max(z, zMax);
      }
      if (!Double.isNaN(m)) {
        mMin = Math.min(m, mMin);
        mMax = Math.max(m, mMax);
      }
    }
  }
}
