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
package org.apache.iceberg.metrics;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.MetricsContext.Counter;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** A Table Scan report that contains all relevant information from a Table Scan. */
public class ScanReport implements Serializable {

  private final String tableName;
  private final long snapshotId;
  private final Expression filter;
  private final Schema projection;
  private final ScanMetrics scanMetrics;

  private ScanReport(
      String tableName,
      long snapshotId,
      Expression filter,
      Schema projection,
      ScanMetrics scanMetrics) {
    this.tableName = tableName;
    this.snapshotId = snapshotId;
    this.filter = filter;
    this.projection = projection;
    this.scanMetrics = scanMetrics;
  }

  public String tableName() {
    return tableName;
  }

  public long snapshotId() {
    return snapshotId;
  }

  public Expression filter() {
    return filter;
  }

  public Schema projection() {
    return projection;
  }

  public ScanMetrics scanMetrics() {
    return scanMetrics;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tableName", tableName)
        .add("snapshotId", snapshotId)
        .add("filter", filter)
        .add("projection", projection)
        .add("scanMetrics", scanMetrics)
        .toString();
  }

  public static class Builder {
    private String tableName;
    private long snapshotId = -1L;
    private Expression filter;
    private Schema projection;
    private ScanMetrics scanMetrics;

    private Builder() {}

    public Builder withTableName(String newTableName) {
      this.tableName = newTableName;
      return this;
    }

    public Builder withSnapshotId(long newSnapshotId) {
      this.snapshotId = newSnapshotId;
      return this;
    }

    public Builder withFilter(Expression newFilter) {
      this.filter = newFilter;
      return this;
    }

    public Builder withProjection(Schema newProjection) {
      this.projection = newProjection;
      return this;
    }

    public Builder fromScanMetrics(ScanMetrics newScanMetrics) {
      this.scanMetrics = newScanMetrics;
      return this;
    }

    public ScanReport build() {
      Preconditions.checkArgument(null != tableName, "Invalid table name: null");
      Preconditions.checkArgument(null != filter, "Invalid expression filter: null");
      Preconditions.checkArgument(null != projection, "Invalid schema projection: null");
      Preconditions.checkArgument(null != scanMetrics, "Invalid scan metrics: null");
      return new ScanReport(tableName, snapshotId, filter, projection, scanMetrics);
    }
  }

  /** Carries all metrics for a particular scan */
  public static class ScanMetrics {
    public static final ScanMetrics NOOP = new ScanMetrics(MetricsContext.nullMetrics());
    private final Timer totalPlanningDuration;
    private final Counter<Integer> resultDataFiles;
    private final Counter<Integer> resultDeleteFiles;
    private final Counter<Integer> totalDataManifests;
    private final Counter<Integer> totalDeleteManifests;
    private final Counter<Integer> scannedDataManifests;
    private final Counter<Integer> skippedDataManifests;
    private final Counter<Long> totalFileSizeInBytes;
    private final Counter<Long> totalDeleteFileSizeInBytes;

    public ScanMetrics(MetricsContext metricsContext) {
      Preconditions.checkArgument(null != metricsContext, "Invalid metrics context: null");
      this.totalPlanningDuration =
          metricsContext.timer("totalPlanningDuration", TimeUnit.NANOSECONDS);
      this.resultDataFiles =
          metricsContext.counter("resultDataFiles", Integer.class, MetricsContext.Unit.COUNT);
      this.resultDeleteFiles =
          metricsContext.counter("resultDeleteFiles", Integer.class, MetricsContext.Unit.COUNT);
      this.scannedDataManifests =
          metricsContext.counter("scannedDataManifests", Integer.class, MetricsContext.Unit.COUNT);
      this.totalDataManifests =
          metricsContext.counter("totalDataManifests", Integer.class, MetricsContext.Unit.COUNT);
      this.totalDeleteManifests =
          metricsContext.counter("totalDeleteManifests", Integer.class, MetricsContext.Unit.COUNT);
      this.totalFileSizeInBytes =
          metricsContext.counter("totalFileSizeInBytes", Long.class, MetricsContext.Unit.BYTES);
      this.totalDeleteFileSizeInBytes =
          metricsContext.counter(
              "totalDeleteFileSizeInBytes", Long.class, MetricsContext.Unit.BYTES);
      this.skippedDataManifests =
          metricsContext.counter("skippedDataManifests", Integer.class, MetricsContext.Unit.COUNT);
    }

    public Timer totalPlanningDuration() {
      return totalPlanningDuration;
    }

    public Counter<Integer> resultDataFiles() {
      return resultDataFiles;
    }

    public Counter<Integer> resultDeleteFiles() {
      return resultDeleteFiles;
    }

    public Counter<Integer> scannedDataManifests() {
      return scannedDataManifests;
    }

    public Counter<Integer> totalDataManifests() {
      return totalDataManifests;
    }

    public Counter<Integer> totalDeleteManifests() {
      return totalDeleteManifests;
    }

    public Counter<Long> totalFileSizeInBytes() {
      return totalFileSizeInBytes;
    }

    public Counter<Long> totalDeleteFileSizeInBytes() {
      return totalDeleteFileSizeInBytes;
    }

    public Counter<Integer> skippedDataManifests() {
      return skippedDataManifests;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .addValue(totalPlanningDuration)
          .addValue(resultDataFiles)
          .addValue(resultDeleteFiles)
          .addValue(scannedDataManifests)
          .addValue(skippedDataManifests)
          .addValue(totalDataManifests)
          .addValue(totalDeleteManifests)
          .addValue(totalFileSizeInBytes)
          .addValue(totalDeleteFileSizeInBytes)
          .toString();
    }
  }
}
