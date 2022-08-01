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
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.MetricsContext.Counter;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** A Table Scan report that contains all relevant information from a Table Scan. */
public class ScanReport implements Serializable {

  private final String tableName;
  private final long snapshotId;
  // TODO: change this to Expression once we have an ExpressionParser
  private final String filter;
  private final Schema projection;
  private final ScanMetricsResult scanMetrics;

  private ScanReport(
      String tableName,
      long snapshotId,
      String filter,
      Schema projection,
      ScanMetricsResult scanMetrics) {
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

  public String filter() {
    return filter;
  }

  public Schema projection() {
    return projection;
  }

  public ScanMetricsResult scanMetrics() {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScanReport that = (ScanReport) o;
    return snapshotId == that.snapshotId
        && Objects.equal(tableName, that.tableName)
        && Objects.equal(filter, that.filter)
        && Objects.equal(projection, that.projection)
        && Objects.equal(scanMetrics, that.scanMetrics);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, snapshotId, filter, projection, scanMetrics);
  }

  public static class Builder {
    private String tableName;
    private long snapshotId = -1L;
    private String filter;
    private Schema projection;
    private ScanMetrics scanMetrics;
    private ScanMetricsResult scanMetricsResult;

    private Builder() {}

    public Builder withTableName(String newTableName) {
      this.tableName = newTableName;
      return this;
    }

    public Builder withSnapshotId(long newSnapshotId) {
      this.snapshotId = newSnapshotId;
      return this;
    }

    // TODO: remove once there's an ExpressionParser
    public Builder withFilter(String newFilter) {
      this.filter = newFilter;
      return this;
    }

    public Builder withFilter(Expression newFilter) {
      this.filter = newFilter.toString();
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

    public Builder fromScanMetricsResult(ScanMetricsResult newScanMetricsResult) {
      this.scanMetricsResult = newScanMetricsResult;
      return this;
    }

    public ScanReport build() {
      Preconditions.checkArgument(null != tableName, "Invalid table name: null");
      Preconditions.checkArgument(null != filter, "Invalid expression filter: null");
      Preconditions.checkArgument(null != projection, "Invalid schema projection: null");
      Preconditions.checkArgument(
          null != scanMetrics || null != scanMetricsResult, "Invalid scan metrics: null");
      return new ScanReport(
          tableName,
          snapshotId,
          filter,
          projection,
          null != scanMetrics ? ScanMetricsResult.fromScanMetrics(scanMetrics) : scanMetricsResult);
    }
  }

  /** A serializable version of a {@link Timer} that carries its result. */
  public static class TimerResult implements Serializable {
    private final String name;
    private final TimeUnit timeUnit;
    private final Duration totalDuration;
    private final long count;

    public TimerResult(String name, TimeUnit timeUnit, Duration totalDuration, long count) {
      Preconditions.checkArgument(null != name, "Invalid timer name: null");
      Preconditions.checkArgument(null != timeUnit, "Invalid time unit: null");
      Preconditions.checkArgument(null != totalDuration, "Invalid duration: null");
      this.name = name;
      this.timeUnit = timeUnit;
      this.totalDuration = totalDuration;
      this.count = count;
    }

    public String name() {
      return name;
    }

    public TimeUnit timeUnit() {
      return timeUnit;
    }

    public Duration totalDuration() {
      return totalDuration;
    }

    public long count() {
      return count;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(name())
          .add("duration", totalDuration())
          .add("count", count)
          .add("timeUnit", timeUnit)
          .toString();
    }

    public static TimerResult fromTimer(Timer timer) {
      Preconditions.checkArgument(null != timer, "Invalid timer: null");
      return new TimerResult(timer.name(), timer.unit(), timer.totalDuration(), timer.count());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TimerResult that = (TimerResult) o;
      return count == that.count
          && Objects.equal(name, that.name)
          && timeUnit == that.timeUnit
          && Objects.equal(totalDuration, that.totalDuration);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, timeUnit, totalDuration, count);
    }
  }

  /**
   * A serializable version of a {@link Counter} that carries its result.
   *
   * @param <T> The type of the {@link CounterResult}.
   */
  public static class CounterResult<T extends Number> implements Serializable {
    private final String name;
    private final MetricsContext.Unit unit;
    private final T value;

    public CounterResult(String name, Unit unit, T value) {
      Preconditions.checkArgument(null != name, "Invalid counter name: null");
      Preconditions.checkArgument(null != unit, "Invalid counter unit: null");
      Preconditions.checkArgument(null != value, "Invalid counter value: null");
      this.name = name;
      this.unit = unit;
      this.value = value;
    }

    public String name() {
      return name;
    }

    public Unit unit() {
      return unit;
    }

    public T value() {
      return value;
    }

    @Override
    public String toString() {
      return String.format("%s{%s=%s}", name(), unit().displayName(), value());
    }

    public static <T extends Number> CounterResult<T> fromCounter(Counter<T> counter) {
      Preconditions.checkArgument(null != counter, "Invalid counter: null");
      return new CounterResult<>(counter.name(), counter.unit(), counter.value());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CounterResult<?> that = (CounterResult<?>) o;
      return Objects.equal(name, that.name)
          && unit == that.unit
          && Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, unit, value);
    }
  }

  /** A serializable version of {@link ScanMetrics} that carries its results. */
  public static class ScanMetricsResult implements Serializable {
    private final TimerResult totalPlanningDuration;
    private final CounterResult<Integer> resultDataFiles;
    private final CounterResult<Integer> resultDeleteFiles;
    private final CounterResult<Integer> totalDataManifests;
    private final CounterResult<Integer> totalDeleteManifests;
    private final CounterResult<Integer> scannedDataManifests;
    private final CounterResult<Integer> skippedDataManifests;
    private final CounterResult<Long> totalFileSizeInBytes;
    private final CounterResult<Long> totalDeleteFileSizeInBytes;

    public ScanMetricsResult(
        TimerResult totalPlanningDuration,
        CounterResult<Integer> resultDataFiles,
        CounterResult<Integer> resultDeleteFiles,
        CounterResult<Integer> totalDataManifests,
        CounterResult<Integer> totalDeleteManifests,
        CounterResult<Integer> scannedDataManifests,
        CounterResult<Integer> skippedDataManifests,
        CounterResult<Long> totalFileSizeInBytes,
        CounterResult<Long> totalDeleteFileSizeInBytes) {
      this.totalPlanningDuration = totalPlanningDuration;
      this.resultDataFiles = resultDataFiles;
      this.resultDeleteFiles = resultDeleteFiles;
      this.totalDataManifests = totalDataManifests;
      this.totalDeleteManifests = totalDeleteManifests;
      this.scannedDataManifests = scannedDataManifests;
      this.skippedDataManifests = skippedDataManifests;
      this.totalFileSizeInBytes = totalFileSizeInBytes;
      this.totalDeleteFileSizeInBytes = totalDeleteFileSizeInBytes;
    }

    public TimerResult totalPlanningDuration() {
      return totalPlanningDuration;
    }

    public CounterResult<Integer> resultDataFiles() {
      return resultDataFiles;
    }

    public CounterResult<Integer> resultDeleteFiles() {
      return resultDeleteFiles;
    }

    public CounterResult<Integer> totalDataManifests() {
      return totalDataManifests;
    }

    public CounterResult<Integer> totalDeleteManifests() {
      return totalDeleteManifests;
    }

    public CounterResult<Integer> scannedDataManifests() {
      return scannedDataManifests;
    }

    public CounterResult<Integer> skippedDataManifests() {
      return skippedDataManifests;
    }

    public CounterResult<Long> totalFileSizeInBytes() {
      return totalFileSizeInBytes;
    }

    public CounterResult<Long> totalDeleteFileSizeInBytes() {
      return totalDeleteFileSizeInBytes;
    }

    public static ScanMetricsResult fromScanMetrics(ScanMetrics scanMetrics) {
      Preconditions.checkArgument(null != scanMetrics, "Invalid scan metrics: null");
      return new ScanMetricsResult(
          TimerResult.fromTimer(scanMetrics.totalPlanningDuration),
          CounterResult.fromCounter(scanMetrics.resultDataFiles),
          CounterResult.fromCounter(scanMetrics.resultDeleteFiles),
          CounterResult.fromCounter(scanMetrics.totalDataManifests),
          CounterResult.fromCounter(scanMetrics.totalDeleteManifests),
          CounterResult.fromCounter(scanMetrics.scannedDataManifests),
          CounterResult.fromCounter(scanMetrics.skippedDataManifests),
          CounterResult.fromCounter(scanMetrics.totalFileSizeInBytes),
          CounterResult.fromCounter(scanMetrics.totalDeleteFileSizeInBytes));
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ScanMetricsResult that = (ScanMetricsResult) o;
      return Objects.equal(totalPlanningDuration, that.totalPlanningDuration)
          && Objects.equal(resultDataFiles, that.resultDataFiles)
          && Objects.equal(resultDeleteFiles, that.resultDeleteFiles)
          && Objects.equal(totalDataManifests, that.totalDataManifests)
          && Objects.equal(totalDeleteManifests, that.totalDeleteManifests)
          && Objects.equal(scannedDataManifests, that.scannedDataManifests)
          && Objects.equal(skippedDataManifests, that.skippedDataManifests)
          && Objects.equal(totalFileSizeInBytes, that.totalFileSizeInBytes)
          && Objects.equal(totalDeleteFileSizeInBytes, that.totalDeleteFileSizeInBytes);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(
          totalPlanningDuration,
          resultDataFiles,
          resultDeleteFiles,
          totalDataManifests,
          totalDeleteManifests,
          scannedDataManifests,
          skippedDataManifests,
          totalFileSizeInBytes,
          totalDeleteFileSizeInBytes);
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
