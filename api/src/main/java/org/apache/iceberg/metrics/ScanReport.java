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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

/** A Table Scan report that contains all relevant information from a Table Scan. */
@Value.Immutable
public interface ScanReport extends MetricsReport {

  String tableName();

  long snapshotId();

  Expression filter();

  Schema projection();

  ScanMetricsResult scanMetrics();

  /** A serializable version of a {@link Timer} that carries its result. */
  @Value.Immutable
  interface TimerResult {

    TimeUnit timeUnit();

    Duration totalDuration();

    long count();

    static TimerResult fromTimer(Timer timer) {
      Preconditions.checkArgument(null != timer, "Invalid timer: null");
      if (timer.isNoop()) {
        return null;
      }

      return ImmutableTimerResult.builder()
          .timeUnit(timer.unit())
          .totalDuration(timer.totalDuration())
          .count(timer.count())
          .build();
    }

    static TimerResult of(TimeUnit timeUnit, Duration duration, long count) {
      return ImmutableTimerResult.builder()
          .timeUnit(timeUnit)
          .totalDuration(duration)
          .count(count)
          .build();
    }
  }

  /** A serializable version of a {@link Counter} that carries its result. */
  @Value.Immutable
  interface CounterResult {

    Unit unit();

    long value();

    static CounterResult fromCounter(Counter counter) {
      Preconditions.checkArgument(null != counter, "Invalid counter: null");
      if (counter.isNoop()) {
        return null;
      }

      return ImmutableCounterResult.builder().unit(counter.unit()).value(counter.value()).build();
    }

    static CounterResult of(Unit unit, long value) {
      return ImmutableCounterResult.builder().unit(unit).value(value).build();
    }
  }

  /** A serializable version of {@link ScanMetrics} that carries its results. */
  @Value.Immutable
  interface ScanMetricsResult {
    @Nullable
    TimerResult totalPlanningDuration();

    @Nullable
    CounterResult resultDataFiles();

    @Nullable
    CounterResult resultDeleteFiles();

    @Nullable
    CounterResult totalDataManifests();

    @Nullable
    CounterResult totalDeleteManifests();

    @Nullable
    CounterResult scannedDataManifests();

    @Nullable
    CounterResult skippedDataManifests();

    @Nullable
    CounterResult totalFileSizeInBytes();

    @Nullable
    CounterResult totalDeleteFileSizeInBytes();

    @Nullable
    CounterResult skippedDataFiles();

    @Nullable
    CounterResult skippedDeleteFiles();

    @Nullable
    CounterResult scannedDeleteManifests();

    @Nullable
    CounterResult skippedDeleteManifests();

    @Nullable
    CounterResult indexedDeleteFiles();

    @Nullable
    CounterResult equalityDeleteFiles();

    @Nullable
    CounterResult positionalDeleteFiles();

    static ScanMetricsResult fromScanMetrics(ScanMetrics scanMetrics) {
      Preconditions.checkArgument(null != scanMetrics, "Invalid scan metrics: null");
      return ImmutableScanMetricsResult.builder()
          .totalPlanningDuration(TimerResult.fromTimer(scanMetrics.totalPlanningDuration()))
          .resultDataFiles(CounterResult.fromCounter(scanMetrics.resultDataFiles()))
          .resultDeleteFiles(CounterResult.fromCounter(scanMetrics.resultDeleteFiles()))
          .totalDataManifests(CounterResult.fromCounter(scanMetrics.totalDataManifests()))
          .totalDeleteManifests(CounterResult.fromCounter(scanMetrics.totalDeleteManifests()))
          .scannedDataManifests(CounterResult.fromCounter(scanMetrics.scannedDataManifests()))
          .skippedDataManifests(CounterResult.fromCounter(scanMetrics.skippedDataManifests()))
          .totalFileSizeInBytes(CounterResult.fromCounter(scanMetrics.totalFileSizeInBytes()))
          .totalDeleteFileSizeInBytes(
              CounterResult.fromCounter(scanMetrics.totalDeleteFileSizeInBytes()))
          .skippedDataFiles(CounterResult.fromCounter(scanMetrics.skippedDataFiles()))
          .skippedDeleteFiles(CounterResult.fromCounter(scanMetrics.skippedDeleteFiles()))
          .scannedDeleteManifests(CounterResult.fromCounter(scanMetrics.scannedDeleteManifests()))
          .skippedDeleteManifests(CounterResult.fromCounter(scanMetrics.skippedDeleteManifests()))
          .indexedDeleteFiles(CounterResult.fromCounter(scanMetrics.indexedDeleteFiles()))
          .equalityDeleteFiles(CounterResult.fromCounter(scanMetrics.equalityDeleteFiles()))
          .positionalDeleteFiles(CounterResult.fromCounter(scanMetrics.positionalDeleteFiles()))
          .build();
    }
  }

  /** Carries all metrics for a particular scan */
  @Value.Immutable
  abstract class ScanMetrics {
    public static final String TOTAL_PLANNING_DURATION = "total-planning-duration";
    public static final String RESULT_DATA_FILES = "result-data-files";
    public static final String RESULT_DELETE_FILES = "result-delete-files";
    public static final String SCANNED_DATA_MANIFESTS = "scanned-data-manifests";
    public static final String SCANNED_DELETE_MANIFESTS = "scanned-delete-manifests";
    public static final String TOTAL_DATA_MANIFESTS = "total-data-manifests";
    public static final String TOTAL_DELETE_MANIFESTS = "total-delete-manifests";
    public static final String TOTAL_FILE_SIZE_IN_BYTES = "total-file-size-in-bytes";
    public static final String TOTAL_DELETE_FILE_SIZE_IN_BYTES = "total-delete-file-size-in-bytes";
    public static final String SKIPPED_DATA_MANIFESTS = "skipped-data-manifests";
    public static final String SKIPPED_DELETE_MANIFESTS = "skipped-delete-manifests";
    public static final String SKIPPED_DATA_FILES = "skipped-data-files";
    public static final String SKIPPED_DELETE_FILES = "skipped-delete-files";
    public static final String INDEXED_DELETE_FILES = "indexed-delete-files";
    public static final String EQUALITY_DELETE_FILES = "equality-delete-files";
    public static final String POSITIONAL_DELETE_FILES = "positional-delete-files";

    public static ScanMetrics noop() {
      return ScanMetrics.of(MetricsContext.nullMetrics());
    }

    public abstract MetricsContext metricsContext();

    @Value.Derived
    public Timer totalPlanningDuration() {
      return metricsContext().timer(TOTAL_PLANNING_DURATION, TimeUnit.NANOSECONDS);
    }

    @Value.Derived
    public Counter resultDataFiles() {
      return metricsContext().counter(RESULT_DATA_FILES);
    }

    @Value.Derived
    public Counter resultDeleteFiles() {
      return metricsContext().counter(RESULT_DELETE_FILES);
    }

    @Value.Derived
    public Counter scannedDataManifests() {
      return metricsContext().counter(SCANNED_DATA_MANIFESTS);
    }

    @Value.Derived
    public Counter totalDataManifests() {
      return metricsContext().counter(TOTAL_DATA_MANIFESTS);
    }

    @Value.Derived
    public Counter totalDeleteManifests() {
      return metricsContext().counter(TOTAL_DELETE_MANIFESTS);
    }

    @Value.Derived
    public Counter totalFileSizeInBytes() {
      return metricsContext().counter(TOTAL_FILE_SIZE_IN_BYTES, MetricsContext.Unit.BYTES);
    }

    @Value.Derived
    public Counter totalDeleteFileSizeInBytes() {
      return metricsContext().counter(TOTAL_DELETE_FILE_SIZE_IN_BYTES, MetricsContext.Unit.BYTES);
    }

    @Value.Derived
    public Counter skippedDataManifests() {
      return metricsContext().counter(SKIPPED_DATA_MANIFESTS);
    }

    @Value.Derived
    public Counter skippedDataFiles() {
      return metricsContext().counter(SKIPPED_DATA_FILES);
    }

    @Value.Derived
    public Counter skippedDeleteFiles() {
      return metricsContext().counter(SKIPPED_DELETE_FILES);
    }

    @Value.Derived
    public Counter scannedDeleteManifests() {
      return metricsContext().counter(SCANNED_DELETE_MANIFESTS);
    }

    @Value.Derived
    public Counter skippedDeleteManifests() {
      return metricsContext().counter(SKIPPED_DELETE_MANIFESTS);
    }

    @Value.Derived
    public Counter indexedDeleteFiles() {
      return metricsContext().counter(INDEXED_DELETE_FILES);
    }

    @Value.Derived
    public Counter equalityDeleteFiles() {
      return metricsContext().counter(EQUALITY_DELETE_FILES);
    }

    @Value.Derived
    public Counter positionalDeleteFiles() {
      return metricsContext().counter(POSITIONAL_DELETE_FILES);
    }

    public static ScanMetrics of(MetricsContext metricsContext) {
      return ImmutableScanMetrics.builder().metricsContext(metricsContext).build();
    }
  }
}
