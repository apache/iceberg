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

import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

/** A serializable version of {@link ScanMetrics} that carries its results. */
@Value.Immutable
public interface ScanMetricsResult {
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

  @Nullable
  MultiDimensionCounterResult resultDataFilesByFormat();

  static ScanMetricsResult fromScanMetrics(ScanMetrics scanMetrics) {
    Preconditions.checkArgument(null != scanMetrics, "Invalid scan metrics: null");
    return ImmutableScanMetricsResult.builder()
        .totalPlanningDuration(TimerResult.fromTimer(scanMetrics.totalPlanningDuration()))
        .resultDataFiles(CounterResult.fromCounter(scanMetrics.resultDataFiles()))
        .resultDataFilesByFormat(
            MultiDimensionCounterResult.fromCounter(scanMetrics.resultDataFilesByFormat()))
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
