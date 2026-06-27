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
package org.apache.iceberg.flink.source.enumerator;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.TimerResult;

/**
 * Metrics for the Iceberg source enumerator (coordinator). All metrics are gauges reporting the
 * values from the most recent scan planning cycle, providing a per-scan snapshot of planning
 * efficiency. This allows monitoring systems to track spikes, trends, and correlate scan behavior
 * with other events over time. Gauge values remain at 0 until the first actual scan completes.
 */
@Internal
public class IcebergSourceEnumeratorMetrics {
  private final AtomicLong lastScanPlanningDurationMs = new AtomicLong();
  private final AtomicLong scannedDataManifests = new AtomicLong();
  private final AtomicLong skippedDataManifests = new AtomicLong();
  private final AtomicLong totalDataManifests = new AtomicLong();
  private final AtomicLong scannedDeleteManifests = new AtomicLong();
  private final AtomicLong skippedDeleteManifests = new AtomicLong();
  private final AtomicLong totalDeleteManifests = new AtomicLong();
  private final AtomicLong resultDataFiles = new AtomicLong();
  private final AtomicLong skippedDataFiles = new AtomicLong();
  private final AtomicLong resultDeleteFiles = new AtomicLong();
  private final AtomicLong skippedDeleteFiles = new AtomicLong();
  private final AtomicLong indexedDeleteFiles = new AtomicLong();
  private final AtomicLong equalityDeleteFiles = new AtomicLong();
  private final AtomicLong positionalDeleteFiles = new AtomicLong();
  // named to match ScanMetricsResult.dvs() (deletion vectors)
  private final AtomicLong dvs = new AtomicLong();
  private final AtomicLong totalFileSizeInBytes = new AtomicLong();
  private final AtomicLong totalDeleteFileSizeInBytes = new AtomicLong();

  public IcebergSourceEnumeratorMetrics(MetricGroup metrics, String fullTableName) {
    MetricGroup enumeratorMetrics =
        metrics.addGroup("IcebergSourceEnumerator").addGroup("table", fullTableName);
    enumeratorMetrics.gauge("lastScanPlanningDurationMs", lastScanPlanningDurationMs::get);
    enumeratorMetrics.gauge("scannedDataManifests", scannedDataManifests::get);
    enumeratorMetrics.gauge("skippedDataManifests", skippedDataManifests::get);
    enumeratorMetrics.gauge("totalDataManifests", totalDataManifests::get);
    enumeratorMetrics.gauge("scannedDeleteManifests", scannedDeleteManifests::get);
    enumeratorMetrics.gauge("skippedDeleteManifests", skippedDeleteManifests::get);
    enumeratorMetrics.gauge("totalDeleteManifests", totalDeleteManifests::get);
    enumeratorMetrics.gauge("resultDataFiles", resultDataFiles::get);
    enumeratorMetrics.gauge("skippedDataFiles", skippedDataFiles::get);
    enumeratorMetrics.gauge("resultDeleteFiles", resultDeleteFiles::get);
    enumeratorMetrics.gauge("skippedDeleteFiles", skippedDeleteFiles::get);
    enumeratorMetrics.gauge("indexedDeleteFiles", indexedDeleteFiles::get);
    enumeratorMetrics.gauge("equalityDeleteFiles", equalityDeleteFiles::get);
    enumeratorMetrics.gauge("positionalDeleteFiles", positionalDeleteFiles::get);
    enumeratorMetrics.gauge("dvs", dvs::get);
    enumeratorMetrics.gauge("totalFileSizeInBytes", totalFileSizeInBytes::get);
    enumeratorMetrics.gauge("totalDeleteFileSizeInBytes", totalDeleteFileSizeInBytes::get);
  }

  /** Updates enumerator metrics from the given scan metrics result. No-op if {@code null}. */
  public void updateWithScanMetrics(ScanMetricsResult scanMetrics) {
    if (scanMetrics == null) {
      return;
    }

    TimerResult planningDuration = scanMetrics.totalPlanningDuration();
    if (planningDuration != null) {
      lastScanPlanningDurationMs.set(planningDuration.totalDuration().toMillis());
    }

    setGauge(scannedDataManifests, scanMetrics.scannedDataManifests());
    setGauge(skippedDataManifests, scanMetrics.skippedDataManifests());
    setGauge(totalDataManifests, scanMetrics.totalDataManifests());
    setGauge(scannedDeleteManifests, scanMetrics.scannedDeleteManifests());
    setGauge(skippedDeleteManifests, scanMetrics.skippedDeleteManifests());
    setGauge(totalDeleteManifests, scanMetrics.totalDeleteManifests());
    setGauge(resultDataFiles, scanMetrics.resultDataFiles());
    setGauge(skippedDataFiles, scanMetrics.skippedDataFiles());
    setGauge(resultDeleteFiles, scanMetrics.resultDeleteFiles());
    setGauge(skippedDeleteFiles, scanMetrics.skippedDeleteFiles());
    setGauge(indexedDeleteFiles, scanMetrics.indexedDeleteFiles());
    setGauge(equalityDeleteFiles, scanMetrics.equalityDeleteFiles());
    setGauge(positionalDeleteFiles, scanMetrics.positionalDeleteFiles());
    setGauge(dvs, scanMetrics.dvs());
    setGauge(totalFileSizeInBytes, scanMetrics.totalFileSizeInBytes());
    setGauge(totalDeleteFileSizeInBytes, scanMetrics.totalDeleteFileSizeInBytes());
  }

  private static void setGauge(AtomicLong gauge, CounterResult counterResult) {
    if (counterResult != null) {
      gauge.set(counterResult.value());
    }
  }
}
