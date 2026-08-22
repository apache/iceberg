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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

/** OpenTelemetry instruments for {@link ScanReport} metrics, keyed by {@link ScanMetricsResult}. */
final class ScanInstruments {
  private final DoubleHistogram planningDuration;
  private final LongCounter resultDataFiles;
  private final LongCounter resultDeleteFiles;
  private final LongCounter totalDataManifests;
  private final LongCounter totalDeleteManifests;
  private final LongCounter scannedDataManifests;
  private final LongCounter skippedDataManifests;
  private final LongCounter totalFileSizeBytes;
  private final LongCounter totalDeleteFileSizeBytes;
  private final LongCounter skippedDataFiles;
  private final LongCounter skippedDeleteFiles;
  private final LongCounter scannedDeleteManifests;
  private final LongCounter skippedDeleteManifests;
  private final LongCounter indexedDeleteFiles;
  private final LongCounter equalityDeleteFiles;
  private final LongCounter positionalDeleteFiles;
  private final LongCounter dvs;

  private ScanInstruments(Meter meter) {
    planningDuration =
        histogram(
            meter, "iceberg.scan.planning.duration", "Time spent planning a table scan", "ms");
    resultDataFiles =
        counter(
            meter,
            "iceberg.scan.result.data_files",
            "Number of data files included in scan result");
    resultDeleteFiles =
        counter(
            meter,
            "iceberg.scan.result.delete_files",
            "Number of delete files included in scan result");
    totalDataManifests =
        counter(meter, "iceberg.scan.data_manifests.total", "Total number of data manifests");
    totalDeleteManifests =
        counter(meter, "iceberg.scan.delete_manifests.total", "Total number of delete manifests");
    scannedDataManifests =
        counter(meter, "iceberg.scan.data_manifests.scanned", "Number of data manifests scanned");
    skippedDataManifests =
        counter(meter, "iceberg.scan.data_manifests.skipped", "Number of data manifests skipped");
    totalFileSizeBytes =
        counter(
            meter,
            "iceberg.scan.file_size.bytes",
            "Total file size of data files in scan result",
            "By");
    totalDeleteFileSizeBytes =
        counter(
            meter,
            "iceberg.scan.delete_file_size.bytes",
            "Total file size of delete files in scan result",
            "By");
    skippedDataFiles =
        counter(
            meter, "iceberg.scan.data_files.skipped", "Number of data files skipped during scan");
    skippedDeleteFiles =
        counter(
            meter,
            "iceberg.scan.delete_files.skipped",
            "Number of delete files skipped during scan");
    scannedDeleteManifests =
        counter(
            meter, "iceberg.scan.delete_manifests.scanned", "Number of delete manifests scanned");
    skippedDeleteManifests =
        counter(
            meter, "iceberg.scan.delete_manifests.skipped", "Number of delete manifests skipped");
    indexedDeleteFiles =
        counter(meter, "iceberg.scan.delete_files.indexed", "Number of indexed delete files");
    equalityDeleteFiles =
        counter(meter, "iceberg.scan.delete_files.equality", "Number of equality delete files");
    positionalDeleteFiles =
        counter(meter, "iceberg.scan.delete_files.positional", "Number of positional delete files");
    dvs = counter(meter, "iceberg.scan.dvs", "Number of deletion vectors in scan result");
  }

  static ScanInstruments create(Meter meter) {
    return new ScanInstruments(meter);
  }

  void record(ScanMetricsResult metrics, Attributes attrs) {
    if (metrics.totalPlanningDuration() != null) {
      planningDuration.record(
          (double) metrics.totalPlanningDuration().totalDuration().toMillis(), attrs);
    }

    recordCounter(resultDataFiles, metrics.resultDataFiles(), attrs);
    recordCounter(resultDeleteFiles, metrics.resultDeleteFiles(), attrs);
    recordCounter(totalDataManifests, metrics.totalDataManifests(), attrs);
    recordCounter(totalDeleteManifests, metrics.totalDeleteManifests(), attrs);
    recordCounter(scannedDataManifests, metrics.scannedDataManifests(), attrs);
    recordCounter(skippedDataManifests, metrics.skippedDataManifests(), attrs);
    recordCounter(totalFileSizeBytes, metrics.totalFileSizeInBytes(), attrs);
    recordCounter(totalDeleteFileSizeBytes, metrics.totalDeleteFileSizeInBytes(), attrs);
    recordCounter(skippedDataFiles, metrics.skippedDataFiles(), attrs);
    recordCounter(skippedDeleteFiles, metrics.skippedDeleteFiles(), attrs);
    recordCounter(scannedDeleteManifests, metrics.scannedDeleteManifests(), attrs);
    recordCounter(skippedDeleteManifests, metrics.skippedDeleteManifests(), attrs);
    recordCounter(indexedDeleteFiles, metrics.indexedDeleteFiles(), attrs);
    recordCounter(equalityDeleteFiles, metrics.equalityDeleteFiles(), attrs);
    recordCounter(positionalDeleteFiles, metrics.positionalDeleteFiles(), attrs);
    recordCounter(dvs, metrics.dvs(), attrs);
  }

  private static LongCounter counter(Meter meter, String name, String description) {
    return meter.counterBuilder(name).setDescription(description).build();
  }

  private static LongCounter counter(Meter meter, String name, String description, String unit) {
    return meter.counterBuilder(name).setDescription(description).setUnit(unit).build();
  }

  private static DoubleHistogram histogram(
      Meter meter, String name, String description, String unit) {
    return meter.histogramBuilder(name).setDescription(description).setUnit(unit).build();
  }

  private static void recordCounter(LongCounter counter, CounterResult result, Attributes attrs) {
    if (result != null) {
      counter.add(result.value(), attrs);
    }
  }
}
