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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricsReporter} that exports Iceberg {@link ScanReport} and {@link CommitReport} via
 * OpenTelemetry.
 *
 * <p>The reporter does not own the OpenTelemetry SDK. It obtains the {@link OpenTelemetry} instance
 * from {@link GlobalOpenTelemetry}, which the host application (Spark, Flink, Trino, ...) is
 * expected to register — typically via {@code
 * OpenTelemetrySdk.builder()...buildAndRegisterGlobal()} or via the OpenTelemetry Java agent. If no
 * SDK has been registered, OpenTelemetry returns a no-op implementation and metric calls are
 * silently dropped, matching the standard OpenTelemetry API contract.
 *
 * <p>Endpoint, exporter, headers, resource attributes, and exporter intervals are configured by the
 * host application or via the standard OpenTelemetry environment variables (e.g. {@code
 * OTEL_EXPORTER_OTLP_ENDPOINT}, {@code OTEL_SERVICE_NAME}, {@code OTEL_EXPORTER_OTLP_HEADERS}).
 *
 * <p>Configure as the Iceberg metrics reporter via catalog properties:
 *
 * <pre>{@code
 * metrics-reporter-impl=org.apache.iceberg.metrics.OtelMetricsReporter
 * }</pre>
 */
public class OtelMetricsReporter implements MetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(OtelMetricsReporter.class);
  private static final String INSTRUMENTATION_NAME = "org.apache.iceberg";

  static final AttributeKey<String> ATTR_TABLE_NAME = AttributeKey.stringKey("iceberg.table.name");
  static final AttributeKey<Long> ATTR_SNAPSHOT_ID = AttributeKey.longKey("iceberg.snapshot.id");
  static final AttributeKey<Long> ATTR_SCHEMA_ID = AttributeKey.longKey("iceberg.schema.id");
  static final AttributeKey<String> ATTR_OPERATION = AttributeKey.stringKey("iceberg.operation");

  private Meter meter;

  // Scan metrics instruments
  private DoubleHistogram scanPlanningDuration;
  private LongCounter scanResultDataFiles;
  private LongCounter scanResultDeleteFiles;
  private LongCounter scanScannedDataManifests;
  private LongCounter scanSkippedDataManifests;
  private LongCounter scanTotalFileSizeBytes;

  // Commit metrics instruments
  private DoubleHistogram commitDuration;
  private LongCounter commitAttempts;
  private LongCounter commitAddedDataFiles;
  private LongCounter commitRemovedDataFiles;
  private LongCounter commitAddedRecords;
  private LongCounter commitAddedFileSizeBytes;

  /** Required no-arg constructor for dynamic class loading via {@code CatalogUtil}. */
  public OtelMetricsReporter() {}

  /**
   * Package-private constructor for testing with an injected {@link OpenTelemetry} instance.
   *
   * @param openTelemetry an externally managed OpenTelemetry instance (caller owns its lifecycle)
   */
  OtelMetricsReporter(OpenTelemetry openTelemetry) {
    this.meter = openTelemetry.getMeter(INSTRUMENTATION_NAME);
    createInstruments();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.meter = GlobalOpenTelemetry.get().getMeter(INSTRUMENTATION_NAME);
    createInstruments();
    LOG.info(
        "OtelMetricsReporter initialized. SDK lifecycle is owned by the host application "
            + "(via GlobalOpenTelemetry).");
  }

  @Override
  public void report(MetricsReport report) {
    if (meter == null) {
      return;
    }

    try {
      if (report instanceof ScanReport) {
        reportScan((ScanReport) report);
      } else if (report instanceof CommitReport) {
        reportCommit((CommitReport) report);
      }
    } catch (Exception e) {
      LOG.warn("Failed to report metrics via OpenTelemetry", e);
    }
  }

  private void createInstruments() {
    scanPlanningDuration =
        meter
            .histogramBuilder("iceberg.scan.planning.duration")
            .setDescription("Time spent planning a table scan")
            .setUnit("ms")
            .build();

    scanResultDataFiles =
        meter
            .counterBuilder("iceberg.scan.result.data_files")
            .setDescription("Number of data files included in scan result")
            .build();

    scanResultDeleteFiles =
        meter
            .counterBuilder("iceberg.scan.result.delete_files")
            .setDescription("Number of delete files included in scan result")
            .build();

    scanScannedDataManifests =
        meter
            .counterBuilder("iceberg.scan.data_manifests.scanned")
            .setDescription("Number of data manifests scanned")
            .build();

    scanSkippedDataManifests =
        meter
            .counterBuilder("iceberg.scan.data_manifests.skipped")
            .setDescription("Number of data manifests skipped")
            .build();

    scanTotalFileSizeBytes =
        meter
            .counterBuilder("iceberg.scan.file_size.bytes")
            .setDescription("Total file size of data files in scan result")
            .setUnit("By")
            .build();

    commitDuration =
        meter
            .histogramBuilder("iceberg.commit.duration")
            .setDescription("Time spent on commit operation")
            .setUnit("ms")
            .build();

    commitAttempts =
        meter
            .counterBuilder("iceberg.commit.attempts")
            .setDescription("Number of commit attempts")
            .build();

    commitAddedDataFiles =
        meter
            .counterBuilder("iceberg.commit.data_files.added")
            .setDescription("Number of data files added by commit")
            .build();

    commitRemovedDataFiles =
        meter
            .counterBuilder("iceberg.commit.data_files.removed")
            .setDescription("Number of data files removed by commit")
            .build();

    commitAddedRecords =
        meter
            .counterBuilder("iceberg.commit.records.added")
            .setDescription("Number of records added by commit")
            .build();

    commitAddedFileSizeBytes =
        meter
            .counterBuilder("iceberg.commit.file_size.added_bytes")
            .setDescription("Total size of data files added by commit")
            .setUnit("By")
            .build();
  }

  private void reportScan(ScanReport report) {
    Attributes attrs =
        Attributes.of(
            ATTR_TABLE_NAME, report.tableName(),
            ATTR_SNAPSHOT_ID, report.snapshotId(),
            ATTR_SCHEMA_ID, (long) report.schemaId());

    ScanMetricsResult metrics = report.scanMetrics();
    if (metrics == null) {
      return;
    }

    if (metrics.totalPlanningDuration() != null) {
      scanPlanningDuration.record(
          (double) metrics.totalPlanningDuration().totalDuration().toMillis(), attrs);
    }

    recordCounter(scanResultDataFiles, metrics.resultDataFiles(), attrs);
    recordCounter(scanResultDeleteFiles, metrics.resultDeleteFiles(), attrs);
    recordCounter(scanScannedDataManifests, metrics.scannedDataManifests(), attrs);
    recordCounter(scanSkippedDataManifests, metrics.skippedDataManifests(), attrs);
    recordCounter(scanTotalFileSizeBytes, metrics.totalFileSizeInBytes(), attrs);
  }

  private void reportCommit(CommitReport report) {
    Attributes attrs =
        Attributes.of(
            ATTR_TABLE_NAME, report.tableName(),
            ATTR_SNAPSHOT_ID, report.snapshotId(),
            ATTR_OPERATION, report.operation());

    CommitMetricsResult metrics = report.commitMetrics();
    if (metrics == null) {
      return;
    }

    if (metrics.totalDuration() != null) {
      commitDuration.record((double) metrics.totalDuration().totalDuration().toMillis(), attrs);
    }

    recordCounter(commitAttempts, metrics.attempts(), attrs);
    recordCounter(commitAddedDataFiles, metrics.addedDataFiles(), attrs);
    recordCounter(commitRemovedDataFiles, metrics.removedDataFiles(), attrs);
    recordCounter(commitAddedRecords, metrics.addedRecords(), attrs);
    recordCounter(commitAddedFileSizeBytes, metrics.addedFilesSizeInBytes(), attrs);
  }

  private static void recordCounter(LongCounter counter, CounterResult result, Attributes attrs) {
    if (result != null) {
      counter.add(result.value(), attrs);
    }
  }
}
