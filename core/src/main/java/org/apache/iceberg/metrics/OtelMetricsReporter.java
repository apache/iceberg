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
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
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
 *
 * <h2>Cardinality</h2>
 *
 * By default every emitted metric carries the following attribute set:
 *
 * <ul>
 *   <li>Scan metrics: {@code iceberg.table.name}
 *   <li>Commit metrics: {@code iceberg.table.name}, {@code iceberg.operation}
 * </ul>
 *
 * <p>The set of attributes is configurable via the {@code iceberg.otel.metrics.attributes} catalog
 * property, which takes a comma-separated allowlist of attribute short names. Recognized names are
 * {@code table-name}, {@code schema-id}, and {@code operation}; any attribute whose short name is
 * not listed is omitted from emitted metric points.
 *
 * <p>For example, to additionally include {@code iceberg.schema.id} (useful for correlating scan
 * performance with schema evolution):
 *
 * <pre>{@code
 * iceberg.otel.metrics.attributes=table-name,schema-id,operation
 * }</pre>
 *
 * <p>Or to exclude {@code iceberg.table.name} entirely in deployments with a very large number of
 * tables:
 *
 * <pre>{@code
 * iceberg.otel.metrics.attributes=operation
 * }</pre>
 *
 * <p>Or to emit metrics with no attributes at all (single aggregate time series per metric), use
 * {@code none} or an empty string:
 *
 * <pre>{@code
 * iceberg.otel.metrics.attributes=none
 * }</pre>
 *
 * <p>When the property is not set, the default attribute set above is used.
 *
 * <p>The snapshot id is deliberately <b>not</b> attached as a metric attribute. Snapshot ids are
 * monotonically increasing and unique per commit, so including them would create a new time series
 * for every commit and risk unbounded cardinality in any time-series backend (Prometheus,
 * CloudWatch, Datadog, etc.).
 */
public class OtelMetricsReporter implements MetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(OtelMetricsReporter.class);
  private static final String INSTRUMENTATION_NAME = "org.apache.iceberg";

  static final String ATTRIBUTES_PROPERTY = "iceberg.otel.metrics.attributes";
  static final String ATTR_NAME_TABLE_NAME = "table-name";
  static final String ATTR_NAME_SCHEMA_ID = "schema-id";
  static final String ATTR_NAME_OPERATION = "operation";
  static final String ATTR_VALUE_NONE = "none";
  private static final Set<String> KNOWN_ATTRIBUTE_NAMES =
      ImmutableSet.of(ATTR_NAME_TABLE_NAME, ATTR_NAME_SCHEMA_ID, ATTR_NAME_OPERATION);

  static final AttributeKey<String> ATTR_TABLE_NAME = AttributeKey.stringKey("iceberg.table.name");
  static final AttributeKey<Long> ATTR_SCHEMA_ID = AttributeKey.longKey("iceberg.schema.id");
  static final AttributeKey<String> ATTR_OPERATION = AttributeKey.stringKey("iceberg.operation");

  private Meter meter;
  private boolean includeTableName = true;
  private boolean includeSchemaId = false;
  private boolean includeOperation = true;

  // Scan metrics instruments
  private DoubleHistogram scanPlanningDuration;
  private LongCounter scanResultDataFiles;
  private LongCounter scanResultDeleteFiles;
  private LongCounter scanTotalDataManifests;
  private LongCounter scanTotalDeleteManifests;
  private LongCounter scanScannedDataManifests;
  private LongCounter scanSkippedDataManifests;
  private LongCounter scanTotalFileSizeBytes;
  private LongCounter scanTotalDeleteFileSizeBytes;
  private LongCounter scanSkippedDataFiles;
  private LongCounter scanSkippedDeleteFiles;
  private LongCounter scanScannedDeleteManifests;
  private LongCounter scanSkippedDeleteManifests;
  private LongCounter scanIndexedDeleteFiles;
  private LongCounter scanEqualityDeleteFiles;
  private LongCounter scanPositionalDeleteFiles;
  private LongCounter scanDVs;

  // Commit metrics instruments
  private DoubleHistogram commitDuration;
  private LongCounter commitAttempts;
  private LongCounter commitAddedDataFiles;
  private LongCounter commitRemovedDataFiles;
  private LongCounter commitTotalDataFiles;
  private LongCounter commitAddedDeleteFiles;
  private LongCounter commitAddedEqualityDeleteFiles;
  private LongCounter commitAddedPositionalDeleteFiles;
  private LongCounter commitAddedDVs;
  private LongCounter commitRemovedDeleteFiles;
  private LongCounter commitRemovedEqualityDeleteFiles;
  private LongCounter commitRemovedPositionalDeleteFiles;
  private LongCounter commitRemovedDVs;
  private LongCounter commitTotalDeleteFiles;
  private LongCounter commitAddedRecords;
  private LongCounter commitRemovedRecords;
  private LongCounter commitTotalRecords;
  private LongCounter commitAddedFileSizeBytes;
  private LongCounter commitRemovedFileSizeBytes;
  private LongCounter commitTotalFileSizeBytes;
  private LongCounter commitAddedPositionalDeletes;
  private LongCounter commitRemovedPositionalDeletes;
  private LongCounter commitTotalPositionalDeletes;
  private LongCounter commitAddedEqualityDeletes;
  private LongCounter commitRemovedEqualityDeletes;
  private LongCounter commitTotalEqualityDeletes;
  private LongCounter commitManifestsCreated;
  private LongCounter commitManifestsReplaced;
  private LongCounter commitManifestsKept;
  private LongCounter commitManifestEntriesProcessed;

  /** Required no-arg constructor for dynamic class loading via {@code CatalogUtil}. */
  public OtelMetricsReporter() {}

  /**
   * Package-private constructor for testing with an injected {@link OpenTelemetry} instance.
   *
   * @param openTelemetry an externally managed OpenTelemetry instance (caller owns its lifecycle)
   */
  OtelMetricsReporter(OpenTelemetry openTelemetry) {
    this(openTelemetry, ImmutableMap.of());
  }

  /**
   * Package-private constructor for testing with an injected {@link OpenTelemetry} instance and
   * reporter properties.
   *
   * @param openTelemetry an externally managed OpenTelemetry instance (caller owns its lifecycle)
   * @param properties reporter properties; supports {@link #ATTRIBUTES_PROPERTY}
   */
  OtelMetricsReporter(OpenTelemetry openTelemetry, Map<String, String> properties) {
    configureAttributes(properties);
    this.meter = openTelemetry.getMeter(INSTRUMENTATION_NAME);
    createInstruments();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    configureAttributes(properties);
    this.meter = GlobalOpenTelemetry.get().getMeter(INSTRUMENTATION_NAME);
    createInstruments();
    LOG.info(
        "OtelMetricsReporter initialized. SDK lifecycle is owned by the host application "
            + "(via GlobalOpenTelemetry).");
  }

  private void configureAttributes(Map<String, String> properties) {
    String configured = properties.get(ATTRIBUTES_PROPERTY);
    if (configured == null) {
      return;
    }
    String trimmed = configured.trim();
    if (trimmed.isEmpty() || ATTR_VALUE_NONE.equalsIgnoreCase(trimmed)) {
      this.includeTableName = false;
      this.includeSchemaId = false;
      this.includeOperation = false;
      return;
    }
    Set<String> enabled =
        Arrays.stream(trimmed.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toSet());
    for (String name : enabled) {
      if (!KNOWN_ATTRIBUTE_NAMES.contains(name)) {
        LOG.warn(
            "Ignoring unknown attribute name in {}: {} (valid: {})",
            ATTRIBUTES_PROPERTY,
            name,
            KNOWN_ATTRIBUTE_NAMES);
      }
    }
    this.includeTableName = enabled.contains(ATTR_NAME_TABLE_NAME);
    this.includeSchemaId = enabled.contains(ATTR_NAME_SCHEMA_ID);
    this.includeOperation = enabled.contains(ATTR_NAME_OPERATION);
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

    scanTotalDataManifests =
        meter
            .counterBuilder("iceberg.scan.data_manifests.total")
            .setDescription("Total number of data manifests")
            .build();

    scanTotalDeleteManifests =
        meter
            .counterBuilder("iceberg.scan.delete_manifests.total")
            .setDescription("Total number of delete manifests")
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

    scanTotalDeleteFileSizeBytes =
        meter
            .counterBuilder("iceberg.scan.delete_file_size.bytes")
            .setDescription("Total file size of delete files in scan result")
            .setUnit("By")
            .build();

    scanSkippedDataFiles =
        meter
            .counterBuilder("iceberg.scan.data_files.skipped")
            .setDescription("Number of data files skipped during scan")
            .build();

    scanSkippedDeleteFiles =
        meter
            .counterBuilder("iceberg.scan.delete_files.skipped")
            .setDescription("Number of delete files skipped during scan")
            .build();

    scanScannedDeleteManifests =
        meter
            .counterBuilder("iceberg.scan.delete_manifests.scanned")
            .setDescription("Number of delete manifests scanned")
            .build();

    scanSkippedDeleteManifests =
        meter
            .counterBuilder("iceberg.scan.delete_manifests.skipped")
            .setDescription("Number of delete manifests skipped")
            .build();

    scanIndexedDeleteFiles =
        meter
            .counterBuilder("iceberg.scan.delete_files.indexed")
            .setDescription("Number of indexed delete files")
            .build();

    scanEqualityDeleteFiles =
        meter
            .counterBuilder("iceberg.scan.delete_files.equality")
            .setDescription("Number of equality delete files")
            .build();

    scanPositionalDeleteFiles =
        meter
            .counterBuilder("iceberg.scan.delete_files.positional")
            .setDescription("Number of positional delete files")
            .build();

    scanDVs =
        meter
            .counterBuilder("iceberg.scan.dvs")
            .setDescription("Number of deletion vectors in scan result")
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

    commitTotalDataFiles =
        meter
            .counterBuilder("iceberg.commit.data_files.total")
            .setDescription("Total number of data files after commit")
            .build();

    commitAddedDeleteFiles =
        meter
            .counterBuilder("iceberg.commit.delete_files.added")
            .setDescription("Number of delete files added by commit")
            .build();

    commitAddedEqualityDeleteFiles =
        meter
            .counterBuilder("iceberg.commit.delete_files.equality.added")
            .setDescription("Number of equality delete files added by commit")
            .build();

    commitAddedPositionalDeleteFiles =
        meter
            .counterBuilder("iceberg.commit.delete_files.positional.added")
            .setDescription("Number of positional delete files added by commit")
            .build();

    commitAddedDVs =
        meter
            .counterBuilder("iceberg.commit.dvs.added")
            .setDescription("Number of deletion vectors added by commit")
            .build();

    commitRemovedDeleteFiles =
        meter
            .counterBuilder("iceberg.commit.delete_files.removed")
            .setDescription("Number of delete files removed by commit")
            .build();

    commitRemovedEqualityDeleteFiles =
        meter
            .counterBuilder("iceberg.commit.delete_files.equality.removed")
            .setDescription("Number of equality delete files removed by commit")
            .build();

    commitRemovedPositionalDeleteFiles =
        meter
            .counterBuilder("iceberg.commit.delete_files.positional.removed")
            .setDescription("Number of positional delete files removed by commit")
            .build();

    commitRemovedDVs =
        meter
            .counterBuilder("iceberg.commit.dvs.removed")
            .setDescription("Number of deletion vectors removed by commit")
            .build();

    commitTotalDeleteFiles =
        meter
            .counterBuilder("iceberg.commit.delete_files.total")
            .setDescription("Total number of delete files after commit")
            .build();

    commitAddedRecords =
        meter
            .counterBuilder("iceberg.commit.records.added")
            .setDescription("Number of records added by commit")
            .build();

    commitRemovedRecords =
        meter
            .counterBuilder("iceberg.commit.records.removed")
            .setDescription("Number of records removed by commit")
            .build();

    commitTotalRecords =
        meter
            .counterBuilder("iceberg.commit.records.total")
            .setDescription("Total number of records after commit")
            .build();

    commitAddedFileSizeBytes =
        meter
            .counterBuilder("iceberg.commit.file_size.added_bytes")
            .setDescription("Total size of data files added by commit")
            .setUnit("By")
            .build();

    commitRemovedFileSizeBytes =
        meter
            .counterBuilder("iceberg.commit.file_size.removed_bytes")
            .setDescription("Total size of data files removed by commit")
            .setUnit("By")
            .build();

    commitTotalFileSizeBytes =
        meter
            .counterBuilder("iceberg.commit.file_size.total_bytes")
            .setDescription("Total size of all data files after commit")
            .setUnit("By")
            .build();

    commitAddedPositionalDeletes =
        meter
            .counterBuilder("iceberg.commit.positional_deletes.added")
            .setDescription("Number of positional deletes added by commit")
            .build();

    commitRemovedPositionalDeletes =
        meter
            .counterBuilder("iceberg.commit.positional_deletes.removed")
            .setDescription("Number of positional deletes removed by commit")
            .build();

    commitTotalPositionalDeletes =
        meter
            .counterBuilder("iceberg.commit.positional_deletes.total")
            .setDescription("Total number of positional deletes after commit")
            .build();

    commitAddedEqualityDeletes =
        meter
            .counterBuilder("iceberg.commit.equality_deletes.added")
            .setDescription("Number of equality deletes added by commit")
            .build();

    commitRemovedEqualityDeletes =
        meter
            .counterBuilder("iceberg.commit.equality_deletes.removed")
            .setDescription("Number of equality deletes removed by commit")
            .build();

    commitTotalEqualityDeletes =
        meter
            .counterBuilder("iceberg.commit.equality_deletes.total")
            .setDescription("Total number of equality deletes after commit")
            .build();

    commitManifestsCreated =
        meter
            .counterBuilder("iceberg.commit.manifests.created")
            .setDescription("Number of manifests created by commit")
            .build();

    commitManifestsReplaced =
        meter
            .counterBuilder("iceberg.commit.manifests.replaced")
            .setDescription("Number of manifests replaced by commit")
            .build();

    commitManifestsKept =
        meter
            .counterBuilder("iceberg.commit.manifests.kept")
            .setDescription("Number of manifests kept by commit")
            .build();

    commitManifestEntriesProcessed =
        meter
            .counterBuilder("iceberg.commit.manifest_entries.processed")
            .setDescription("Number of manifest entries processed by commit")
            .build();
  }

  private void reportScan(ScanReport report) {
    AttributesBuilder attrsBuilder = Attributes.builder();
    if (includeTableName) {
      attrsBuilder.put(ATTR_TABLE_NAME, report.tableName());
    }
    if (includeSchemaId) {
      attrsBuilder.put(ATTR_SCHEMA_ID, (long) report.schemaId());
    }
    Attributes attrs = attrsBuilder.build();

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
    recordCounter(scanTotalDataManifests, metrics.totalDataManifests(), attrs);
    recordCounter(scanTotalDeleteManifests, metrics.totalDeleteManifests(), attrs);
    recordCounter(scanScannedDataManifests, metrics.scannedDataManifests(), attrs);
    recordCounter(scanSkippedDataManifests, metrics.skippedDataManifests(), attrs);
    recordCounter(scanTotalFileSizeBytes, metrics.totalFileSizeInBytes(), attrs);
    recordCounter(scanTotalDeleteFileSizeBytes, metrics.totalDeleteFileSizeInBytes(), attrs);
    recordCounter(scanSkippedDataFiles, metrics.skippedDataFiles(), attrs);
    recordCounter(scanSkippedDeleteFiles, metrics.skippedDeleteFiles(), attrs);
    recordCounter(scanScannedDeleteManifests, metrics.scannedDeleteManifests(), attrs);
    recordCounter(scanSkippedDeleteManifests, metrics.skippedDeleteManifests(), attrs);
    recordCounter(scanIndexedDeleteFiles, metrics.indexedDeleteFiles(), attrs);
    recordCounter(scanEqualityDeleteFiles, metrics.equalityDeleteFiles(), attrs);
    recordCounter(scanPositionalDeleteFiles, metrics.positionalDeleteFiles(), attrs);
    recordCounter(scanDVs, metrics.dvs(), attrs);
  }

  private void reportCommit(CommitReport report) {
    AttributesBuilder attrsBuilder = Attributes.builder();
    if (includeTableName) {
      attrsBuilder.put(ATTR_TABLE_NAME, report.tableName());
    }
    if (includeOperation) {
      attrsBuilder.put(ATTR_OPERATION, report.operation());
    }
    Attributes attrs = attrsBuilder.build();

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
    recordCounter(commitTotalDataFiles, metrics.totalDataFiles(), attrs);
    recordCounter(commitAddedDeleteFiles, metrics.addedDeleteFiles(), attrs);
    recordCounter(commitAddedEqualityDeleteFiles, metrics.addedEqualityDeleteFiles(), attrs);
    recordCounter(commitAddedPositionalDeleteFiles, metrics.addedPositionalDeleteFiles(), attrs);
    recordCounter(commitAddedDVs, metrics.addedDVs(), attrs);
    recordCounter(commitRemovedDeleteFiles, metrics.removedDeleteFiles(), attrs);
    recordCounter(commitRemovedEqualityDeleteFiles, metrics.removedEqualityDeleteFiles(), attrs);
    recordCounter(
        commitRemovedPositionalDeleteFiles, metrics.removedPositionalDeleteFiles(), attrs);
    recordCounter(commitRemovedDVs, metrics.removedDVs(), attrs);
    recordCounter(commitTotalDeleteFiles, metrics.totalDeleteFiles(), attrs);
    recordCounter(commitAddedRecords, metrics.addedRecords(), attrs);
    recordCounter(commitRemovedRecords, metrics.removedRecords(), attrs);
    recordCounter(commitTotalRecords, metrics.totalRecords(), attrs);
    recordCounter(commitAddedFileSizeBytes, metrics.addedFilesSizeInBytes(), attrs);
    recordCounter(commitRemovedFileSizeBytes, metrics.removedFilesSizeInBytes(), attrs);
    recordCounter(commitTotalFileSizeBytes, metrics.totalFilesSizeInBytes(), attrs);
    recordCounter(commitAddedPositionalDeletes, metrics.addedPositionalDeletes(), attrs);
    recordCounter(commitRemovedPositionalDeletes, metrics.removedPositionalDeletes(), attrs);
    recordCounter(commitTotalPositionalDeletes, metrics.totalPositionalDeletes(), attrs);
    recordCounter(commitAddedEqualityDeletes, metrics.addedEqualityDeletes(), attrs);
    recordCounter(commitRemovedEqualityDeletes, metrics.removedEqualityDeletes(), attrs);
    recordCounter(commitTotalEqualityDeletes, metrics.totalEqualityDeletes(), attrs);
    recordCounter(commitManifestsCreated, metrics.manifestsCreated(), attrs);
    recordCounter(commitManifestsReplaced, metrics.manifestsReplaced(), attrs);
    recordCounter(commitManifestsKept, metrics.manifestsKept(), attrs);
    recordCounter(commitManifestEntriesProcessed, metrics.manifestEntriesProcessed(), attrs);
  }

  private static void recordCounter(LongCounter counter, CounterResult result, Attributes attrs) {
    if (result != null) {
      counter.add(result.value(), attrs);
    }
  }
}
