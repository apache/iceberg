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
package org.apache.iceberg.gcp.metrics;

import com.google.api.Metric;
import com.google.api.MonitoredResource;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricsReporter} that exports {@link ScanReport} and {@link CommitReport} as time series
 * to Google Cloud Monitoring.
 *
 * <p>Each report becomes a small batch of {@link TimeSeries} points: one "operation" series ({@code
 * scan_operations} / {@code commit_operations}) plus one series per non-null counter or timer in
 * the corresponding {@link ScanMetricsResult} / {@link CommitMetricsResult}.
 *
 * <h2>Label policy</h2>
 *
 * Labels are intentionally fixed to bound time-series cardinality:
 *
 * <ul>
 *   <li>{@code table} — fully qualified table name from the report.
 *   <li>{@code catalog} — catalog impl name when available, otherwise omitted.
 *   <li>{@code metric_name} — the stable name of the underlying counter/timer (see {@link
 *       #SCAN_METRICS} / {@link #COMMIT_METRICS}). These names are independent of the Java method
 *       names on {@link ScanMetricsResult} / {@link CommitMetricsResult} so they survive upstream
 *       renames.
 *   <li>{@code operation} — commit operation (only on commit metrics).
 * </ul>
 *
 * Query predicates, projected column names and projected field ids are <b>not</b> attached as
 * labels. Each unique label combination is a billable time series in Cloud Monitoring; we keep the
 * key set small by design.
 *
 * <h2>Metric kinds</h2>
 *
 * All series are reported as {@code GAUGE} of {@code INT64}. Iceberg counters carry per-report
 * totals (not running totals across reports), so each point is a point-in-time observation.
 * Downstream aggregations (e.g. {@code sum}, {@code count}, {@code rate}) operate over time
 * windows.
 *
 * <h2>Thread model</h2>
 *
 * {@link #report(MetricsReport)} hands work off to a shared, JVM-managed daemon executor (see
 * {@link ThreadPools#newExitingWorkerPool(String, int)}) and returns immediately. Failures are
 * logged via {@link Tasks.Builder#suppressFailureWhenFinished()} and never propagate to the caller.
 *
 * <h2>Lifecycle</h2>
 *
 * This reporter owns the underlying {@link MetricServiceClient}. {@link #close()} is invoked
 * automatically by the catalog.
 */
public class CloudMonitoringMetricsReporter implements MetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(CloudMonitoringMetricsReporter.class);

  /** Cloud Monitoring limit on time series per CreateTimeSeries request. */
  private static final int MAX_TIME_SERIES_PER_REQUEST = 200;

  /** Label keys emitted on every scan time series. */
  @VisibleForTesting
  static final Set<String> SCAN_LABEL_KEYS = ImmutableSet.of("table", "catalog", "metric_name");

  /** Label keys emitted on every commit time series. */
  @VisibleForTesting
  static final Set<String> COMMIT_LABEL_KEYS =
      ImmutableSet.of("table", "catalog", "metric_name", "operation");

  /**
   * Label keys emitted on the predicate-column metric type. Only emitted when {@link
   * GCPProperties#GCP_MONITORING_PREDICATE_TRACKING_ENABLED} is true.
   */
  @VisibleForTesting
  static final Set<String> PREDICATE_LABEL_KEYS =
      ImmutableSet.of("table", "catalog", "column", "op_class");

  /**
   * Label keys emitted on the projection-column metric type. Only emitted when {@link
   * GCPProperties#GCP_MONITORING_PREDICATE_TRACKING_ENABLED} is true.
   */
  @VisibleForTesting
  static final Set<String> PROJECTION_LABEL_KEYS = ImmutableSet.of("table", "catalog", "column");

  /**
   * Names are decoupled from upstream Java method names so a rename in {@code ScanMetricsResult}
   * cannot silently change exported metric names.
   */
  @VisibleForTesting
  static final Map<String, Function<ScanMetricsResult, ?>> SCAN_METRICS =
      ImmutableMap.<String, Function<ScanMetricsResult, ?>>builder()
          .put("total_planning_duration_ms", ScanMetricsResult::totalPlanningDuration)
          .put("result_data_files", ScanMetricsResult::resultDataFiles)
          .put("result_delete_files", ScanMetricsResult::resultDeleteFiles)
          .put("total_data_manifests", ScanMetricsResult::totalDataManifests)
          .put("total_delete_manifests", ScanMetricsResult::totalDeleteManifests)
          .put("scanned_data_manifests", ScanMetricsResult::scannedDataManifests)
          .put("skipped_data_manifests", ScanMetricsResult::skippedDataManifests)
          .put("total_file_size_bytes", ScanMetricsResult::totalFileSizeInBytes)
          .put("total_delete_file_size_bytes", ScanMetricsResult::totalDeleteFileSizeInBytes)
          .put("skipped_data_files", ScanMetricsResult::skippedDataFiles)
          .put("skipped_delete_files", ScanMetricsResult::skippedDeleteFiles)
          .put("scanned_delete_manifests", ScanMetricsResult::scannedDeleteManifests)
          .put("skipped_delete_manifests", ScanMetricsResult::skippedDeleteManifests)
          .put("indexed_delete_files", ScanMetricsResult::indexedDeleteFiles)
          .put("equality_delete_files", ScanMetricsResult::equalityDeleteFiles)
          .put("positional_delete_files", ScanMetricsResult::positionalDeleteFiles)
          .put("dvs", ScanMetricsResult::dvs)
          .build();

  @VisibleForTesting
  static final Map<String, Function<CommitMetricsResult, ?>> COMMIT_METRICS =
      ImmutableMap.<String, Function<CommitMetricsResult, ?>>builder()
          .put("total_duration_ms", CommitMetricsResult::totalDuration)
          .put("attempts", CommitMetricsResult::attempts)
          .put("added_data_files", CommitMetricsResult::addedDataFiles)
          .put("removed_data_files", CommitMetricsResult::removedDataFiles)
          .put("total_data_files", CommitMetricsResult::totalDataFiles)
          .put("added_delete_files", CommitMetricsResult::addedDeleteFiles)
          .put("added_equality_delete_files", CommitMetricsResult::addedEqualityDeleteFiles)
          .put("added_positional_delete_files", CommitMetricsResult::addedPositionalDeleteFiles)
          .put("added_dvs", CommitMetricsResult::addedDVs)
          .put("removed_delete_files", CommitMetricsResult::removedDeleteFiles)
          .put("removed_equality_delete_files", CommitMetricsResult::removedEqualityDeleteFiles)
          .put("removed_positional_delete_files", CommitMetricsResult::removedPositionalDeleteFiles)
          .put("removed_dvs", CommitMetricsResult::removedDVs)
          .put("total_delete_files", CommitMetricsResult::totalDeleteFiles)
          .put("added_records", CommitMetricsResult::addedRecords)
          .put("removed_records", CommitMetricsResult::removedRecords)
          .put("total_records", CommitMetricsResult::totalRecords)
          .put("added_files_size_bytes", CommitMetricsResult::addedFilesSizeInBytes)
          .put("removed_files_size_bytes", CommitMetricsResult::removedFilesSizeInBytes)
          .put("total_files_size_bytes", CommitMetricsResult::totalFilesSizeInBytes)
          .put("added_positional_deletes", CommitMetricsResult::addedPositionalDeletes)
          .put("removed_positional_deletes", CommitMetricsResult::removedPositionalDeletes)
          .put("total_positional_deletes", CommitMetricsResult::totalPositionalDeletes)
          .put("added_equality_deletes", CommitMetricsResult::addedEqualityDeletes)
          .put("removed_equality_deletes", CommitMetricsResult::removedEqualityDeletes)
          .put("total_equality_deletes", CommitMetricsResult::totalEqualityDeletes)
          .put("manifests_created", CommitMetricsResult::manifestsCreated)
          .put("manifests_replaced", CommitMetricsResult::manifestsReplaced)
          .put("manifests_kept", CommitMetricsResult::manifestsKept)
          .put("manifest_entries_processed", CommitMetricsResult::manifestEntriesProcessed)
          .build();

  private static final ExecutorService DEFAULT_EXECUTOR =
      ThreadPools.newExitingWorkerPool("gcp-cloud-monitoring-metrics-reporter", 1);

  private final ExecutorService executor;

  private MetricServiceClient client;
  private ProjectName projectName;
  private MonitoredResource resource;
  private String catalogName;
  private String scanMetricType;
  private String commitMetricType;
  private String predicateMetricType;
  private String projectionMetricType;
  private boolean predicateTrackingEnabled;
  private int columnCap;
  private volatile boolean closed = false;

  /** Required by {@link org.apache.iceberg.CatalogUtil#loadMetricsReporter}. */
  public CloudMonitoringMetricsReporter() {
    this.executor = DEFAULT_EXECUTOR;
  }

  @VisibleForTesting
  CloudMonitoringMetricsReporter(
      GCPProperties properties, MetricServiceClient client, ExecutorService executor) {
    this.executor = executor;
    bind(properties, client);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    GCPProperties props = new GCPProperties(properties);
    this.catalogName = properties.get(CatalogProperties.CATALOG_IMPL);
    bind(props, createClient(props));
  }

  private void bind(GCPProperties properties, MetricServiceClient metricServiceClient) {
    Preconditions.checkArgument(
        properties.gcpMonitoringProjectId().isPresent(),
        "Cannot initialize CloudMonitoringMetricsReporter: %s (or %s as fallback) must be set",
        GCPProperties.GCP_MONITORING_PROJECT_ID,
        GCPProperties.GCS_PROJECT_ID);
    this.client = metricServiceClient;
    this.projectName = ProjectName.of(properties.gcpMonitoringProjectId().get());
    this.resource =
        MonitoredResource.newBuilder().setType(properties.gcpMonitoringResourceType()).build();
    String prefix = properties.gcpMonitoringMetricPrefix();
    this.scanMetricType = prefix + "/scan_metrics";
    this.commitMetricType = prefix + "/commit_metrics";
    this.predicateMetricType = prefix + "/predicate_columns";
    this.projectionMetricType = prefix + "/projection_columns";
    this.predicateTrackingEnabled = properties.gcpMonitoringPredicateTrackingEnabled();
    this.columnCap = properties.gcpMonitoringColumnCap();
  }

  private static MetricServiceClient createClient(GCPProperties props) {
    try {
      MetricServiceSettings.Builder builder = MetricServiceSettings.newBuilder();
      if (props.gcpMonitoringEndpoint().isPresent()) {
        builder.setEndpoint(props.gcpMonitoringEndpoint().get());
      }
      if (props.gcpMonitoringQuotaProjectId().isPresent()) {
        builder.setQuotaProjectId(props.gcpMonitoringQuotaProjectId().get());
      }
      if (props.noAuth()) {
        builder.setCredentialsProvider(NoCredentialsProvider.create());
      }
      return MetricServiceClient.create(builder.build());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create Cloud Monitoring MetricServiceClient", e);
    }
  }

  @Override
  public void report(MetricsReport report) {
    if (closed) {
      LOG.warn("CloudMonitoringMetricsReporter is closed; dropping report");
      return;
    }
    if (report == null) {
      LOG.warn("Received invalid metrics report: null");
      return;
    }

    Tasks.range(1)
        .executeWith(executor)
        .suppressFailureWhenFinished()
        .onFailure(
            (item, exception) ->
                LOG.warn("Failed to report metrics to Cloud Monitoring", exception))
        .run(
            item -> {
              if (report instanceof ScanReport scanReport) {
                sendTimeSeries(buildScanSeries(scanReport));
              } else if (report instanceof CommitReport commitReport) {
                sendTimeSeries(buildCommitSeries(commitReport));
              } else {
                LOG.debug("Ignoring unsupported metrics report type: {}", report.getClass());
              }
            });
  }

  @VisibleForTesting
  List<TimeSeries> buildScanSeries(ScanReport report) {
    List<TimeSeries> series = Lists.newArrayList();
    TimeInterval interval = currentInterval();

    series.add(
        timeSeries(
            scanMetricType, scanLabels(report.tableName(), "scan_operations"), interval, 1L));

    ScanMetricsResult metrics = report.scanMetrics();
    if (metrics != null) {
      for (Map.Entry<String, Function<ScanMetricsResult, ?>> entry : SCAN_METRICS.entrySet()) {
        Long value = toLong(entry.getValue().apply(metrics));
        if (value != null) {
          series.add(
              timeSeries(
                  scanMetricType, scanLabels(report.tableName(), entry.getKey()), interval, value));
        }
      }
    }

    if (predicateTrackingEnabled) {
      addPredicateAndProjectionSeries(report, interval, series);
    }
    return series;
  }

  private void addPredicateAndProjectionSeries(
      ScanReport report, TimeInterval interval, List<TimeSeries> series) {
    Set<PredicateColumnExtractor.Entry> predicates =
        PredicateColumnExtractor.extract(report.filter());
    List<String> projection =
        report.projectedFieldNames() == null ? List.of() : report.projectedFieldNames();

    Set<String> uniqueColumns = Sets.newHashSet();
    predicates.forEach(e -> uniqueColumns.add(e.column()));
    uniqueColumns.addAll(projection);

    if (uniqueColumns.size() > columnCap) {
      LOG.debug(
          "Skipping predicate/projection emission for table {}: {} unique columns exceeds cap {}",
          report.tableName(),
          uniqueColumns.size(),
          columnCap);
      return;
    }

    for (PredicateColumnExtractor.Entry entry : predicates) {
      series.add(
          timeSeries(
              predicateMetricType,
              predicateLabels(report.tableName(), entry.column(), entry.opClass()),
              interval,
              1L));
    }
    for (String column : projection) {
      series.add(
          timeSeries(
              projectionMetricType, projectionLabels(report.tableName(), column), interval, 1L));
    }
  }

  @VisibleForTesting
  List<TimeSeries> buildCommitSeries(CommitReport report) {
    List<TimeSeries> series = Lists.newArrayList();
    TimeInterval interval = currentInterval();

    series.add(
        timeSeries(
            commitMetricType,
            commitLabels(report.tableName(), report.operation(), "commit_operations"),
            interval,
            1L));

    CommitMetricsResult metrics = report.commitMetrics();
    if (metrics != null) {
      for (Map.Entry<String, Function<CommitMetricsResult, ?>> entry : COMMIT_METRICS.entrySet()) {
        Long value = toLong(entry.getValue().apply(metrics));
        if (value != null) {
          series.add(
              timeSeries(
                  commitMetricType,
                  commitLabels(report.tableName(), report.operation(), entry.getKey()),
                  interval,
                  value));
        }
      }
    }
    return series;
  }

  private Map<String, String> scanLabels(String tableName, String metricName) {
    return ImmutableMap.of(
        "table", nullSafe(tableName), "catalog", nullSafe(catalogName), "metric_name", metricName);
  }

  private Map<String, String> predicateLabels(String tableName, String column, String opClass) {
    return ImmutableMap.of(
        "table",
        nullSafe(tableName),
        "catalog",
        nullSafe(catalogName),
        "column",
        nullSafe(column),
        "op_class",
        opClass);
  }

  private Map<String, String> projectionLabels(String tableName, String column) {
    return ImmutableMap.of(
        "table", nullSafe(tableName), "catalog", nullSafe(catalogName), "column", nullSafe(column));
  }

  private Map<String, String> commitLabels(String tableName, String operation, String metricName) {
    return ImmutableMap.of(
        "table",
        nullSafe(tableName),
        "catalog",
        nullSafe(catalogName),
        "metric_name",
        metricName,
        "operation",
        nullSafe(operation));
  }

  private TimeSeries timeSeries(
      String metricType, Map<String, String> labels, TimeInterval interval, long value) {
    Point point =
        Point.newBuilder()
            .setInterval(interval)
            .setValue(TypedValue.newBuilder().setInt64Value(value).build())
            .build();
    return TimeSeries.newBuilder()
        .setMetric(Metric.newBuilder().setType(metricType).putAllLabels(labels).build())
        .setResource(resource)
        .addPoints(point)
        .build();
  }

  private static TimeInterval currentInterval() {
    return TimeInterval.newBuilder()
        .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
        .build();
  }

  private static Long toLong(Object metricResult) {
    if (metricResult instanceof CounterResult counter) {
      return counter.value();
    }
    if (metricResult instanceof TimerResult timer) {
      return timer.totalDuration().toMillis();
    }
    return null;
  }

  private static String nullSafe(String value) {
    return value == null ? "" : value;
  }

  private void sendTimeSeries(List<TimeSeries> series) {
    if (series.isEmpty()) {
      return;
    }
    for (int start = 0; start < series.size(); start += MAX_TIME_SERIES_PER_REQUEST) {
      List<TimeSeries> chunk =
          series.subList(start, Math.min(start + MAX_TIME_SERIES_PER_REQUEST, series.size()));
      CreateTimeSeriesRequest request =
          CreateTimeSeriesRequest.newBuilder()
              .setName(projectName.toString())
              .addAllTimeSeries(chunk)
              .build();
      client.createTimeSeries(request);
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        LOG.warn("Failed to close Cloud Monitoring MetricServiceClient", e);
      }
    }
  }

  /**
   * Walks an {@link Expression} and collects {@code (column, op_class)} pairs from leaf predicates.
   * Op classes bucket Iceberg's {@link Expression.Operation} into clustering-relevant groups:
   * {@code point} (eq, in), {@code range} (lt/lte/gt/gte), {@code null} (is_null/not_null and the
   * nan variants), and {@code other} (everything else, including negations and starts_with).
   *
   * <p>Aggregate operations and structural nodes (and/or/not/true/false) are not emitted; the
   * structure is irrelevant for column-frequency analytics.
   */
  @VisibleForTesting
  static final class PredicateColumnExtractor extends ExpressionVisitors.ExpressionVisitor<Void> {

    /** A single column reference with its bucketed operator class. */
    static final class Entry {
      private final String column;
      private final String opClass;

      Entry(String column, String opClass) {
        this.column = column;
        this.opClass = opClass;
      }

      String column() {
        return column;
      }

      String opClass() {
        return opClass;
      }

      @Override
      public boolean equals(Object other) {
        if (this == other) {
          return true;
        }
        if (!(other instanceof Entry that)) {
          return false;
        }
        return column.equals(that.column) && opClass.equals(that.opClass);
      }

      @Override
      public int hashCode() {
        return java.util.Objects.hash(column, opClass);
      }
    }

    private final Set<Entry> entries = Sets.newLinkedHashSet();

    static Set<Entry> extract(Expression expression) {
      PredicateColumnExtractor visitor = new PredicateColumnExtractor();
      ExpressionVisitors.visit(expression, visitor);
      return visitor.entries;
    }

    @Override
    public <T> Void predicate(BoundPredicate<T> pred) {
      if (pred.term() instanceof BoundReference<?> ref) {
        entries.add(new Entry(ref.name(), opClass(pred.op())));
      }
      return null;
    }

    @Override
    public <T> Void predicate(UnboundPredicate<T> pred) {
      String name = ExpressionUtil.describe(pred.term());
      if (name != null) {
        entries.add(new Entry(name, opClass(pred.op())));
      }
      return null;
    }

    @Override
    public Void and(Void left, Void right) {
      return null;
    }

    @Override
    public Void or(Void left, Void right) {
      return null;
    }

    @Override
    public Void not(Void result) {
      return null;
    }

    @Override
    public Void alwaysTrue() {
      return null;
    }

    @Override
    public Void alwaysFalse() {
      return null;
    }

    @VisibleForTesting
    static String opClass(Expression.Operation op) {
      return switch (op) {
        case EQ, IN -> "point";
        case LT, LT_EQ, GT, GT_EQ -> "range";
        case IS_NULL, NOT_NULL, IS_NAN, NOT_NAN -> "null";
        default -> "other";
      };
    }
  }
}
