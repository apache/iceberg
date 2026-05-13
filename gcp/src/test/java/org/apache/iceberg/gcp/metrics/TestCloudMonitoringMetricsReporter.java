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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeSeries;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.metrics.ImmutableCommitMetricsResult;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableScanMetricsResult;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.TimerResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestCloudMonitoringMetricsReporter {

  private static final String PROJECT_ID = "test-project";
  private static final String TABLE_NAME = "db.t";
  private static final String METRIC_PREFIX = "custom.googleapis.com/iceberg";

  private static GCPProperties properties() {
    return new GCPProperties(ImmutableMap.of(GCPProperties.GCP_MONITORING_PROJECT_ID, PROJECT_ID));
  }

  private static GCPProperties propertiesWithPredicateTracking() {
    return new GCPProperties(
        ImmutableMap.of(
            GCPProperties.GCP_MONITORING_PROJECT_ID,
            PROJECT_ID,
            GCPProperties.GCP_MONITORING_PREDICATE_TRACKING_ENABLED,
            "true"));
  }

  private static GCPProperties propertiesWithColumnCap(int cap) {
    return new GCPProperties(
        ImmutableMap.of(
            GCPProperties.GCP_MONITORING_PROJECT_ID,
            PROJECT_ID,
            GCPProperties.GCP_MONITORING_PREDICATE_TRACKING_ENABLED,
            "true",
            GCPProperties.GCP_MONITORING_COLUMN_CAP,
            String.valueOf(cap)));
  }

  private static CloudMonitoringMetricsReporter reporter(MetricServiceClient client) {
    return reporter(client, MoreExecutors.newDirectExecutorService());
  }

  private static CloudMonitoringMetricsReporter reporter(
      MetricServiceClient client, ExecutorService executor) {
    return new CloudMonitoringMetricsReporter(properties(), client, executor);
  }

  private static CloudMonitoringMetricsReporter reporter(
      GCPProperties props, MetricServiceClient client) {
    return new CloudMonitoringMetricsReporter(
        props, client, MoreExecutors.newDirectExecutorService());
  }

  private static ScanReport scanReport(ScanMetricsResult metrics) {
    return ImmutableScanReport.builder()
        .tableName(TABLE_NAME)
        .snapshotId(1L)
        .filter(Expressions.alwaysTrue())
        .schemaId(0)
        .scanMetrics(metrics)
        .build();
  }

  private static CommitReport commitReport(CommitMetricsResult metrics) {
    return ImmutableCommitReport.builder()
        .tableName(TABLE_NAME)
        .snapshotId(1L)
        .sequenceNumber(1L)
        .operation("append")
        .commitMetrics(metrics)
        .build();
  }

  private static ScanMetricsResult emptyScanMetrics() {
    return ImmutableScanMetricsResult.builder().build();
  }

  private static CommitMetricsResult emptyCommitMetrics() {
    return ImmutableCommitMetricsResult.builder().build();
  }

  private static CreateTimeSeriesRequest captureRequest(MetricServiceClient client) {
    ArgumentCaptor<CreateTimeSeriesRequest> captor =
        ArgumentCaptor.forClass(CreateTimeSeriesRequest.class);
    verify(client).createTimeSeries(captor.capture());
    return captor.getValue();
  }

  private static TimeSeries seriesByMetricName(CreateTimeSeriesRequest request, String metricName) {
    return request.getTimeSeriesList().stream()
        .filter(ts -> metricName.equals(ts.getMetric().getLabelsOrDefault("metric_name", "")))
        .findFirst()
        .orElseThrow(() -> new AssertionError("metric_name=" + metricName + " not found"));
  }

  private static Set<String> metricNamesIn(CreateTimeSeriesRequest request) {
    return request.getTimeSeriesList().stream()
        .map(ts -> ts.getMetric().getLabelsOrDefault("metric_name", ""))
        .collect(Collectors.toSet());
  }

  @Test
  void reportScan_emitsExpectedSeriesWithFixedLabelSet() {
    MetricServiceClient client = mock(MetricServiceClient.class);

    ScanMetricsResult metrics =
        ImmutableScanMetricsResult.builder()
            .skippedDataFiles(CounterResult.of(Unit.COUNT, 10L))
            .resultDataFiles(CounterResult.of(Unit.COUNT, 3L))
            .totalPlanningDuration(TimerResult.of(TimeUnit.MILLISECONDS, Duration.ofMillis(42), 1))
            .build();

    reporter(client).report(scanReport(metrics));

    CreateTimeSeriesRequest request = captureRequest(client);
    assertThat(request.getName()).isEqualTo(ProjectName.of(PROJECT_ID).toString());

    // Operation + 3 non-null metrics = 4 series total.
    assertThat(request.getTimeSeriesCount()).isEqualTo(4);
    assertThat(metricNamesIn(request))
        .containsExactlyInAnyOrder(
            "scan_operations",
            "skipped_data_files",
            "result_data_files",
            "total_planning_duration_ms");

    TimeSeries scanOps = seriesByMetricName(request, "scan_operations");
    assertThat(scanOps.getMetric().getType()).isEqualTo(METRIC_PREFIX + "/scan_metrics");
    assertThat(scanOps.getMetric().getLabelsMap().keySet())
        .isEqualTo(CloudMonitoringMetricsReporter.SCAN_LABEL_KEYS);
    assertThat(scanOps.getMetric().getLabelsOrThrow("table")).isEqualTo(TABLE_NAME);
    assertThat(scanOps.getPointsList()).hasSize(1);
    assertThat(scanOps.getPoints(0).getValue().getInt64Value()).isEqualTo(1L);

    assertThat(
            seriesByMetricName(request, "skipped_data_files")
                .getPoints(0)
                .getValue()
                .getInt64Value())
        .isEqualTo(10L);
    assertThat(
            seriesByMetricName(request, "result_data_files")
                .getPoints(0)
                .getValue()
                .getInt64Value())
        .isEqualTo(3L);
    assertThat(
            seriesByMetricName(request, "total_planning_duration_ms")
                .getPoints(0)
                .getValue()
                .getInt64Value())
        .isEqualTo(42L);
  }

  @Test
  void reportCommit_emitsExpectedSeriesWithOperationLabel() {
    MetricServiceClient client = mock(MetricServiceClient.class);

    CommitMetricsResult metrics =
        ImmutableCommitMetricsResult.builder()
            .addedDataFiles(CounterResult.of(Unit.COUNT, 5L))
            .totalDuration(TimerResult.of(TimeUnit.MILLISECONDS, Duration.ofMillis(123), 1))
            .build();

    reporter(client).report(commitReport(metrics));

    CreateTimeSeriesRequest request = captureRequest(client);

    assertThat(metricNamesIn(request))
        .containsExactlyInAnyOrder("commit_operations", "added_data_files", "total_duration_ms");

    TimeSeries commitOps = seriesByMetricName(request, "commit_operations");
    assertThat(commitOps.getMetric().getType()).isEqualTo(METRIC_PREFIX + "/commit_metrics");
    assertThat(commitOps.getMetric().getLabelsMap().keySet())
        .isEqualTo(CloudMonitoringMetricsReporter.COMMIT_LABEL_KEYS);
    assertThat(commitOps.getMetric().getLabelsOrThrow("operation")).isEqualTo("append");
    assertThat(
            seriesByMetricName(request, "added_data_files").getPoints(0).getValue().getInt64Value())
        .isEqualTo(5L);
    assertThat(
            seriesByMetricName(request, "total_duration_ms")
                .getPoints(0)
                .getValue()
                .getInt64Value())
        .isEqualTo(123L);
  }

  @Test
  void reportScan_emitsOnlyOperationSeriesWhenAllMetricsAreNull() {
    MetricServiceClient client = mock(MetricServiceClient.class);

    reporter(client).report(scanReport(emptyScanMetrics()));

    CreateTimeSeriesRequest request = captureRequest(client);
    assertThat(request.getTimeSeriesCount()).isEqualTo(1);
    assertThat(metricNamesIn(request)).containsExactly("scan_operations");
  }

  @Test
  void reportCommit_emitsOnlyOperationSeriesWhenAllMetricsAreNull() {
    MetricServiceClient client = mock(MetricServiceClient.class);

    reporter(client).report(commitReport(emptyCommitMetrics()));

    CreateTimeSeriesRequest request = captureRequest(client);
    assertThat(request.getTimeSeriesCount()).isEqualTo(1);
    assertThat(metricNamesIn(request)).containsExactly("commit_operations");
  }

  @Test
  void labelSetIsFixed_noPredicateOrColumnLabels() {
    MetricServiceClient client = mock(MetricServiceClient.class);
    ScanReport report =
        ImmutableScanReport.builder()
            .tableName(TABLE_NAME)
            .snapshotId(1L)
            .filter(Expressions.equal("id", 1))
            .schemaId(0)
            .addProjectedFieldIds(1, 2, 3)
            .addProjectedFieldNames("a", "b", "c")
            .scanMetrics(emptyScanMetrics())
            .build();

    reporter(client).report(report);

    CreateTimeSeriesRequest request = captureRequest(client);
    for (TimeSeries ts : request.getTimeSeriesList()) {
      Map<String, String> labels = ts.getMetric().getLabelsMap();
      assertThat(labels).doesNotContainKey("filter");
      assertThat(labels).doesNotContainKey("queried_field_ids");
      assertThat(labels).doesNotContainKey("queried_column_names");
      assertThat(labels).doesNotContainKey("projected_field_ids");
      assertThat(labels.keySet()).isEqualTo(CloudMonitoringMetricsReporter.SCAN_LABEL_KEYS);
    }
  }

  @Test
  void report_nullReport_isNoOp() {
    MetricServiceClient client = mock(MetricServiceClient.class);

    reporter(client).report(null);

    verify(client, never()).createTimeSeries(any(CreateTimeSeriesRequest.class));
  }

  @Test
  void report_unsupportedReportType_isNoOp() {
    MetricServiceClient client = mock(MetricServiceClient.class);

    reporter(client).report(new UnknownReport());

    verify(client, never()).createTimeSeries(any(CreateTimeSeriesRequest.class));
  }

  @Test
  void report_clientThrows_doesNotPropagate() {
    MetricServiceClient client = mock(MetricServiceClient.class);
    doThrow(new RuntimeException("boom"))
        .when(client)
        .createTimeSeries(any(CreateTimeSeriesRequest.class));

    // Must not throw; Tasks.suppressFailureWhenFinished swallows and logs.
    reporter(client).report(scanReport(emptyScanMetrics()));
  }

  @Test
  void close_closesClientAndIsIdempotent() {
    MetricServiceClient client = mock(MetricServiceClient.class);
    CloudMonitoringMetricsReporter underTest = reporter(client);

    underTest.close();
    underTest.close();

    verify(client, times(1)).close();
  }

  @Test
  void report_afterClose_isNoOp() {
    MetricServiceClient client = mock(MetricServiceClient.class);
    CloudMonitoringMetricsReporter underTest = reporter(client);

    underTest.close();
    underTest.report(scanReport(emptyScanMetrics()));

    verify(client, never()).createTimeSeries(any(CreateTimeSeriesRequest.class));
  }

  @Test
  void initialize_missingProjectId_throws() {
    GCPProperties bad = new GCPProperties(ImmutableMap.of());
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                new CloudMonitoringMetricsReporter(
                    bad, mock(MetricServiceClient.class), MoreExecutors.newDirectExecutorService()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(GCPProperties.GCP_MONITORING_PROJECT_ID);
  }

  @Test
  void scanAndCommitMetricMaps_keysAreStableSnakeCase() {
    // Snapshot the expected metric names to catch accidental renames/removals.
    assertThat(CloudMonitoringMetricsReporter.SCAN_METRICS.keySet())
        .containsExactlyInAnyOrder(
            "total_planning_duration_ms",
            "result_data_files",
            "result_delete_files",
            "total_data_manifests",
            "total_delete_manifests",
            "scanned_data_manifests",
            "skipped_data_manifests",
            "total_file_size_bytes",
            "total_delete_file_size_bytes",
            "skipped_data_files",
            "skipped_delete_files",
            "scanned_delete_manifests",
            "skipped_delete_manifests",
            "indexed_delete_files",
            "equality_delete_files",
            "positional_delete_files",
            "dvs");

    assertThat(CloudMonitoringMetricsReporter.COMMIT_METRICS.keySet())
        .containsExactlyInAnyOrder(
            "total_duration_ms",
            "attempts",
            "added_data_files",
            "removed_data_files",
            "total_data_files",
            "added_delete_files",
            "added_equality_delete_files",
            "added_positional_delete_files",
            "added_dvs",
            "removed_delete_files",
            "removed_equality_delete_files",
            "removed_positional_delete_files",
            "removed_dvs",
            "total_delete_files",
            "added_records",
            "removed_records",
            "total_records",
            "added_files_size_bytes",
            "removed_files_size_bytes",
            "total_files_size_bytes",
            "added_positional_deletes",
            "removed_positional_deletes",
            "total_positional_deletes",
            "added_equality_deletes",
            "removed_equality_deletes",
            "total_equality_deletes",
            "manifests_created",
            "manifests_replaced",
            "manifests_kept",
            "manifest_entries_processed");
  }

  // ---------------------------------------------------------------------------------------------
  // Predicate / projection metric tests
  // ---------------------------------------------------------------------------------------------

  private static ScanReport scanReportWith(
      org.apache.iceberg.expressions.Expression filter, java.util.List<String> projection) {
    ImmutableScanReport.Builder builder =
        ImmutableScanReport.builder()
            .tableName(TABLE_NAME)
            .snapshotId(1L)
            .filter(filter)
            .schemaId(0)
            .scanMetrics(ImmutableScanMetricsResult.builder().build());
    for (int i = 0; i < projection.size(); i++) {
      builder.addProjectedFieldIds(i + 1);
      builder.addProjectedFieldNames(projection.get(i));
    }
    return builder.build();
  }

  private static java.util.List<TimeSeries> seriesByMetricType(
      CreateTimeSeriesRequest request, String metricType) {
    return request.getTimeSeriesList().stream()
        .filter(ts -> metricType.equals(ts.getMetric().getType()))
        .collect(Collectors.toList());
  }

  @Test
  void predicateTrackingDisabled_emitsNoPredicateOrProjectionSeries() {
    MetricServiceClient client = mock(MetricServiceClient.class);

    reporter(client)
        .report(
            scanReportWith(
                org.apache.iceberg.expressions.Expressions.equal("user_id", 1),
                java.util.List.of("user_id", "ts")));

    CreateTimeSeriesRequest request = captureRequest(client);
    assertThat(seriesByMetricType(request, METRIC_PREFIX + "/predicate_columns")).isEmpty();
    assertThat(seriesByMetricType(request, METRIC_PREFIX + "/projection_columns")).isEmpty();
  }

  @Test
  void predicateTrackingEnabled_emitsPredicateAndProjectionSeriesWithFixedLabels() {
    MetricServiceClient client = mock(MetricServiceClient.class);

    reporter(propertiesWithPredicateTracking(), client)
        .report(
            scanReportWith(
                org.apache.iceberg.expressions.Expressions.and(
                    org.apache.iceberg.expressions.Expressions.equal("user_id", 1),
                    org.apache.iceberg.expressions.Expressions.greaterThan("ts", 100)),
                java.util.List.of("user_id", "ts", "value")));

    CreateTimeSeriesRequest request = captureRequest(client);

    java.util.List<TimeSeries> predicateSeries =
        seriesByMetricType(request, METRIC_PREFIX + "/predicate_columns");
    java.util.List<TimeSeries> projectionSeries =
        seriesByMetricType(request, METRIC_PREFIX + "/projection_columns");

    assertThat(predicateSeries).hasSize(2);
    for (TimeSeries ts : predicateSeries) {
      assertThat(ts.getMetric().getLabelsMap().keySet())
          .isEqualTo(CloudMonitoringMetricsReporter.PREDICATE_LABEL_KEYS);
      assertThat(ts.getPoints(0).getValue().getInt64Value()).isEqualTo(1L);
    }

    assertThat(predicateColumnsByOpClass(predicateSeries))
        .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("user_id", "point", "ts", "range"));

    assertThat(projectionSeries).hasSize(3);
    for (TimeSeries ts : projectionSeries) {
      assertThat(ts.getMetric().getLabelsMap().keySet())
          .isEqualTo(CloudMonitoringMetricsReporter.PROJECTION_LABEL_KEYS);
      assertThat(ts.getPoints(0).getValue().getInt64Value()).isEqualTo(1L);
    }
    assertThat(
            projectionSeries.stream()
                .map(ts -> ts.getMetric().getLabelsOrThrow("column"))
                .collect(Collectors.toSet()))
        .containsExactlyInAnyOrder("user_id", "ts", "value");
  }

  @Test
  void predicateTracking_dropsEmissionWhenColumnCapExceeded() {
    MetricServiceClient client = mock(MetricServiceClient.class);

    // 3 unique columns; cap = 2 → predicate/projection emission must be dropped.
    reporter(propertiesWithColumnCap(2), client)
        .report(
            scanReportWith(
                org.apache.iceberg.expressions.Expressions.equal("a", 1),
                java.util.List.of("b", "c")));

    CreateTimeSeriesRequest request = captureRequest(client);
    assertThat(seriesByMetricType(request, METRIC_PREFIX + "/predicate_columns")).isEmpty();
    assertThat(seriesByMetricType(request, METRIC_PREFIX + "/projection_columns")).isEmpty();
    // Base scan series still present.
    assertThat(seriesByMetricType(request, METRIC_PREFIX + "/scan_metrics")).isNotEmpty();
  }

  @Test
  void opClassBuckets_coverAllRelevantOperations() {
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.EQ))
        .isEqualTo("point");
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.IN))
        .isEqualTo("point");
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.LT))
        .isEqualTo("range");
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.LT_EQ))
        .isEqualTo("range");
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.GT))
        .isEqualTo("range");
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.GT_EQ))
        .isEqualTo("range");
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.IS_NULL))
        .isEqualTo("null");
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.NOT_NULL))
        .isEqualTo("null");
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.NOT_EQ))
        .isEqualTo("other");
    assertThat(
            CloudMonitoringMetricsReporter.PredicateColumnExtractor.opClass(
                org.apache.iceberg.expressions.Expression.Operation.STARTS_WITH))
        .isEqualTo("other");
  }

  private static Map<String, String> predicateColumnsByOpClass(java.util.List<TimeSeries> series) {
    Map<String, String> out = new java.util.HashMap<>();
    for (TimeSeries ts : series) {
      out.put(
          ts.getMetric().getLabelsOrThrow("column"), ts.getMetric().getLabelsOrThrow("op_class"));
    }
    return out;
  }

  private static final class UnknownReport implements org.apache.iceberg.metrics.MetricsReport {
    // Anonymous report type that is neither ScanReport nor CommitReport.
  }
}
