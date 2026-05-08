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

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestOtelMetricsReporter {

  private InMemoryMetricReader metricReader;
  private SdkMeterProvider meterProvider;
  private OtelMetricsReporter reporter;

  @BeforeEach
  public void before() {
    metricReader = InMemoryMetricReader.create();
    meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
    reporter = new OtelMetricsReporter(openTelemetry);
  }

  @AfterEach
  public void after() {
    if (meterProvider != null) {
      meterProvider.close();
    }
  }

  @Test
  public void testScanReportMetrics() {
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(42L)
            .filter(Expressions.alwaysTrue())
            .schemaId(1)
            .projectedFieldIds(ImmutableList.of(1, 2, 3))
            .projectedFieldNames(ImmutableList.of("id", "name", "value"))
            .scanMetrics(
                ImmutableScanMetricsResult.builder()
                    .totalPlanningDuration(
                        TimerResult.of(TimeUnit.NANOSECONDS, Duration.ofMillis(150), 1))
                    .resultDataFiles(CounterResult.of(Unit.COUNT, 10))
                    .resultDeleteFiles(CounterResult.of(Unit.COUNT, 2))
                    .scannedDataManifests(CounterResult.of(Unit.COUNT, 5))
                    .skippedDataManifests(CounterResult.of(Unit.COUNT, 3))
                    .totalFileSizeInBytes(CounterResult.of(Unit.BYTES, 1024000))
                    .build())
            .metadata(ImmutableMap.of())
            .build();

    reporter.report(scanReport);

    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    assertThat(metrics).isNotEmpty();

    assertMetricExists(metrics, "iceberg.scan.planning.duration");
    assertMetricExists(metrics, "iceberg.scan.result.data_files");
    assertMetricExists(metrics, "iceberg.scan.result.delete_files");
    assertMetricExists(metrics, "iceberg.scan.data_manifests.scanned");
    assertMetricExists(metrics, "iceberg.scan.data_manifests.skipped");
    assertMetricExists(metrics, "iceberg.scan.file_size.bytes");

    assertSumValue(metrics, "iceberg.scan.result.data_files", 10);
    assertSumValue(metrics, "iceberg.scan.result.delete_files", 2);
    assertSumValue(metrics, "iceberg.scan.data_manifests.scanned", 5);
    assertSumValue(metrics, "iceberg.scan.data_manifests.skipped", 3);
    assertSumValue(metrics, "iceberg.scan.file_size.bytes", 1024000);
  }

  @Test
  public void testCommitReportMetrics() {
    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(43L)
            .sequenceNumber(1L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .totalDuration(TimerResult.of(TimeUnit.NANOSECONDS, Duration.ofMillis(200), 1))
                    .attempts(CounterResult.of(Unit.COUNT, 1))
                    .addedDataFiles(CounterResult.of(Unit.COUNT, 5))
                    .removedDataFiles(CounterResult.of(Unit.COUNT, 0))
                    .addedRecords(CounterResult.of(Unit.COUNT, 1000))
                    .addedFilesSizeInBytes(CounterResult.of(Unit.BYTES, 512000))
                    .build())
            .metadata(ImmutableMap.of())
            .build();

    reporter.report(commitReport);

    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    assertThat(metrics).isNotEmpty();

    assertMetricExists(metrics, "iceberg.commit.duration");
    assertMetricExists(metrics, "iceberg.commit.attempts");
    assertMetricExists(metrics, "iceberg.commit.data_files.added");
    assertMetricExists(metrics, "iceberg.commit.records.added");
    assertMetricExists(metrics, "iceberg.commit.file_size.added_bytes");

    assertSumValue(metrics, "iceberg.commit.attempts", 1);
    assertSumValue(metrics, "iceberg.commit.data_files.added", 5);
    assertSumValue(metrics, "iceberg.commit.records.added", 1000);
    assertSumValue(metrics, "iceberg.commit.file_size.added_bytes", 512000);
  }

  @Test
  public void testNullMetricsAreHandled() {
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(44L)
            .filter(Expressions.alwaysTrue())
            .schemaId(1)
            .projectedFieldIds(ImmutableList.of())
            .projectedFieldNames(ImmutableList.of())
            .scanMetrics(ImmutableScanMetricsResult.builder().build())
            .metadata(ImmutableMap.of())
            .build();

    reporter.report(scanReport);

    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    assertThat(metrics).isEmpty();
  }

  @Test
  public void testMultipleReports() {
    for (int i = 0; i < 3; i++) {
      CommitReport commitReport =
          ImmutableCommitReport.builder()
              .tableName("test_db.test_table")
              .snapshotId(50L + i)
              .sequenceNumber(i + 1L)
              .operation("append")
              .commitMetrics(
                  ImmutableCommitMetricsResult.builder()
                      .attempts(CounterResult.of(Unit.COUNT, 1))
                      .addedDataFiles(CounterResult.of(Unit.COUNT, 10))
                      .build())
              .metadata(ImmutableMap.of())
              .build();
      reporter.report(commitReport);
    }

    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    assertSumValue(metrics, "iceberg.commit.data_files.added", 30);
    assertSumValue(metrics, "iceberg.commit.attempts", 3);
  }

  @Test
  public void testInitializeFallsBackToGlobalOpenTelemetry() {
    // No global SDK registered → GlobalOpenTelemetry returns no-op; init must not throw and
    // subsequent reports must be silently dropped.
    OtelMetricsReporter noopReporter = new OtelMetricsReporter();
    noopReporter.initialize(ImmutableMap.of());

    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(60L)
            .sequenceNumber(1L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .attempts(CounterResult.of(Unit.COUNT, 1))
                    .build())
            .metadata(ImmutableMap.of())
            .build();
    noopReporter.report(commitReport);
    noopReporter.close();
  }

  private static void assertMetricExists(Collection<MetricData> metrics, String name) {
    assertThat(metrics).extracting(MetricData::getName).contains(name);
  }

  private static void assertSumValue(
      Collection<MetricData> metrics, String name, long expectedValue) {
    MetricData metric =
        metrics.stream()
            .filter(m -> m.getName().equals(name))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Metric not found: " + name));

    long actualSum =
        metric.getLongSumData().getPoints().stream().mapToLong(LongPointData::getValue).sum();
    assertThat(actualSum)
        .as("Expected sum of '%s' to be %d", name, expectedValue)
        .isEqualTo(expectedValue);
  }
}
