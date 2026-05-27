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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
  public void testDefaultAttributeSet() {
    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(42L)
            .filter(Expressions.alwaysTrue())
            .schemaId(1)
            .projectedFieldIds(ImmutableList.of(1))
            .projectedFieldNames(ImmutableList.of("id"))
            .scanMetrics(
                ImmutableScanMetricsResult.builder()
                    .resultDataFiles(CounterResult.of(Unit.COUNT, 1))
                    .build())
            .metadata(ImmutableMap.of())
            .build();
    reporter.report(scanReport);

    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(43L)
            .sequenceNumber(1L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .attempts(CounterResult.of(Unit.COUNT, 1))
                    .build())
            .metadata(ImmutableMap.of())
            .build();
    reporter.report(commitReport);

    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    Set<String> allAttributeKeys =
        metrics.stream()
            .flatMap(m -> m.getLongSumData().getPoints().stream())
            .flatMap(p -> p.getAttributes().asMap().keySet().stream())
            .map(AttributeKey::getKey)
            .collect(Collectors.toSet());

    assertThat(allAttributeKeys)
        .as("snapshot.id is excluded by design")
        .doesNotContain("iceberg.snapshot.id");

    assertThat(allAttributeKeys)
        .as("schema.id is opt-in and not part of the default attribute set")
        .doesNotContain("iceberg.schema.id");

    assertThat(allAttributeKeys)
        .as("default attributes are present on the recorded data points")
        .contains("iceberg.table.name", "iceberg.operation");
  }

  @Test
  public void testAttributesAllowlistOptInSchemaId() {
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
    OtelMetricsReporter customReporter =
        new OtelMetricsReporter(
            openTelemetry,
            ImmutableMap.of("iceberg.otel.metrics.attributes", "table-name,schema-id,operation"));

    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(80L)
            .filter(Expressions.alwaysTrue())
            .schemaId(7)
            .projectedFieldIds(ImmutableList.of(1))
            .projectedFieldNames(ImmutableList.of("id"))
            .scanMetrics(
                ImmutableScanMetricsResult.builder()
                    .resultDataFiles(CounterResult.of(Unit.COUNT, 1))
                    .build())
            .metadata(ImmutableMap.of())
            .build();
    customReporter.report(scanReport);

    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    Set<String> allAttributeKeys =
        metrics.stream()
            .flatMap(m -> m.getLongSumData().getPoints().stream())
            .flatMap(p -> p.getAttributes().asMap().keySet().stream())
            .map(AttributeKey::getKey)
            .collect(Collectors.toSet());

    assertThat(allAttributeKeys)
        .as("schema.id is emitted when explicitly included in the allowlist")
        .contains("iceberg.schema.id", "iceberg.table.name");
  }

  @Test
  public void testAttributesAllowlistExcludesTableName() {
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
    OtelMetricsReporter customReporter =
        new OtelMetricsReporter(
            openTelemetry, ImmutableMap.of("iceberg.otel.metrics.attributes", "operation"));

    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(90L)
            .sequenceNumber(1L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .attempts(CounterResult.of(Unit.COUNT, 1))
                    .build())
            .metadata(ImmutableMap.of())
            .build();
    customReporter.report(commitReport);

    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    Set<String> allAttributeKeys =
        metrics.stream()
            .flatMap(m -> m.getLongSumData().getPoints().stream())
            .flatMap(p -> p.getAttributes().asMap().keySet().stream())
            .map(AttributeKey::getKey)
            .collect(Collectors.toSet());

    assertThat(allAttributeKeys)
        .as("table.name is omitted when not in the allowlist")
        .doesNotContain("iceberg.table.name");
    assertThat(allAttributeKeys)
        .as("operation remains when in the allowlist")
        .contains("iceberg.operation");
  }

  @Test
  public void testAttributesAllowlistEmptyEmitsNoAttributes() {
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
    OtelMetricsReporter customReporter =
        new OtelMetricsReporter(
            openTelemetry, ImmutableMap.of("iceberg.otel.metrics.attributes", ""));

    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(95L)
            .sequenceNumber(1L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .attempts(CounterResult.of(Unit.COUNT, 1))
                    .build())
            .metadata(ImmutableMap.of())
            .build();
    customReporter.report(commitReport);

    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    Set<String> allAttributeKeys =
        metrics.stream()
            .flatMap(m -> m.getLongSumData().getPoints().stream())
            .flatMap(p -> p.getAttributes().asMap().keySet().stream())
            .map(AttributeKey::getKey)
            .collect(Collectors.toSet());

    assertThat(allAttributeKeys).as("empty allowlist emits no attributes").isEmpty();
  }

  @Test
  public void testAttributesAllowlistUnknownTokenIgnored() {
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
    OtelMetricsReporter customReporter =
        new OtelMetricsReporter(
            openTelemetry,
            ImmutableMap.of("iceberg.otel.metrics.attributes", "table-name, bogus , operation"));

    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(99L)
            .sequenceNumber(1L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .attempts(CounterResult.of(Unit.COUNT, 1))
                    .build())
            .metadata(ImmutableMap.of())
            .build();
    customReporter.report(commitReport);

    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    Set<String> allAttributeKeys =
        metrics.stream()
            .flatMap(m -> m.getLongSumData().getPoints().stream())
            .flatMap(p -> p.getAttributes().asMap().keySet().stream())
            .map(AttributeKey::getKey)
            .collect(Collectors.toSet());

    assertThat(allAttributeKeys)
        .as("known names in the allowlist are honored; unknown tokens are silently ignored")
        .containsExactlyInAnyOrder("iceberg.table.name", "iceberg.operation");
  }

  @Test
  public void testCombinedWithAnotherReporter() {
    InMemoryMetricsReporter inMemory = new InMemoryMetricsReporter();
    MetricsReporter combined = MetricsReporters.combine(reporter, inMemory);

    ScanReport scanReport =
        ImmutableScanReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(70L)
            .filter(Expressions.alwaysTrue())
            .schemaId(1)
            .projectedFieldIds(ImmutableList.of(1))
            .projectedFieldNames(ImmutableList.of("id"))
            .scanMetrics(
                ImmutableScanMetricsResult.builder()
                    .resultDataFiles(CounterResult.of(Unit.COUNT, 7))
                    .build())
            .metadata(ImmutableMap.of())
            .build();
    combined.report(scanReport);

    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("test_db.test_table")
            .snapshotId(71L)
            .sequenceNumber(1L)
            .operation("append")
            .commitMetrics(
                ImmutableCommitMetricsResult.builder()
                    .attempts(CounterResult.of(Unit.COUNT, 1))
                    .addedRecords(CounterResult.of(Unit.COUNT, 123))
                    .build())
            .metadata(ImmutableMap.of())
            .build();
    combined.report(commitReport);

    // OtelMetricsReporter side
    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    assertSumValue(metrics, "iceberg.scan.result.data_files", 7);
    assertSumValue(metrics, "iceberg.commit.records.added", 123);

    // The other reporter side — InMemoryMetricsReporter remembers the most recent report only,
    // so the commit report (reported last) is what we should see.
    assertThat(inMemory.commitReport()).isSameAs(commitReport);
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
