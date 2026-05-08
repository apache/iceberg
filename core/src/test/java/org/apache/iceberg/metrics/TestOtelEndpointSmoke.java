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
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Vendor-neutral E2E smoke test for {@link OtelMetricsReporter}. Builds an {@link
 * OpenTelemetrySdk}, registers it as the global SDK, then exports synthetic ScanReport and
 * CommitReport against any OTLP-compatible backend (e.g. ADOT Collector, Databricks Zerobus,
 * Datadog OTLP, Grafana Cloud OTLP). Only runs when {@code OTEL_SMOKE_ENABLED=true}.
 *
 * <p>Required env vars when enabled:
 *
 * <ul>
 *   <li>{@code OTEL_SMOKE_ENDPOINT} — full OTLP endpoint URL
 *   <li>{@code OTEL_SMOKE_PROTOCOL} — {@code grpc} or {@code http/protobuf}
 * </ul>
 *
 * <p>Optional env vars:
 *
 * <ul>
 *   <li>{@code OTEL_SMOKE_HEADERS} — comma-separated {@code key=value} pairs (e.g. for bearer auth:
 *       {@code Authorization=Bearer <token>})
 *   <li>{@code OTEL_SMOKE_SERVICE_NAME} — defaults to {@code iceberg-smoke-test}
 *   <li>{@code OTEL_SMOKE_EXPORT_INTERVAL_MS} — defaults to {@code 2000}
 *   <li>{@code OTEL_SMOKE_FLUSH_WAIT_MS} — sleep before close, defaults to {@code 5000}
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "OTEL_SMOKE_ENABLED", matches = "true")
public class TestOtelEndpointSmoke {

  @Test
  public void exportToOtlpEndpoint() throws Exception {
    String endpoint = requireEnv("OTEL_SMOKE_ENDPOINT");
    String protocol = requireEnv("OTEL_SMOKE_PROTOCOL");
    String headers = System.getenv("OTEL_SMOKE_HEADERS");
    String serviceName = envOrDefault("OTEL_SMOKE_SERVICE_NAME", "iceberg-smoke-test");
    long exportIntervalMs = Long.parseLong(envOrDefault("OTEL_SMOKE_EXPORT_INTERVAL_MS", "2000"));
    long flushWaitMs = Long.parseLong(envOrDefault("OTEL_SMOKE_FLUSH_WAIT_MS", "5000"));

    GlobalOpenTelemetry.resetForTest();
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder()
            .setResource(Resource.getDefault().toBuilder().put("service.name", serviceName).build())
            .registerMetricReader(
                PeriodicMetricReader.builder(buildExporter(protocol, endpoint, headers))
                    .setInterval(Duration.ofMillis(exportIntervalMs))
                    .build())
            .build();
    OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
    GlobalOpenTelemetry.set(sdk);

    OtelMetricsReporter reporter = new OtelMetricsReporter();
    try {
      reporter.initialize(ImmutableMap.of());

      ScanReport scanReport =
          ImmutableScanReport.builder()
              .tableName("smoke.test_table")
              .snapshotId(1001L)
              .filter(Expressions.alwaysTrue())
              .schemaId(1)
              .projectedFieldIds(ImmutableList.of(1, 2))
              .projectedFieldNames(ImmutableList.of("id", "data"))
              .scanMetrics(
                  ImmutableScanMetricsResult.builder()
                      .totalPlanningDuration(
                          TimerResult.of(TimeUnit.NANOSECONDS, Duration.ofMillis(123), 1))
                      .resultDataFiles(CounterResult.of(Unit.COUNT, 7))
                      .resultDeleteFiles(CounterResult.of(Unit.COUNT, 1))
                      .scannedDataManifests(CounterResult.of(Unit.COUNT, 3))
                      .skippedDataManifests(CounterResult.of(Unit.COUNT, 0))
                      .totalFileSizeInBytes(CounterResult.of(Unit.BYTES, 4_096_000L))
                      .build())
              .metadata(ImmutableMap.of("env", "smoke"))
              .build();
      reporter.report(scanReport);

      CommitReport commitReport =
          ImmutableCommitReport.builder()
              .tableName("smoke.test_table")
              .snapshotId(1002L)
              .sequenceNumber(2L)
              .operation("append")
              .commitMetrics(
                  ImmutableCommitMetricsResult.builder()
                      .totalDuration(
                          TimerResult.of(TimeUnit.NANOSECONDS, Duration.ofMillis(231), 1))
                      .attempts(CounterResult.of(Unit.COUNT, 1))
                      .addedDataFiles(CounterResult.of(Unit.COUNT, 4))
                      .removedDataFiles(CounterResult.of(Unit.COUNT, 0))
                      .addedRecords(CounterResult.of(Unit.COUNT, 12345))
                      .addedFilesSizeInBytes(CounterResult.of(Unit.BYTES, 2_048_000L))
                      .build())
              .metadata(ImmutableMap.of("env", "smoke"))
              .build();
      reporter.report(commitReport);

      // Wait so PeriodicMetricReader exports at least once before close().
      Thread.sleep(flushWaitMs);
    } finally {
      reporter.close();
      meterProvider.close();
      GlobalOpenTelemetry.resetForTest();
    }
  }

  private static MetricExporter buildExporter(String protocol, String endpoint, String headers) {
    if ("grpc".equals(protocol)) {
      OtlpGrpcMetricExporterBuilder builder =
          OtlpGrpcMetricExporter.builder().setEndpoint(endpoint);
      addHeaders(headers, builder::addHeader);
      return builder.build();
    } else {
      OtlpHttpMetricExporterBuilder builder =
          OtlpHttpMetricExporter.builder().setEndpoint(endpoint);
      addHeaders(headers, builder::addHeader);
      return builder.build();
    }
  }

  private static void addHeaders(
      String headers, java.util.function.BiConsumer<String, String> add) {
    if (headers == null || headers.isEmpty()) {
      return;
    }
    for (String pair : headers.split(",")) {
      int eq = pair.indexOf('=');
      if (eq > 0) {
        add.accept(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
      }
    }
  }

  private static String requireEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException("Missing env var: " + name);
    }
    return value;
  }

  private static String envOrDefault(String name, String fallback) {
    String value = System.getenv(name);
    return (value == null || value.isEmpty()) ? fallback : value;
  }
}
