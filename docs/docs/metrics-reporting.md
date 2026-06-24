---
title: "Metrics Reporting"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Metrics Reporting

As of 1.1.0 Iceberg supports the [`MetricsReporter`](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/metrics/MetricsReporter.java) and the [`MetricsReport`](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/metrics/MetricsReport.java) APIs. These two APIs allow expressing different metrics reports while supporting a pluggable way of reporting these reports.

## Type of Reports

### ScanReport
A [`ScanReport`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/metrics/ScanReport.java) carries metrics being collected during scan planning against a given table. Amongst some general information about the involved table, such as the snapshot id or the table name, it includes metrics like:

* total scan planning duration
* number of data/delete files included in the result
* number of data/delete manifests scanned/skipped
* number of data/delete files scanned/skipped
* number of equality/positional delete files scanned

### CommitReport
A [`CommitReport`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/metrics/CommitReport.java) carries metrics being collected after committing changes to a table (aka producing a snapshot). Amongst some general information about the involved table, such as the snapshot id or the table name, it includes metrics like:

* total duration
* number of attempts required for the commit to succeed
* number of added/removed data/delete files
* number of added/removed equality/positional delete files
* number of added/removed equality/positional deletes

## Available Metrics Reporters

### [`LoggingMetricsReporter`](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/metrics/LoggingMetricsReporter.java)

This is the default metrics reporter when nothing else is configured and its purpose is to log results to the log file. Example output would look as shown below:

```
INFO org.apache.iceberg.metrics.LoggingMetricsReporter - Received metrics report: 
ScanReport{
    tableName=scan-planning-with-eq-and-pos-delete-files, 
    snapshotId=2, 
    filter=ref(name="data") == "(hash-27fa7cc0)", 
    schemaId=0, 
    projectedFieldIds=[1, 2], 
    projectedFieldNames=[id, data], 
    scanMetrics=ScanMetricsResult{
        totalPlanningDuration=TimerResult{timeUnit=NANOSECONDS, totalDuration=PT0.026569404S, count=1}, 
        resultDataFiles=CounterResult{unit=COUNT, value=1}, 
        resultDeleteFiles=CounterResult{unit=COUNT, value=2}, 
        totalDataManifests=CounterResult{unit=COUNT, value=1}, 
        totalDeleteManifests=CounterResult{unit=COUNT, value=1}, 
        scannedDataManifests=CounterResult{unit=COUNT, value=1}, 
        skippedDataManifests=CounterResult{unit=COUNT, value=0}, 
        totalFileSizeInBytes=CounterResult{unit=BYTES, value=10}, 
        totalDeleteFileSizeInBytes=CounterResult{unit=BYTES, value=20}, 
        skippedDataFiles=CounterResult{unit=COUNT, value=0}, 
        skippedDeleteFiles=CounterResult{unit=COUNT, value=0}, 
        scannedDeleteManifests=CounterResult{unit=COUNT, value=1}, 
        skippedDeleteManifests=CounterResult{unit=COUNT, value=0}, 
        indexedDeleteFiles=CounterResult{unit=COUNT, value=2}, 
        equalityDeleteFiles=CounterResult{unit=COUNT, value=1}, 
        positionalDeleteFiles=CounterResult{unit=COUNT, value=1}}, 
    metadata={
        iceberg-version=Apache Iceberg 1.4.0-SNAPSHOT (commit 4868d2823004c8c256a50ea7c25cff94314cc135)}}
```

```
INFO org.apache.iceberg.metrics.LoggingMetricsReporter - Received metrics report: 
CommitReport{
    tableName=scan-planning-with-eq-and-pos-delete-files, 
    snapshotId=1, 
    sequenceNumber=1, 
    operation=append, 
    commitMetrics=CommitMetricsResult{
        totalDuration=TimerResult{timeUnit=NANOSECONDS, totalDuration=PT0.098429626S, count=1}, 
        attempts=CounterResult{unit=COUNT, value=1}, 
        addedDataFiles=CounterResult{unit=COUNT, value=1}, 
        removedDataFiles=null, 
        totalDataFiles=CounterResult{unit=COUNT, value=1}, 
        addedDeleteFiles=null, 
        addedEqualityDeleteFiles=null, 
        addedPositionalDeleteFiles=null, 
        removedDeleteFiles=null, 
        removedEqualityDeleteFiles=null, 
        removedPositionalDeleteFiles=null, 
        totalDeleteFiles=CounterResult{unit=COUNT, value=0}, 
        addedRecords=CounterResult{unit=COUNT, value=1}, 
        removedRecords=null, 
        totalRecords=CounterResult{unit=COUNT, value=1}, 
        addedFilesSizeInBytes=CounterResult{unit=BYTES, value=10}, 
        removedFilesSizeInBytes=null, 
        totalFilesSizeInBytes=CounterResult{unit=BYTES, value=10}, 
        addedPositionalDeletes=null, 
        removedPositionalDeletes=null, 
        totalPositionalDeletes=CounterResult{unit=COUNT, value=0}, 
        addedEqualityDeletes=null, 
        removedEqualityDeletes=null, 
        totalEqualityDeletes=CounterResult{unit=COUNT, value=0}}, 
    metadata={
        iceberg-version=Apache Iceberg 1.4.0-SNAPSHOT (commit 4868d2823004c8c256a50ea7c25cff94314cc135)}}
```

### [`RESTMetricsReporter`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/rest/RESTMetricsReporter.java)

This is the default when using the [`RESTCatalog`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/rest/RESTCatalog.java) and its purpose is to send metrics to a REST server at the `/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` endpoint as defined in the [REST OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).

Sending metrics via REST can be controlled with the `rest-metrics-reporting-enabled` (defaults to `true`) property.

### [`OtelMetricsReporter`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/metrics/OtelMetricsReporter.java)

Exports [ScanReport](#scanreport) and [CommitReport](#commitreport) through the [OpenTelemetry](https://opentelemetry.io/) API as `iceberg.scan.*` and `iceberg.commit.*` metrics. Any OTLP-compatible backend (Prometheus, CloudWatch, Datadog, Grafana Cloud, Honeycomb, etc.) can receive them through a host-owned OpenTelemetry SDK.

#### Host responsibilities

`OtelMetricsReporter` does not own the OpenTelemetry SDK. It calls `GlobalOpenTelemetry.get().getMeter("org.apache.iceberg")` in `initialize(...)` and reports through whatever SDK the host application has registered. If no SDK has been registered, OpenTelemetry returns the no-op implementation and metric calls are silently dropped — the standard OpenTelemetry API contract.

The host application is therefore responsible for:

1. Adding the OpenTelemetry **API**, **SDK**, and a **metric exporter** matching the target backend to the runtime classpath. Iceberg core compiles against `opentelemetry-api` only; it does not bundle the SDK or any exporter.
2. Building and registering an `OpenTelemetrySdk`, either via the [OpenTelemetry Java agent](https://opentelemetry.io/docs/zero-code/java/agent/) (which auto-instruments the SDK at JVM startup) or programmatically with `OpenTelemetrySdk.builder()...buildAndRegisterGlobal()`.
3. Configuring the exporter's endpoint, credentials, batching, retry, and resource attributes — typically via the [standard OpenTelemetry environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/) (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_HEADERS`, ...).

Because the host owns the SDK, Iceberg has no reporter-specific catalog properties for endpoint, protocol, headers, intervals, or resource attributes. The catalog only needs to know the reporter class, which can be specified via:

```
metrics-reporter-impl=org.apache.iceberg.metrics.OtelMetricsReporter
```

#### Attribute set

By default the reporter attaches the following attributes to every emitted metric point:

| Metrics | Default attributes |
|---|---|
| Scan metrics (`iceberg.scan.*`) | `iceberg.table.name` |
| Commit metrics (`iceberg.commit.*`) | `iceberg.table.name`, `iceberg.operation` |

The attribute set is configurable via the `iceberg.otel.metrics.attributes` catalog property, which takes a comma-separated allowlist of attribute short names. Recognized names:

- `table-name` — emits `iceberg.table.name`
- `schema-id` — emits `iceberg.schema.id` (opt-in; useful for correlating scan performance with schema evolution)
- `operation` — emits `iceberg.operation`

Attributes whose short names are not listed are omitted from emitted metric points. To additionally include the schema id:

```
iceberg.otel.metrics.attributes=table-name,schema-id,operation
```

To omit `iceberg.table.name` entirely in deployments with a very large number of tables:

```
iceberg.otel.metrics.attributes=operation
```

To emit metrics with no attributes at all (single aggregate time series per metric), use `none` or an empty string:

```
iceberg.otel.metrics.attributes=none
```

When the property is not set, the default attribute set above is used.

The snapshot id is deliberately not exposed as a metric attribute because snapshot ids are monotonically increasing and unique per commit; including them would create a new time series for every commit and risk unbounded cardinality in any time-series backend.

#### Getting started

To use `OtelMetricsReporter`, add the OpenTelemetry API, SDK, and a metric exporter to the runtime classpath and register the SDK before the Iceberg catalog is loaded. Iceberg core compiles against `opentelemetry-api` only; it does not bundle the SDK or any exporter.

**Dependencies**

Add the API, SDK, and an exporter matching the target backend. For example, with Gradle:

```groovy
dependencies {
  runtimeOnly "io.opentelemetry:opentelemetry-api:1.61.0"
  runtimeOnly "io.opentelemetry:opentelemetry-sdk:1.61.0"

  // Pick one exporter for your backend
  runtimeOnly "io.opentelemetry:opentelemetry-exporter-otlp:1.61.0"
  // or "io.opentelemetry:opentelemetry-exporter-prometheus:1.61.0-alpha"
}
```

For Spark `spark-submit`, the same artifacts can be passed via `--packages`:

```bash
spark-submit \
  --packages io.opentelemetry:opentelemetry-api:1.61.0,io.opentelemetry:opentelemetry-sdk:1.61.0,io.opentelemetry:opentelemetry-exporter-otlp:1.61.0 \
  ...
```

For Flink, add the same jars to the `lib/` directory of the Flink distribution. For Trino, OpenTelemetry is part of the platform classpath.

**Registering the SDK**

Register the SDK once at application startup, either programmatically or via the [OpenTelemetry Java agent](https://opentelemetry.io/docs/zero-code/java/agent/). A programmatic example:

```java
SdkMeterProvider meterProvider =
    SdkMeterProvider.builder()
        .setResource(
            Resource.getDefault().toBuilder()
                .put(AttributeKey.stringKey("service.name"), "my-iceberg-app")
                .build())
        .registerMetricReader(
            PeriodicMetricReader.builder(
                    OtlpHttpMetricExporter.builder()
                        .setEndpoint("http://collector.example:4318/v1/metrics")
                        .build())
                .setInterval(Duration.ofSeconds(30))
                .build())
        .build();

OpenTelemetrySdk.builder()
    .setMeterProvider(meterProvider)
    .buildAndRegisterGlobal();
```

When using the Java agent, the agent registers the SDK automatically and no programmatic bootstrap is required.

**Sending metrics to a backend**

The reporter is backend-neutral. Any OTLP-compatible backend works directly; for backends that do not accept OTLP natively, route through an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/). Below are common patterns.

*OpenTelemetry Collector* -- export OTLP to a local Collector and let the Collector forward to one or more backends:

```java
OtlpGrpcMetricExporter.builder()
    .setEndpoint("http://localhost:4317")
    .build();
```

A minimal Collector configuration (`otel-config.yaml`):

```yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: 0.0.0.0:4317 }
      http: { endpoint: 0.0.0.0:4318 }
processors:
  batch: {}
exporters:
  debug: { verbosity: detailed }
service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [debug]
```

Replace the `debug` exporter with any [Collector exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter) for the target backend (Prometheus, CloudWatch, Datadog, Grafana Cloud, etc.).

*Prometheus (pull)* -- expose a `/metrics` endpoint via the OpenTelemetry Prometheus exporter and let Prometheus scrape it:

```java
SdkMeterProvider.builder()
    .registerMetricReader(
        PrometheusHttpServer.builder().setPort(9464).build())
    .build();
```

*Prometheus (push via Collector)* -- export OTLP to a Collector configured with the `prometheusremotewrite` exporter.

*Amazon CloudWatch* -- CloudWatch's OTLP endpoint requires SigV4 signing. Route through a Collector with the [`sigv4auth`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/sigv4authextension) extension:

```yaml
extensions:
  sigv4auth:
    service: monitoring
    region: us-west-2
exporters:
  otlphttp:
    endpoint: "https://monitoring.us-west-2.amazonaws.com"
    auth:
      authenticator: sigv4auth
service:
  extensions: [sigv4auth]
  pipelines:
    metrics:
      exporters: [otlphttp]
```

#### Emitted metrics

The reporter maps every field from [`ScanMetricsResult`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/metrics/ScanMetricsResult.java) and [`CommitMetricsResult`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/metrics/CommitMetricsResult.java) to an OpenTelemetry instrument. Fields that are `null` in a given report are silently skipped (no time series is created).

**Scan metrics** (from `ScanReport.scanMetrics()`):

| Metric | Type | Unit | Source field |
|---|---|---|---|
| `iceberg.scan.planning.duration` | histogram | ms | `totalPlanningDuration()` |
| `iceberg.scan.result.data_files` | sum | | `resultDataFiles()` |
| `iceberg.scan.result.delete_files` | sum | | `resultDeleteFiles()` |
| `iceberg.scan.data_manifests.total` | sum | | `totalDataManifests()` |
| `iceberg.scan.delete_manifests.total` | sum | | `totalDeleteManifests()` |
| `iceberg.scan.data_manifests.scanned` | sum | | `scannedDataManifests()` |
| `iceberg.scan.data_manifests.skipped` | sum | | `skippedDataManifests()` |
| `iceberg.scan.file_size.bytes` | sum | By | `totalFileSizeInBytes()` |
| `iceberg.scan.delete_file_size.bytes` | sum | By | `totalDeleteFileSizeInBytes()` |
| `iceberg.scan.data_files.skipped` | sum | | `skippedDataFiles()` |
| `iceberg.scan.delete_files.skipped` | sum | | `skippedDeleteFiles()` |
| `iceberg.scan.delete_manifests.scanned` | sum | | `scannedDeleteManifests()` |
| `iceberg.scan.delete_manifests.skipped` | sum | | `skippedDeleteManifests()` |
| `iceberg.scan.delete_files.indexed` | sum | | `indexedDeleteFiles()` |
| `iceberg.scan.delete_files.equality` | sum | | `equalityDeleteFiles()` |
| `iceberg.scan.delete_files.positional` | sum | | `positionalDeleteFiles()` |
| `iceberg.scan.dvs` | sum | | `dvs()` |

**Commit metrics** (from `CommitReport.commitMetrics()`):

| Metric | Type | Unit | Source field |
|---|---|---|---|
| `iceberg.commit.duration` | histogram | ms | `totalDuration()` |
| `iceberg.commit.attempts` | sum | | `attempts()` |
| `iceberg.commit.data_files.added` | sum | | `addedDataFiles()` |
| `iceberg.commit.data_files.removed` | sum | | `removedDataFiles()` |
| `iceberg.commit.data_files.total` | sum | | `totalDataFiles()` |
| `iceberg.commit.delete_files.added` | sum | | `addedDeleteFiles()` |
| `iceberg.commit.delete_files.equality.added` | sum | | `addedEqualityDeleteFiles()` |
| `iceberg.commit.delete_files.positional.added` | sum | | `addedPositionalDeleteFiles()` |
| `iceberg.commit.dvs.added` | sum | | `addedDVs()` |
| `iceberg.commit.delete_files.removed` | sum | | `removedDeleteFiles()` |
| `iceberg.commit.delete_files.equality.removed` | sum | | `removedEqualityDeleteFiles()` |
| `iceberg.commit.delete_files.positional.removed` | sum | | `removedPositionalDeleteFiles()` |
| `iceberg.commit.dvs.removed` | sum | | `removedDVs()` |
| `iceberg.commit.delete_files.total` | sum | | `totalDeleteFiles()` |
| `iceberg.commit.records.added` | sum | | `addedRecords()` |
| `iceberg.commit.records.removed` | sum | | `removedRecords()` |
| `iceberg.commit.records.total` | sum | | `totalRecords()` |
| `iceberg.commit.file_size.added_bytes` | sum | By | `addedFilesSizeInBytes()` |
| `iceberg.commit.file_size.removed_bytes` | sum | By | `removedFilesSizeInBytes()` |
| `iceberg.commit.file_size.total_bytes` | sum | By | `totalFilesSizeInBytes()` |
| `iceberg.commit.positional_deletes.added` | sum | | `addedPositionalDeletes()` |
| `iceberg.commit.positional_deletes.removed` | sum | | `removedPositionalDeletes()` |
| `iceberg.commit.positional_deletes.total` | sum | | `totalPositionalDeletes()` |
| `iceberg.commit.equality_deletes.added` | sum | | `addedEqualityDeletes()` |
| `iceberg.commit.equality_deletes.removed` | sum | | `removedEqualityDeletes()` |
| `iceberg.commit.equality_deletes.total` | sum | | `totalEqualityDeletes()` |
| `iceberg.commit.manifests.created` | sum | | `manifestsCreated()` |
| `iceberg.commit.manifests.replaced` | sum | | `manifestsReplaced()` |
| `iceberg.commit.manifests.kept` | sum | | `manifestsKept()` |
| `iceberg.commit.manifest_entries.processed` | sum | | `manifestEntriesProcessed()` |

The default attribute set is described in the [Attribute set](#attribute-set) section above.

## Implementing a custom Metrics Reporter

Implementing the [`MetricsReporter`](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/metrics/MetricsReporter.java) API gives full flexibility in dealing with incoming [`MetricsReport`](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/metrics/MetricsReport.java) instances. For example, it would be possible to send results to a Prometheus endpoint or any other observability framework/system.

Below is a short example illustrating an `InMemoryMetricsReporter` that stores reports in a list and makes them available:
```java
public class InMemoryMetricsReporter implements MetricsReporter {

  private List<MetricsReport> metricsReports = Lists.newArrayList();

  @Override
  public void report(MetricsReport report) {
    metricsReports.add(report);
  }

  public List<MetricsReport> reports() {
    return metricsReports;
  }
}
```

## Registering a custom Metrics Reporter

### Via Catalog Configuration

The [catalog property](catalog-properties.md) `metrics-reporter-impl` allows registering a given [`MetricsReporter`](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/metrics/MetricsReporter.java) by specifying its fully-qualified class name, e.g. `metrics-reporter-impl=org.apache.iceberg.metrics.InMemoryMetricsReporter`.

### Via the Java API during Scan planning

Independently of the [`MetricsReporter`](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/metrics/MetricsReporter.java) being registered at the catalog level via the `metrics-reporter-impl` property, it is also possible to supply additional reporters during scan planning as shown below:

```java
TableScan tableScan = 
    table
        .newScan()
        .metricsReporter(customReporterOne)
        .metricsReporter(customReporterTwo);

try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
  // ...
}
```
