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

### Filtering which tables are reported

Reports forwarded to the configured `MetricsReporter` can be filtered on two levels, either of which may be used on its own:

| Property | Effect |
|---|---|
| `metrics-reporter.namespace.include` | Forward only reports for tables whose namespace matches; drop the rest. |
| `metrics-reporter.namespace.exclude` | Drop reports for tables whose namespace matches; forward the rest. |
| `metrics-reporter.table-name.include` | Forward only reports whose table name matches; drop the rest. |
| `metrics-reporter.table-name.exclude` | Drop reports whose table name matches; forward the rest. |

Each property accepts a comma-separated list of Java regular expressions. Namespace patterns are matched against the table's namespace levels joined by dots, with the catalog name removed; table-name patterns are matched against `ScanReport.tableName()` and `CommitReport.tableName()`, which include the catalog name.

Patterns are matched against the **entire** namespace or table name rather than any substring of it. This matters in practice: `prod\..*` matches `prod.db.table` but not `production.db.table` or `prod_sandbox.db.table`, which a substring match would wrongly accept.

An `exclude` match always wins over an `include` match, and the two levels are applied independently — a report must survive both to be forwarded. When no property is set, behavior is identical to today: every report is forwarded, and no wrapper is instantiated. Empty values are treated as not set, to avoid accidentally silencing all metrics on misconfiguration.

Filtering by namespace is less error-prone than filtering by table name, because a namespace is part of a table's identity rather than a naming convention: a table added to an included namespace later is picked up automatically, and no table outside that namespace can match by accident. Table-name patterns remain available for cases a namespace cannot express, such as excluding a few noisy tables inside an otherwise interesting namespace.

For example, to report on the `prod` and `analytics` namespaces while dropping benchmark tables anywhere:

```
metrics-reporter-impl=org.apache.iceberg.metrics.LoggingMetricsReporter
metrics-reporter.namespace.include=prod,analytics
metrics-reporter.table-name.exclude=.*\.bench_.*
```

The filter applies uniformly to all `MetricsReporter` implementations (`LoggingMetricsReporter`, `RESTMetricsReporter`, and custom user-supplied ones). Reports whose subtype does not identify a table (i.e. anything other than `ScanReport` and `CommitReport`) are forwarded without filtering.

Namespace filtering requires the catalog name, which the catalog supplies when it loads the reporter. Configuring a namespace filter where no catalog name is available fails at initialization with a clear error rather than silently dropping reports.

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
