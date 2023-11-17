---
title: "Metrics Reporting"
url: metrics-reporting
aliases:
    - "tables/metrics-reporting"
menu:
    main:
        parent: Tables
        identifier: metrics_reporting
        weight: 0
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

As of 1.1.0 Iceberg supports the [`MetricsReporter`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/metrics/MetricsReporter.html) and the [`MetricsReport`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/metrics/MetricsReport.html) APIs. These two APIs allow expressing different metrics reports while supporting a pluggable way of reporting these reports.

## Type of Reports

### ScanReport
A [`ScanReport`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/metrics/ScanReport.html) carries metrics being collected during scan planning against a given table. Amongst some general information about the involved table, such as the snapshot id or the table name, it includes metrics like:
* total scan planning duration
* number of data/delete files included in the result
* number of data/delete manifests scanned/skipped
* number of data/delete files scanned/skipped
* number of equality/positional delete files scanned


### CommitReport
A [`CommitReport`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/metrics/CommitReport.html) carries metrics being collected after committing changes to a table (aka producing a snapshot). Amongst some general information about the involved table, such as the snapshot id or the table name, it includes metrics like:
* total duration
* number of attempts required for the commit to succeed
* number of added/removed data/delete files
* number of added/removed equality/positional delete files
* number of added/removed equality/positional deletes


## Available Metrics Reporters

### [`LoggingMetricsReporter`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/metrics/LoggingMetricsReporter.html)

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


### [`RESTMetricsReporter`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/rest/RESTMetricsReporter.html)

This is the default when using the [`RESTCatalog`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/rest/RESTCatalog.html) and its purpose is to send metrics to a REST server at the `/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` endpoint as defined in the [REST OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).

Sending metrics via REST can be controlled with the `rest-metrics-reporting-enabled` (defaults to `true`) property.


## Implementing a custom Metrics Reporter

Implementing the [`MetricsReporter`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/metrics/MetricsReporter.html) API gives full flexibility in dealing with incoming [`MetricsReport`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/metrics/MetricsReport.html) instances. For example, it would be possible to send results to a Prometheus endpoint or any other observability framework/system.

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

The [catalog property](../configuration#catalog-properties) `metrics-reporter-impl` allows registering a given [`MetricsReporter`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/metrics/MetricsReporter.html) by specifying its fully-qualified class name, e.g. `metrics-reporter-impl=org.apache.iceberg.metrics.InMemoryMetricsReporter`.

### Via the Java API during Scan planning

Independently of the [`MetricsReporter`](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/metrics/MetricsReporter.html) being registered at the catalog level via the `metrics-reporter-impl` property, it is also possible to supply additional reporters during scan planning as shown below:

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