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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestIncrementalScanPlanningAndReporting extends TestBase {

  private final TestMetricsReporter reporter = new TestMetricsReporter();

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> formatVersions() {
    return TestHelpers.ALL_VERSIONS;
  }

  @TestTemplate
  public void incrementalAppendScanEmitsScanReport() throws IOException {
    String tableName = "incremental-append-scan-report";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);

    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();
    table.refresh();

    IncrementalAppendScan scan =
        table.newIncrementalAppendScan().fromSnapshotExclusive(snapshotId1);

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      tasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(snapshotId2);

    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    assertThat(result.resultDataFiles().value()).isEqualTo(1);
    assertThat(result.totalDataManifests().value()).isEqualTo(1);
    assertThat(result.scannedDataManifests().value()).isEqualTo(1);
    assertThat(result.skippedDataManifests().value()).isEqualTo(0);
    assertThat(result.totalFileSizeInBytes().value()).isEqualTo(10L);
  }

  @TestTemplate
  public void incrementalAppendScanWithMultipleSnapshots() throws IOException {
    String tableName = "incremental-append-multi-snapshot";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);

    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();
    table.newAppend().appendFile(FILE_C).commit();
    long snapshotId3 = table.currentSnapshot().snapshotId();
    table.refresh();

    IncrementalAppendScan scan =
        table.newIncrementalAppendScan().fromSnapshotExclusive(snapshotId1);

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      tasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(snapshotId3);

    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    assertThat(result.resultDataFiles().value()).isEqualTo(2);
    assertThat(result.totalDataManifests().value()).isEqualTo(2);
    assertThat(result.scannedDataManifests().value()).isEqualTo(2);
  }

  @TestTemplate
  public void incrementalAppendScanFromInclusiveEmitsScanReport() throws IOException {
    String tableName = "incremental-append-inclusive-report";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);

    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();
    table.refresh();

    IncrementalAppendScan scan =
        table.newIncrementalAppendScan().fromSnapshotInclusive(snapshotId1);

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      tasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(snapshotId2);

    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    // fromSnapshotInclusive means FILE_A + FILE_B
    assertThat(result.resultDataFiles().value()).isEqualTo(2);
    assertThat(result.totalDataManifests().value()).isEqualTo(2);
  }

  @TestTemplate
  public void incrementalAppendScanWithCustomReporter() throws IOException {
    String tableName = "incremental-append-custom-reporter";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);

    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();
    table.refresh();

    TestMetricsReporter customReporter = new TestMetricsReporter();
    IncrementalAppendScan scan =
        table
            .newIncrementalAppendScan()
            .fromSnapshotExclusive(snapshotId1)
            .metricsReporter(customReporter);

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      tasks.forEach(task -> {});
    }

    // Both the table-level and custom reporters should receive the report
    assertThat(reporter.lastReport()).isNotNull();
    assertThat(customReporter.lastReport()).isNotNull();
    assertThat(customReporter.lastReport().tableName()).isEqualTo(tableName);
  }

  @TestTemplate
  public void incrementalAppendScanEmptyResult() throws IOException {
    String tableName = "incremental-append-empty";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);

    // Only non-append operations after the start snapshot
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();
    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).commit();
    table.refresh();

    IncrementalAppendScan scan =
        table.newIncrementalAppendScan().fromSnapshotExclusive(snapshotId1);

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      tasks.forEach(task -> {});
    }

    // ScanReport should still be emitted even with no append snapshots
    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);

    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    assertThat(result.resultDataFiles().value()).isEqualTo(0);
  }

  static class TestMetricsReporter implements MetricsReporter {
    private final List<MetricsReport> reports = Lists.newArrayList();
    private final LoggingMetricsReporter delegate = LoggingMetricsReporter.instance();

    @Override
    public void report(MetricsReport report) {
      reports.add(report);
      delegate.report(report);
    }

    public ScanReport lastReport() {
      if (reports.isEmpty()) {
        return null;
      }

      return (ScanReport) reports.get(reports.size() - 1);
    }

    public CommitReport lastCommitReport() {
      if (reports.isEmpty()) {
        return null;
      }

      return (CommitReport) reports.get(reports.size() - 1);
    }
  }
}
