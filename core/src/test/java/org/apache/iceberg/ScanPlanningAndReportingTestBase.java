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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.Test;

public abstract class ScanPlanningAndReportingTestBase<
        ScanT extends Scan<ScanT, T, G>, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends TableTestBase {

  private final TestMetricsReporter reporter = new TestMetricsReporter();

  public ScanPlanningAndReportingTestBase() {
    super(2);
  }

  protected abstract ScanT newScan(Table table);

  @Test
  public void noDuplicatesInScanContext() {
    TableScanContext context = TableScanContext.empty();
    assertThat(context.metricsReporter()).isInstanceOf(LoggingMetricsReporter.class);

    MetricsReporter first = report -> {};
    MetricsReporter second = report -> {};

    context = context.reportWith(first).reportWith(first);
    assertThat(context.metricsReporter()).isSameAs(first);

    context = context.reportWith(second);
    assertThat(context.metricsReporter())
        .as("should be a CompositeMetricsReporter")
        .extracting("reporters")
        .asInstanceOf(InstanceOfAssertFactories.collection(MetricsReporter.class))
        .hasSize(2)
        .containsExactlyInAnyOrder(first, second);

    context = context.reportWith(LoggingMetricsReporter.instance()).reportWith(second);
    assertThat(context.metricsReporter())
        .as("should be a CompositeMetricsReporter")
        .extracting("reporters")
        .asInstanceOf(InstanceOfAssertFactories.collection(MetricsReporter.class))
        .hasSize(3)
        .containsExactlyInAnyOrder(LoggingMetricsReporter.instance(), first, second);
  }

  @Test
  public void scanningWithMultipleReporters() throws IOException {
    String tableName = "scan-with-multiple-reporters";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);
    table.newAppend().appendFile(FILE_A).commit();
    table.refresh();

    AtomicInteger reportedCount = new AtomicInteger();
    ScanT tableScan =
        newScan(table)
            .metricsReporter((MetricsReporter) -> reportedCount.getAndIncrement())
            .metricsReporter((MetricsReporter) -> reportedCount.getAndIncrement());
    try (CloseableIterable<T> fileScanTasks = tableScan.planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    // verify if metrics are reported to default reporter
    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(1L);
    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    assertThat(result.resultDataFiles().value()).isEqualTo(1);

    assertThat(reportedCount.get()).isEqualTo(2);
  }

  @Test
  public void scanningWithMultipleDataManifests() throws IOException {
    String tableName = "multiple-data-manifests";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newAppend().appendFile(FILE_D).commit();
    table.refresh();
    ScanT tableScan = newScan(table);

    // should be 3 files
    try (CloseableIterable<T> fileScanTasks = tableScan.planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();

    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(2L);
    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    assertThat(result.resultDataFiles().value()).isEqualTo(3);
    assertThat(result.resultDeleteFiles().value()).isEqualTo(0);
    assertThat(result.scannedDataManifests().value()).isEqualTo(2);
    assertThat(result.scannedDeleteManifests().value()).isEqualTo(0);
    assertThat(result.skippedDataManifests().value()).isEqualTo(0);
    assertThat(result.skippedDeleteManifests().value()).isEqualTo(0);
    assertThat(result.totalDataManifests().value()).isEqualTo(2);
    assertThat(result.totalDeleteManifests().value()).isEqualTo(0);
    assertThat(result.totalFileSizeInBytes().value()).isEqualTo(30L);
    assertThat(result.totalDeleteFileSizeInBytes().value()).isEqualTo(0L);
    assertThat(result.skippedDataFiles().value()).isEqualTo(0);
    assertThat(result.skippedDeleteFiles().value()).isEqualTo(0);

    // we should hit only a single data manifest and only a single data file
    try (CloseableIterable<FileScanTask> fileScanTasks =
        table.newScan().filter(Expressions.equal("data", "1")).planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    scanReport = reporter.lastReport();
    result = scanReport.scanMetrics();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(2L);
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    assertThat(result.resultDataFiles().value()).isEqualTo(1);
    assertThat(result.resultDeleteFiles().value()).isEqualTo(0);
    assertThat(result.scannedDataManifests().value()).isEqualTo(1);
    assertThat(result.scannedDeleteManifests().value()).isEqualTo(0);
    assertThat(result.skippedDataManifests().value()).isEqualTo(1);
    assertThat(result.skippedDeleteManifests().value()).isEqualTo(0);
    assertThat(result.totalDataManifests().value()).isEqualTo(2);
    assertThat(result.totalDeleteManifests().value()).isEqualTo(0);
    assertThat(result.totalFileSizeInBytes().value()).isEqualTo(10L);
    assertThat(result.totalDeleteFileSizeInBytes().value()).isEqualTo(0L);
    assertThat(result.skippedDataFiles().value()).isEqualTo(0);
    assertThat(result.skippedDeleteFiles().value()).isEqualTo(0);
  }

  @Test
  public void scanningWithDeletes() throws IOException {
    Table table =
        TestTables.create(
            tableDir,
            "scan-planning-with-deletes",
            SCHEMA,
            SPEC,
            SortOrder.unsorted(),
            formatVersion,
            reporter);

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_B_DELETES).commit();
    ScanT tableScan = newScan(table);

    try (CloseableIterable<T> fileScanTasks = tableScan.planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo("scan-planning-with-deletes");
    assertThat(scanReport.snapshotId()).isEqualTo(2L);
    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    assertThat(result.resultDataFiles().value()).isEqualTo(3);
    assertThat(result.resultDeleteFiles().value()).isEqualTo(2);
    assertThat(result.scannedDataManifests().value()).isEqualTo(1);
    assertThat(result.scannedDeleteManifests().value()).isEqualTo(1);
    assertThat(result.skippedDataManifests().value()).isEqualTo(0);
    assertThat(result.skippedDeleteManifests().value()).isEqualTo(0);
    assertThat(result.totalDataManifests().value()).isEqualTo(1);
    assertThat(result.totalDeleteManifests().value()).isEqualTo(1);
    assertThat(result.totalFileSizeInBytes().value()).isEqualTo(30L);
    assertThat(result.totalDeleteFileSizeInBytes().value()).isEqualTo(20L);
    assertThat(result.skippedDataFiles().value()).isEqualTo(0);
    assertThat(result.skippedDeleteFiles().value()).isEqualTo(0);
    assertThat(result.indexedDeleteFiles().value()).isEqualTo(2);
    assertThat(result.equalityDeleteFiles().value()).isEqualTo(0);
    assertThat(result.positionalDeleteFiles().value()).isEqualTo(2);
  }

  @Test
  public void scanningWithSkippedDataFiles() throws IOException {
    String tableName = "scan-planning-with-skipped-data-files";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_D).commit();
    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_A2).commit();
    table.newAppend().appendFile(FILE_C).commit();
    ScanT tableScan = newScan(table);

    List<FileScanTask> fileTasks = Lists.newArrayList();
    try (CloseableIterable<T> scanTasks =
        tableScan.filter(Expressions.equal("data", "1")).planFiles()) {
      scanTasks.forEach(task -> fileTasks.add((FileScanTask) task));
    }
    assertThat(fileTasks)
        .singleElement()
        .satisfies(task -> assertThat(task.file().path()).isEqualTo(FILE_D.path()));

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(3L);
    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.skippedDataFiles().value()).isEqualTo(1);
    assertThat(result.skippedDeleteFiles().value()).isEqualTo(0);
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    assertThat(result.resultDataFiles().value()).isEqualTo(1);
    assertThat(result.resultDeleteFiles().value()).isEqualTo(0);
    assertThat(result.scannedDataManifests().value()).isEqualTo(1);
    assertThat(result.scannedDeleteManifests().value()).isEqualTo(0);
    assertThat(result.skippedDataManifests().value()).isEqualTo(2);
    assertThat(result.skippedDeleteManifests().value()).isEqualTo(0);
    assertThat(result.totalDataManifests().value()).isEqualTo(3);
    assertThat(result.totalDeleteManifests().value()).isEqualTo(0);
    assertThat(result.totalFileSizeInBytes().value()).isEqualTo(10L);
    assertThat(result.totalDeleteFileSizeInBytes().value()).isEqualTo(0L);
  }

  @Test
  public void scanningWithSkippedDeleteFiles() throws IOException {
    String tableName = "scan-planning-with-skipped-delete-files";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_D).commit();
    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_A2).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_D2_DELETES).commit();
    table.newRowDelta().addDeletes(FILE_B_DELETES).addDeletes(FILE_C2_DELETES).commit();
    ScanT tableScan = newScan(table);

    List<FileScanTask> fileTasks = Lists.newArrayList();
    try (CloseableIterable<T> scanTasks =
        tableScan.filter(Expressions.equal("data", "1")).planFiles()) {
      scanTasks.forEach(task -> fileTasks.add((FileScanTask) task));
    }
    assertThat(fileTasks)
        .singleElement()
        .satisfies(task -> assertThat(task.file().path()).isEqualTo(FILE_D.path()));

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(4L);
    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.totalPlanningDuration().totalDuration()).isGreaterThan(Duration.ZERO);
    assertThat(result.resultDataFiles().value()).isEqualTo(1);
    assertThat(result.resultDeleteFiles().value()).isEqualTo(1);
    assertThat(result.skippedDataFiles().value()).isEqualTo(1);
    assertThat(result.skippedDeleteFiles().value()).isEqualTo(1);
    assertThat(result.scannedDataManifests().value()).isEqualTo(1);
    assertThat(result.scannedDeleteManifests().value()).isEqualTo(1);
    assertThat(result.skippedDataManifests().value()).isEqualTo(1);
    assertThat(result.skippedDeleteManifests().value()).isEqualTo(1);
    assertThat(result.totalDataManifests().value()).isEqualTo(2);
    assertThat(result.totalDeleteManifests().value()).isEqualTo(2);
    assertThat(result.totalFileSizeInBytes().value()).isEqualTo(10L);
    assertThat(result.totalDeleteFileSizeInBytes().value()).isEqualTo(10L);
    assertThat(result.indexedDeleteFiles().value()).isEqualTo(1);
    assertThat(result.equalityDeleteFiles().value()).isEqualTo(1);
    assertThat(result.positionalDeleteFiles().value()).isEqualTo(0);
  }

  @Test
  public void scanningWithEqualityAndPositionalDeleteFiles() throws IOException {
    String tableName = "scan-planning-with-eq-and-pos-delete-files";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);
    table.newAppend().appendFile(FILE_A).commit();
    // FILE_A_DELETES = positionalDelete / FILE_A2_DELETES = equalityDelete
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();
    ScanT tableScan = newScan(table);

    try (CloseableIterable<T> fileScanTasks =
        tableScan.filter(Expressions.equal("data", "6")).planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    ScanMetricsResult result = scanReport.scanMetrics();
    assertThat(result.indexedDeleteFiles().value()).isEqualTo(2);
    assertThat(result.equalityDeleteFiles().value()).isEqualTo(1);
    assertThat(result.positionalDeleteFiles().value()).isEqualTo(1);
  }

  static class TestMetricsReporter implements MetricsReporter {
    private final List<MetricsReport> reports = Lists.newArrayList();
    // this is mainly so that we see scan reports being logged during tests
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
