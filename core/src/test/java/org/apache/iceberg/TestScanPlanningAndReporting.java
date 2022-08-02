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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.LoggingScanReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReporter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Test;

public class TestScanPlanningAndReporting extends TableTestBase {

  private final TestScanReporter reporter = new TestScanReporter();

  public TestScanPlanningAndReporting() {
    super(2);
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
    TableScan tableScan = table.newScan();

    // should be 3 files
    try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();

    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(2L);
    assertThat(scanReport.filter()).isEqualTo(Expressions.alwaysTrue().toString());
    assertThat(scanReport.scanMetrics().totalPlanningDuration().totalDuration())
        .isGreaterThan(Duration.ZERO);
    assertThat(scanReport.scanMetrics().resultDataFiles().value()).isEqualTo(3);
    assertThat(scanReport.scanMetrics().resultDeleteFiles().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().scannedDataManifests().value()).isEqualTo(2);
    assertThat(scanReport.scanMetrics().skippedDataManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalDataManifests().value()).isEqualTo(2);
    assertThat(scanReport.scanMetrics().totalDeleteManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalFileSizeInBytes().value()).isEqualTo(30L);
    assertThat(scanReport.scanMetrics().totalDeleteFileSizeInBytes().value()).isEqualTo(0L);

    // we should hit only a single data manifest and only a single data file
    try (CloseableIterable<FileScanTask> fileScanTasks =
        table.newScan().filter(Expressions.equal("data", "1")).planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(2L);
    assertThat(scanReport.scanMetrics().totalPlanningDuration().totalDuration())
        .isGreaterThan(Duration.ZERO);
    assertThat(scanReport.scanMetrics().resultDataFiles().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().resultDeleteFiles().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().scannedDataManifests().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().skippedDataManifests().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().totalDataManifests().value()).isEqualTo(2);
    assertThat(scanReport.scanMetrics().totalDeleteManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalFileSizeInBytes().value()).isEqualTo(10L);
    assertThat(scanReport.scanMetrics().totalDeleteFileSizeInBytes().value()).isEqualTo(0L);
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
    TableScan tableScan = table.newScan();

    try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo("scan-planning-with-deletes");
    assertThat(scanReport.snapshotId()).isEqualTo(2L);
    assertThat(scanReport.scanMetrics().totalPlanningDuration().totalDuration())
        .isGreaterThan(Duration.ZERO);
    assertThat(scanReport.scanMetrics().resultDataFiles().value()).isEqualTo(3);
    assertThat(scanReport.scanMetrics().resultDeleteFiles().value()).isEqualTo(2);
    assertThat(scanReport.scanMetrics().scannedDataManifests().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().skippedDataManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalDataManifests().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().totalDeleteManifests().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().totalFileSizeInBytes().value()).isEqualTo(30L);
    assertThat(scanReport.scanMetrics().totalDeleteFileSizeInBytes().value()).isEqualTo(20L);
  }

  private static class TestScanReporter implements ScanReporter {
    private final List<ScanReport> reports = Lists.newArrayList();
    // this is mainly so that we see scan reports being logged during tests
    private final LoggingScanReporter delegate = new LoggingScanReporter();

    @Override
    public void reportScan(ScanReport scanReport) {
      reports.add(scanReport);
      delegate.reportScan(scanReport);
    }

    public ScanReport lastReport() {
      if (reports.isEmpty()) {
        return null;
      }
      return reports.get(reports.size() - 1);
    }
  }
}
