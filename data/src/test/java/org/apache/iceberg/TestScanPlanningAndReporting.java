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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.metrics.LoggingScanReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReporter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Test;

public class TestScanPlanningAndReporting extends TableTestBase {

  private final TestScanReporter reporter = new TestScanReporter();

  public TestScanPlanningAndReporting() {
    super(2);
  }

  @Test
  public void testScanPlanningWithReport() throws IOException {
    String tableName = "simple-scan-planning";
    Table table = createTableWithCustomRecords(tableName);
    TableScan tableScan = table.newScan();

    // should be 3 files
    try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();

    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(1L);
    assertThat(scanReport.filter()).isEqualTo(Expressions.alwaysTrue());
    assertThat(scanReport.scanMetrics().totalPlanningDuration().totalDuration())
        .isGreaterThan(Duration.ZERO);
    assertThat(scanReport.scanMetrics().resultDataFiles().value()).isEqualTo(3);
    assertThat(scanReport.scanMetrics().resultDeleteFiles().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().scannedDataManifests().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().skippedDataManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalDataManifests().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().totalDeleteManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalFileSizeInBytes().value()).isEqualTo(1850L);
    assertThat(scanReport.scanMetrics().totalDeleteFileSizeInBytes().value()).isEqualTo(0L);

    // should be 1 file
    try (CloseableIterable<FileScanTask> fileScanTasks =
        tableScan.filter(Expressions.lessThan("x", "30")).planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo(tableName);
    assertThat(scanReport.snapshotId()).isEqualTo(1L);
    assertThat(scanReport.scanMetrics().totalPlanningDuration().totalDuration())
        .isGreaterThan(Duration.ZERO);
    assertThat(scanReport.scanMetrics().resultDataFiles().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().resultDeleteFiles().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().scannedDataManifests().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().skippedDataManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalDataManifests().value()).isEqualTo(1);
    assertThat(scanReport.scanMetrics().totalDeleteManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalFileSizeInBytes().value()).isEqualTo(616L);
    assertThat(scanReport.scanMetrics().totalDeleteFileSizeInBytes().value()).isEqualTo(0L);
  }

  private Table createTableWithCustomRecords(String tableName) throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()), required(2, "x", Types.StringType.get()));

    Table table =
        TestTables.create(
            tableDir,
            tableName,
            schema,
            PartitionSpec.builderFor(schema).build(),
            SortOrder.unsorted(),
            formatVersion,
            reporter);
    GenericRecord record = GenericRecord.create(schema);
    record.setField("id", 1);
    record.setField("x", "23");

    GenericRecord record2 = record.copy(ImmutableMap.of("id", 2, "x", "30"));
    GenericRecord record3 = record.copy(ImmutableMap.of("id", 3, "x", "45"));
    GenericRecord record4 = record.copy(ImmutableMap.of("id", 4, "x", "51"));

    DataFile dataFile = writeParquetFile(table, Arrays.asList(record, record3));
    DataFile dataFile2 = writeParquetFile(table, Arrays.asList(record2));
    DataFile dataFile3 = writeParquetFile(table, Arrays.asList(record4));
    table.newFastAppend().appendFile(dataFile).appendFile(dataFile2).appendFile(dataFile3).commit();
    return table;
  }

  @Test
  public void deleteScanning() throws IOException {
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

  @Test
  public void multipleDataManifests() throws IOException {
    Table table =
        TestTables.create(
            tableDir,
            "multiple-data-manifests",
            SCHEMA,
            SPEC,
            SortOrder.unsorted(),
            formatVersion,
            reporter);

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    TableScan tableScan = table.newScan();

    try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    ScanReport scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo("multiple-data-manifests");
    assertThat(scanReport.snapshotId()).isEqualTo(2L);
    assertThat(scanReport.scanMetrics().totalPlanningDuration().totalDuration())
        .isGreaterThan(Duration.ZERO);
    assertThat(scanReport.scanMetrics().resultDataFiles().value()).isEqualTo(4);
    assertThat(scanReport.scanMetrics().resultDeleteFiles().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().scannedDataManifests().value()).isEqualTo(2);
    assertThat(scanReport.scanMetrics().skippedDataManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalDataManifests().value()).isEqualTo(2);
    assertThat(scanReport.scanMetrics().totalDeleteManifests().value()).isEqualTo(0);
    assertThat(scanReport.scanMetrics().totalFileSizeInBytes().value()).isEqualTo(40L);
    assertThat(scanReport.scanMetrics().totalDeleteFileSizeInBytes().value()).isEqualTo(0L);

    // we should hit only a single data manifest and only a single data file
    try (CloseableIterable<FileScanTask> fileScanTasks =
        tableScan.filter(Expressions.equal("data", "1")).planFiles()) {
      fileScanTasks.forEach(task -> {});
    }

    scanReport = reporter.lastReport();
    assertThat(scanReport).isNotNull();
    assertThat(scanReport.tableName()).isEqualTo("multiple-data-manifests");
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

  private DataFile writeParquetFile(Table table, List<GenericRecord> records) throws IOException {
    File parquetFile = temp.newFile();
    assertTrue(parquetFile.delete());
    FileAppender<GenericRecord> appender =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build();
    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    return DataFiles.builder(table.spec())
        .withInputFile(localInput(parquetFile))
        .withMetrics(appender.metrics())
        .withFormat(FileFormat.PARQUET)
        .build();
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
