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
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.ScanPlanningAndReportingTestBase.TestMetricsReporter;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCommitReporting extends TestBase {

  private final TestMetricsReporter reporter = new TestMetricsReporter();

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(2);
  }

  @TestTemplate
  public void addAndDeleteDataFiles() {
    String tableName = "add-and-delete-data-files";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);
    table.newAppend().appendFile(FILE_A).appendFile(FILE_D).commit();

    CommitReport report = reporter.lastCommitReport();
    assertThat(report).isNotNull();
    assertThat(report.operation()).isEqualTo("append");
    assertThat(report.snapshotId()).isEqualTo(1L);
    assertThat(report.sequenceNumber()).isEqualTo(1L);
    assertThat(report.tableName()).isEqualTo(tableName);

    CommitMetricsResult metrics = report.commitMetrics();
    assertThat(metrics.addedDataFiles().value()).isEqualTo(2L);
    assertThat(metrics.totalDataFiles().value()).isEqualTo(2L);

    assertThat(metrics.addedRecords().value()).isEqualTo(2L);
    assertThat(metrics.totalRecords().value()).isEqualTo(2L);

    assertThat(metrics.addedFilesSizeInBytes().value()).isEqualTo(20L);
    assertThat(metrics.totalFilesSizeInBytes().value()).isEqualTo(20L);

    // now remove those 2 data files
    table.newDelete().deleteFile(FILE_A).deleteFile(FILE_D).commit();
    report = reporter.lastCommitReport();
    assertThat(report).isNotNull();
    assertThat(report.operation()).isEqualTo("delete");
    assertThat(report.snapshotId()).isEqualTo(2L);
    assertThat(report.sequenceNumber()).isEqualTo(2L);
    assertThat(report.tableName()).isEqualTo(tableName);

    metrics = report.commitMetrics();
    assertThat(metrics.removedDataFiles().value()).isEqualTo(2L);
    assertThat(metrics.totalDeleteFiles().value()).isEqualTo(0L);

    assertThat(metrics.removedRecords().value()).isEqualTo(2L);
    assertThat(metrics.totalRecords().value()).isEqualTo(0L);

    assertThat(metrics.removedFilesSizeInBytes().value()).isEqualTo(20L);
    assertThat(metrics.totalFilesSizeInBytes().value()).isEqualTo(0L);
  }

  @TestTemplate
  public void addAndDeleteDeleteFiles() {
    String tableName = "add-and-delete-delete-files";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);

    // 2 positional + 1 equality
    table
        .newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .addDeletes(FILE_C2_DELETES)
        .commit();

    CommitReport report = reporter.lastCommitReport();
    assertThat(report).isNotNull();
    assertThat(report.operation()).isEqualTo("overwrite");
    assertThat(report.snapshotId()).isEqualTo(1L);
    assertThat(report.sequenceNumber()).isEqualTo(1L);
    assertThat(report.tableName()).isEqualTo(tableName);

    CommitMetricsResult metrics = report.commitMetrics();
    assertThat(metrics.addedDeleteFiles().value()).isEqualTo(3L);
    assertThat(metrics.totalDeleteFiles().value()).isEqualTo(3L);
    assertThat(metrics.addedPositionalDeleteFiles().value()).isEqualTo(2L);
    assertThat(metrics.addedEqualityDeleteFiles().value()).isEqualTo(1L);

    assertThat(metrics.addedPositionalDeletes().value()).isEqualTo(2L);
    assertThat(metrics.totalPositionalDeletes().value()).isEqualTo(2L);

    assertThat(metrics.addedEqualityDeletes().value()).isEqualTo(1L);
    assertThat(metrics.totalEqualityDeletes().value()).isEqualTo(1L);

    assertThat(metrics.addedFilesSizeInBytes().value()).isEqualTo(30L);
    assertThat(metrics.totalFilesSizeInBytes().value()).isEqualTo(30L);

    // now remove those 2 positional + 1 equality delete files
    table
        .newRewrite()
        .rewriteFiles(
            ImmutableSet.of(),
            ImmutableSet.of(FILE_A_DELETES, FILE_B_DELETES, FILE_C2_DELETES),
            ImmutableSet.of(),
            ImmutableSet.of())
        .commit();

    report = reporter.lastCommitReport();
    assertThat(report).isNotNull();
    assertThat(report.operation()).isEqualTo("replace");
    assertThat(report.snapshotId()).isEqualTo(2L);
    assertThat(report.sequenceNumber()).isEqualTo(2L);
    assertThat(report.tableName()).isEqualTo(tableName);

    metrics = report.commitMetrics();
    assertThat(metrics.removedDeleteFiles().value()).isEqualTo(3L);
    assertThat(metrics.totalDeleteFiles().value()).isEqualTo(0L);
    assertThat(metrics.removedPositionalDeleteFiles().value()).isEqualTo(2L);
    assertThat(metrics.removedEqualityDeleteFiles().value()).isEqualTo(1L);

    assertThat(metrics.removedPositionalDeletes().value()).isEqualTo(2L);
    assertThat(metrics.totalPositionalDeletes().value()).isEqualTo(0L);

    assertThat(metrics.removedEqualityDeletes().value()).isEqualTo(1L);
    assertThat(metrics.totalEqualityDeletes().value()).isEqualTo(0L);

    assertThat(metrics.removedFilesSizeInBytes().value()).isEqualTo(30L);
    assertThat(metrics.totalFilesSizeInBytes().value()).isEqualTo(0L);
  }

  @TestTemplate
  public void addAndDeleteManifests() throws IOException {
    String tableName = "add-and-delete-manifests";
    Table table =
        TestTables.create(
            tableDir, tableName, SCHEMA, SPEC, SortOrder.unsorted(), formatVersion, reporter);

    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    table.newAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    ManifestFile newManifest =
        writeManifest(
            "manifest-file.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, snap1.snapshotId(), FILE_A),
            manifestEntry(ManifestEntry.Status.EXISTING, snap2.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    for (ManifestFile manifest : snap2.dataManifests(table.io())) {
      rewriteManifests.deleteManifest(manifest);
    }

    rewriteManifests.addManifest(newManifest).commit();

    CommitReport report = reporter.lastCommitReport();
    assertThat(report).isNotNull();
    assertThat(report.operation()).isEqualTo("append");
    assertThat(report.snapshotId()).isEqualTo(2L);
    assertThat(report.sequenceNumber()).isEqualTo(2L);
    assertThat(report.tableName()).isEqualTo(tableName);

    CommitMetricsResult metrics = report.commitMetrics();
    assertThat(metrics.addedDataFiles().value()).isEqualTo(1L);
    assertThat(metrics.addedRecords().value()).isEqualTo(1L);
    assertThat(metrics.addedFilesSizeInBytes().value()).isEqualTo(10L);
  }
}
