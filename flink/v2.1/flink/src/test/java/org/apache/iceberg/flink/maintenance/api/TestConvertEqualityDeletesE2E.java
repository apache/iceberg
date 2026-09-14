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
package org.apache.iceberg.flink.maintenance.api;

import static org.apache.iceberg.flink.SimpleDataUtil.createRecord;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.operator.OperatorTestBase;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ContentFileUtil;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End-to-end test for {@link ConvertEqualityDeletes} wired through the {@link TableMaintenance}
 * framework. Verifies that the converter actually runs and commits a DV when the framework triggers
 * it, exercising the full operator graph including the framework's monitor source, trigger manager,
 * and lock remover.
 */
class TestConvertEqualityDeletesE2E extends OperatorTestBase {
  private static final String STAGING_BRANCH = "staging";

  @TempDir private Path tempDir;
  private StreamExecutionEnvironment env;

  @BeforeEach
  public void beforeEach() {
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
  }

  @ParameterizedTest
  @ValueSource(strings = {STAGING_BRANCH, SnapshotRef.MAIN_BRANCH})
  void testConvertEqualityDeletesE2E(String stagingBranch) throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    // When staging is a separate branch, fork it from main first.
    if (!stagingBranch.equals(SnapshotRef.MAIN_BRANCH)) {
      table.manageSnapshots().createBranch(stagingBranch).commit();
      table.refresh();
    }

    // Commit a new data file + eq delete to staging. This pre-job snapshot exercises both the
    // "new data file" and "eq delete" paths in one cycle.
    DataFile newData = writeDataFile(table, 3, "c");
    DeleteFile firstDelete = writeEqualityDelete(table, 1, "a");
    table.newRowDelta().addRows(newData).addDeletes(firstDelete).toBranch(stagingBranch).commit();
    table.refresh();
    assertThat(dvCountOnMain(table)).isZero();

    TableMaintenance.forTable(env, tableLoader(), LOCK_FACTORY)
        .uidSuffix("ConvertEqualityDeletesE2EUID-" + stagingBranch)
        .rateLimit(Duration.ofMillis(50))
        .lockCheckDelay(Duration.ofMillis(50))
        .add(
            ConvertEqualityDeletes.builder()
                .scheduleOnInterval(Duration.ofMillis(100))
                .stagingBranch(stagingBranch)
                .targetBranch(SnapshotRef.MAIN_BRANCH)
                .equalityFieldColumns(ImmutableList.of("id", "data"))
                .parallelism(2))
        .append();

    JobClient jobClient = null;
    try {
      jobClient = env.executeAsync();

      // Cycle 1: row 1 deleted by the converted DV; row 3 added on staging and committed to main.
      Awaitility.await()
          .atMost(Duration.ofMinutes(5))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(() -> assertThat(dvCountOnMain(table)).isEqualTo(1));
      SimpleDataUtil.assertTableRecords(
          table, ImmutableList.of(createRecord(2, "b"), createRecord(3, "c")));

      // Cycle 2: commit a second staging snapshot while the job is still running. The framework's
      // next interval-trigger should pick it up and produce a second DV against the data file
      // holding id=2.
      table.refresh();
      DeleteFile secondDelete = writeEqualityDelete(table, 2, "b");
      table.newRowDelta().addDeletes(secondDelete).toBranch(stagingBranch).commit();

      Awaitility.await()
          .atMost(Duration.ofMinutes(5))
          .pollInterval(Duration.ofMillis(200))
          .untilAsserted(() -> assertThat(dvCountOnMain(table)).isEqualTo(2));
      SimpleDataUtil.assertTableRecords(table, ImmutableList.of(createRecord(3, "c")));

      // In-place conversion (staging == main) removes the converted equality deletes so reads no
      // longer apply them. On a separate staging branch, main never holds equality deletes.
      assertThat(eqDeleteCountOnMain(table)).isZero();
    } finally {
      closeJobClient(jobClient);
    }
  }

  private DataFile writeDataFile(Table table, Integer id, String data) throws IOException {
    return new GenericAppenderHelper(table, FileFormat.PARQUET, tempDir)
        .writeFile(Lists.newArrayList(SimpleDataUtil.createRecord(id, data)));
  }

  private DeleteFile writeEqualityDelete(Table table, Integer id, String data) throws IOException {
    File file = File.createTempFile("junit", null, tempDir.toFile());
    assertThat(file.delete()).isTrue();
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        new PartitionData(PartitionSpec.unpartitioned().partitionType()),
        Lists.newArrayList(SimpleDataUtil.createRecord(id, data)),
        table.schema());
  }

  private static long dvCountOnMain(Table table) throws IOException {
    table.refresh();
    if (table.currentSnapshot() == null) {
      return 0;
    }

    long count = 0;
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          if (ContentFileUtil.isDV(file)) {
            count++;
          }
        }
      }
    }

    return count;
  }

  private static long eqDeleteCountOnMain(Table table) throws IOException {
    table.refresh();
    if (table.currentSnapshot() == null) {
      return 0;
    }

    long count = 0;
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          if (file.content() == FileContent.EQUALITY_DELETES) {
            count++;
          }
        }
      }
    }

    return count;
  }
}
