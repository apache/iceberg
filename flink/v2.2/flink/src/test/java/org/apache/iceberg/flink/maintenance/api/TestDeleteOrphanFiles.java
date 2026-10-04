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

import static org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles.DELETE_FILES_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles.FILESYSTEM_FILES_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles.METADATA_FILES_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles.PLANNER_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles.READER_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.DELETE_FILE_FAILED_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.DELETE_FILE_SUCCEEDED_COUNTER;
import static org.apache.iceberg.flink.maintenance.operator.TableMaintenanceMetrics.ERROR_COUNTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.operator.MetricsReporterFactoryForTests;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestDeleteOrphanFiles extends MaintenanceTaskTestBase {

  private Path relative(Table table, String relativePath) {
    return FileSystems.getDefault().getPath(table.location().substring(5), relativePath);
  }

  private void createFiles(Path... paths) throws IOException {
    for (Path path : paths) {
      Files.write(path, "DUMMY".getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  void testDeleteOrphanFilesUnPartitioned() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");

    assertFileNum(table, 4, 0);

    Path inData = relative(table, "metadata/in_data");
    Path inMetadata = relative(table, "metadata/in_metadata");

    createFiles(inData);
    createFiles(inMetadata);
    assertThat(inMetadata).exists();
    assertThat(inData).exists();

    appendDeleteOrphanFiles();

    runAndWaitForSuccess(
        infra.env(), infra.source(), infra.sink(), () -> checkDeleteFinished(table.name(), 2L));
    assertThat(inMetadata).doesNotExist();
    assertThat(inData).doesNotExist();
    assertFileNum(table, 4, 0);

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    READER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    FILESYSTEM_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    METADATA_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    DELETE_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    DELETE_FILE_FAILED_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    DELETE_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    DELETE_FILE_SUCCEEDED_COUNTER),
                2L)
            .build());
  }

  @Test
  void testDeleteOrphanFilesPartitioned() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");

    assertFileNum(table, 4, 0);

    Path inMetadata = relative(table, "metadata/in_metadata");
    Path inData = relative(table, "metadata/in_data");

    createFiles(inMetadata);
    createFiles(inData);
    assertThat(inMetadata).exists();
    assertThat(inData).exists();

    appendDeleteOrphanFiles();

    runAndWaitForSuccess(
        infra.env(), infra.source(), infra.sink(), () -> checkDeleteFinished(table.name(), 2L));
    assertThat(inMetadata).doesNotExist();
    assertThat(inData).doesNotExist();

    assertFileNum(table, 4, 0);

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    READER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    FILESYSTEM_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    METADATA_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    DELETE_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    DELETE_FILE_FAILED_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    DELETE_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    DELETE_FILE_SUCCEEDED_COUNTER),
                2L)
            .build());
  }

  @Test
  void testDeleteOrphanFilesFailure() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");

    assertFileNum(table, 4, 0);

    Path inData = relative(table, "metadata/in_data");
    Path inMetadata = relative(table, "metadata/in_metadata");

    createFiles(inData);
    createFiles(inMetadata);
    assertThat(inMetadata).exists();
    assertThat(inData).exists();

    appendDeleteOrphanFiles();

    // Mock error in the delete files operator
    Long parentId = table.currentSnapshot().parentId();
    for (ManifestFile manifestFile : table.snapshot(parentId).allManifests(table.io())) {
      table.io().deleteFile(manifestFile.path());
    }

    runAndWaitForResult(
        infra.env(),
        infra.source(),
        infra.sink(),
        false /* generateFailure */,
        () -> checkDeleteFinished(table.name(), 0L),
        false /* resultSuccess*/);

    // An error occurred; the file should not be deleted. And the job should not be failed.
    assertThat(inMetadata).exists();
    assertThat(inData).exists();

    // Check the metrics
    MetricsReporterFactoryForTests.assertCounters(
        new ImmutableMap.Builder<List<String>, Long>()
            .put(
                ImmutableList.of(
                    PLANNER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    READER_TASK_NAME + "[0]", table.name(), DUMMY_TASK_NAME, "0", ERROR_COUNTER),
                1L)
            .put(
                ImmutableList.of(
                    FILESYSTEM_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    METADATA_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    ERROR_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    DELETE_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    DELETE_FILE_FAILED_COUNTER),
                0L)
            .put(
                ImmutableList.of(
                    DELETE_FILES_TASK_NAME + "[0]",
                    table.name(),
                    DUMMY_TASK_NAME,
                    "0",
                    DELETE_FILE_SUCCEEDED_COUNTER),
                0L)
            .build());
  }

  private void appendDeleteOrphanFiles() {
    appendDeleteOrphanFiles(DeleteOrphanFiles.builder().minAge(Duration.ZERO));
  }

  private void appendDeleteOrphanFiles(DeleteOrphanFiles.Builder builder) {
    builder
        .append(
            infra.triggerStream(),
            DUMMY_TABLE_NAME,
            DUMMY_TASK_NAME,
            0,
            tableLoader(),
            UID_SUFFIX,
            StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP,
            1)
        .sinkTo(infra.sink());
  }

  private static void assertFileNum(
      Table table, int expectedDataFileNum, int expectedDeleteFileNum) {
    table.refresh();
    assertThat(
            table.currentSnapshot().dataManifests(table.io()).stream()
                .flatMap(
                    m ->
                        StreamSupport.stream(
                            ManifestFiles.read(m, table.io(), table.specs()).spliterator(), false))
                .count())
        .isEqualTo(expectedDataFileNum);
    assertThat(
            table.currentSnapshot().deleteManifests(table.io()).stream()
                .flatMap(
                    m ->
                        StreamSupport.stream(
                            ManifestFiles.readDeleteManifest(m, table.io(), table.specs())
                                .spliterator(),
                            false))
                .count())
        .isEqualTo(expectedDeleteFileNum);
  }

  private static boolean checkDeleteFinished(String tableName, Long expectedDeleteNum) {
    return expectedDeleteNum.equals(
        MetricsReporterFactoryForTests.counter(
            ImmutableList.of(
                DELETE_FILES_TASK_NAME + "[0]",
                tableName,
                DUMMY_TASK_NAME,
                "0",
                DELETE_FILE_SUCCEEDED_COUNTER)));
  }
}
