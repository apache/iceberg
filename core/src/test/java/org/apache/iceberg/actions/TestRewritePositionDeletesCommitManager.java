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
package org.apache.iceberg.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestRewritePositionDeletesCommitManager extends TestBase {

  // v3 and later tables reject position delete files that are not DVs
  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> formatVersions() {
    return List.of(2);
  }

  @TestTemplate
  void concurrentCommitsDoNotConflict() throws Exception {
    int numGroups = 8;
    table
        .updateProperties()
        // fail on the first conflict instead of hiding it behind a retry
        .set(TableProperties.COMMIT_NUM_RETRIES, "0")
        // test tables otherwise assign sequential snapshot IDs, and a reused ID drops a commit
        .set("random-snapshot-ids", "true")
        .commit();
    RowDelta rowDelta = table.newRowDelta().addRows(FILE_A);
    for (int i = 0; i < numGroups; i++) {
      rowDelta.addDeletes(copy(FILE_A_DELETES, "/path/to/data-a-deletes-" + i + ".parquet"));
    }

    rowDelta.commit();

    List<RewritePositionDeletesGroup> groups = Lists.newArrayList();
    List<String> outputLocations = Lists.newArrayList();
    for (PositionDeletesScanTask task : planFiles()) {
      DeleteFile output = copy(task.file(), task.file().location() + ".rewritten");
      groups.add(group(List.of(task), output));
      outputLocations.add(output.location());
    }

    RewritePositionDeletesCommitManager commitManager =
        new RewritePositionDeletesCommitManager(table);
    CyclicBarrier start = new CyclicBarrier(numGroups);
    ExecutorService executorService = Executors.newFixedThreadPool(numGroups);
    try {
      List<Future<?>> commits = Lists.newArrayList();
      for (RewritePositionDeletesGroup group : groups) {
        commits.add(
            executorService.submit(
                () -> {
                  start.await();
                  commitManager.commit(Set.of(group));
                  return null;
                }));
      }

      for (Future<?> commit : commits) {
        commit.get(10, TimeUnit.SECONDS);
      }
    } finally {
      executorService.shutdownNow();
    }

    assertThat(table.snapshots()).hasSize(numGroups + 1);
    assertThat(planFiles())
        .extracting(task -> task.file().location())
        .containsExactlyInAnyOrderElementsOf(outputLocations);
    assertThat(metadataFiles()).containsExactlyInAnyOrderElementsOf(referencedMetadataFiles());
  }

  @TestTemplate
  void failedCommitRemovesWrittenManifests() throws IOException {
    table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES).commit();
    table.newRowDelta().addRows(FILE_B).addDeletes(FILE_B_DELETES).commit();
    RewritePositionDeletesCommitManager commitManager =
        new RewritePositionDeletesCommitManager(table);
    DeleteFile output = copy(FILE_A_DELETES, "/path/to/data-a-deletes-rewritten.parquet");
    RewritePositionDeletesGroup group = group(planFiles(), output);
    table.newRowDelta().removeDeletes(FILE_B_DELETES).commit();
    List<String> metadataFilesBeforeCommit = metadataFiles();

    // apply() rewrites the FILE_A_DELETES manifest, then fails on the missing FILE_B_DELETES
    assertThatThrownBy(() -> commitManager.commit(Set.of(group)))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Missing required files to delete");

    assertThat(metadataFiles()).containsExactlyInAnyOrderElementsOf(metadataFilesBeforeCommit);
  }

  private List<PositionDeletesScanTask> planFiles() throws IOException {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);
    try (CloseableIterable<PositionDeletesScanTask> tasks =
        CloseableIterable.transform(
            deletesTable.newBatchScan().planFiles(), PositionDeletesScanTask.class::cast)) {
      return Lists.newArrayList(tasks);
    }
  }

  private RewritePositionDeletesGroup group(
      List<PositionDeletesScanTask> tasks, DeleteFile outputFile) {
    RewritePositionDeletesGroup group =
        new RewritePositionDeletesGroup(
            ImmutableRewritePositionDeleteFiles.FileGroupInfo.builder()
                .globalIndex(0)
                .partitionIndex(0)
                .partition(outputFile.partition())
                .build(),
            tasks,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            1);
    group.setOutputFiles(Set.of(outputFile));
    return group;
  }

  private static DeleteFile copy(DeleteFile file, String path) {
    return FileMetadata.deleteFileBuilder(SPEC).copy(file).withPath(path).build();
  }

  private List<String> metadataFiles() {
    return Arrays.stream(metadataDir.listFiles()).map(File::getName).toList();
  }

  private Set<String> referencedMetadataFiles() {
    Set<String> referencedFiles = Sets.newHashSet();
    for (Snapshot snapshot : table.snapshots()) {
      referencedFiles.add(new File(snapshot.manifestListLocation()).getName());
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        referencedFiles.add(new File(manifest.path()).getName());
      }
    }

    return referencedFiles;
  }
}
