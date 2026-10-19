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
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
class TestRewriteDataFilesCommitManager extends TestBase {

  @TestTemplate
  void concurrentCommitsDoNotConflict() throws Exception {
    int numGroups = 8;
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "0")
        // test tables otherwise assign sequential snapshot IDs, which concurrent producers reuse
        .set("random-snapshot-ids", "true")
        .commit();
    AppendFiles append = table.newFastAppend();
    for (int i = 0; i < numGroups; i++) {
      append.appendFile(copy(FILE_A, "/path/to/data-" + i + ".parquet"));
    }

    append.commit();

    List<RewriteFileGroup> groups = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile output = copy(task.file(), task.file().location() + ".rewritten");
        groups.add(group(List.of(task), output));
      }
    }

    RewriteDataFilesCommitManager commitManager = new RewriteDataFilesCommitManager(table);
    CyclicBarrier start = new CyclicBarrier(numGroups);
    ExecutorService executorService = Executors.newFixedThreadPool(numGroups);
    try {
      List<Future<?>> commits = Lists.newArrayList();
      for (RewriteFileGroup group : groups) {
        commits.add(
            executorService.submit(
                () -> {
                  start.await();
                  commitManager.commitFileGroups(Set.of(group));
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
  }

  @TestTemplate
  void commitRemovesUnusedManifestLists() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();
    RewriteDataFilesCommitManager commitManager = new RewriteDataFilesCommitManager(table);

    commitManager.commitFileGroups(Set.of(group(planFiles(), FILE_C)));

    assertThat(table.snapshots()).hasSize(2);
    assertThat(metadataFiles("snap-")).hasSize(2);
  }

  @TestTemplate
  void failedCommitRemovesWrittenManifests() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    RewriteDataFilesCommitManager commitManager = new RewriteDataFilesCommitManager(table);
    RewriteFileGroup group = group(planFiles(), FILE_C);
    table.newDelete().deleteFile(FILE_B).commit();
    List<String> metadataFilesBeforeCommit = metadataFiles("");

    assertThatThrownBy(() -> commitManager.commitFileGroups(Set.of(group)))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Missing required files to delete");

    assertThat(metadataFiles("")).containsExactlyInAnyOrderElementsOf(metadataFilesBeforeCommit);
  }

  private List<FileScanTask> planFiles() throws IOException {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return Lists.newArrayList(tasks);
    }
  }

  private RewriteFileGroup group(List<FileScanTask> tasks, DataFile outputFile) {
    RewriteFileGroup group =
        new RewriteFileGroup(
            ImmutableRewriteDataFiles.FileGroupInfo.builder()
                .globalIndex(0)
                .partitionIndex(0)
                .partition(outputFile.partition())
                .build(),
            tasks,
            table.spec().specId(),
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            1);
    group.setOutputFiles(Set.of(outputFile));
    return group;
  }

  private static DataFile copy(DataFile file, String path) {
    return DataFiles.builder(SPEC).copy(file).withPath(path).build();
  }

  private List<String> metadataFiles(String prefix) {
    return Arrays.stream(metadataDir.listFiles())
        .map(File::getName)
        .filter(name -> name.startsWith(prefix) && name.endsWith(".avro"))
        .toList();
  }
}
