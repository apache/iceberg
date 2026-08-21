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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.MicroBatches.MicroBatchBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMicroBatchBuilder extends TestBase {

  @BeforeEach
  public void setupTableProperties() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "3").commit();
  }

  @TestTemplate
  public void testGenerateMicroBatch() {
    add(table.newAppend(), files("A", "B", "C", "D", "E"));

    MicroBatch batch =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(0, 6, Long.MAX_VALUE, true);
    assertThat(batch.snapshotId()).isEqualTo(1L);
    assertThat(batch.startFileIndex()).isEqualTo(0);
    assertThat(batch.endFileIndex()).isEqualTo(5);
    assertThat(batch.sizeInBytes()).isEqualTo(50);
    assertThat(batch.lastIndexOfSnapshot()).isTrue();
    filesMatch(Lists.newArrayList("A", "B", "C", "D", "E"), filesToScan(batch.tasks()));

    MicroBatch batch1 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(0, 1, 15L, true);
    assertThat(batch1.endFileIndex()).isEqualTo(1);
    assertThat(batch1.sizeInBytes()).isEqualTo(10);
    assertThat(batch1.lastIndexOfSnapshot()).isFalse();
    filesMatch(Lists.newArrayList("A"), filesToScan(batch1.tasks()));

    MicroBatch batch2 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch1.endFileIndex(), 4, 30L, true);
    assertThat(batch2.endFileIndex()).isEqualTo(4);
    assertThat(batch2.sizeInBytes()).isEqualTo(30);
    assertThat(batch2.lastIndexOfSnapshot()).isFalse();
    filesMatch(Lists.newArrayList("B", "C", "D"), filesToScan(batch2.tasks()));

    MicroBatch batch3 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch2.endFileIndex(), 5, 50L, true);
    assertThat(batch3.endFileIndex()).isEqualTo(5);
    assertThat(batch3.sizeInBytes()).isEqualTo(10);
    assertThat(batch3.lastIndexOfSnapshot()).isTrue();
    filesMatch(Lists.newArrayList("E"), filesToScan(batch3.tasks()));
  }

  @TestTemplate
  public void testGenerateMicroBatchWithSmallTargetSize() {
    add(table.newAppend(), files("A", "B", "C", "D", "E"));

    MicroBatch batch =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(0, 1, 10L, true);
    assertThat(batch.snapshotId()).isEqualTo(1L);
    assertThat(batch.startFileIndex()).isEqualTo(0);
    assertThat(batch.endFileIndex()).isEqualTo(1);
    assertThat(batch.sizeInBytes()).isEqualTo(10);
    assertThat(batch.lastIndexOfSnapshot()).isFalse();
    filesMatch(Lists.newArrayList("A"), filesToScan(batch.tasks()));

    MicroBatch batch1 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch.endFileIndex(), 2, 5L, true);
    assertThat(batch1.endFileIndex()).isEqualTo(2);
    assertThat(batch1.sizeInBytes()).isEqualTo(10);
    filesMatch(Lists.newArrayList("B"), filesToScan(batch1.tasks()));
    assertThat(batch1.lastIndexOfSnapshot()).isFalse();

    MicroBatch batch2 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch1.endFileIndex(), 3, 10L, true);
    assertThat(batch2.endFileIndex()).isEqualTo(3);
    assertThat(batch2.sizeInBytes()).isEqualTo(10);
    filesMatch(Lists.newArrayList("C"), filesToScan(batch2.tasks()));
    assertThat(batch2.lastIndexOfSnapshot()).isFalse();

    MicroBatch batch3 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch2.endFileIndex(), 4, 10L, true);
    assertThat(batch3.endFileIndex()).isEqualTo(4);
    assertThat(batch3.sizeInBytes()).isEqualTo(10);
    filesMatch(Lists.newArrayList("D"), filesToScan(batch3.tasks()));
    assertThat(batch3.lastIndexOfSnapshot()).isFalse();

    MicroBatch batch4 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch3.endFileIndex(), 5, 5L, true);
    assertThat(batch4.endFileIndex()).isEqualTo(5);
    assertThat(batch4.sizeInBytes()).isEqualTo(10);
    filesMatch(Lists.newArrayList("E"), filesToScan(batch4.tasks()));
    assertThat(batch4.lastIndexOfSnapshot()).isTrue();

    MicroBatch batch5 =
        MicroBatches.from(table.snapshot(1L), table.io())
            .specsById(table.specs())
            .generate(batch4.endFileIndex(), 5, 5L, true);
    assertThat(batch5.endFileIndex()).isEqualTo(5);
    assertThat(batch5.sizeInBytes()).isEqualTo(0);
    assertThat(batch5.tasks()).isEmpty();
    assertThat(batch5.lastIndexOfSnapshot()).isTrue();
  }

  @TestTemplate
  public void testFullScanFileCountMatchesManifestMetadata() {
    add(table.newAppend(), files("A", "B", "C"));

    MicroBatchBuilder builder = MicroBatches.from(table.snapshot(1L), table.io());

    assertThat(builder.fullScanFileCount()).isEqualTo(3);
  }

  @TestTemplate
  public void testPlanFullScanEmptyRangeReturnsNoTasks() {
    add(table.newAppend(), files("A"));

    MicroBatchBuilder builder = MicroBatches.from(table.snapshot(1L), table.io());

    assertThat(builder.planFullScan(0, 0)).isEmpty();
    assertThat(builder.planFullScan(1, 1)).isEmpty();
  }

  @TestTemplate
  public void testPlanFullScanDeletesAttachAcrossASliceBoundaryThatSplitsAManifest()
      throws IOException {
    // positional deletes / DVs require format version 2+
    assumeThat(formatVersion).isGreaterThan(1);

    // FILE_A and FILE_B land in one manifest; their deletes land in a separate delete
    // manifest from a later row-delta commit.
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newRowDelta().addDeletes(fileADeletes()).addDeletes(fileBDeletes()).commit();

    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.dataManifests(table.io())).hasSize(1);
    assertThat(snapshot.deleteManifests(table.io())).hasSize(1);

    List<FileScanTask> expected;
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
      expected = Lists.newArrayList(tasks);
    }
    assertThat(expected).hasSize(2);

    MicroBatchBuilder builder = MicroBatches.from(snapshot, table.io()).specsById(table.specs());
    assertThat(builder.fullScanFileCount()).isEqualTo(2);

    // Slice at the boundary between the two files in the single manifest: each half must still
    // resolve to the correct file with the correct deletes attached
    List<FileScanTask> firstHalf = builder.planFullScan(0, 1);
    List<FileScanTask> secondHalf = builder.planFullScan(1, 2);
    assertThat(firstHalf).hasSize(1);
    assertThat(secondHalf).hasSize(1);

    FileScanTask first = firstHalf.get(0);
    FileScanTask second = secondHalf.get(0);
    assertThat(first.file().location()).isNotEqualTo(second.file().location());

    FileScanTask expectedFirst = expectedTaskFor(expected, first.file().location());
    FileScanTask expectedSecond = expectedTaskFor(expected, second.file().location());

    assertThat(deleteLocations(first)).isEqualTo(deleteLocations(expectedFirst));
    assertThat(deleteLocations(second)).isEqualTo(deleteLocations(expectedSecond));
    assertThat(deleteLocations(first)).isNotEmpty();
    assertThat(deleteLocations(second)).isNotEmpty();

    // a single full-range slice must agree with the two boundary-split half-slices
    List<FileScanTask> full = builder.planFullScan(0, 2);
    assertThat(full).hasSize(2);
    assertThat(deleteLocations(expectedTaskFor(full, first.file().location())))
        .isEqualTo(deleteLocations(first));
    assertThat(deleteLocations(expectedTaskFor(full, second.file().location())))
        .isEqualTo(deleteLocations(second));
  }

  private static FileScanTask expectedTaskFor(List<FileScanTask> tasks, String location) {
    return tasks.stream()
        .filter(t -> t.file().location().equals(location))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No task found for location: " + location));
  }

  private static List<String> deleteLocations(FileScanTask task) {
    return task.deletes().stream().map(ContentFile::location).sorted().collect(Collectors.toList());
  }

  private static DataFile file(String name) {
    return DataFiles.builder(SPEC)
        .withPath(name + ".parquet")
        .withFileSizeInBytes(10)
        .withPartitionPath("data_bucket=0") // easy way to set partition data for now
        .withRecordCount(1)
        .build();
  }

  private static void add(AppendFiles appendFiles, List<DataFile> adds) {
    for (DataFile f : adds) {
      appendFiles.appendFile(f);
    }
    appendFiles.commit();
  }

  private static List<DataFile> files(String... names) {
    return Lists.transform(Lists.newArrayList(names), TestMicroBatchBuilder::file);
  }

  private static List<String> filesToScan(Iterable<FileScanTask> tasks) {
    Iterable<String> filesToRead =
        Iterables.transform(
            tasks,
            t -> {
              String path = t.file().location();
              return path.split("\\.")[0];
            });
    return Lists.newArrayList(filesToRead);
  }

  private static void filesMatch(List<String> expected, List<String> actual) {
    Collections.sort(expected);
    Collections.sort(actual);
    assertThat(actual).isEqualTo(expected);
  }
}
