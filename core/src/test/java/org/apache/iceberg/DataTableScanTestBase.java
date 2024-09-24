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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceMap;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class DataTableScanTestBase<
        ScanT extends Scan<ScanT, T, G>, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends ScanTestBase<ScanT, T, G> {

  protected abstract ScanT useRef(ScanT scan, String ref);

  protected abstract ScanT useSnapshot(ScanT scan, long snapshotId);

  protected abstract ScanT asOfTime(ScanT scan, long timestampMillis);

  @TestTemplate
  public void testTaskRowCounts() {
    assumeThat(formatVersion).isEqualTo(2);

    DataFile dataFile1 = newDataFile("data_bucket=0");
    table.newFastAppend().appendFile(dataFile1).commit();

    DataFile dataFile2 = newDataFile("data_bucket=1");
    table.newFastAppend().appendFile(dataFile2).commit();

    DeleteFile deleteFile1 = newDeleteFile("data_bucket=0");
    table.newRowDelta().addDeletes(deleteFile1).commit();

    DeleteFile deleteFile2 = newDeleteFile("data_bucket=1");
    table.newRowDelta().addDeletes(deleteFile2).commit();

    ScanT scan = newScan().option(TableProperties.SPLIT_SIZE, "50");

    List<T> fileScanTasks = Lists.newArrayList(scan.planFiles());
    assertThat(fileScanTasks).as("Must have 2 FileScanTasks").hasSize(2);
    for (T task : fileScanTasks) {
      assertThat(task.estimatedRowsCount()).as("Rows count must match").isEqualTo(10);
    }

    List<G> combinedScanTasks = Lists.newArrayList(scan.planTasks());
    assertThat(combinedScanTasks).as("Must have 4 CombinedScanTask").hasSize(4);
    for (G task : combinedScanTasks) {
      assertThat(task.estimatedRowsCount()).as("Rows count must match").isEqualTo(5);
    }
  }

  protected DataFile newDataFile(String partitionPath) {
    return DataFiles.builder(table.spec())
        .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100)
        .withPartitionPath(partitionPath)
        .withRecordCount(10)
        .build();
  }

  protected DeleteFile newDeleteFile(String partitionPath) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withPath("/path/to/delete-" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100)
        .withPartitionPath(partitionPath)
        .withRecordCount(10)
        .build();
  }

  @TestTemplate
  public void testScanFromBranchTip() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    // Add B and C to new branch
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_C).toBranch("testBranch").commit();
    // Add D to main
    table.newFastAppend().appendFile(FILE_D).commit();

    ScanT testBranchScan = useRef(newScan(), "testBranch");
    validateExpectedFileScanTasks(
        testBranchScan, ImmutableList.of(FILE_A.path(), FILE_B.path(), FILE_C.path()));

    ScanT mainScan = newScan();
    validateExpectedFileScanTasks(mainScan, ImmutableList.of(FILE_A.path(), FILE_D.path()));
  }

  @TestTemplate
  public void testScanFromTag() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.manageSnapshots().createTag("tagB", table.currentSnapshot().snapshotId()).commit();
    table.newFastAppend().appendFile(FILE_C).commit();
    ScanT tagScan = useRef(newScan(), "tagB");
    validateExpectedFileScanTasks(tagScan, ImmutableList.of(FILE_A.path(), FILE_B.path()));
    ScanT mainScan = newScan();
    validateExpectedFileScanTasks(
        mainScan, ImmutableList.of(FILE_A.path(), FILE_B.path(), FILE_C.path()));
  }

  @TestTemplate
  public void testScanFromRefWhenSnapshotSetFails() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.manageSnapshots().createTag("tagB", table.currentSnapshot().snapshotId()).commit();

    assertThatThrownBy(
            () -> useRef(useSnapshot(newScan(), table.currentSnapshot().snapshotId()), "tagB"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot override ref, already set snapshot id=1");
  }

  @TestTemplate
  public void testSettingSnapshotWhenRefSetFails() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snapshotA = table.currentSnapshot();
    table.newFastAppend().appendFile(FILE_B).commit();
    table.manageSnapshots().createTag("tagB", table.currentSnapshot().snapshotId()).commit();

    assertThatThrownBy(() -> useSnapshot(useRef(newScan(), "tagB"), snapshotA.snapshotId()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot override snapshot, already set snapshot id=2");
  }

  @TestTemplate
  public void testBranchTimeTravelFails() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table
        .manageSnapshots()
        .createBranch("testBranch", table.currentSnapshot().snapshotId())
        .commit();

    assertThatThrownBy(() -> asOfTime(useRef(newScan(), "testBranch"), System.currentTimeMillis()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot override snapshot, already set snapshot id=1");
  }

  @TestTemplate
  public void testSettingMultipleRefsFails() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.manageSnapshots().createTag("tagA", table.currentSnapshot().snapshotId()).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
    table.manageSnapshots().createTag("tagB", table.currentSnapshot().snapshotId()).commit();

    assertThatThrownBy(() -> useRef(useRef(newScan(), "tagB"), "tagA"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot override ref, already set snapshot id=2");
  }

  @TestTemplate
  public void testSettingInvalidRefFails() {
    assertThatThrownBy(() -> useRef(newScan(), "nonexisting"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find ref nonexisting");
  }

  private void validateExpectedFileScanTasks(ScanT scan, List<CharSequence> expectedFileScanPaths)
      throws IOException {
    validateExpectedFileScanTasks(scan, expectedFileScanPaths, null);
  }

  private void validateExpectedFileScanTasks(
      ScanT scan,
      Collection<CharSequence> expectedFileScanPaths,
      CharSequenceMap<String> fileToManifest)
      throws IOException {
    try (CloseableIterable<T> scanTasks = scan.planFiles()) {
      assertThat(scanTasks).hasSameSizeAs(expectedFileScanPaths);
      List<CharSequence> actualFiles = Lists.newArrayList();
      for (T task : scanTasks) {
        DataFile dataFile = ((FileScanTask) task).file();
        actualFiles.add(dataFile.path());
        if (fileToManifest != null) {
          assertThat(fileToManifest.get(dataFile.path())).isEqualTo(dataFile.manifestLocation());
        }
      }

      assertThat(actualFiles).containsAll(expectedFileScanPaths);
    }
  }

  @TestTemplate
  public void testSequenceNumbersThroughPlanFiles() {
    assumeThat(formatVersion).isEqualTo(2);

    DataFile dataFile1 = newDataFile("data_bucket=0");
    table.newFastAppend().appendFile(dataFile1).commit();

    DataFile dataFile2 = newDataFile("data_bucket=1");
    table.newFastAppend().appendFile(dataFile2).commit();

    DeleteFile deleteFile1 = newDeleteFile("data_bucket=0");
    table.newRowDelta().addDeletes(deleteFile1).commit();

    DeleteFile deleteFile2 = newDeleteFile("data_bucket=1");
    table.newRowDelta().addDeletes(deleteFile2).commit();

    ScanT scan = newScan();

    List<T> fileScanTasks = Lists.newArrayList(scan.planFiles());
    assertThat(fileScanTasks).as("Must have 2 FileScanTasks").hasSize(2);
    for (T task : fileScanTasks) {
      FileScanTask fileScanTask = (FileScanTask) task;
      DataFile file = fileScanTask.file();
      long expectedDataSequenceNumber = 0L;
      long expectedDeleteSequenceNumber = 0L;
      if (file.path().equals(dataFile1.path())) {
        expectedDataSequenceNumber = 1L;
        expectedDeleteSequenceNumber = 3L;
      }

      if (file.path().equals(dataFile2.path())) {
        expectedDataSequenceNumber = 2L;
        expectedDeleteSequenceNumber = 4L;
      }

      assertThat(file.dataSequenceNumber().longValue())
          .as("Data sequence number mismatch")
          .isEqualTo(expectedDataSequenceNumber);

      assertThat(file.fileSequenceNumber().longValue())
          .as("File sequence number mismatch")
          .isEqualTo(expectedDataSequenceNumber);

      List<DeleteFile> deleteFiles = fileScanTask.deletes();
      assertThat(deleteFiles).as("Must have 1 delete file").hasSize(1);

      DeleteFile deleteFile = Iterables.getOnlyElement(deleteFiles);
      assertThat(deleteFile.dataSequenceNumber().longValue())
          .as("Data sequence number mismatch")
          .isEqualTo(expectedDeleteSequenceNumber);

      assertThat(deleteFile.fileSequenceNumber().longValue())
          .as("File sequence number mismatch")
          .isEqualTo(expectedDeleteSequenceNumber);
    }
  }

  @TestTemplate
  public void testManifestLocationsInScan() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    ManifestFile firstDataManifest = table.currentSnapshot().allManifests(table.io()).get(0);
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    ManifestFile secondDataManifest =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .filter(manifest -> manifest.snapshotId() == table.currentSnapshot().snapshotId())
            .collect(Collectors.toList())
            .get(0);
    CharSequenceMap<String> fileToManifest = CharSequenceMap.create();
    fileToManifest.put(FILE_A.path(), firstDataManifest.path());
    fileToManifest.put(FILE_B.path(), secondDataManifest.path());
    fileToManifest.put(FILE_C.path(), secondDataManifest.path());

    validateExpectedFileScanTasks(newScan(), fileToManifest.keySet(), fileToManifest);
  }

  @TestTemplate
  public void testManifestLocationsInScanWithDeleteFiles() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).commit();
    ManifestFile firstManifest = table.currentSnapshot().allManifests(table.io()).get(0);
    DeleteFile deleteFile = newDeleteFile("data_bucket=0");
    table.newRowDelta().addDeletes(deleteFile).commit();
    CharSequenceMap<String> fileToManifest = CharSequenceMap.create();
    fileToManifest.put(FILE_A.path(), firstManifest.path());
    ScanT scan = newScan();
    validateExpectedFileScanTasks(scan, ImmutableList.of(FILE_A.path()), fileToManifest);
    List<DeleteFile> deletes = Lists.newArrayList();
    try (CloseableIterable<T> scanTasks = scan.planFiles()) {
      for (T task : scanTasks) {
        FileScanTask fileScanTask = (FileScanTask) task;
        deletes.addAll(fileScanTask.deletes());
      }
    }

    assertThat(deletes.size()).isEqualTo(1);
    ManifestFile deleteManifest =
        table.currentSnapshot().deleteManifests(table.io()).stream()
            .filter(manifest -> manifest.snapshotId() == table.currentSnapshot().snapshotId())
            .collect(Collectors.toList())
            .get(0);
    assertThat(deletes.get(0).manifestLocation()).isEqualTo(deleteManifest.path());
  }

  @TestTemplate
  public void testTimeTravelScanWithAlterColumn() throws Exception {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    long timeTravelSnapshotId = table.currentSnapshot().snapshotId();
    table.updateSchema().renameColumn("id", "re_id").commit();
    TableScan scan =
        table.newScan().useSnapshot(timeTravelSnapshotId).filter(Expressions.equal("id", 5));
    assertThat(Iterables.size(scan.planFiles())).isEqualTo(2);
  }
}
