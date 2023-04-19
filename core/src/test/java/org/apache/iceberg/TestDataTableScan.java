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

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestDataTableScan extends ScanTestBase<TableScan, FileScanTask, CombinedScanTask> {
  public TestDataTableScan(int formatVersion) {
    super(formatVersion);
  }

  @Override
  protected TableScan newScan() {
    return table.newScan();
  }

  @Test
  public void testTaskRowCounts() {
    Assume.assumeTrue(formatVersion == 2);

    DataFile dataFile1 = newDataFile("data_bucket=0");
    table.newFastAppend().appendFile(dataFile1).commit();

    DataFile dataFile2 = newDataFile("data_bucket=1");
    table.newFastAppend().appendFile(dataFile2).commit();

    DeleteFile deleteFile1 = newDeleteFile("data_bucket=0");
    table.newRowDelta().addDeletes(deleteFile1).commit();

    DeleteFile deleteFile2 = newDeleteFile("data_bucket=1");
    table.newRowDelta().addDeletes(deleteFile2).commit();

    TableScan scan = table.newScan().option(TableProperties.SPLIT_SIZE, "50");

    List<FileScanTask> fileScanTasks = Lists.newArrayList(scan.planFiles());
    Assert.assertEquals("Must have 2 FileScanTasks", 2, fileScanTasks.size());
    for (FileScanTask task : fileScanTasks) {
      Assert.assertEquals("Rows count must match", 10, task.estimatedRowsCount());
    }

    List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(scan.planTasks());
    Assert.assertEquals("Must have 4 CombinedScanTask", 4, combinedScanTasks.size());
    for (CombinedScanTask task : combinedScanTasks) {
      Assert.assertEquals("Rows count must match", 5, task.estimatedRowsCount());
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

  @Test
  public void testScanFromBranchTip() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    // Add B and C to new branch
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_C).toBranch("testBranch").commit();
    // Add D to main
    table.newFastAppend().appendFile(FILE_D).commit();

    TableScan testBranchScan = table.newScan().useRef("testBranch");
    validateExpectedFileScanTasks(
        testBranchScan, ImmutableList.of(FILE_A.path(), FILE_B.path(), FILE_C.path()));

    TableScan mainScan = table.newScan();
    validateExpectedFileScanTasks(mainScan, ImmutableList.of(FILE_A.path(), FILE_D.path()));
  }

  @Test
  public void testScanFromTag() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.manageSnapshots().createTag("tagB", table.currentSnapshot().snapshotId()).commit();
    table.newFastAppend().appendFile(FILE_C).commit();
    TableScan tagScan = table.newScan().useRef("tagB");
    validateExpectedFileScanTasks(tagScan, ImmutableList.of(FILE_A.path(), FILE_B.path()));
    TableScan mainScan = table.newScan();
    validateExpectedFileScanTasks(
        mainScan, ImmutableList.of(FILE_A.path(), FILE_B.path(), FILE_C.path()));
  }

  @Test
  public void testScanFromRefWhenSnapshotSetFails() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.manageSnapshots().createTag("tagB", table.currentSnapshot().snapshotId()).commit();

    AssertHelpers.assertThrows(
        "Should throw when attempting to use a ref for scanning when a snapshot is set",
        IllegalArgumentException.class,
        "Cannot override ref, already set snapshot id=1",
        () -> table.newScan().useSnapshot(table.currentSnapshot().snapshotId()).useRef("tagB"));
  }

  @Test
  public void testSettingSnapshotWhenRefSetFails() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snapshotA = table.currentSnapshot();
    table.newFastAppend().appendFile(FILE_B).commit();
    table.manageSnapshots().createTag("tagB", table.currentSnapshot().snapshotId()).commit();

    AssertHelpers.assertThrows(
        "Should throw when attempting to use a snapshot for scanning when a ref is set",
        IllegalArgumentException.class,
        "Cannot override snapshot, already set snapshot id=2",
        () -> table.newScan().useRef("tagB").useSnapshot(snapshotA.snapshotId()));
  }

  @Test
  public void testBranchTimeTravelFails() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table
        .manageSnapshots()
        .createBranch("testBranch", table.currentSnapshot().snapshotId())
        .commit();
    AssertHelpers.assertThrows(
        "Should throw when attempting to use a snapshot for scanning when a ref is set",
        IllegalArgumentException.class,
        "Cannot override snapshot, already set snapshot id=1",
        () -> table.newScan().useRef("testBranch").asOfTime(System.currentTimeMillis()));
  }

  @Test
  public void testSettingMultipleRefsFails() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.manageSnapshots().createTag("tagA", table.currentSnapshot().snapshotId()).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
    table.manageSnapshots().createTag("tagB", table.currentSnapshot().snapshotId()).commit();

    AssertHelpers.assertThrows(
        "Should throw when attempting to use multiple refs",
        IllegalArgumentException.class,
        "Cannot override ref, already set snapshot id=2",
        () -> table.newScan().useRef("tagB").useRef("tagA"));
  }

  @Test
  public void testSettingInvalidRefFails() {
    AssertHelpers.assertThrows(
        "Should throw when attempting to use an invalid ref for scanning",
        IllegalArgumentException.class,
        "Cannot find ref nonexisting",
        () -> table.newScan().useRef("nonexisting"));
  }

  private void validateExpectedFileScanTasks(
      TableScan scan, List<CharSequence> expectedFileScanPaths) throws IOException {
    try (CloseableIterable<FileScanTask> scanTasks = scan.planFiles()) {
      Assert.assertEquals(expectedFileScanPaths.size(), Iterables.size(scanTasks));
      List<CharSequence> actualFiles = Lists.newArrayList();
      scanTasks.forEach(task -> actualFiles.add(task.file().path()));
      Assert.assertTrue(actualFiles.containsAll(expectedFileScanPaths));
    }
  }
}
