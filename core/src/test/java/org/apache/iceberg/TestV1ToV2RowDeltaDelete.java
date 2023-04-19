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

import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.equal;

import java.util.List;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class TestV1ToV2RowDeltaDelete extends TableTestBase {

  public TestV1ToV2RowDeltaDelete() {
    super(1 /* table format version */);
  }

  static final DeleteFile FILE_A_POS_1 =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-a-pos-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartition(FILE_A.partition())
          .withRecordCount(1)
          .build();

  static final DeleteFile FILE_A_EQ_1 =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofEqualityDeletes()
          .withPath("/path/to/data-a-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartition(FILE_A.partition())
          .withRecordCount(1)
          .build();

  private void verifyManifestSequenceNumber(
      ManifestFile mf, long sequenceNum, long minSequenceNum) {
    Assert.assertEquals(
        "sequence number should be " + sequenceNum, mf.sequenceNumber(), sequenceNum);
    Assert.assertEquals(
        "min sequence number should be " + minSequenceNum, mf.minSequenceNumber(), minSequenceNum);
  }

  @Test
  public void testPartitionedTableWithPartitionEqDeletes() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();

    List<ManifestFile> dataManifests = table.currentSnapshot().dataManifests(table.io());
    List<ManifestFile> deleteManifests = table.currentSnapshot().deleteManifests(table.io());
    Assert.assertEquals("Should have one data manifest file", 1, dataManifests.size());
    Assert.assertEquals("Should have zero delete manifest file", 0, deleteManifests.size());
    ManifestFile dataManifest = dataManifests.get(0);
    verifyManifestSequenceNumber(dataManifest, 0, 0);

    // update table version to 2
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata base = ops.current();
    ops.commit(base, base.upgradeToFormatVersion(2));

    table.newRowDelta().addDeletes(FILE_A_EQ_1).commit();

    dataManifests = table.currentSnapshot().dataManifests(ops.io());
    deleteManifests = table.currentSnapshot().deleteManifests(ops.io());
    Assert.assertEquals("Should have one data manifest file", 1, dataManifests.size());
    Assert.assertEquals("Should have one delete manifest file", 1, deleteManifests.size());
    Assert.assertEquals(dataManifest, dataManifests.get(0)); // data manifest not changed
    ManifestFile deleteManifest = deleteManifests.get(0);
    verifyManifestSequenceNumber(deleteManifest, 1, 1);
    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles().iterator());
    Assert.assertEquals("Should have three task", 3, tasks.size());
    Optional<FileScanTask> task =
        tasks.stream().filter(t -> t.file().path().equals(FILE_A.path())).findFirst();
    Assert.assertTrue(task.isPresent());
    Assert.assertEquals("Should have one associated delete file", 1, task.get().deletes().size());
    Assert.assertEquals(
        "Should have only pos delete file", FILE_A_EQ_1.path(), task.get().deletes().get(0).path());

    // first commit after row-delta changes
    table.newDelete().deleteFile(FILE_B).commit();

    dataManifests = table.currentSnapshot().dataManifests(ops.io());
    deleteManifests = table.currentSnapshot().deleteManifests(ops.io());
    Assert.assertEquals("Should have one data manifest file", 1, dataManifests.size());
    Assert.assertEquals("Should have one delete manifest file", 1, deleteManifests.size());
    ManifestFile dataManifest2 = dataManifests.get(0);
    verifyManifestSequenceNumber(dataManifest2, 2, 0);
    Assert.assertNotEquals(dataManifest, dataManifest2);
    Assert.assertEquals(deleteManifest, deleteManifests.get(0)); // delete manifest not changed
    tasks = Lists.newArrayList(table.newScan().planFiles().iterator());
    Assert.assertEquals("Should have two task", 2, tasks.size());
    task = tasks.stream().filter(t -> t.file().path().equals(FILE_A.path())).findFirst();
    Assert.assertTrue(task.isPresent());
    Assert.assertEquals("Should have one associated delete file", 1, task.get().deletes().size());

    // second commit after row-delta changes
    table.newDelete().deleteFile(FILE_C).commit();

    dataManifests = table.currentSnapshot().dataManifests(ops.io());
    deleteManifests = table.currentSnapshot().deleteManifests(ops.io());
    Assert.assertEquals("Should have one data manifest file", 1, dataManifests.size());
    Assert.assertEquals("Should have one delete manifest file", 1, deleteManifests.size());
    ManifestFile dataManifest3 = dataManifests.get(0);
    verifyManifestSequenceNumber(dataManifest3, 3, 0);
    Assert.assertNotEquals(dataManifest2, dataManifest3);
    Assert.assertEquals(deleteManifest, deleteManifests.get(0)); // delete manifest not changed
    tasks = Lists.newArrayList(table.newScan().planFiles().iterator());
    Assert.assertEquals("Should have one task", 1, tasks.size());
    task = tasks.stream().filter(t -> t.file().path().equals(FILE_A.path())).findFirst();
    Assert.assertTrue(task.isPresent());
    Assert.assertEquals("Should have one associated delete file", 1, task.get().deletes().size());
  }

  @Test
  public void testPartitionedTableWithUnrelatedPartitionDeletes() {
    table.newAppend().appendFile(FILE_B).appendFile(FILE_C).appendFile(FILE_D).commit();

    // update table version to 2
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata base = ops.current();
    ops.commit(base, base.upgradeToFormatVersion(2));

    table.newRowDelta().addDeletes(FILE_A_POS_1).addDeletes(FILE_A_EQ_1).commit();

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles().iterator());
    Assert.assertEquals("Should have three task", 3, tasks.size());
    Assert.assertEquals(
        "Should have the correct data file path", FILE_B.path(), tasks.get(0).file().path());
    Assert.assertEquals(
        "Should have zero associated delete file", 0, tasks.get(0).deletes().size());

    table.newDelete().deleteFile(FILE_B).commit();
    tasks = Lists.newArrayList(table.newScan().planFiles().iterator());
    Assert.assertEquals("Should have two task", 2, tasks.size());
    Assert.assertEquals(
        "Should have zero associated delete file", 0, tasks.get(0).deletes().size());

    table.newDelete().deleteFile(FILE_C).commit();
    tasks = Lists.newArrayList(table.newScan().planFiles().iterator());
    Assert.assertEquals("Should have one task", 1, tasks.size());
    Assert.assertEquals(
        "Should have zero associated delete file", 0, tasks.get(0).deletes().size());
  }

  @Test
  public void testPartitionedTableWithExistingDeleteFile() {
    table.updateProperties().set(TableProperties.MANIFEST_MERGE_ENABLED, "false").commit();

    table.newAppend().appendFile(FILE_A).commit();

    // update table version to 2
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata base = ops.current();
    ops.commit(base, base.upgradeToFormatVersion(2));

    table.newRowDelta().addDeletes(FILE_A_EQ_1).commit();

    table.newRowDelta().addDeletes(FILE_A_POS_1).commit();

    table
        .updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
        .set(TableProperties.MANIFEST_MERGE_ENABLED, "true")
        .commit();

    Assert.assertEquals(
        "Should have two delete manifests",
        2,
        table.currentSnapshot().deleteManifests(table.io()).size());

    // merge delete manifests
    table.newAppend().appendFile(FILE_B).commit();

    Assert.assertEquals(
        "Should have one delete manifest",
        1,
        table.currentSnapshot().deleteManifests(table.io()).size());
    Assert.assertEquals(
        "Should have zero added delete file",
        0,
        table.currentSnapshot().deleteManifests(table.io()).get(0).addedFilesCount().intValue());
    Assert.assertEquals(
        "Should have zero deleted delete file",
        0,
        table.currentSnapshot().deleteManifests(table.io()).get(0).deletedFilesCount().intValue());
    Assert.assertEquals(
        "Should have two existing delete files",
        2,
        table.currentSnapshot().deleteManifests(table.io()).get(0).existingFilesCount().intValue());

    List<FileScanTask> tasks =
        Lists.newArrayList(
            table
                .newScan()
                .filter(equal(bucket("data", BUCKETS_NUMBER), 0))
                .planFiles()
                .iterator());
    Assert.assertEquals("Should have one task", 1, tasks.size());

    FileScanTask task = tasks.get(0);
    Assert.assertEquals(
        "Should have the correct data file path", FILE_A.path(), task.file().path());
    Assert.assertEquals("Should have two associated delete files", 2, task.deletes().size());
    Assert.assertEquals(
        "Should have expected delete files",
        Sets.newHashSet(FILE_A_EQ_1.path(), FILE_A_POS_1.path()),
        Sets.newHashSet(Iterables.transform(task.deletes(), ContentFile::path)));
  }

  @Test
  public void testSequenceNumbersInUpgradedTables() {
    // add initial data
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot appendSnapshot = table.currentSnapshot();

    validateManifest(
        appendSnapshot.dataManifests(table.io()).get(0),
        dataSeqs(0L, 0L),
        fileSeqs(0L, 0L),
        ids(appendSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));

    // upgrade the table to v2
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();

    V1Assert.disable();
    V2Assert.enable();

    // add a row delta with delete files
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();

    Snapshot deltaSnapshot = table.currentSnapshot();

    validateManifest(
        deltaSnapshot.dataManifests(table.io()).get(0),
        dataSeqs(0L, 0L),
        fileSeqs(0L, 0L),
        ids(appendSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));

    validateDeleteManifest(
        deltaSnapshot.deleteManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deltaSnapshot.snapshotId()),
        files(FILE_A_DELETES),
        statuses(ManifestEntry.Status.ADDED));

    // delete one of the initial data files to rewrite the v1 data manifest
    table.newDelete().deleteFile(FILE_B).commit();

    Snapshot deleteSnapshot = table.currentSnapshot();

    validateManifest(
        deleteSnapshot.dataManifests(table.io()).get(0),
        dataSeqs(0L, 0L),
        fileSeqs(0L, 0L),
        ids(appendSnapshot.snapshotId(), deleteSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.DELETED));

    validateDeleteManifest(
        deleteSnapshot.deleteManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deltaSnapshot.snapshotId()),
        files(FILE_A_DELETES),
        statuses(ManifestEntry.Status.ADDED));
  }
}
