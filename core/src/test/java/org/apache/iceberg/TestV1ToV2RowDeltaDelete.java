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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;

public class TestV1ToV2RowDeltaDelete extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1);
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
    assertThat(sequenceNum).isEqualTo(mf.sequenceNumber());
    assertThat(minSequenceNum).isEqualTo(mf.minSequenceNumber());
  }

  @TestTemplate
  public void testPartitionedTableWithPartitionEqDeletes() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();

    List<ManifestFile> dataManifests = table.currentSnapshot().dataManifests(table.io());
    List<ManifestFile> deleteManifests = table.currentSnapshot().deleteManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    assertThat(deleteManifests).isEmpty();
    ManifestFile dataManifest = dataManifests.get(0);
    verifyManifestSequenceNumber(dataManifest, 0, 0);

    // update table version to 2
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata base = ops.current();
    ops.commit(base, base.upgradeToFormatVersion(2));

    table.newRowDelta().addDeletes(FILE_A_EQ_1).commit();

    dataManifests = table.currentSnapshot().dataManifests(ops.io());
    deleteManifests = table.currentSnapshot().deleteManifests(ops.io());
    assertThat(dataManifests).hasSize(1).first().isEqualTo(dataManifest);
    assertThat(deleteManifests).hasSize(1);
    ManifestFile deleteManifest = deleteManifests.get(0);
    verifyManifestSequenceNumber(deleteManifest, 1, 1);
    assertThat(table.newScan().planFiles())
        .hasSize(3)
        .filteredOn(fileScanTask -> fileScanTask.file().path().equals(FILE_A.path()))
        .first()
        .satisfies(
            fileScanTask -> {
              assertThat(fileScanTask.deletes()).hasSize(1);
              assertThat(fileScanTask.deletes().get(0).path()).isEqualTo(FILE_A_EQ_1.path());
            });

    // first commit after row-delta changes
    table.newDelete().deleteFile(FILE_B).commit();

    dataManifests = table.currentSnapshot().dataManifests(ops.io());
    deleteManifests = table.currentSnapshot().deleteManifests(ops.io());
    assertThat(dataManifests).hasSize(1).first().isNotEqualTo(dataManifest);
    assertThat(deleteManifests).hasSize(1).first().isEqualTo(deleteManifest);
    ManifestFile dataManifest2 = dataManifests.get(0);
    verifyManifestSequenceNumber(dataManifest2, 2, 0);
    assertThat(table.newScan().planFiles())
        .hasSize(2)
        .filteredOn(fileScanTask -> fileScanTask.file().path().equals(FILE_A.path()))
        .first()
        .satisfies(fileScanTask -> assertThat(fileScanTask.deletes()).hasSize(1));

    // second commit after row-delta changes
    table.newDelete().deleteFile(FILE_C).commit();

    dataManifests = table.currentSnapshot().dataManifests(ops.io());
    deleteManifests = table.currentSnapshot().deleteManifests(ops.io());
    assertThat(dataManifests).hasSize(1).first().isNotEqualTo(dataManifest2);
    assertThat(deleteManifests).hasSize(1).first().isEqualTo(deleteManifest);
    verifyManifestSequenceNumber(dataManifests.get(0), 3, 0);
    assertThat(table.newScan().planFiles())
        .hasSize(1)
        .filteredOn(fileScanTask -> fileScanTask.file().path().equals(FILE_A.path()))
        .first()
        .satisfies(fileScanTask -> assertThat(fileScanTask.deletes()).hasSize(1));
  }

  @TestTemplate
  public void testPartitionedTableWithUnrelatedPartitionDeletes() {
    table.newAppend().appendFile(FILE_B).appendFile(FILE_C).appendFile(FILE_D).commit();

    // update table version to 2
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata base = ops.current();
    ops.commit(base, base.upgradeToFormatVersion(2));

    table.newRowDelta().addDeletes(FILE_A_POS_1).addDeletes(FILE_A_EQ_1).commit();

    assertThat(table.newScan().planFiles())
        .hasSize(3)
        .first()
        .satisfies(
            fileScanTask -> {
              assertThat(fileScanTask.file().path()).isEqualTo(FILE_B.path());
              assertThat(fileScanTask.deletes()).isEmpty();
            });

    table.newDelete().deleteFile(FILE_B).commit();
    assertThat(table.newScan().planFiles())
        .hasSize(2)
        .first()
        .satisfies(fileScanTask -> assertThat(fileScanTask.deletes()).isEmpty());

    table.newDelete().deleteFile(FILE_C).commit();
    assertThat(table.newScan().planFiles())
        .hasSize(1)
        .first()
        .satisfies(fileScanTask -> assertThat(fileScanTask.deletes()).isEmpty());
  }

  @TestTemplate
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

    assertThat(table.currentSnapshot().deleteManifests(table.io())).hasSize(2);

    // merge delete manifests
    table.newAppend().appendFile(FILE_B).commit();

    assertThat(table.currentSnapshot().deleteManifests(table.io())).hasSize(1);
    assertThat(table.currentSnapshot().deleteManifests(table.io()).get(0).addedFilesCount())
        .isEqualTo(0);
    assertThat(table.currentSnapshot().deleteManifests(table.io()).get(0).deletedFilesCount())
        .isEqualTo(0);
    assertThat(table.currentSnapshot().deleteManifests(table.io()).get(0).existingFilesCount())
        .isEqualTo(2);

    List<FileScanTask> tasks =
        Lists.newArrayList(
            table
                .newScan()
                .filter(equal(bucket("data", BUCKETS_NUMBER), 0))
                .planFiles()
                .iterator());
    assertThat(tasks).hasSize(1);

    FileScanTask task = tasks.get(0);
    assertThat(task.file().path()).isEqualTo(FILE_A.path());
    assertThat(task.deletes()).hasSize(2);
    assertThat(task.deletes().get(0).path()).isEqualTo(FILE_A_EQ_1.path());
    assertThat(task.deletes().get(1).path()).isEqualTo(FILE_A_POS_1.path());
  }

  @TestTemplate
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
