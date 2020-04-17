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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.google.common.collect.Iterators.concat;

@RunWith(Parameterized.class)
public class TestMergeAppend extends TableTestBase {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
        new Object[] { 2 },
    };
  }

  public TestMergeAppend(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testEmptyTableAppend() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    Snapshot pending = table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .apply();

    Assert.assertEquals("Should create 1 manifest for initial write",
        1, pending.manifests().size());

    long pendingId = pending.snapshotId();

    validateManifest(pending.manifests().get(0), ids(pendingId, pendingId), files(FILE_A, FILE_B));
  }

  @Test
  public void testEmptyTableAppendManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    Snapshot pending = table.newAppend()
        .appendManifest(manifest)
        .apply();

    validateSnapshot(base.currentSnapshot(), pending, FILE_A, FILE_B);

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals("Summary metadata should include 2 added files",
        "2", pending.summary().get("added-data-files"));
  }

  @Test
  public void testEmptyTableAppendFilesAndManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    Snapshot pending = table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .appendManifest(manifest)
        .apply();

    long pendingId = pending.snapshotId();

    validateManifest(pending.manifests().get(0),
        ids(pendingId, pendingId),
        files(FILE_C, FILE_D));
    validateManifest(pending.manifests().get(1),
        ids(pendingId, pendingId),
        files(FILE_A, FILE_B));
  }

  @Test
  public void testMergeWithAppendFilesAndManifest() throws IOException {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    Snapshot pending = table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .appendManifest(manifest)
        .apply();

    long pendingId = pending.snapshotId();

    Assert.assertEquals("Should create 1 merged manifest", 1, pending.manifests().size());
    validateManifest(pending.manifests().get(0),
        ids(pendingId, pendingId, pendingId, pendingId),
        files(FILE_C, FILE_D, FILE_A, FILE_B));
  }

  @Test
  public void testMergeWithExistingManifest() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    Snapshot pending = table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .apply();

    Assert.assertEquals("Should contain 1 merged manifest for second write",
        1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long pendingId = pending.snapshotId();

    validateManifest(newManifest,
        ids(pendingId, pendingId, baseId, baseId),
        concat(files(FILE_C, FILE_D), files(initialManifest)));
  }

  @Test
  public void testManifestMergeMinCount() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2")
        // each manifest file is 4554 bytes, so 10000 bytes limit will give us 2 bins with 3 manifest/data files.
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "10000")
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(FILE_A);
    ManifestFile manifest2 = writeManifestWithName("FILE_C", FILE_C);
    ManifestFile manifest3 = writeManifestWithName("FILE_D", FILE_D);
    table.newAppend()
        .appendManifest(manifest)
        .appendManifest(manifest2)
        .appendManifest(manifest3)
        .commit();

    Assert.assertEquals("Should contain 2 merged manifest for first write",
        2, readMetadata().currentSnapshot().manifests().size());

    table.newAppend()
        .appendManifest(manifest)
        .appendManifest(manifest2)
        .appendManifest(manifest3)
        .commit();

    Assert.assertEquals("Should contain 3 merged manifest for second write",
        3, readMetadata().currentSnapshot().manifests().size());

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals("Summary metadata should include 3 added files",
        "3", readMetadata().currentSnapshot().summary().get("added-data-files"));
  }

  @Test
  public void testManifestDoNotMergeMinCount() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());
    table.updateProperties().set("commit.manifest.min-count-to-merge", "4").commit();

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    ManifestFile manifest2 = writeManifestWithName("FILE_C", FILE_C);
    ManifestFile manifest3 = writeManifestWithName("FILE_D", FILE_D);
    Snapshot pending = table.newAppend()
        .appendManifest(manifest)
        .appendManifest(manifest2)
        .appendManifest(manifest3)
        .apply();

    Assert.assertEquals("Should contain 3 merged manifest after 1st write write",
        3, pending.manifests().size());

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals("Summary metadata should include 4 added files",
        "4", pending.summary().get("added-data-files"));
  }

  @Test
  public void testMergeWithExistingManifestAfterDelete() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    table.newDelete()
        .deleteFile(FILE_A)
        .commit();

    TableMetadata delete = readMetadata();
    long deleteId = delete.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 filtered manifest for delete",
        1, delete.currentSnapshot().manifests().size());
    ManifestFile deleteManifest = delete.currentSnapshot().manifests().get(0);

    validateManifestEntries(deleteManifest,
        ids(deleteId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    Snapshot pending = table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .apply();

    Assert.assertEquals("Should contain 1 merged manifest for second write",
        1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long pendingId = pending.snapshotId();

    // the deleted entry from the previous manifest should be removed
    validateManifestEntries(newManifest,
        ids(pendingId, pendingId, baseId),
        files(FILE_C, FILE_D, FILE_B),
        statuses(Status.ADDED, Status.ADDED, Status.EXISTING));
  }

  @Test
  public void testMinMergeCount() {
    // only merge when there are at least 4 manifests
    table.updateProperties().set("commit.manifest.min-count-to-merge", "4").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    long idFileA = readMetadata().currentSnapshot().snapshotId();

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();
    long idFileB = readMetadata().currentSnapshot().snapshotId();

    Assert.assertEquals("Should have 2 manifests from setup writes",
        2, readMetadata().currentSnapshot().manifests().size());

    table.newAppend()
        .appendFile(FILE_C)
        .commit();
    long idFileC = readMetadata().currentSnapshot().snapshotId();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should have 3 unmerged manifests",
        3, base.currentSnapshot().manifests().size());
    Set<ManifestFile> unmerged = Sets.newHashSet(base.currentSnapshot().manifests());

    Snapshot pending = table.newAppend()
        .appendFile(FILE_D)
        .apply();

    Assert.assertEquals("Should contain 1 merged manifest after the 4th write",
        1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertFalse("Should not contain previous manifests", unmerged.contains(newManifest));

    long pendingId = pending.snapshotId();

    validateManifest(newManifest,
        ids(pendingId, idFileC, idFileB, idFileA),
        files(FILE_D, FILE_C, FILE_B, FILE_A));
  }

  @Test
  public void testMergeSizeTargetWithExistingManifest() {
    // use a small limit on manifest size to prevent merging
    table.updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "10")
        .commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    Snapshot pending = table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .apply();

    Assert.assertEquals("Should contain 2 unmerged manifests after second write",
        2, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long pendingId = pending.snapshotId();
    validateManifest(newManifest, ids(pendingId, pendingId), files(FILE_C, FILE_D));

    validateManifest(pending.manifests().get(1), ids(baseId, baseId), files(initialManifest));
  }

  @Test
  public void testChangedPartitionSpec() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("data", 16)
        .bucket("id", 4)
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    DataFile newFileC = DataFiles.builder(newSpec)
        .copy(FILE_C)
        .withPartitionPath("data_bucket=2/id_bucket=3")
        .build();

    Snapshot pending = table.newAppend()
        .appendFile(newFileC)
        .apply();

    Assert.assertEquals("Should use 2 manifest files",
        2, pending.manifests().size());

    // new manifest comes first
    validateManifest(pending.manifests().get(0), ids(pending.snapshotId()), files(newFileC));

    Assert.assertEquals("Second manifest should be the initial manifest with the old spec",
        initialManifest, pending.manifests().get(1));
  }

  @Test
  public void testChangedPartitionSpecMergeExisting() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    long id1 = readMetadata().currentSnapshot().snapshotId();

    // create a second compatible manifest
    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();
    long id2 = readMetadata().currentSnapshot().snapshotId();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should contain 2 manifests",
        2, base.currentSnapshot().manifests().size());
    ManifestFile manifest = base.currentSnapshot().manifests().get(0);

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("data", 16)
        .bucket("id", 4)
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    DataFile newFileC = DataFiles.builder(newSpec)
        .copy(FILE_C)
        .withPartitionPath("data_bucket=2/id_bucket=3")
        .build();

    Snapshot pending = table.newAppend()
        .appendFile(newFileC)
        .apply();

    Assert.assertEquals("Should use 2 manifest files",
        2, pending.manifests().size());
    Assert.assertFalse("First manifest should not be in the new snapshot",
        pending.manifests().contains(manifest));

    validateManifest(pending.manifests().get(0), ids(pending.snapshotId()), files(newFileC));
    validateManifest(pending.manifests().get(1), ids(id2, id1), files(FILE_B, FILE_A));
  }

  @Test
  public void testFailure() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    table.ops().failCommits(5);

    AppendFiles append = table.newAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();

    Assert.assertEquals("Should merge to 1 manifest", 1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);

    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());
    validateManifest(newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(FILE_B), files(initialManifest)));

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest", new File(newManifest.path()).exists());
  }

  @Test
  public void testAppendManifestCleanup() throws IOException {
    // inject 5 failures
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(5);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    AppendFiles append = table.newAppend().appendManifest(manifest);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest", new File(newManifest.path()).exists());
  }

  @Test
  public void testRecovery() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    table.ops().failCommits(3);

    AppendFiles append = table.newAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();

    Assert.assertEquals("Should merge to 1 manifest", 1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);

    Assert.assertTrue("Should create new manifest", new File(newManifest.path()).exists());
    validateManifest(newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(FILE_B), files(initialManifest)));

    append.commit();

    TableMetadata metadata = readMetadata();
    Assert.assertTrue("Should reuse the new manifest", new File(newManifest.path()).exists());
    Assert.assertEquals("Should commit the same new manifest during retry",
        Lists.newArrayList(newManifest), metadata.currentSnapshot().manifests());
  }

  @Test
  public void testAppendManifestWithSnapshotIdInheritance() throws IOException {
    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    table.newAppend()
        .appendManifest(manifest)
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    long snapshotId = snapshot.snapshotId();
    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals("Should have 1 committed manifest", 1, manifests.size());

    validateManifestEntries(manifests.get(0),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals("Summary metadata should include 2 added files",
        "2", snapshot.summary().get("added-data-files"));
    Assert.assertEquals("Summary metadata should include 2 added records",
        "2", snapshot.summary().get("added-records"));
    Assert.assertEquals("Summary metadata should include 2 files in total",
        "2", snapshot.summary().get("total-data-files"));
    Assert.assertEquals("Summary metadata should include 2 records in total",
        "2", snapshot.summary().get("total-records"));
  }

  @Test
  public void testMergedAppendManifestCleanupWithSnapshotIdInheritance() throws IOException {
    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    table.updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
        .commit();

    ManifestFile manifest1 = writeManifestWithName("manifest-file-1.avro", FILE_A, FILE_B);
    table.newAppend()
        .appendManifest(manifest1)
        .commit();

    Assert.assertTrue("Unmerged append manifest should not be deleted", new File(manifest1.path()).exists());

    ManifestFile manifest2 = writeManifestWithName("manifest-file-2.avro", FILE_C, FILE_D);
    table.newAppend()
        .appendManifest(manifest2)
        .commit();

    Snapshot snapshot = table.currentSnapshot();

    Assert.assertEquals("Manifests should be merged into 1", 1, snapshot.manifests().size());
    Assert.assertFalse("Merged append manifest should be deleted", new File(manifest2.path()).exists());
  }

  @Test
  public void testAppendManifestFailureWithSnapshotIdInheritance() throws IOException {
    table.updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .commit();

    table.ops().failCommits(5);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);

    AppendFiles append = table.newAppend();
    append.appendManifest(manifest);

    AssertHelpers.assertThrows("Should reject commit",
        CommitFailedException.class, "Injected failure",
        append::commit);

    Assert.assertTrue("Append manifest should not be deleted", new File(manifest.path()).exists());
  }

  @Test
  public void testInvalidAppendManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifestWithExistingFiles = writeManifest(
        "manifest-file-1.avro",
        manifestEntry(Status.EXISTING, null, FILE_A));
    AssertHelpers.assertThrows("Should reject commit",
        IllegalArgumentException.class, "Cannot append manifest with existing files",
        () -> table.newAppend()
            .appendManifest(manifestWithExistingFiles)
            .commit());

    ManifestFile manifestWithDeletedFiles = writeManifest(
        "manifest-file-2.avro",
        manifestEntry(Status.DELETED, null, FILE_A));
    AssertHelpers.assertThrows("Should reject commit",
        IllegalArgumentException.class, "Cannot append manifest with deleted files",
        () -> table.newAppend()
            .appendManifest(manifestWithDeletedFiles)
            .commit());
  }


  @Test
  public void testUpdatePartitionSpecFieldIdsForV1Table() {
    TableMetadata base = readMetadata();

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("id", 16)
        .identity("data")
        .bucket("data", 4)
        .bucket("data", 16, "data_partition") // reuse field id although different target name
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    List<PartitionSpec> partitionSpecs = table.ops().current().specs();
    PartitionSpec partitionSpec = partitionSpecs.get(0);
    Assert.assertEquals(1000, partitionSpec.lastAssignedFieldId());

    Types.StructType structType = partitionSpec.partitionType();
    List<Types.NestedField> fields = structType.fields();
    Assert.assertEquals(1, fields.size());
    Assert.assertEquals("data_bucket", fields.get(0).name());
    Assert.assertEquals(1000, fields.get(0).fieldId());

    partitionSpec = partitionSpecs.get(1);
    Assert.assertEquals(1003, partitionSpec.lastAssignedFieldId());

    structType = partitionSpec.partitionType();
    fields = structType.fields();
    Assert.assertEquals(4, fields.size());
    Assert.assertEquals("id_bucket", fields.get(0).name());
    Assert.assertEquals(1000, fields.get(0).fieldId());
    Assert.assertEquals("data", fields.get(1).name());
    Assert.assertEquals(1001, fields.get(1).fieldId());
    Assert.assertEquals("data_bucket", fields.get(2).name());
    Assert.assertEquals(1002, fields.get(2).fieldId());
    Assert.assertEquals("data_partition", fields.get(3).name());
    Assert.assertEquals(1003, fields.get(3).fieldId());
  }

  @Test
  public void testManifestEntryFieldIdsForChangedPartitionSpecForV1Table() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("id", 8)
        .bucket("data", 8)
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    DataFile newFile = DataFiles.builder(table.spec())
        .copy(FILE_B)
        .build();

    Snapshot pending = table.newAppend()
        .appendFile(newFile)
        .apply();

    Assert.assertEquals("Should use 2 manifest files",
        2, pending.manifests().size());

    // new manifest comes first
    validateManifest(pending.manifests().get(0), ids(pending.snapshotId()), files(newFile));

    Assert.assertEquals("Second manifest should be the initial manifest with the old spec",
        initialManifest, pending.manifests().get(1));

    // field ids of manifest entries in two manifests with different specs of the same source field should be different
    ManifestEntry entry = ManifestFiles.read(pending.manifests().get(0), FILE_IO).entries().iterator().next();
    Types.NestedField field = ((PartitionData) entry.file().partition()).getPartitionType().fields().get(0);
    Assert.assertEquals(1000, field.fieldId());
    Assert.assertEquals("id_bucket", field.name());
    field = ((PartitionData) entry.file().partition()).getPartitionType().fields().get(1);
    Assert.assertEquals(1001, field.fieldId());
    Assert.assertEquals("data_bucket", field.name());

    entry = ManifestFiles.read(pending.manifests().get(1), FILE_IO).entries().iterator().next();
    field = ((PartitionData) entry.file().partition()).getPartitionType().fields().get(0);
    Assert.assertEquals(1000, field.fieldId());
    Assert.assertEquals("data_bucket", field.name());
  }
}
