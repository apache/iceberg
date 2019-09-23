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

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestReplacePartitions extends TableTestBase {

  static DataFile fileE;

  static DataFile fileF;

  static DataFile fileG;

  @Before
  public void before() {
    fileE = DataFiles.builder(SPEC, table.location())
            .withPath("/path/to/data-e.parquet")
            .withFileSizeInBytes(0)
            .withPartitionPath("data_bucket=0") // same partition as FILE_A
            .withRecordCount(0)
            .build();

    fileF = DataFiles.builder(SPEC, table.location())
            .withPath("/path/to/data-f.parquet")
            .withFileSizeInBytes(0)
            .withPartitionPath("data_bucket=1") // same partition as FILE_B
            .withRecordCount(0)
            .build();

    fileG = DataFiles.builder(SPEC, table.location())
            .withPath("/path/to/data-g.parquet")
            .withFileSizeInBytes(0)
            .withPartitionPath("data_bucket=10") // no other partition
            .withRecordCount(0)
            .build();
  }

  @Test
  public void testReplaceOnePartition() {
    table.newFastAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();

    table.newReplacePartitions()
        .addFile(fileE)
        .commit();

    long replaceId = readMetadata().currentSnapshot().snapshotId();
    Assert.assertNotEquals("Should create a new snapshot", baseId, replaceId);
    Assert.assertEquals("Table should have 2 manifests",
        2, table.currentSnapshot().manifests().size());

    // manifest is not merged because it is less than the minimum
    validateManifestEntries(table.currentSnapshot().manifests().get(0),
        ids(replaceId),
        files(fileE),
        statuses(Status.ADDED),
        table.location());

    validateManifestEntries(table.currentSnapshot().manifests().get(1),
        ids(replaceId, baseId),
        files(fileA, fileB),
        statuses(Status.DELETED, Status.EXISTING),
        table.location());
  }

  @Test
  public void testReplaceAndMergeOnePartition() {
    // ensure the overwrite results in a merge
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    table.newFastAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();

    table.newReplacePartitions()
        .addFile(fileE)
        .commit();

    long replaceId = readMetadata().currentSnapshot().snapshotId();
    Assert.assertNotEquals("Should create a new snapshot", baseId, replaceId);
    Assert.assertEquals("Table should have 1 manifest",
        1, table.currentSnapshot().manifests().size());

    validateManifestEntries(table.currentSnapshot().manifests().get(0),
        ids(replaceId, replaceId, baseId),
        files(fileE, fileA, fileB),
        statuses(Status.ADDED, Status.DELETED, Status.EXISTING),
        table.location());
  }

  @Test
  public void testReplaceWithUnpartitionedTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Table unpartitioned = TestTables.create(
        tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned());

    Assert.assertEquals("Table version should be 0",
        0, (long) TestTables.metadataVersion("unpartitioned"));

    unpartitioned.newAppend()
        .appendFile(fileA)
        .commit();

    // make sure the data was successfully added
    Assert.assertEquals("Table version should be 1",
        1, (long) TestTables.metadataVersion("unpartitioned"));
    validateSnapshot(
        null,
        TestTables.readMetadata("unpartitioned").currentSnapshot(),
        unpartitioned.location(),
        fileA);

    unpartitioned.newReplacePartitions()
        .addFile(fileB)
        .commit();

    Assert.assertEquals("Table version should be 2",
        2, (long) TestTables.metadataVersion("unpartitioned"));
    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceId = replaceMetadata.currentSnapshot().snapshotId();

    Assert.assertEquals("Table should have 2 manifests",
        2, replaceMetadata.currentSnapshot().manifests().size());

    validateManifestEntries(replaceMetadata.currentSnapshot().manifests().get(0),
        ids(replaceId), files(fileB), statuses(Status.ADDED),
        replaceMetadata.location());

    validateManifestEntries(replaceMetadata.currentSnapshot().manifests().get(1),
        ids(replaceId), files(fileA), statuses(Status.DELETED),
        replaceMetadata.location());
  }

  @Test
  public void testReplaceAndMergeWithUnpartitionedTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Table unpartitioned = TestTables.create(
        tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned());

    // ensure the overwrite results in a merge
    unpartitioned.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    Assert.assertEquals("Table version should be 1",
        1, (long) TestTables.metadataVersion("unpartitioned"));

    unpartitioned.newAppend()
        .appendFile(fileA)
        .commit();

    // make sure the data was successfully added
    Assert.assertEquals("Table version should be 2",
        2, (long) TestTables.metadataVersion("unpartitioned"));
    validateSnapshot(
        null,
        TestTables.readMetadata("unpartitioned").currentSnapshot(),
        unpartitioned.location(),
        fileA
    );

    unpartitioned.newReplacePartitions()
        .addFile(fileB)
        .commit();

    Assert.assertEquals("Table version should be 3",
        3, (long) TestTables.metadataVersion("unpartitioned"));
    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceId = replaceMetadata.currentSnapshot().snapshotId();

    Assert.assertEquals("Table should have 1 manifest",
        1, replaceMetadata.currentSnapshot().manifests().size());

    validateManifestEntries(replaceMetadata.currentSnapshot().manifests().get(0),
        ids(replaceId, replaceId), files(fileB, fileA), statuses(Status.ADDED, Status.DELETED),
        replaceMetadata.location());
  }

  @Test
  public void testValidationFailure() {
    table.newFastAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();

    ReplacePartitions replace = table.newReplacePartitions()
        .addFile(fileF)
        .addFile(fileG)
        .validateAppendOnly();

    AssertHelpers.assertThrows("Should reject commit with file not matching delete expression",
        ValidationException.class, "Cannot commit file that conflicts with existing partition",
        replace::commit);

    Assert.assertEquals("Should not create a new snapshot",
        baseId, readMetadata().currentSnapshot().snapshotId());
  }

  @Test
  public void testValidationSuccess() {
    table.newFastAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();

    table.newReplacePartitions()
        .addFile(fileG)
        .validateAppendOnly()
        .commit();

    long replaceId = readMetadata().currentSnapshot().snapshotId();
    Assert.assertNotEquals("Should create a new snapshot", baseId, replaceId);
    Assert.assertEquals("Table should have 2 manifests",
        2, table.currentSnapshot().manifests().size());

    // manifest is not merged because it is less than the minimum
    validateManifestEntries(table.currentSnapshot().manifests().get(0),
        ids(replaceId),
        files(fileG),
        statuses(Status.ADDED),
        table.location());

    validateManifestEntries(table.currentSnapshot().manifests().get(1),
        ids(baseId, baseId),
        files(fileA, fileB),
        statuses(Status.ADDED, Status.ADDED),
        table.location());
  }
}
