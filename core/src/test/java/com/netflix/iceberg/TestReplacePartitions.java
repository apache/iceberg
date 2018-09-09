/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.netflix.iceberg.ManifestEntry.Status;
import com.netflix.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Test;

public class TestReplacePartitions extends TableTestBase {

  static final DataFile FILE_E = DataFiles.builder(SPEC)
      .withPath("/path/to/data-e.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=0") // same partition as FILE_A
      .withRecordCount(0)
      .build();

  static final DataFile FILE_F = DataFiles.builder(SPEC)
      .withPath("/path/to/data-f.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=1") // same partition as FILE_B
      .withRecordCount(0)
      .build();

  static final DataFile FILE_G = DataFiles.builder(SPEC)
      .withPath("/path/to/data-g.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=10") // no other partition
      .withRecordCount(0)
      .build();

  @Test
  public void testReplaceOnePartition() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();

    table.newReplacePartitions()
        .addFile(FILE_E)
        .commit();

    long replaceId = readMetadata().currentSnapshot().snapshotId();
    Assert.assertNotEquals("Should create a new snapshot", baseId, replaceId);
    Assert.assertEquals("Table should have 2 manifests",
        2, table.currentSnapshot().manifests().size());

    // manifest is not merged because it is less than the minimum
    validateManifestEntries(table.currentSnapshot().manifests().get(0),
        ids(replaceId),
        files(FILE_E),
        statuses(Status.ADDED));

    validateManifestEntries(table.currentSnapshot().manifests().get(1),
        ids(replaceId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testReplaceOnePartitionWithMerge() {
    // ensure the overwrite results in a merge
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();

    table.newReplacePartitions()
        .addFile(FILE_E)
        .commit();

    long replaceId = readMetadata().currentSnapshot().snapshotId();
    Assert.assertNotEquals("Should create a new snapshot", baseId, replaceId);
    Assert.assertEquals("Table should have 2 manifests",
        1, table.currentSnapshot().manifests().size());

    validateManifestEntries(table.currentSnapshot().manifests().get(0),
        ids(replaceId, replaceId, baseId),
        files(FILE_E, FILE_A, FILE_B),
        statuses(Status.ADDED, Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testValidationFailure() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();

    ReplacePartitions replace = table.newReplacePartitions()
        .addFile(FILE_F)
        .addFile(FILE_G)
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
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();

    table.newReplacePartitions()
        .addFile(FILE_G)
        .validateAppendOnly()
        .commit();

    long replaceId = readMetadata().currentSnapshot().snapshotId();
    Assert.assertNotEquals("Should create a new snapshot", baseId, replaceId);
    Assert.assertEquals("Table should have 2 manifests",
        2, table.currentSnapshot().manifests().size());

    // manifest is not merged because it is less than the minimum
    validateManifestEntries(table.currentSnapshot().manifests().get(0),
        ids(replaceId),
        files(FILE_G),
        statuses(Status.ADDED));

    validateManifestEntries(table.currentSnapshot().manifests().get(1),
        ids(baseId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
  }
}
