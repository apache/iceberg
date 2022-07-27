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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestDeleteFiles extends TableTestBase {

  private static final DataFile DATA_FILE_BUCKET_0_IDS_0_2 =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withMetrics(
              new Metrics(
                  5L,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L, 2, 5L), // value count
                  ImmutableMap.of(1, 0L, 2, 0L), // null count
                  null, // no nan value counts
                  ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(2L)) // upper bounds
                  ))
          .build();

  private static final DataFile DATA_FILE_BUCKET_0_IDS_8_10 =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-2.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withMetrics(
              new Metrics(
                  5L,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L, 2, 5L), // value count
                  ImmutableMap.of(1, 0L, 2, 0L), // null count
                  null, // no nan value counts
                  ImmutableMap.of(1, longToBuffer(8L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(10L)) // upper bounds
                  ))
          .build();

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestDeleteFiles(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testMultipleDeletes() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();

    Assert.assertEquals("Metadata should be at version 1", 1L, (long) version());
    Snapshot append = readMetadata().currentSnapshot();
    validateSnapshot(null, append, FILE_A, FILE_B, FILE_C);

    table.newDelete().deleteFile(FILE_A).commit();

    Assert.assertEquals("Metadata should be at version 2", 2L, (long) version());
    Snapshot delete = readMetadata().currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, delete.allManifests(FILE_IO).size());
    validateManifestEntries(
        delete.allManifests(table.io()).get(0),
        ids(delete.snapshotId(), append.snapshotId(), append.snapshotId()),
        files(FILE_A, FILE_B, FILE_C),
        statuses(Status.DELETED, Status.EXISTING, Status.EXISTING));

    table.newDelete().deleteFile(FILE_B).commit();

    Assert.assertEquals("Metadata should be at version 3", 3L, (long) version());
    Snapshot delete2 = readMetadata().currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, delete2.allManifests(FILE_IO).size());
    validateManifestEntries(
        delete2.allManifests(FILE_IO).get(0),
        ids(delete2.snapshotId(), append.snapshotId()),
        files(FILE_B, FILE_C),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testAlreadyDeletedFilesAreIgnoredDuringDeletesByRowFilter() {
    PartitionSpec spec = table.spec();

    DataFile firstDataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data-2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withMetrics(
                new Metrics(
                    5L,
                    null, // no column sizes
                    ImmutableMap.of(1, 5L, 2, 5L), // value count
                    ImmutableMap.of(1, 0L, 2, 0L), // null count
                    null, // no nan value counts
                    ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                    ImmutableMap.of(1, longToBuffer(10L)) // upper bounds
                    ))
            .build();

    DataFile secondDataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data-1.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withMetrics(
                new Metrics(
                    5L,
                    null, // no column sizes
                    ImmutableMap.of(1, 5L, 2, 5L), // value count
                    ImmutableMap.of(1, 0L, 2, 0L), // null count
                    null, // no nan value counts
                    ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                    ImmutableMap.of(1, longToBuffer(4L)) // upper bounds
                    ))
            .build();

    // add both data files
    table.newFastAppend().appendFile(firstDataFile).appendFile(secondDataFile).commit();

    Snapshot initialSnapshot = table.currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, initialSnapshot.allManifests(FILE_IO).size());
    validateManifestEntries(
        initialSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), initialSnapshot.snapshotId()),
        files(firstDataFile, secondDataFile),
        statuses(Status.ADDED, Status.ADDED));

    // delete the first data file
    table.newDelete().deleteFile(firstDataFile).commit();

    Snapshot deleteSnapshot = table.currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, deleteSnapshot.allManifests(FILE_IO).size());
    validateManifestEntries(
        deleteSnapshot.allManifests(FILE_IO).get(0),
        ids(deleteSnapshot.snapshotId(), initialSnapshot.snapshotId()),
        files(firstDataFile, secondDataFile),
        statuses(Status.DELETED, Status.EXISTING));

    // delete the second data file using a row filter
    // the commit should succeed as there is only one live data file
    table.newDelete().deleteFromRowFilter(Expressions.lessThan("id", 7)).commit();

    Snapshot finalSnapshot = table.currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, finalSnapshot.allManifests(FILE_IO).size());
    validateManifestEntries(
        finalSnapshot.allManifests(FILE_IO).get(0),
        ids(finalSnapshot.snapshotId()),
        files(secondDataFile),
        statuses(Status.DELETED));
  }

  @Test
  public void testDeleteSomeFilesByRowFilterWithoutPartitionPredicates() {
    // add both data files
    table
        .newFastAppend()
        .appendFile(DATA_FILE_BUCKET_0_IDS_0_2)
        .appendFile(DATA_FILE_BUCKET_0_IDS_8_10)
        .commit();

    Snapshot initialSnapshot = table.currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, initialSnapshot.allManifests(FILE_IO).size());
    validateManifestEntries(
        initialSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), initialSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2, DATA_FILE_BUCKET_0_IDS_8_10),
        statuses(Status.ADDED, Status.ADDED));

    // delete the second one using a metrics filter (no partition filter)
    table.newDelete().deleteFromRowFilter(Expressions.greaterThan("id", 5)).commit();

    Snapshot deleteSnapshot = table.currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, deleteSnapshot.allManifests(FILE_IO).size());
    validateManifestEntries(
        deleteSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), deleteSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2, DATA_FILE_BUCKET_0_IDS_8_10),
        statuses(Status.EXISTING, Status.DELETED));
  }

  @Test
  public void testDeleteSomeFilesByRowFilterWithCombinedPredicates() {
    // add both data files
    table
        .newFastAppend()
        .appendFile(DATA_FILE_BUCKET_0_IDS_0_2)
        .appendFile(DATA_FILE_BUCKET_0_IDS_8_10)
        .commit();

    Snapshot initialSnapshot = table.currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, initialSnapshot.allManifests(FILE_IO).size());
    validateManifestEntries(
        initialSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), initialSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2, DATA_FILE_BUCKET_0_IDS_8_10),
        statuses(Status.ADDED, Status.ADDED));

    // delete the second one using a filter that relies on metrics and partition data
    Expression partPredicate = Expressions.equal(Expressions.bucket("data", 16), 0);
    Expression rowPredicate = Expressions.greaterThan("id", 5);
    Expression predicate = Expressions.and(partPredicate, rowPredicate);
    table.newDelete().deleteFromRowFilter(predicate).commit();

    Snapshot deleteSnapshot = table.currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, deleteSnapshot.allManifests(FILE_IO).size());
    validateManifestEntries(
        deleteSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), deleteSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2, DATA_FILE_BUCKET_0_IDS_8_10),
        statuses(Status.EXISTING, Status.DELETED));
  }

  @Test
  public void testCannotDeleteFileWhereNotAllRowsMatchPartitionFilter() {
    Assume.assumeTrue(formatVersion == 2);

    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.truncate("data", 2))
        .commit();

    PartitionSpec spec = table.spec();

    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data-1.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartitionPath("data_trunc_2=aa")
            .build();

    table.newFastAppend().appendFile(dataFile).commit();

    AssertHelpers.assertThrows(
        "Should reject as not all rows match filter",
        ValidationException.class,
        "Cannot delete file where some, but not all, rows match filter",
        () -> table.newDelete().deleteFromRowFilter(Expressions.equal("data", "aa")).commit());
  }

  @Test
  public void testDeleteCaseSensitivity() {
    table.newFastAppend().appendFile(DATA_FILE_BUCKET_0_IDS_0_2).commit();

    Expression rowFilter = Expressions.lessThan("iD", 5);

    AssertHelpers.assertThrows(
        "Should use case sensitive binding by default",
        ValidationException.class,
        "Cannot find field 'iD'",
        () -> table.newDelete().deleteFromRowFilter(rowFilter).commit());

    AssertHelpers.assertThrows(
        "Should fail with case sensitive binding",
        ValidationException.class,
        "Cannot find field 'iD'",
        () -> table.newDelete().deleteFromRowFilter(rowFilter).caseSensitive(true).commit());

    table.newDelete().deleteFromRowFilter(rowFilter).caseSensitive(false).commit();

    Snapshot deleteSnapshot = table.currentSnapshot();
    Assert.assertEquals("Should have 1 manifest", 1, deleteSnapshot.allManifests(FILE_IO).size());
    validateManifestEntries(
        deleteSnapshot.allManifests(FILE_IO).get(0),
        ids(deleteSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2),
        statuses(Status.DELETED));
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}
