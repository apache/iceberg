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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSnapshotChanges {
  @TempDir private File tableDir;

  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  // Partition spec used to create tables
  protected static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

  private static final Map<Integer, Long> COLUMN_SIZES = ImmutableMap.of(3, 20L);
  private static final Map<Integer, Long> VALUE_COUNTS = ImmutableMap.of(3, 1L);
  private static final Map<Integer, Long> NULL_VALUE_COUNTS = ImmutableMap.of(3, 0L);
  private static final Map<Integer, Long> NAN_VALUE_COUNTS = ImmutableMap.of(3, 1L);
  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 2));

  private static final DataFile FILE_WITH_STATS =
      DataFiles.builder(SPEC)
          .withPath("/path/to/file-with-stats.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .withMetrics(
              new Metrics(
                  1L,
                  COLUMN_SIZES,
                  VALUE_COUNTS,
                  NULL_VALUE_COUNTS,
                  NAN_VALUE_COUNTS,
                  LOWER_BOUNDS,
                  UPPER_BOUNDS))
          .build();

  public TestTables.TestTable table = null;

  @BeforeEach
  public void before() throws Exception {
    new File(tableDir, "metadata");
    this.table = TestTables.create(tableDir, "test", SCHEMA, SPEC, 2);
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testAddedDataFiles() {
    DataFile addedFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/test-data.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    table.newFastAppend().appendFile(addedFile).commit();
    Snapshot snapshotWithAddedFile = table.currentSnapshot();

    // Test using SnapshotChanges object directly
    SnapshotChanges changes =
        SnapshotChanges.builderFor(table).snapshot(snapshotWithAddedFile).build();
    Iterable<DataFile> filesFromChanges = changes.addedDataFiles();
    assertThat(filesFromChanges).hasSize(1);

    // Verify the file path matches
    DataFile resultFile = filesFromChanges.iterator().next();
    assertThat(resultFile.path().toString()).isEqualTo(addedFile.path().toString());
  }

  @Test
  public void testRemovedDataFiles() {
    DataFile fileToRemove =
        DataFiles.builder(SPEC)
            .withPath("/path/to/file-to-remove.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    DataFile fileToKeep =
        DataFiles.builder(SPEC)
            .withPath("/path/to/file-to-keep.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    // Add both files
    table.newAppend().appendFile(fileToRemove).appendFile(fileToKeep).commit();

    // Remove one file
    table.newDelete().deleteFile(fileToRemove).commit();

    Snapshot snapshotAfterDelete = table.currentSnapshot();

    // Test using SnapshotChanges object directly (for caching multiple calls)
    SnapshotChanges changes =
        SnapshotChanges.builderFor(table).snapshot(snapshotAfterDelete).build();
    Iterable<DataFile> filesFromChangesFirstCall = changes.removedDataFiles();
    Iterable<DataFile> filesFromChangesSecondCall = changes.removedDataFiles();
    assertThat(filesFromChangesFirstCall).isSameAs(filesFromChangesSecondCall);

    // Verify the file path matches
    DataFile resultFile = filesFromChangesFirstCall.iterator().next();
    assertThat(resultFile.path().toString()).isEqualTo(fileToRemove.path().toString());
  }

  @Test
  public void testSnapshotChangesCaching() {
    DataFile firstFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/file1.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    DataFile secondFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/file2.parquet")
            .withFileSizeInBytes(20)
            .withRecordCount(2)
            .build();

    table.newAppend().appendFile(firstFile).appendFile(secondFile).commit();
    table.newDelete().deleteFile(firstFile).commit();

    Snapshot snapshotAfterDelete = table.currentSnapshot();

    SnapshotChanges changes =
        SnapshotChanges.builderFor(table).snapshot(snapshotAfterDelete).build();

    // First call should cache the data file changes
    Iterable<DataFile> firstCallResult = changes.removedDataFiles();
    assertThat(firstCallResult).hasSize(1);

    // Second call should return the cached results
    Iterable<DataFile> secondCallResult = changes.removedDataFiles();
    assertThat(secondCallResult).hasSize(1);

    // Both calls should return the same reference (cached)
    assertThat(firstCallResult).isSameAs(secondCallResult);
  }

  @Test
  public void testColumnStatsRetainedByDefault() {
    table.newFastAppend().appendFile(FILE_WITH_STATS).commit();

    DataFile file =
        SnapshotChanges.builderFor(table)
            .snapshot(table.currentSnapshot())
            .build()
            .addedDataFiles()
            .iterator()
            .next();

    assertThat(file.columnSizes()).isEqualTo(COLUMN_SIZES);
    assertThat(file.valueCounts()).isEqualTo(VALUE_COUNTS);
    assertThat(file.nullValueCounts()).isEqualTo(NULL_VALUE_COUNTS);
    assertThat(file.nanValueCounts()).isEqualTo(NAN_VALUE_COUNTS);
    assertThat(file.lowerBounds()).isEqualTo(LOWER_BOUNDS);
    assertThat(file.upperBounds()).isEqualTo(UPPER_BOUNDS);
  }

  @Test
  public void testIncludeColumnStatsFalseDropsStats() {
    table.newFastAppend().appendFile(FILE_WITH_STATS).commit();

    DataFile file =
        SnapshotChanges.builderFor(table)
            .snapshot(table.currentSnapshot())
            .includeColumnStats(false)
            .build()
            .addedDataFiles()
            .iterator()
            .next();

    assertThat(file.columnSizes()).isNull();
    assertThat(file.valueCounts()).isNull();
    assertThat(file.nullValueCounts()).isNull();
    assertThat(file.nanValueCounts()).isNull();
    assertThat(file.lowerBounds()).isNull();
    assertThat(file.upperBounds()).isNull();

    assertThat(file.location()).isEqualTo(FILE_WITH_STATS.location());
    assertThat(file.format()).isEqualTo(FileFormat.PARQUET);
    assertThat(file.content()).isEqualTo(FileContent.DATA);
    assertThat(file.fileSizeInBytes()).isEqualTo(10);
    assertThat(file.recordCount()).isEqualTo(1);
    assertThat(file.specId()).isEqualTo(table.spec().specId());
  }

  @Test
  public void testIncludeColumnStatsFalseRetainsPartition() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    TestTables.TestTable partitioned =
        TestTables.create(new File(tableDir, "partitioned"), "partitioned", SCHEMA, spec, 2);
    partitioned
        .newFastAppend()
        .appendFile(
            DataFiles.builder(spec)
                .withPath("/path/to/partitioned.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withPartitionPath("data=a")
                .build())
        .commit();

    DataFile file =
        SnapshotChanges.builderFor(partitioned)
            .snapshot(partitioned.currentSnapshot())
            .includeColumnStats(false)
            .build()
            .addedDataFiles()
            .iterator()
            .next();

    assertThat(file.partition().get(0, String.class)).isEqualTo("a");
  }

  @Test
  public void testIncludeColumnStatsFalseRetainsDeleteFileFields() {
    DeleteFile positionDeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-deletes.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    DeleteFile equalityDeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(3)
            .withPath("/path/to/equality-deletes.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    table.newRowDelta().addDeletes(positionDeletes).addDeletes(equalityDeletes).commit();

    Iterable<DeleteFile> files =
        SnapshotChanges.builderFor(table)
            .snapshot(table.currentSnapshot())
            .includeColumnStats(false)
            .build()
            .addedDeleteFiles();

    // content is read from the manifest rather than inherited, so it must stay in the projection
    assertThat(files)
        .extracting(DeleteFile::content)
        .containsExactlyInAnyOrder(FileContent.POSITION_DELETES, FileContent.EQUALITY_DELETES);
    assertThat(files)
        .filteredOn(file -> file.content() == FileContent.EQUALITY_DELETES)
        .singleElement()
        .satisfies(file -> assertThat(file.equalityFieldIds()).containsExactly(3));
  }
}
