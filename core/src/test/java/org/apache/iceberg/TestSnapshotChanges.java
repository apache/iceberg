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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
  public void testAddedDataFilesIterable() {
    DataFile added = dataFile("/path/to/added.parquet");
    table.newFastAppend().appendFile(added).commit();

    SnapshotChanges changes =
        SnapshotChanges.builderFor(table).snapshot(table.currentSnapshot()).build();

    List<DataFile> files = drain(changes.addedDataFilesIterable());
    assertThat(files).hasSize(1);
    assertThat(files.get(0).location()).isEqualTo(added.location());
  }

  @Test
  public void testRemovedDataFilesIterable() {
    DataFile fileToRemove = dataFile("/path/to/remove.parquet");
    DataFile fileToKeep = dataFile("/path/to/keep.parquet");
    table.newAppend().appendFile(fileToRemove).appendFile(fileToKeep).commit();
    table.newDelete().deleteFile(fileToRemove).commit();

    SnapshotChanges changes =
        SnapshotChanges.builderFor(table).snapshot(table.currentSnapshot()).build();

    List<DataFile> removed = drain(changes.removedDataFilesIterable());
    assertThat(removed).hasSize(1);
    assertThat(removed.get(0).location()).isEqualTo(fileToRemove.location());
  }

  @Test
  public void testAddedDeleteFilesIterable() {
    table.newAppend().appendFile(dataFile("/path/to/data.parquet")).commit();
    DeleteFile positionDelete = positionDeleteFile("/path/to/pos-deletes.parquet");
    table.newRowDelta().addDeletes(positionDelete).commit();

    SnapshotChanges changes =
        SnapshotChanges.builderFor(table).snapshot(table.currentSnapshot()).build();

    List<DeleteFile> added = drain(changes.addedDeleteFilesIterable());
    assertThat(added).hasSize(1);
    assertThat(added.get(0).location()).isEqualTo(positionDelete.location());
  }

  @Test
  public void testRemovedDeleteFilesIterable() {
    table.newAppend().appendFile(dataFile("/path/to/data.parquet")).commit();
    DeleteFile delete = positionDeleteFile("/path/to/pos-deletes.parquet");
    table.newRowDelta().addDeletes(delete).commit();
    table.newRowDelta().removeDeletes(delete).commit();

    SnapshotChanges changes =
        SnapshotChanges.builderFor(table).snapshot(table.currentSnapshot()).build();

    List<DeleteFile> removed = drain(changes.removedDeleteFilesIterable());
    assertThat(removed).hasSize(1);
    assertThat(removed.get(0).location()).isEqualTo(delete.location());
  }

  @Test
  public void testStreamingResultsEqualCachedResults() {
    DataFile fileA = dataFile("/path/to/a.parquet");
    DataFile fileB = dataFile("/path/to/b.parquet");
    table.newAppend().appendFile(fileA).appendFile(fileB).commit();
    Snapshot appendSnapshot = table.currentSnapshot();
    table.newDelete().deleteFile(fileA).commit();
    Snapshot deleteSnapshot = table.currentSnapshot();

    SnapshotChanges appendChanges =
        SnapshotChanges.builderFor(table).snapshot(appendSnapshot).build();
    assertThat(locations(drain(appendChanges.addedDataFilesIterable())))
        .containsExactlyInAnyOrderElementsOf(locations(appendChanges.addedDataFiles()));

    SnapshotChanges deleteChanges =
        SnapshotChanges.builderFor(table).snapshot(deleteSnapshot).build();
    assertThat(locations(drain(deleteChanges.removedDataFilesIterable())))
        .containsExactlyInAnyOrderElementsOf(locations(deleteChanges.removedDataFiles()));

    DeleteFile delete = positionDeleteFile("/path/to/pos-deletes.parquet");
    table.newRowDelta().addDeletes(delete).commit();
    SnapshotChanges addDeleteChanges =
        SnapshotChanges.builderFor(table).snapshot(table.currentSnapshot()).build();
    assertThat(locations(drain(addDeleteChanges.addedDeleteFilesIterable())))
        .containsExactlyInAnyOrderElementsOf(locations(addDeleteChanges.addedDeleteFiles()));
  }

  @Test
  public void testStreamingDoesNotCache() {
    table.newFastAppend().appendFile(dataFile("/path/to/a.parquet")).commit();
    SnapshotChanges changes =
        SnapshotChanges.builderFor(table).snapshot(table.currentSnapshot()).build();

    CloseableIterable<DataFile> first = changes.addedDataFilesIterable();
    CloseableIterable<DataFile> second = changes.addedDataFilesIterable();

    // each call builds a fresh, independent pipeline (unlike the cached accessors)
    assertThat(first).isNotSameAs(second);
    assertThat(drain(first)).hasSize(1);
    assertThat(drain(second)).hasSize(1);

    // the cached accessor must still return the same cached reference across calls
    assertThat(changes.addedDataFiles()).isSameAs(changes.addedDataFiles());
  }

  @Test
  public void testAddedDataFilesRetainStatsRemovedFilesDropStats() {
    DataFile fileWithStats = FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of());
    assertThat(fileWithStats.columnSizes()).isNotEmpty();

    table.newAppend().appendFile(fileWithStats).commit();
    Snapshot appendSnapshot = table.currentSnapshot();
    table.newDelete().deleteFile(fileWithStats).commit();
    Snapshot deleteSnapshot = table.currentSnapshot();

    SnapshotChanges added = SnapshotChanges.builderFor(table).snapshot(appendSnapshot).build();
    List<DataFile> addedFiles = drain(added.addedDataFilesIterable());
    assertThat(addedFiles).hasSize(1);
    assertThat(addedFiles.get(0).columnSizes()).isNotEmpty();

    SnapshotChanges removed = SnapshotChanges.builderFor(table).snapshot(deleteSnapshot).build();
    List<DataFile> removedFiles = drain(removed.removedDataFilesIterable());
    assertThat(removedFiles).hasSize(1);
    assertThat(removedFiles.get(0).columnSizes()).isNullOrEmpty();
  }

  @Test
  public void testExistingEntriesExcluded() {
    DataFile fileA = dataFile("/path/to/a.parquet");
    DataFile fileB = dataFile("/path/to/b.parquet");
    table.newAppend().appendFile(fileA).appendFile(fileB).commit();
    table.newDelete().deleteFile(fileA).commit();

    SnapshotChanges changes =
        SnapshotChanges.builderFor(table).snapshot(table.currentSnapshot()).build();

    // fileA is DELETED in this snapshot; fileB is carried forward as EXISTING and excluded
    assertThat(locations(drain(changes.removedDataFilesIterable())))
        .containsExactly(fileA.location());
    assertThat(drain(changes.addedDataFilesIterable())).isEmpty();
  }

  @Test
  public void testOnlyTargetSnapshotChangesReturned() {
    DataFile fileA = dataFile("/path/to/a.parquet");
    table.newFastAppend().appendFile(fileA).commit();
    Snapshot first = table.currentSnapshot();

    DataFile fileB = dataFile("/path/to/b.parquet");
    table.newFastAppend().appendFile(fileB).commit();
    Snapshot second = table.currentSnapshot();

    SnapshotChanges firstChanges = SnapshotChanges.builderFor(table).snapshot(first).build();
    assertThat(locations(drain(firstChanges.addedDataFilesIterable())))
        .containsExactly(fileA.location());

    // the second snapshot must not surface fileA, whose manifest belongs to the first snapshot
    SnapshotChanges secondChanges = SnapshotChanges.builderFor(table).snapshot(second).build();
    assertThat(locations(drain(secondChanges.addedDataFilesIterable())))
        .containsExactly(fileB.location());
  }

  @Test
  public void testParallelStreamingMatchesSerial() {
    AppendFiles append = table.newFastAppend();
    for (int i = 0; i < 8; i++) {
      append.appendFile(dataFile("/path/to/file-" + i + ".parquet"));
    }
    append.commit();
    Snapshot snapshot = table.currentSnapshot();

    ExecutorService executor = Executors.newFixedThreadPool(3);
    try {
      SnapshotChanges serial = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
      SnapshotChanges parallel =
          SnapshotChanges.builderFor(table).snapshot(snapshot).executeWith(executor).build();

      assertThat(locations(drain(parallel.addedDataFilesIterable())))
          .containsExactlyInAnyOrderElementsOf(locations(drain(serial.addedDataFilesIterable())));
    } finally {
      executor.shutdownNow();
    }
  }

  private DataFile dataFile(String path) {
    return DataFiles.builder(SPEC)
        .withPath(path)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private DeleteFile positionDeleteFile(String path) {
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath(path)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private <T> List<T> drain(CloseableIterable<T> iterable) {
    try (CloseableIterable<T> closeable = iterable) {
      return Lists.newArrayList(closeable);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close iterable", e);
    }
  }

  private static List<String> locations(Iterable<? extends ContentFile<?>> files) {
    List<String> paths = Lists.newArrayList();
    for (ContentFile<?> file : files) {
      paths.add(file.location());
    }
    return paths;
  }
}
