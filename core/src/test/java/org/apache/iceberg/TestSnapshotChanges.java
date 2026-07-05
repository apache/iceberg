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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
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
  public void testSingleSnapshotBackCompatThroughMultiSnapshotFactory() {
    DataFile file = newDataFile("/path/to/single-snap.parquet");
    table.newFastAppend().appendFile(file).commit();
    Snapshot snapshot = table.currentSnapshot();

    Iterable<DataFile> viaSingle =
        SnapshotChanges.builderFor(table).snapshot(snapshot).build().addedDataFiles();
    Iterable<DataFile> viaMulti =
        SnapshotChanges.builderFor(table, ImmutableList.of(snapshot)).build().addedDataFiles();

    assertThat(paths(viaMulti)).containsExactlyInAnyOrderElementsOf(paths(viaSingle));
    assertThat(viaMulti).hasSize(1);
  }

  @Test
  public void testMultiSnapshotUnionForDataAndDeleteFiles() {
    DataFile fileA = newDataFile("/path/to/A.parquet");
    DataFile fileB = newDataFile("/path/to/B.parquet");
    DataFile fileC = newDataFile("/path/to/C.parquet");

    table.newFastAppend().appendFile(fileA).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newFastAppend().appendFile(fileB).appendFile(fileC).commit();
    Snapshot snap2 = table.currentSnapshot();

    table.newDelete().deleteFile(fileA).commit();
    Snapshot snap3 = table.currentSnapshot();

    SnapshotChanges union =
        SnapshotChanges.builderFor(table, ImmutableList.of(snap1, snap2, snap3)).build();

    assertThat(paths(union.addedDataFiles()))
        .containsExactlyInAnyOrder(
            fileA.path().toString(), fileB.path().toString(), fileC.path().toString());
    assertThat(paths(union.removedDataFiles())).containsExactly(fileA.path().toString());
  }

  @Test
  public void testMultiSnapshotDoesNotDoubleCountSharedAncestorManifests() throws Exception {
    DataFile fileA = newDataFile("/path/to/A.parquet");
    DataFile fileB = newDataFile("/path/to/B.parquet");
    DataFile fileC = newDataFile("/path/to/C.parquet");

    // Fast appends chain the snapshots: snap2 sees snap1's manifest and snap3 sees both.
    // Attribution by ManifestFile#snapshotId() must emit each file exactly once.
    table.newFastAppend().appendFile(fileA).commit();
    Snapshot snap1 = table.currentSnapshot();
    table.newFastAppend().appendFile(fileB).commit();
    Snapshot snap2 = table.currentSnapshot();
    table.newFastAppend().appendFile(fileC).commit();
    Snapshot snap3 = table.currentSnapshot();

    SnapshotChanges union =
        SnapshotChanges.builderFor(table, ImmutableList.of(snap1, snap2, snap3)).build();

    // Assert on the raw List, not the Set: a dedup regression would grow this beyond 3.
    List<DataFile> cached = Lists.newArrayList(union.addedDataFiles());
    assertThat(cached).hasSize(3);
    assertThat(paths(cached))
        .containsExactlyInAnyOrder(
            fileA.path().toString(), fileB.path().toString(), fileC.path().toString());

    try (CloseableIterable<DataFile> streamed = union.readAddedDataFiles()) {
      assertThat(Lists.newArrayList(streamed)).hasSize(3);
    }
  }

  @Test
  public void testMultiSnapshotUnionForDeleteFiles() {
    DataFile fileA = newDataFile("/path/to/A.parquet");
    table.newFastAppend().appendFile(fileA).commit();
    Snapshot dataSnap = table.currentSnapshot();

    DeleteFile delA = newDeleteFile("/path/to/A-deletes.parquet");
    DeleteFile delB = newDeleteFile("/path/to/B-deletes.parquet");
    table.newRowDelta().addDeletes(delA).commit();
    Snapshot snap1 = table.currentSnapshot();
    table.newRowDelta().addDeletes(delB).commit();
    Snapshot snap2 = table.currentSnapshot();

    SnapshotChanges union =
        SnapshotChanges.builderFor(table, ImmutableList.of(dataSnap, snap1, snap2)).build();

    assertThat(paths(union.addedDeleteFiles()))
        .containsExactlyInAnyOrder(delA.path().toString(), delB.path().toString());
  }

  @Test
  public void testMultiSnapshotUnionForRemovedDeleteFiles() {
    DataFile fileA = newDataFile("/path/to/A.parquet");
    table.newFastAppend().appendFile(fileA).commit();

    DeleteFile delA = newDeleteFile("/path/to/A-deletes.parquet");
    DeleteFile delB = newDeleteFile("/path/to/B-deletes.parquet");
    table.newRowDelta().addDeletes(delA).addDeletes(delB).commit();

    table.newRowDelta().removeDeletes(delA).commit();
    Snapshot removeDelA = table.currentSnapshot();
    table.newRowDelta().removeDeletes(delB).commit();
    Snapshot removeDelB = table.currentSnapshot();

    SnapshotChanges union =
        SnapshotChanges.builderFor(table, ImmutableList.of(removeDelA, removeDelB)).build();

    // exercise the cached removedDeleteFiles() path; only the streaming variant was covered before
    Iterable<DeleteFile> first = union.removedDeleteFiles();
    Iterable<DeleteFile> second = union.removedDeleteFiles();
    assertThat(first).isSameAs(second);
    assertThat(paths(first))
        .containsExactlyInAnyOrder(delA.path().toString(), delB.path().toString());
  }

  @Test
  public void testStreamingAccessorsForMultiSnapshotChanges() throws Exception {
    DataFile fileA = newDataFile("/path/to/A.parquet");
    DataFile fileB = newDataFile("/path/to/B.parquet");

    table.newFastAppend().appendFile(fileA).commit();
    Snapshot addFileA = table.currentSnapshot();
    table.newFastAppend().appendFile(fileB).commit();
    Snapshot addFileB = table.currentSnapshot();
    table.newDelete().deleteFile(fileA).commit();
    Snapshot removeFileA = table.currentSnapshot();

    DeleteFile deleteFileA = newDeleteFile("/path/to/A-deletes.parquet");
    table.newRowDelta().addDeletes(deleteFileA).commit();
    Snapshot addDeleteFileA = table.currentSnapshot();
    table.newRowDelta().removeDeletes(deleteFileA).commit();
    Snapshot removeDeleteFileA = table.currentSnapshot();

    SnapshotChanges changes =
        SnapshotChanges.builderFor(
                table,
                ImmutableList.of(
                    addFileA, addFileB, removeFileA, addDeleteFileA, removeDeleteFileA))
            .build();

    try (CloseableIterable<DataFile> addedDataFiles = changes.readAddedDataFiles();
        CloseableIterable<DataFile> removedDataFiles = changes.readRemovedDataFiles();
        CloseableIterable<DeleteFile> addedDeleteFiles = changes.readAddedDeleteFiles();
        CloseableIterable<DeleteFile> removedDeleteFiles = changes.readRemovedDeleteFiles()) {
      assertThat(paths(addedDataFiles))
          .containsExactlyInAnyOrder(fileA.path().toString(), fileB.path().toString());
      assertThat(paths(removedDataFiles)).containsExactly(fileA.path().toString());
      assertThat(paths(addedDeleteFiles)).containsExactly(deleteFileA.path().toString());
      assertThat(paths(removedDeleteFiles)).containsExactly(deleteFileA.path().toString());
    }
  }

  @Test
  public void testMultiSnapshotCachingReturnsSameInstance() {
    DataFile fileA = newDataFile("/path/to/A.parquet");
    DataFile fileB = newDataFile("/path/to/B.parquet");

    table.newFastAppend().appendFile(fileA).commit();
    Snapshot snap1 = table.currentSnapshot();
    table.newFastAppend().appendFile(fileB).commit();
    Snapshot snap2 = table.currentSnapshot();

    SnapshotChanges changes =
        SnapshotChanges.builderFor(table, ImmutableList.of(snap1, snap2)).build();

    Iterable<DataFile> first = changes.addedDataFiles();
    Iterable<DataFile> second = changes.addedDataFiles();
    assertThat(first).isSameAs(second);
    assertThat(paths(first))
        .containsExactlyInAnyOrder(fileA.path().toString(), fileB.path().toString());
  }

  @Test
  public void testWithCustomExecutor() throws Exception {
    DataFile fileA = newDataFile("/path/to/A.parquet");
    DataFile fileB = newDataFile("/path/to/B.parquet");
    DataFile fileC = newDataFile("/path/to/C.parquet");

    table.newFastAppend().appendFile(fileA).commit();
    Snapshot snap1 = table.currentSnapshot();
    table.newFastAppend().appendFile(fileB).commit();
    Snapshot snap2 = table.currentSnapshot();
    table.newFastAppend().appendFile(fileC).commit();
    Snapshot snap3 = table.currentSnapshot();

    ExecutorService executor = Executors.newFixedThreadPool(3);
    try {
      SnapshotChanges changes =
          SnapshotChanges.builderFor(table, ImmutableList.of(snap1, snap2, snap3))
              .executeWith(executor)
              .build();

      assertThat(paths(changes.addedDataFiles()))
          .containsExactlyInAnyOrder(
              fileA.path().toString(), fileB.path().toString(), fileC.path().toString());
    } finally {
      executor.shutdownNow();
      assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testEquivalenceWithDeprecatedNewFilesBetween() throws Exception {
    DataFile fileA = newDataFile("/path/to/A.parquet");
    DataFile fileB = newDataFile("/path/to/B.parquet");
    DataFile fileC = newDataFile("/path/to/C.parquet");

    table.newFastAppend().appendFile(fileA).commit();
    Snapshot snap1 = table.currentSnapshot();
    table.newFastAppend().appendFile(fileB).commit();
    table.newFastAppend().appendFile(fileC).commit();
    Snapshot snap3 = table.currentSnapshot();

    Set<String> viaDeprecated = Sets.newHashSet();
    try (CloseableIterable<DataFile> deprecated =
        SnapshotUtil.newFilesBetween(
            snap1.snapshotId(), snap3.snapshotId(), table::snapshot, table.io())) {
      for (DataFile f : deprecated) {
        viaDeprecated.add(f.path().toString());
      }
    }

    List<Snapshot> ancestorsAfterSnap1 =
        Lists.newArrayList(
            SnapshotUtil.ancestorsBetween(snap3.snapshotId(), snap1.snapshotId(), table::snapshot));

    SnapshotChanges changes = SnapshotChanges.builderFor(table, ancestorsAfterSnap1).build();

    assertThat(paths(changes.addedDataFiles())).containsExactlyInAnyOrderElementsOf(viaDeprecated);
  }

  @Test
  public void testBuilderForTableWithNoCurrentSnapshotFailsOnBuild() {
    // Fresh table has no current snapshot, so builderFor(table) configures no snapshots.
    assertThatThrownBy(() -> SnapshotChanges.builderFor(table).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Snapshots cannot be empty");
  }

  @Test
  public void testSnapshotsNullRejected() {
    assertThatThrownBy(() -> SnapshotChanges.builderFor(table).snapshots(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Snapshots cannot be null");
  }

  @Test
  public void testEmptySnapshotsRejected() {
    assertThatThrownBy(
            () -> SnapshotChanges.builderFor(table, ImmutableList.<Snapshot>of()).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Snapshots cannot be empty");
  }

  @Test
  public void testNullSnapshotInListRejected() {
    List<Snapshot> withNull = Lists.newArrayList((Snapshot) null);
    assertThatThrownBy(() -> SnapshotChanges.builderFor(table, withNull).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Snapshot cannot be null");
  }

  private static DataFile newDataFile(String path) {
    return DataFiles.builder(SPEC)
        .withPath(path)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static DeleteFile newDeleteFile(String path) {
    return FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath(path)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static Set<String> paths(Iterable<? extends ContentFile<?>> files) {
    return StreamSupport.stream(files.spliterator(), false)
        .map(f -> f.path().toString())
        .collect(Collectors.toSet());
  }
}
