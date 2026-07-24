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
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end smoke tests for {@link SnapshotProducer}'s v4 write path: asserts that committing to a
 * v4 table writes a root manifest ({@code .parquet}) instead of a manifest list ({@code .avro}),
 * and that manifest reference entries carry the correct {@link EntryStatus} and format_version.
 *
 * <p>Tests use a <em>partitioned</em> v4 table to avoid the Phase 2 known-issue with empty Parquet
 * row-groups in unpartitioned cases.
 */
public class TestV4SnapshotProducer {

  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  // Partitioned spec - bucket(data, 16).
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(1)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=1")
          .withRecordCount(1)
          .build();

  @TempDir File tableDir;

  private TestTables.TestTable table;

  @BeforeEach
  public void before() {
    // Use the temp dir name as the table name to guarantee uniqueness across test methods
    // (TestTables stores metadata in a static map keyed by table name).
    table = TestTables.create(tableDir, tableDir.getName(), SCHEMA, SPEC, SortOrder.unsorted(), 4);
  }

  // ---- helpers ----------------------------------------------------------------

  private List<TrackedFileStruct> readRootManifestRows(String rootManifestLocation)
      throws IOException {
    Schema contentEntrySchema =
        new Schema(
            TrackedFile.schemaWithContentStats(
                    RootManifestWriter.emptyPartitionPlaceholderIfNeeded(
                        org.apache.iceberg.types.Types.StructType.of()),
                    RootManifestWriter.ROOT_CONTENT_STATS_TYPE)
                .fields());

    CloseableIterable<TrackedFileStruct> rows =
        InternalData.read(FileFormat.PARQUET, table.io().newInputFile(rootManifestLocation))
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .build();

    ImmutableList.Builder<TrackedFileStruct> result = ImmutableList.builder();
    try {
      for (TrackedFileStruct row : rows) {
        result.add(row);
      }
    } finally {
      rows.close();
    }
    return result.build();
  }

  private List<ManifestEntry<DataFile>> readLeafManifestEntries(ManifestFile manifest)
      throws IOException {
    ImmutableList.Builder<ManifestEntry<DataFile>> result = ImmutableList.builder();
    try (CloseableIterable<ManifestEntry<DataFile>> entries =
        ManifestFiles.read(manifest, table.io(), table.ops().current().specsById()).entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        result.add(entry);
      }
    }
    return result.build();
  }

  // ---- tests ------------------------------------------------------------------

  /**
   * First append to a v4 table:
   *
   * <ul>
   *   <li>Snapshot has rootManifestLocation set (.parquet), manifestListLocation null.
   *   <li>Root manifest has one DATA_MANIFEST entry with format_version=4, status=ADDED.
   *   <li>The leaf manifest path in the root entry matches the leaf written by the snapshot.
   * </ul>
   */
  @Test
  public void testAppendV4() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot snap = table.currentSnapshot();
    assertThat(snap).isNotNull();

    // v4: root manifest not manifest list
    assertThat(snap.rootManifestLocation())
        .as("root manifest location must be set for v4")
        .isNotNull()
        .endsWith(".parquet");
    assertThat(snap.manifestListLocation())
        .as("manifest list location must be null for v4")
        .isNull();

    // root manifest must carry exactly one DATA_MANIFEST entry
    List<TrackedFileStruct> rootRows = readRootManifestRows(snap.rootManifestLocation());
    assertThat(rootRows).hasSize(1);

    TrackedFileStruct rootEntry = rootRows.get(0);
    assertThat(rootEntry.contentType())
        .as("root entry must be DATA_MANIFEST")
        .isEqualTo(FileContent.DATA_MANIFEST);
    assertThat(rootEntry.formatVersion())
        .as("format_version must be 4 for v4 leaf")
        .isEqualTo(TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE);

    // tracking status: newly written by this snapshot => ADDED
    Tracking tracking = rootEntry.tracking();
    assertThat(tracking).isNotNull();
    assertThat(tracking.status())
        .as("root entry for new manifest must be ADDED")
        .isEqualTo(EntryStatus.ADDED);

    // the leaf referred to by the root entry is the only data manifest in the snapshot
    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    assertThat(rootEntry.location())
        .as("root entry location must match the leaf manifest path")
        .isEqualTo(dataManifests.get(0).path());
  }

  /**
   * Two sequential appends:
   *
   * <ul>
   *   <li>After the second append the root manifest has two DATA_MANIFEST entries.
   *   <li>The first (carried-over) entry is EXISTING; the second (new) entry is ADDED.
   * </ul>
   */
  @Test
  public void testTwoAppendsV4() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    String firstLeafPath = snap1.dataManifests(table.io()).get(0).path();

    table.newAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    // basic v4 assertions for second snapshot
    assertThat(snap2.rootManifestLocation()).isNotNull().endsWith(".parquet");
    assertThat(snap2.manifestListLocation()).isNull();

    List<ManifestFile> dataManifests = snap2.dataManifests(table.io());
    assertThat(dataManifests).hasSize(2);

    List<TrackedFileStruct> rootRows = readRootManifestRows(snap2.rootManifestLocation());
    assertThat(rootRows).hasSize(2);

    // locate the two entries by their leaf paths
    TrackedFileStruct existingEntry = null;
    TrackedFileStruct addedEntry = null;
    for (TrackedFileStruct row : rootRows) {
      if (row.location().equals(firstLeafPath)) {
        existingEntry = row;
      } else {
        addedEntry = row;
      }
    }

    assertThat(existingEntry)
        .as("carried-over leaf from snapshot 1 must appear in root manifest")
        .isNotNull();
    assertThat(addedEntry)
        .as("newly written leaf from snapshot 2 must appear in root manifest")
        .isNotNull();

    assertThat(existingEntry.tracking().status())
        .as("carried-over manifest entry must be EXISTING")
        .isEqualTo(EntryStatus.EXISTING);
    assertThat(addedEntry.tracking().status())
        .as("newly added manifest entry must be ADDED")
        .isEqualTo(EntryStatus.ADDED);

    // both entries must declare v4+ leaf format
    assertThat(existingEntry.formatVersion())
        .isEqualTo(TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE);
    assertThat(addedEntry.formatVersion())
        .isEqualTo(TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE);

    // both DATA manifest references must carry a first-row-id for row lineage tracking. The
    // existing entry carries over the value assigned in snapshot 1; the newly added entry gets a
    // fresh assignment from snapshot 2's counter.
    assertThat(existingEntry.tracking().firstRowId())
        .as("carried-over DATA manifest must preserve its prior first-row-id")
        .isNotNull();
    assertThat(addedEntry.tracking().firstRowId())
        .as("newly added DATA manifest must be assigned a first-row-id")
        .isNotNull();
  }

  /**
   * Append then delete a file:
   *
   * <ul>
   *   <li>After deleting FILE_A from the first leaf, the rewritten leaf is ADDED in the root.
   *   <li>FILE_B's unchanged leaf remains EXISTING in the root.
   *   <li>The rewritten leaf contains a DELETED entry for FILE_A.
   * </ul>
   */
  @Test
  public void testDeleteFileV4() throws IOException {
    // snapshot 1: append FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    String leafAPath = snap1.dataManifests(table.io()).get(0).path();

    // snapshot 2: append FILE_B
    table.newAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    // identify which leaf holds FILE_B
    String leafBPath = null;
    for (ManifestFile mf : snap2.dataManifests(table.io())) {
      if (!mf.path().equals(leafAPath)) {
        leafBPath = mf.path();
      }
    }
    assertThat(leafBPath).isNotNull();

    // snapshot 3: delete FILE_A (rewrites the leaf that contains it)
    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap3 = table.currentSnapshot();

    assertThat(snap3.rootManifestLocation()).isNotNull().endsWith(".parquet");
    assertThat(snap3.manifestListLocation()).isNull();

    // the rewritten leaf must have a DELETED entry for FILE_A
    boolean foundDeletedFileA = false;
    for (ManifestFile mf : snap3.dataManifests(table.io())) {
      for (ManifestEntry<DataFile> entry : readLeafManifestEntries(mf)) {
        if (FILE_A.location().equals(entry.file().location())) {
          assertThat(entry.status())
              .as("FILE_A must be DELETED in the rewritten leaf")
              .isEqualTo(ManifestEntry.Status.DELETED);
          foundDeletedFileA = true;
        }
      }
    }
    assertThat(foundDeletedFileA).as("must find a DELETED entry for FILE_A").isTrue();

    // inspect the root manifest: rewritten leaf => ADDED, FILE_B's leaf => EXISTING
    List<TrackedFileStruct> rootRows = readRootManifestRows(snap3.rootManifestLocation());

    boolean foundAddedRewrittenLeaf = false;
    boolean foundExistingLeafB = false;
    for (TrackedFileStruct row : rootRows) {
      assertThat(row.contentType()).isEqualTo(FileContent.DATA_MANIFEST);
      assertThat(row.formatVersion())
          .isEqualTo(TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE);
      if (row.location().equals(leafBPath)) {
        // FILE_B's leaf is unchanged => EXISTING
        assertThat(row.tracking().status())
            .as("unchanged leaf for FILE_B must be EXISTING")
            .isEqualTo(EntryStatus.EXISTING);
        foundExistingLeafB = true;
      } else if (!row.location().equals(leafAPath)) {
        // This is the rewritten leaf (new path, not the old leaf A path) => ADDED
        assertThat(row.tracking().status())
            .as("rewritten leaf must be ADDED in the root manifest")
            .isEqualTo(EntryStatus.ADDED);
        foundAddedRewrittenLeaf = true;
      }
    }

    assertThat(foundExistingLeafB)
        .as("FILE_B's unchanged leaf must be EXISTING in the root manifest")
        .isTrue();
    assertThat(foundAddedRewrittenLeaf)
        .as("rewritten leaf must be present as ADDED in the root manifest")
        .isTrue();
  }

  /**
   * A no-op commit (here: deleting a file that does not exist) must not leave an orphan root
   * manifest. The follow-up commit reuses the parent's root manifest location so the cleanup path
   * cannot invalidate the snapshot's root reference.
   */
  @Test
  public void testNoOpCommitReusesParentRootManifest() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // delete a file that does not exist: apply() returns parent's manifests unchanged
    table.newDelete().deleteFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    assertThat(snap2.snapshotId())
        .as("a no-op commit still produces a new snapshot ID")
        .isNotEqualTo(snap1.snapshotId());
    assertThat(snap2.rootManifestLocation())
        .as("no-op commit must reuse parent's root manifest location")
        .isEqualTo(snap1.rootManifestLocation());
    assertThat(table.io().newInputFile(snap2.rootManifestLocation()).exists())
        .as("reused root manifest must still exist on disk")
        .isTrue();
  }

  /**
   * A commit whose apply() yields an empty manifest set (e.g., a follow-up commit after the only
   * data file was already deleted in a prior snapshot) must still produce a valid, readable root
   * manifest file. The writer opts into ParquetWriter's MATERIALIZE_EMPTY_FILE property so the
   * empty file is materialized at close, matching v3's empty-Avro-manifest-list behavior.
   */
  @Test
  public void testEmptyManifestSetProducesReadableRootManifest() {
    // snap1: append FILE_A so the table has one data manifest with one live entry
    table.newAppend().appendFile(FILE_A).commit();
    // snap2: delete FILE_A. The rewritten manifest is kept (snapshotId == this commit) and carries
    // FILE_A as DELETED, so allManifests for snap2 is non-empty.
    table.newDelete().deleteFile(FILE_A).commit();
    // snap3: delete FILE_A again. apply() filters out the inherited manifest (no live files and
    // written by a different snapshot) and returns an empty manifest list. The producer must still
    // write a root manifest that reads as empty.
    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap3 = table.currentSnapshot();

    assertThat(snap3.rootManifestLocation())
        .as("commit with empty apply() must reference its own root manifest")
        .isNotEqualTo(table.snapshot(snap3.parentId()).rootManifestLocation());
    assertThat(table.io().newInputFile(snap3.rootManifestLocation()).exists())
        .as("root manifest file must exist on disk")
        .isTrue();
    assertThat(snap3.allManifests(table.io()))
        .as("root manifest must read as an empty manifest list")
        .isEmpty();
  }
}
