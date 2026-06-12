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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests for Phase 6: colocated DV writes in v4 leaf data manifests.
 *
 * <ul>
 *   <li>Adding a DV to an existing data file produces a REPLACED/MODIFIED pair in the leaf manifest
 *       and no separate position-delete manifest.
 *   <li>Replacing an existing DV produces a REPLACED/MODIFIED pair again.
 *   <li>A data file born with a DV in the same commit produces a single ADDED entry with the DV
 *       embedded.
 *   <li>The leaf {@link ManifestFile} stats remain correct (MODIFIED entries fold into existing
 *       counts so that {@link ManifestFilterManager} pruning works).
 * </ul>
 */
public class TestV4ColocatedDV {

  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(100)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(5)
          .build();

  @TempDir File tableDir;

  private TestTables.TestTable table;

  @BeforeEach
  public void before() {
    table = TestTables.create(tableDir, tableDir.getName(), SCHEMA, SPEC, SortOrder.unsorted(), 4);
  }

  // ---- helpers ----------------------------------------------------------------

  /** Reads raw {@link TrackedFileStruct} rows from a v4 leaf manifest file. */
  private List<TrackedFileStruct> readLeafRows(ManifestFile manifest) throws IOException {
    PartitionSpec spec = table.ops().current().specsById().get(manifest.partitionSpecId());
    if (spec == null) {
      spec = PartitionSpec.unpartitioned();
    }

    Types.StructType statsType =
        StatsUtil.statsReadSchema(spec.schema(), TypeUtil.getProjectedIds(spec.schema()));
    Schema contentEntrySchema =
        new Schema(
            TrackedFile.schemaWithContentStats(spec.rawPartitionType(), statsType).fields());

    InternalData.ReadBuilder readBuilder =
        InternalData.read(FileFormat.PARQUET, table.io().newInputFile(manifest.path()))
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.CONTENT_STATS_ID, ContentStatsStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class);
    for (Types.NestedField statsField : statsType.fields()) {
      readBuilder.setCustomType(statsField.fieldId(), FieldStatsStruct.class);
    }
    CloseableIterable<TrackedFileStruct> rows = readBuilder.build();

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

  /** Reads a leaf manifest via the standard {@link ManifestFiles} API (EXISTING/ADDED/DELETED). */
  private List<ManifestEntry<DataFile>> readLeafEntries(ManifestFile manifest) throws IOException {
    List<ManifestEntry<DataFile>> result = Lists.newArrayList();
    try (CloseableIterable<ManifestEntry<DataFile>> iter =
        ManifestFiles.read(manifest, table.io(), table.ops().current().specsById()).entries()) {
      for (ManifestEntry<DataFile> entry : iter) {
        result.add(entry.copy());
      }
    }
    return result;
  }

  // ---- tests ------------------------------------------------------------------

  /**
   * Adding a DV to an existing data file via {@code RowDelta} on a v4 table:
   *
   * <ul>
   *   <li>The leaf manifest is rewritten with a REPLACED/MODIFIED pair for FILE_A.
   *   <li>No separate position-delete manifest is produced.
   *   <li>The leaf manifest file reports {@code hasExistingFiles()} = true.
   *   <li>The leaf manifest file reports {@code replacedFilesCount()} = 1.
   * </ul>
   */
  @Test
  public void testAddDVToExistingFile() throws IOException {
    // snapshot 1: append FILE_A
    table.newAppend().appendFile(FILE_A).commit();

    // snapshot 2: add a DV for FILE_A
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    // no separate position-delete manifests in v4 with colocated DVs
    assertThat(snap2.deleteManifests(table.io()))
        .as("v4 colocated DV must not produce a position-delete manifest")
        .isEmpty();

    // exactly one data manifest (the rewritten leaf)
    List<ManifestFile> dataManifests = snap2.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    ManifestFile leaf = dataManifests.get(0);

    // leaf manifest stats: MODIFIED folds into existing counts
    assertThat(leaf.hasExistingFiles())
        .as("rewritten leaf must report hasExistingFiles()=true for MODIFIED entry")
        .isTrue();

    // raw rows: REPLACED + MODIFIED pair for FILE_A
    List<TrackedFileStruct> rows = readLeafRows(leaf);
    assertThat(rows).as("REPLACED + MODIFIED = 2 rows").hasSize(2);

    TrackedFileStruct replacedRow = null;
    TrackedFileStruct modifiedRow = null;
    for (TrackedFileStruct row : rows) {
      if (row.tracking().status() == EntryStatus.REPLACED) {
        replacedRow = row;
      } else if (row.tracking().status() == EntryStatus.MODIFIED) {
        modifiedRow = row;
      }
    }

    assertThat(replacedRow).as("must have a REPLACED row for FILE_A").isNotNull();
    assertThat(modifiedRow).as("must have a MODIFIED row for FILE_A").isNotNull();

    // REPLACED row must not carry a DV
    assertThat(replacedRow.deletionVector())
        .as("REPLACED row must not carry a deletion_vector")
        .isNull();

    // MODIFIED row must carry the DV
    assertThat(modifiedRow.deletionVector())
        .as("MODIFIED row must carry an embedded deletion_vector")
        .isNotNull();
    assertThat(modifiedRow.deletionVector().location())
        .as("embedded DV location must match the committed DV")
        .isEqualTo(dv.location());

    // both rows reference FILE_A
    assertThat(replacedRow.location()).isEqualTo(FILE_A.location());
    assertThat(modifiedRow.location()).isEqualTo(FILE_A.location());
  }

  /**
   * Replacing an existing DV on an existing data file:
   *
   * <ul>
   *   <li>First commit attaches DV1 to FILE_A (REPLACED/MODIFIED).
   *   <li>Second commit attaches DV2 to FILE_A (REPLACED/MODIFIED again).
   *   <li>After the second commit the leaf has DV2 in the MODIFIED row.
   * </ul>
   */
  @Test
  public void testReplaceExistingDV() throws IOException {
    // snapshot 1: FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // snapshot 2: add DV1
    DeleteFile dv1 = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();
    Snapshot snap2 = table.currentSnapshot();

    // snapshot 3: replace DV1 with DV2 — scope validation from snap2 so the validator only looks
    // at concurrent commits between snap2 and the new parent (none here), and does not flag DV1
    // (already in the parent chain) as a conflict.
    DeleteFile dv2 = FileGenerationUtil.generateDV(table, FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1)
        .addDeletes(dv2)
        .validateFromSnapshot(snap2.snapshotId())
        .commit();
    Snapshot snap3 = table.currentSnapshot();

    assertThat(snap3.deleteManifests(table.io()))
        .as("still no position-delete manifests after DV replacement")
        .isEmpty();

    List<ManifestFile> dataManifests = snap3.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    List<TrackedFileStruct> rows = readLeafRows(dataManifests.get(0));

    // find the live row (MODIFIED)
    TrackedFileStruct liveRow = null;
    for (TrackedFileStruct row : rows) {
      EntryStatus status = row.tracking().status();
      if (status == EntryStatus.MODIFIED || status == EntryStatus.ADDED) {
        liveRow = row;
      }
    }
    assertThat(liveRow).as("must have a live row for FILE_A").isNotNull();
    assertThat(liveRow.deletionVector()).as("live row must carry a DV").isNotNull();
    assertThat(liveRow.deletionVector().location())
        .as("live row must carry DV2, not DV1")
        .isEqualTo(dv2.location());

    // stale REPLACED + MODIFIED rows from snapshot 2 (carrying DV1) must NOT survive into the
    // new leaf manifest. The new leaf should have exactly one REPLACED + one MODIFIED for
    // FILE_A — the rows produced by snapshot 3.
    TrackedFileStruct replacedRow = null;
    TrackedFileStruct modifiedRow = null;
    for (TrackedFileStruct row : rows) {
      if (!row.location().equals(FILE_A.location().toString())) {
        continue;
      }
      if (row.tracking().status() == EntryStatus.REPLACED) {
        assertThat(replacedRow).as("exactly one REPLACED row for FILE_A").isNull();
        replacedRow = row;
      } else if (row.tracking().status() == EntryStatus.MODIFIED) {
        assertThat(modifiedRow).as("exactly one MODIFIED row for FILE_A").isNull();
        modifiedRow = row;
      }
    }
    assertThat(replacedRow).as("must have a REPLACED row for FILE_A").isNotNull();
    assertThat(modifiedRow).as("must have a MODIFIED row for FILE_A").isNotNull();

    // REPLACED row snapshot_id records the commit performing the replacement (snap3).
    assertThat(replacedRow.tracking().snapshotId())
        .as("REPLACED row must record the snapshot performing the replacement")
        .isEqualTo(snap3.snapshotId());

    // MODIFIED row snapshot_id preserves FILE_A's original ADD snapshot (snap1) so consumers can
    // trace when the base file was added; dv_snapshot_id advances to snap3 (the commit that
    // updated the DV).
    assertThat(modifiedRow.tracking().snapshotId())
        .as("MODIFIED row must preserve FILE_A's original ADD snapshot")
        .isEqualTo(snap1.snapshotId());
    assertThat(modifiedRow.tracking().dvSnapshotId())
        .as("MODIFIED row dv_snapshot_id must record the commit that updated the DV")
        .isEqualTo(snap3.snapshotId());
  }

  /**
   * A data file born with a DV in the same commit emits a single ADDED entry with the DV embedded —
   * no REPLACED/MODIFIED pair, no separate delete manifest.
   */
  @Test
  public void testBornWithDV() throws IOException {
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addRows(FILE_A).addDeletes(dv).commit();
    Snapshot snap = table.currentSnapshot();

    // no position-delete manifests
    assertThat(snap.deleteManifests(table.io()))
        .as("born-with-DV must not produce a separate position-delete manifest")
        .isEmpty();

    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    List<TrackedFileStruct> rows = readLeafRows(dataManifests.get(0));
    assertThat(rows).as("born-with-DV must produce exactly one row").hasSize(1);

    TrackedFileStruct row = rows.get(0);
    assertThat(row.tracking().status())
        .as("born-with-DV entry must be ADDED")
        .isEqualTo(EntryStatus.ADDED);
    assertThat(row.location()).isEqualTo(FILE_A.location());
    assertThat(row.deletionVector())
        .as("born-with-DV ADDED row must carry an embedded deletion_vector")
        .isNotNull();
    assertThat(row.deletionVector().location())
        .as("embedded DV location must match the committed DV")
        .isEqualTo(dv.location());
  }

  /**
   * After a v4 DV commit the legacy {@link ManifestFiles} reader maps REPLACED → DELETED (non-live)
   * and MODIFIED → EXISTING (live) so that {@link ManifestEntry#isLive()} agrees with the v4
   * tracking semantics: a REPLACED row is the prior state being superseded.
   */
  @Test
  public void testLegacyReaderSeesExistingForModified() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    List<ManifestFile> dataManifests = snap2.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    List<ManifestEntry<DataFile>> entries = readLeafEntries(dataManifests.get(0));

    // The REPLACED/MODIFIED pair surfaces to the legacy reader as DELETED (REPLACED) + EXISTING
    // (MODIFIED). isLive() returns false for the REPLACED row and true for the MODIFIED row.
    List<ManifestEntry<DataFile>> existingEntries = Lists.newArrayList();
    List<ManifestEntry<DataFile>> deletedEntries = Lists.newArrayList();
    for (ManifestEntry<DataFile> entry : entries) {
      if (entry.status() == ManifestEntry.Status.EXISTING) {
        existingEntries.add(entry);
      } else if (entry.status() == ManifestEntry.Status.DELETED) {
        deletedEntries.add(entry);
      }
    }

    assertThat(existingEntries).as("legacy reader must see MODIFIED as EXISTING (live)").hasSize(1);
    assertThat(deletedEntries)
        .as("legacy reader must see REPLACED as DELETED (non-live)")
        .hasSize(1);
    assertThat(existingEntries.get(0).file().location()).isEqualTo(FILE_A.location());
    assertThat(deletedEntries.get(0).file().location()).isEqualTo(FILE_A.location());
  }

  /**
   * Manifest file stats for a DV-rewritten leaf are correct: MODIFIED entries fold into
   * existingFilesCount so that manifest-level pruning ({@link ManifestFilterManager}) does not skip
   * live manifests.
   */
  @Test
  public void testManifestFileStatsAfterDVRewrite() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    List<ManifestFile> dataManifests = snap2.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    ManifestFile leaf = dataManifests.get(0);

    // existingFilesCount must be 1 (the MODIFIED entry, folded in via toManifestFile())
    assertThat(leaf.existingFilesCount())
        .as("MODIFIED entry must be counted as existingFilesCount")
        .isEqualTo(1);

    // addedFilesCount must be 0 (no ADDED entries in the rewrite)
    assertThat(leaf.addedFilesCount()).as("no ADDED entries in a DV-rewrite manifest").isEqualTo(0);

    // hasExistingFiles() must be true so filter manager does not prune this manifest
    assertThat(leaf.hasExistingFiles()).isTrue();

    // Verify the raw leaf has REPLACED + MODIFIED rows (replacedFilesCount is not round-tripped
    // through the root manifest in Phase 6 — that is a Phase 7 RootManifestReader concern).
    List<TrackedFileStruct> rows = readLeafRows(leaf);
    long replacedCount =
        rows.stream().filter(r -> r.tracking().status() == EntryStatus.REPLACED).count();
    assertThat(replacedCount).as("raw leaf must have exactly 1 REPLACED row").isEqualTo(1L);

    // ManifestFile-level REPLACED counts are populated by ManifestWriter.toManifestFile() and
    // persisted via the v4 root manifest's manifest_info struct. The read-side ManifestFile object
    // exposes them via the v4 interface default methods.
    assertThat(leaf.replacedFilesCount())
        .as("ManifestFile must report 1 REPLACED entry after the DV rewrite")
        .isEqualTo(1);
    assertThat(leaf.replacedRowsCount())
        .as("ManifestFile must report REPLACED rows = FILE_A.recordCount()")
        .isEqualTo(FILE_A.recordCount());
  }

  /**
   * Two concurrent {@code RowDelta} commits that both add a DV for the same data file must
   * conflict. The first commit lands; the second commit's {@code
   * validateNoConflictingDeleteFiles()} (run as part of commit) must detect the colocated DV in the
   * concurrent data manifest and throw {@link ValidationException}.
   */
  @Test
  public void testConcurrentlyAddedColocatedDVsConflict() {
    // snapshot 1: append FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot baseSnapshot = table.currentSnapshot();

    // prepare two RowDelta builders from the same starting snapshot, each adding a DV for FILE_A
    DeleteFile dv1 = FileGenerationUtil.generateDV(table, FILE_A);
    RowDelta rowDelta1 =
        table
            .newRowDelta()
            .addDeletes(dv1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    DeleteFile dv2 = FileGenerationUtil.generateDV(table, FILE_A);
    RowDelta rowDelta2 =
        table
            .newRowDelta()
            .addDeletes(dv2)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // commit rowDelta1 first — succeeds
    rowDelta1.commit();

    // rowDelta2 must fail with a concurrent-DV validation error: the v4 path walks the data
    // manifest from snap2 and sees the MODIFIED row for FILE_A carrying dv1.
    assertThatThrownBy(rowDelta2::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Found concurrently added DV for %s", FILE_A.location());
  }

  /**
   * Adding a DV to an existing data file must credit the snapshot summary with the same metrics as
   * a v3 standalone DV write: {@code added-dvs}, {@code added-delete-files}, {@code
   * added-position-deletes}, and {@code added-files-size}.
   */
  @Test
  public void testAddDVToExistingFilePopulatesSummary() {
    table.newAppend().appendFile(FILE_A).commit();
    Map<String, String> snap1Summary = table.currentSnapshot().summary();
    assertThat(snap1Summary)
        .as("baseline append must not credit DV-related counters")
        .doesNotContainKey(SnapshotSummary.ADDED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.ADDED_POS_DELETES_PROP);

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();

    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(dv.recordCount()))
        .containsEntry(
            SnapshotSummary.ADDED_FILE_SIZE_PROP, String.valueOf(dv.contentSizeInBytes()))
        .doesNotContainKey(SnapshotSummary.ADD_POS_DELETE_FILES_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_POS_DELETES_PROP);
  }

  /**
   * A born-with-DV commit (data file + DV in the same snapshot) must credit BOTH the added data
   * file and the embedded DV on the snapshot summary.
   */
  @Test
  public void testBornWithDVPopulatesSummary() {
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addRows(FILE_A).addDeletes(dv).commit();

    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, String.valueOf(FILE_A.recordCount()))
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(dv.recordCount()))
        .doesNotContainKey(SnapshotSummary.REMOVED_DVS_PROP);
  }

  /**
   * Replacing DV1 with DV2 in a follow-up commit must credit DV2 as added and DV1 as removed on
   * that commit's summary, mirroring the v3 standalone-DV behavior.
   */
  @Test
  public void testReplaceDVPopulatesSummary() {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv1 = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();
    Snapshot snap2 = table.currentSnapshot();
    Map<String, String> snap2Summary = snap2.summary();
    assertThat(snap2Summary)
        .as("snap2 must credit DV1 as added with no removals")
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(dv1.recordCount()))
        .doesNotContainKey(SnapshotSummary.REMOVED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_POS_DELETES_PROP);

    DeleteFile dv2 = FileGenerationUtil.generateDV(table, FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1)
        .addDeletes(dv2)
        .validateFromSnapshot(snap2.snapshotId())
        .commit();

    Map<String, String> snap3Summary = table.currentSnapshot().summary();
    assertThat(snap3Summary)
        .as("snap3 must credit DV2 as added and DV1 as removed")
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(dv2.recordCount()))
        .containsEntry(SnapshotSummary.REMOVED_POS_DELETES_PROP, String.valueOf(dv1.recordCount()))
        .containsEntry(
            SnapshotSummary.ADDED_FILE_SIZE_PROP, String.valueOf(dv2.contentSizeInBytes()))
        .containsEntry(
            SnapshotSummary.REMOVED_FILE_SIZE_PROP, String.valueOf(dv1.contentSizeInBytes()));
  }

  /** Deleting a data file without any DV operation must not credit DV counters on the summary. */
  @Test
  public void testDataFileDeleteDoesNotPopulateDVSummary() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newDelete().deleteFile(FILE_A).commit();

    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary)
        .as("plain data-file delete must not credit DV counters")
        .doesNotContainKey(SnapshotSummary.ADDED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.ADDED_POS_DELETES_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_POS_DELETES_PROP);
  }
}
