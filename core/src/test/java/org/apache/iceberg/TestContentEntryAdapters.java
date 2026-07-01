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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestContentEntryAdapters {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();
  private static final PartitionData EMPTY_PARTITION =
      new PartitionData(UNPARTITIONED.partitionType());
  private static final Types.StructType UNION_PARTITION_TYPE = UNPARTITIONED.partitionType();

  private static final long SNAPSHOT_ID = 42L;
  private static final long DATA_SEQ = 7L;
  private static final long FILE_SEQ = 11L;
  private static final String DATA_PATH = "s3://bucket/data/file.parquet";
  private static final String DELETE_PATH = "s3://bucket/data/eq-delete.parquet";
  private static final String MANIFEST_PATH = "s3://bucket/metadata/manifest.parquet";

  private static final Metrics METRICS_WITH_BOUNDS =
      new Metrics(
          100L,
          ImmutableMap.of(1, 16L, 2, 64L),
          ImmutableMap.of(1, 100L, 2, 100L),
          ImmutableMap.of(1, 0L, 2, 5L),
          ImmutableMap.of(),
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1)),
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1000)));

  @Test
  void fromDataFileAdded() {
    DataFile file = dataFile();
    TrackedFile result =
        ContentEntryAdapters.fromDataFile(
            wrapAdded(file), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.ADDED);

    assertAddedTracking(result, SNAPSHOT_ID);
    assertThat(result.tracking().firstRowId()).isEqualTo(file.firstRowId());
    assertCommonDataFields(result, file);
  }

  @Test
  void fromDataFileAddedWithNullSnapshotId() {
    // Staged-write ADDED pattern (e.g., FastAppend writing the manifest before the commit snapshot
    // is assigned). The adapter must accept a null snapshot ID and produce an ADDED tracking row
    // whose snapshot_id is null; readers inherit it from the manifest list's added_snapshot_id.
    DataFile file = dataFile();
    GenericManifestEntry<DataFile> entry =
        new GenericManifestEntry<>(
            ManifestEntry.getSchema(UNPARTITIONED.partitionType()).asStruct());
    entry.wrapAppend(null /* snapshotId inherited at read time */, file);

    TrackedFile result =
        ContentEntryAdapters.fromDataFile(entry, SCHEMA, UNION_PARTITION_TYPE, EntryStatus.ADDED);

    assertThat(result.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(result.tracking().snapshotId()).isNull();
    assertThat(result.tracking().dataSequenceNumber()).isNull();
    assertThat(result.tracking().fileSequenceNumber()).isNull();
    assertCommonDataFields(result, file);
  }

  @Test
  void fromDeleteFileAddedWithNullSnapshotId() {
    // Staged-write ADDED pattern for equality delete manifests.
    DeleteFile file = equalityDeleteFile();
    GenericManifestEntry<DeleteFile> entry =
        new GenericManifestEntry<>(
            ManifestEntry.getSchema(UNPARTITIONED.partitionType()).asStruct());
    entry.wrapAppend(null /* snapshotId inherited at read time */, file);

    TrackedFile result =
        ContentEntryAdapters.fromDeleteFile(entry, SCHEMA, UNION_PARTITION_TYPE, EntryStatus.ADDED);

    assertThat(result.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(result.tracking().snapshotId()).isNull();
    assertThat(result.tracking().dataSequenceNumber()).isNull();
    assertThat(result.tracking().fileSequenceNumber()).isNull();
    assertCommonDeleteFields(result, file);
  }

  @Test
  void fromDataFileExisting() {
    DataFile file = dataFile();
    TrackedFile result =
        ContentEntryAdapters.fromDataFile(
            existingDataEntry(), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.EXISTING);

    assertExistingTracking(result, SNAPSHOT_ID, DATA_SEQ, FILE_SEQ);
    assertCommonDataFields(result, file);
  }

  @Test
  void fromDataFileDeleted() {
    DataFile file = dataFile();
    TrackedFile result =
        ContentEntryAdapters.fromDataFile(
            existingDataEntry(), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.DELETED);

    assertDeletedTracking(result, SNAPSHOT_ID, DATA_SEQ, FILE_SEQ);
    assertCommonDataFields(result, file);
  }

  @Test
  void fromDeleteFileAdded() {
    DeleteFile file = equalityDeleteFile();
    TrackedFile result =
        ContentEntryAdapters.fromDeleteFile(
            addedEqualityDeleteEntry(), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.ADDED);

    assertAddedTracking(result, SNAPSHOT_ID);
    assertThat(result.tracking().firstRowId()).isNull();
    assertCommonDeleteFields(result, file);
  }

  @Test
  void fromDeleteFileExisting() {
    DeleteFile file = equalityDeleteFile();
    TrackedFile result =
        ContentEntryAdapters.fromDeleteFile(
            existingEqualityDeleteEntry(), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.EXISTING);

    assertExistingTracking(result, SNAPSHOT_ID, DATA_SEQ, FILE_SEQ);
    assertCommonDeleteFields(result, file);
  }

  @Test
  void fromDeleteFileDeleted() {
    DeleteFile file = equalityDeleteFile();
    TrackedFile result =
        ContentEntryAdapters.fromDeleteFile(
            existingEqualityDeleteEntry(), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.DELETED);

    assertDeletedTracking(result, SNAPSHOT_ID, DATA_SEQ, FILE_SEQ);
    assertCommonDeleteFields(result, file);
  }

  @Test
  void fromDataFileRejectsReplaced() {
    // REPLACED has no legacy ManifestEntry representation; the adapter only handles
    // ADDED/EXISTING/DELETED.
    assertThatThrownBy(
            () ->
                ContentEntryAdapters.fromDataFile(
                    existingDataEntry(), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.REPLACED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported status for content file entry: REPLACED");
  }

  @Test
  void fromDataFileRejectsModified() {
    // MODIFIED has no legacy ManifestEntry representation; the adapter only handles
    // ADDED/EXISTING/DELETED.
    assertThatThrownBy(
            () ->
                ContentEntryAdapters.fromDataFile(
                    existingDataEntry(), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.MODIFIED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported status for content file entry: MODIFIED");
  }

  @Test
  void fromDataFileWithPartitionSpec() {
    // Multi-spec carry-over: the writer spec has one field but the union partition type carries an
    // additional field from another live spec. The adapter must place the writer's value at the
    // union-schema position keyed by field ID and leave the extra union field null.
    PartitionSpec writerSpec = PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build();
    Types.NestedField writerField = writerSpec.partitionType().fields().get(0);
    Types.NestedField extraField =
        Types.NestedField.optional(2000, "extra", Types.IntegerType.get());
    Types.StructType unionType = Types.StructType.of(writerField, extraField);

    PartitionData partition = new PartitionData(writerSpec.partitionType());
    partition.set(0, 7);
    DataFile file =
        new GenericDataFile(
            writerSpec.specId(),
            DATA_PATH,
            FileFormat.PARQUET,
            partition,
            1024L,
            new Metrics(100L, null, null, null, null),
            null,
            ImmutableList.of(0L),
            null,
            null);

    TrackedFile result =
        ContentEntryAdapters.fromDataFile(wrapAdded(file), SCHEMA, unionType, EntryStatus.ADDED);

    assertThat(result.partition()).isInstanceOf(PartitionData.class);
    PartitionData projected = (PartitionData) result.partition();
    assertThat(projected.getPartitionType()).isEqualTo(unionType);
    assertThat(projected.size()).isEqualTo(2);

    int writerPos = unionType.fields().indexOf(writerField);
    int extraPos = unionType.fields().indexOf(extraField);
    assertThat(projected.get(writerPos, Integer.class)).isEqualTo(7);
    assertThat(projected.get(extraPos, Integer.class)).isNull();
  }

  @Test
  void fromDeleteFilePopulatesEqualityIds() {
    TrackedFile result =
        ContentEntryAdapters.fromDeleteFile(
            addedEqualityDeleteEntry(), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.ADDED);

    assertThat(result.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(result.location()).isEqualTo(DELETE_PATH);
    assertThat(result.equalityIds()).containsExactly(1);
    assertThat(result.tracking().status()).isEqualTo(EntryStatus.ADDED);
  }

  @Test
  void fromDataFilePopulatesContentStatsBounds() {
    DataFile file = dataFileWithMetrics();
    TrackedFile result =
        ContentEntryAdapters.fromDataFile(
            wrapAdded(file), SCHEMA, UNION_PARTITION_TYPE, EntryStatus.ADDED);

    ContentStats stats = result.contentStats();
    assertThat(stats).isNotNull();
    assertThat(stats.fieldStats()).extracting(FieldStats::fieldId).containsExactlyInAnyOrder(1, 2);

    FieldStats<?> idStats =
        stats.fieldStats().stream().filter(s -> s.fieldId() == 1).findFirst().orElseThrow();
    assertThat(idStats.valueCount()).isEqualTo(100L);
    assertThat(idStats.lowerBound()).isEqualTo(1);
    assertThat(idStats.upperBound()).isEqualTo(1000);
  }

  @Test
  void fromDataFilePopulatesContentStatsForListAndMapElements() {
    // primitiveTypesFor must recurse into list element and map key/value types so that bounds
    // keyed by the inner field IDs are preserved. Without the list/map branches in
    // collectPrimitiveTypes the inner types would be absent from originalTypes and
    // MetricsUtil.fromMetrics would silently drop these field stats.
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(
                2, "tags", Types.ListType.ofRequired(10 /* elementId */, Types.IntegerType.get())),
            required(
                3,
                "props",
                Types.MapType.ofRequired(
                    11 /* keyId */,
                    12 /* valueId */,
                    Types.StringType.get(),
                    Types.IntegerType.get())));

    Metrics metrics =
        new Metrics(
            100L,
            null /* columnSizes */,
            ImmutableMap.of(1, 100L, 10, 100L, 11, 100L, 12, 100L),
            ImmutableMap.of(1, 0L, 10, 0L, 11, 0L, 12, 0L),
            ImmutableMap.of(),
            ImmutableMap.of(
                10, Conversions.toByteBuffer(Types.IntegerType.get(), 1),
                11, Conversions.toByteBuffer(Types.StringType.get(), "a"),
                12, Conversions.toByteBuffer(Types.IntegerType.get(), 5)),
            ImmutableMap.of(
                10, Conversions.toByteBuffer(Types.IntegerType.get(), 999),
                11, Conversions.toByteBuffer(Types.StringType.get(), "z"),
                12, Conversions.toByteBuffer(Types.IntegerType.get(), 50)));

    DataFile file =
        new GenericDataFile(
            UNPARTITIONED.specId(),
            DATA_PATH,
            FileFormat.PARQUET,
            EMPTY_PARTITION,
            1024L,
            metrics,
            null,
            ImmutableList.of(0L),
            null,
            null);

    TrackedFile result =
        ContentEntryAdapters.fromDataFile(
            wrapAdded(file), schema, UNION_PARTITION_TYPE, EntryStatus.ADDED);

    ContentStats stats = result.contentStats();
    assertThat(stats).isNotNull();
    assertThat(stats.fieldStats()).extracting(FieldStats::fieldId).contains(10, 11, 12);

    FieldStats<?> elementStats =
        stats.fieldStats().stream().filter(s -> s.fieldId() == 10).findFirst().orElseThrow();
    assertThat(elementStats.lowerBound()).isEqualTo(1);
    assertThat(elementStats.upperBound()).isEqualTo(999);

    FieldStats<?> keyStats =
        stats.fieldStats().stream().filter(s -> s.fieldId() == 11).findFirst().orElseThrow();
    assertThat(keyStats.lowerBound()).isEqualTo("a");
    assertThat(keyStats.upperBound()).isEqualTo("z");

    FieldStats<?> valueStats =
        stats.fieldStats().stream().filter(s -> s.fieldId() == 12).findFirst().orElseThrow();
    assertThat(valueStats.lowerBound()).isEqualTo(5);
    assertThat(valueStats.upperBound()).isEqualTo(50);
  }

  @Test
  void fromManifestFileForDataManifest() {
    GenericManifestFile manifest = v4ManifestFile(ManifestContent.DATA, 4, 6L);
    TrackedFile result = ContentEntryAdapters.fromManifestFile(manifest, EntryStatus.ADDED, 1000L);

    assertThat(result.contentType()).isEqualTo(FileContent.DATA_MANIFEST);
    assertThat(result.formatVersion()).isEqualTo(4);
    assertThat(result.location()).isEqualTo(MANIFEST_PATH);
    assertThat(result.tracking().firstRowId()).isEqualTo(1000L);
    // record_count for a manifest-reference content_entry is sourced from the manifest's
    // persisted record_count (v4) or summed from per-status file counts (pre-v4). Data-row totals
    // live in manifest_info, not in this field.
    assertThat(result.recordCount()).isEqualTo(6L);
    assertThat(result.manifestInfo()).isNotNull();
    assertThat(result.manifestInfo().addedFilesCount()).isEqualTo(2);
    assertThat(result.manifestInfo().existingFilesCount()).isEqualTo(3);
    assertThat(result.manifestInfo().deletedFilesCount()).isEqualTo(1);
    assertThat(result.manifestInfo().addedRowsCount()).isEqualTo(200L);
    assertThat(result.manifestInfo().existingRowsCount()).isEqualTo(300L);
    assertThat(result.manifestInfo().deletedRowsCount()).isEqualTo(100L);
    // replaced/modified counts default to 0 when the source manifest does not track them
    assertThat(result.manifestInfo().replacedFilesCount()).isEqualTo(0);
    assertThat(result.manifestInfo().replacedRowsCount()).isEqualTo(0L);
  }

  @Test
  void fromManifestFileUsesPersistedRecordCount() {
    // When the source manifest carries a persisted record_count (e.g., a v4+ leaf writer that
    // tracked MODIFIED entries, or a row projected from a content_entry on read), the adapter
    // must propagate it instead of recomputing from per-status file counts. The persisted value
    // here (99) deliberately differs from the per-status sum (2 + 3 + 1 = 6) to prove which
    // path is taken.
    GenericManifestFile manifest = v4ManifestFile(ManifestContent.DATA, 4, 99L);

    TrackedFile result = ContentEntryAdapters.fromManifestFile(manifest, EntryStatus.ADDED, null);

    assertThat(result.recordCount()).isEqualTo(99L);
  }

  @Test
  void fromManifestFileRejectsV4ManifestWithoutRecordCount() {
    // v4+ manifests must carry a persisted record_count: the per-status sum cannot reflect
    // MODIFIED entries, so v4+ writers and root-manifest readers populate it directly.
    GenericManifestFile manifest =
        v4ManifestFile(ManifestContent.DATA, 4, null /* recordCount intentionally null */);
    assertThatThrownBy(
            () -> ContentEntryAdapters.fromManifestFile(manifest, EntryStatus.ADDED, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("record_count must be set");
  }

  @Test
  void fromManifestFileForDeleteManifestPreV4() {
    // Pre-v4 manifests have formatVersion=0 by default (not tracked in the v1-v3 manifest list
    // schema, so reads land on the interface default). The adapter dispatches to the legacy sum.
    ManifestFile manifest = manifestFile(ManifestContent.DELETES);
    TrackedFile result =
        ContentEntryAdapters.fromManifestFile(manifest, EntryStatus.EXISTING, null);

    assertThat(result.contentType()).isEqualTo(FileContent.DELETE_MANIFEST);
    assertThat(result.formatVersion()).isEqualTo(0);
    assertThat(result.recordCount()).isEqualTo(6L);
    assertThat(result.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(result.tracking().firstRowId()).isNull();
  }

  @Test
  void fromManifestFileRejectsFirstRowIdOnDeleteManifest() {
    ManifestFile manifest = manifestFile(ManifestContent.DELETES);

    assertThatThrownBy(
            () -> ContentEntryAdapters.fromManifestFile(manifest, EntryStatus.ADDED, 100L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("firstRowId is only valid for DATA manifests");
  }

  @Test
  void fromManifestFileRejectsInvalidFormatVersion() {
    // The manifest's intrinsic format_version is the source of truth. Values strictly between 0
    // and 4 (1, 2, 3) and negative values have no defined meaning at the v4+ content_entry layer
    // and are rejected.
    GenericManifestFile manifest = v4ManifestFile(ManifestContent.DATA, 2, null);
    assertThatThrownBy(
            () -> ContentEntryAdapters.fromManifestFile(manifest, EntryStatus.ADDED, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid manifest format_version: 2");
  }

  @Test
  void fromManifestFileRejectsUnassignedMinSequenceNumber() {
    ManifestFile manifest = manifestFile(ManifestContent.DATA, ManifestWriter.UNASSIGNED_SEQ);
    assertThatThrownBy(
            () -> ContentEntryAdapters.fromManifestFile(manifest, EntryStatus.ADDED, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("min_sequence_number is unassigned");
  }

  @Test
  void fromManifestFileRejectsUnassignedSequenceNumber() {
    ManifestFile manifest =
        manifestFileWithSequenceNumbers(
            ManifestContent.DATA, ManifestWriter.UNASSIGNED_SEQ, 4L /* minSeq, valid */);
    assertThatThrownBy(
            () -> ContentEntryAdapters.fromManifestFile(manifest, EntryStatus.ADDED, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sequence_number is unassigned");
  }

  @Test
  void fromManifestFileRejectsNullManifest() {
    assertThatThrownBy(() -> ContentEntryAdapters.fromManifestFile(null, EntryStatus.ADDED, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid manifest file: null");
  }

  @Test
  void fromDataFileRejectsNullEntry() {
    assertThatThrownBy(
            () ->
                ContentEntryAdapters.fromDataFile(
                    null, SCHEMA, UNION_PARTITION_TYPE, EntryStatus.ADDED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid manifest entry: null");
  }

  @Test
  void fromDataFileRejectsNullStatus() {
    assertThatThrownBy(
            () ->
                ContentEntryAdapters.fromDataFile(
                    addedDataEntry(), SCHEMA, UNION_PARTITION_TYPE, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid status: null");
  }

  @Test
  void fromDeleteFileRejectsV3DeleteVector() {
    // A v3 delete vector is shaped as POSITION_DELETES stored in a Puffin file. v4+ colocates DVs
    // on the data file's content_entry, so this should be rejected at the delete-manifest writer
    // boundary.
    DeleteFile dv =
        new GenericDeleteFile(
            UNPARTITIONED.specId(),
            FileContent.POSITION_DELETES,
            DELETE_PATH,
            FileFormat.PUFFIN,
            EMPTY_PARTITION,
            512L,
            new Metrics(10L, null, null, null, null),
            null,
            null,
            null,
            null,
            DATA_PATH,
            0L,
            512L);
    GenericManifestEntry<DeleteFile> entry =
        new GenericManifestEntry<>(
            ManifestEntry.getSchema(UNPARTITIONED.partitionType()).asStruct());
    entry.wrapAppend(SNAPSHOT_ID, dv);

    assertThatThrownBy(
            () ->
                ContentEntryAdapters.fromDeleteFile(
                    entry, SCHEMA, UNION_PARTITION_TYPE, EntryStatus.ADDED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid content for delete file: POSITION_DELETES");
  }

  @Test
  void fromDeleteFileRejectsV2PositionDeleteFile() {
    // A v2 standalone position delete file is shaped as POSITION_DELETES stored in Parquet/Avro/ORC
    // (anything other than Puffin). It has no v4+ representation; carry it over only via a legacy
    // v3 manifest with format_version=0.
    DeleteFile positionDelete =
        new GenericDeleteFile(
            UNPARTITIONED.specId(),
            FileContent.POSITION_DELETES,
            DELETE_PATH,
            FileFormat.PARQUET,
            EMPTY_PARTITION,
            512L,
            new Metrics(10L, null, null, null, null),
            null,
            null,
            null,
            null,
            DATA_PATH,
            null /* contentOffset */,
            null /* contentSizeInBytes */);
    GenericManifestEntry<DeleteFile> entry =
        new GenericManifestEntry<>(
            ManifestEntry.getSchema(UNPARTITIONED.partitionType()).asStruct());
    entry.wrapAppend(SNAPSHOT_ID, positionDelete);

    assertThatThrownBy(
            () ->
                ContentEntryAdapters.fromDeleteFile(
                    entry, SCHEMA, UNION_PARTITION_TYPE, EntryStatus.ADDED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid content for delete file: POSITION_DELETES");
  }

  private static void assertAddedTracking(TrackedFile result, long snapshotId) {
    assertThat(result.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(result.tracking().snapshotId()).isEqualTo(snapshotId);
    assertThat(result.tracking().dataSequenceNumber()).isNull();
    assertThat(result.tracking().fileSequenceNumber()).isNull();
  }

  private static void assertExistingTracking(
      TrackedFile result, long snapshotId, long dataSeq, long fileSeq) {
    assertThat(result.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(result.tracking().snapshotId()).isEqualTo(snapshotId);
    assertThat(result.tracking().dataSequenceNumber()).isEqualTo(dataSeq);
    assertThat(result.tracking().fileSequenceNumber()).isEqualTo(fileSeq);
  }

  private static void assertDeletedTracking(
      TrackedFile result, long snapshotId, long dataSeq, long fileSeq) {
    assertThat(result.tracking().status()).isEqualTo(EntryStatus.DELETED);
    assertThat(result.tracking().snapshotId()).isEqualTo(snapshotId);
    assertThat(result.tracking().dataSequenceNumber()).isEqualTo(dataSeq);
    assertThat(result.tracking().fileSequenceNumber()).isEqualTo(fileSeq);
  }

  private static void assertCommonDataFields(TrackedFile result, DataFile file) {
    assertThat(result.contentType()).isEqualTo(FileContent.DATA);
    assertThat(result.formatVersion()).isEqualTo(4);
    assertThat(result.location()).isEqualTo(file.location());
    assertThat(result.fileFormat()).isEqualTo(file.format());
    assertThat(result.recordCount()).isEqualTo(file.recordCount());
    assertThat(result.fileSizeInBytes()).isEqualTo(file.fileSizeInBytes());
    assertThat(result.specId()).isEqualTo(file.specId());
    assertThat(result.partition().size()).isEqualTo(0);
    assertThat(result.splitOffsets()).isEqualTo(file.splitOffsets());
  }

  private static void assertCommonDeleteFields(TrackedFile result, DeleteFile file) {
    assertThat(result.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(result.formatVersion()).isEqualTo(4);
    assertThat(result.location()).isEqualTo(file.location());
    assertThat(result.fileFormat()).isEqualTo(file.format());
    assertThat(result.recordCount()).isEqualTo(file.recordCount());
    assertThat(result.fileSizeInBytes()).isEqualTo(file.fileSizeInBytes());
    assertThat(result.specId()).isEqualTo(file.specId());
    assertThat(result.partition().size()).isEqualTo(0);
    assertThat(result.equalityIds())
        .containsExactlyElementsOf(ImmutableList.copyOf(file.equalityFieldIds()));
  }

  private static DataFile dataFile() {
    return new GenericDataFile(
        UNPARTITIONED.specId(),
        DATA_PATH,
        FileFormat.PARQUET,
        EMPTY_PARTITION,
        1024L,
        new Metrics(100L, null, null, null, null),
        null,
        ImmutableList.of(0L),
        null,
        null);
  }

  private static DataFile dataFileWithMetrics() {
    return new GenericDataFile(
        UNPARTITIONED.specId(),
        DATA_PATH,
        FileFormat.PARQUET,
        EMPTY_PARTITION,
        1024L,
        METRICS_WITH_BOUNDS,
        null,
        ImmutableList.of(0L),
        null,
        null);
  }

  private static DeleteFile equalityDeleteFile() {
    return new GenericDeleteFile(
        UNPARTITIONED.specId(),
        FileContent.EQUALITY_DELETES,
        DELETE_PATH,
        FileFormat.PARQUET,
        EMPTY_PARTITION,
        512L,
        new Metrics(50L, null, null, null, null),
        new int[] {1},
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static ManifestEntry<DataFile> addedDataEntry() {
    return wrapAdded(dataFile());
  }

  private static ManifestEntry<DataFile> existingDataEntry() {
    GenericManifestEntry<DataFile> entry =
        new GenericManifestEntry<>(
            ManifestEntry.getSchema(UNPARTITIONED.partitionType()).asStruct());
    entry.wrapExisting(SNAPSHOT_ID, DATA_SEQ, FILE_SEQ, dataFile());
    return entry;
  }

  private static ManifestEntry<DeleteFile> addedEqualityDeleteEntry() {
    GenericManifestEntry<DeleteFile> entry =
        new GenericManifestEntry<>(
            ManifestEntry.getSchema(UNPARTITIONED.partitionType()).asStruct());
    entry.wrapAppend(SNAPSHOT_ID, equalityDeleteFile());
    return entry;
  }

  private static ManifestEntry<DeleteFile> existingEqualityDeleteEntry() {
    GenericManifestEntry<DeleteFile> entry =
        new GenericManifestEntry<>(
            ManifestEntry.getSchema(UNPARTITIONED.partitionType()).asStruct());
    entry.wrapExisting(SNAPSHOT_ID, DATA_SEQ, FILE_SEQ, equalityDeleteFile());
    return entry;
  }

  private static ManifestEntry<DataFile> wrapAdded(DataFile file) {
    GenericManifestEntry<DataFile> entry =
        new GenericManifestEntry<>(
            ManifestEntry.getSchema(UNPARTITIONED.partitionType()).asStruct());
    entry.wrapAppend(SNAPSHOT_ID, file);
    return entry;
  }

  private static ManifestFile manifestFile(ManifestContent content) {
    return manifestFile(content, 5L /* sequenceNumber */, 4L /* minSequenceNumber */);
  }

  private static ManifestFile manifestFile(ManifestContent content, long minSequenceNumber) {
    return manifestFile(content, 5L /* sequenceNumber */, minSequenceNumber);
  }

  private static ManifestFile manifestFileWithSequenceNumbers(
      ManifestContent content, long sequenceNumber, long minSequenceNumber) {
    return manifestFile(content, sequenceNumber, minSequenceNumber);
  }

  private static GenericManifestFile v4ManifestFile(
      ManifestContent content, int formatVersion, Long recordCount) {
    List<ManifestFile.PartitionFieldSummary> partitions = ImmutableList.of();
    return new GenericManifestFile(
        MANIFEST_PATH,
        2048L,
        UNPARTITIONED.specId(),
        content,
        5L /* sequenceNumber */,
        4L /* minSequenceNumber */,
        SNAPSHOT_ID,
        partitions,
        null,
        2 /* addedFilesCount */,
        200L /* addedRowsCount */,
        3 /* existingFilesCount */,
        300L /* existingRowsCount */,
        1 /* deletedFilesCount */,
        100L /* deletedRowsCount */,
        null /* firstRowId */,
        recordCount,
        formatVersion);
  }

  private static ManifestFile manifestFile(
      ManifestContent content, long sequenceNumber, long minSequenceNumber) {
    List<ManifestFile.PartitionFieldSummary> partitions = ImmutableList.of();
    return new GenericManifestFile(
        MANIFEST_PATH,
        2048L,
        UNPARTITIONED.specId(),
        content,
        sequenceNumber,
        minSequenceNumber,
        SNAPSHOT_ID,
        partitions,
        null,
        2 /* addedFilesCount */,
        200L /* addedRowsCount */,
        3 /* existingFilesCount */,
        300L /* existingRowsCount */,
        1 /* deletedFilesCount */,
        100L /* deletedRowsCount */,
        null);
  }
}
