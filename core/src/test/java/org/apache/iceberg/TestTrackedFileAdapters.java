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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.FieldSource;

class TestTrackedFileAdapters {

  private static final int FORMAT_VERSION_V4 = 4;
  private static final String MANIFEST_LOCATION = "s3://bucket/table/manifest.parquet";
  private static final String DATA_FILE_LOCATION = "s3://bucket/data/file.parquet";
  private static final String DV_LOCATION = "s3://bucket/puffin/dv-file.bin";

  // Tracking values that the delegation tests validate.
  private static final long MANIFEST_POS = 3L;
  private static final long DATA_SEQUENCE_NUMBER = 10L;
  private static final long FILE_SEQUENCE_NUMBER = 11L;
  private static final long FIRST_ROW_ID = 1000L;

  private static final int UNPARTITIONED_SPEC_ID = PartitionSpec.unpartitioned().specId();
  private static final Map<Integer, PartitionSpec> UNPARTITIONED =
      ImmutableMap.of(UNPARTITIONED_SPEC_ID, PartitionSpec.unpartitioned());

  private static final Schema PARTITION_SCHEMA =
      new Schema(Types.NestedField.required(1, "category", Types.StringType.get()));
  private static final int PARTITIONED_SPEC_ID = 1;
  private static final PartitionSpec PARTITIONED_SPEC =
      PartitionSpec.builderFor(PARTITION_SCHEMA)
          .identity("category")
          .withSpecId(PARTITIONED_SPEC_ID)
          .build();
  private static final PartitionData PARTITION = partition("books");

  // manifestPos is populated by readers using the setter with the position of the field.
  private static final int MANIFEST_POS_ORDINAL = Tracking.schema().fields().size();

  private static final Schema STATS_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "score", Types.FloatType.get()));
  private static final Types.StructType CONTENT_STATS_TYPE =
      StatsUtil.statsReadSchema(STATS_SCHEMA, ImmutableList.of(1, 2));
  private static final FieldStats<Integer> ID_STATS =
      new FieldStatsStruct<>(
          CONTENT_STATS_TYPE.fieldType("id").asStructType(), 1, 1000, true, 100L, 5L, 0L, null);
  private static final FieldStats<Float> SCORE_STATS =
      new FieldStatsStruct<>(
          CONTENT_STATS_TYPE.fieldType("score").asStructType(),
          1.0f,
          100.0f,
          true,
          100L,
          10L,
          3L,
          null);
  private static final ContentStatsStruct CONTENT_STATS =
      new ContentStatsStruct(CONTENT_STATS_TYPE);

  static {
    CONTENT_STATS.setStats(1, ID_STATS);
    CONTENT_STATS.setStats(2, SCORE_STATS);
  }

  private static final Schema TABLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));
  private static final MetricsConfig METRICS_CONFIG =
      MetricsConfig.from(ImmutableMap.of(), TABLE_SCHEMA, SortOrder.unsorted());
  private static final PartitionSpec UNPARTITIONED_SPEC = PartitionSpec.unpartitioned();
  private static final PartitionData EMPTY_PARTITION_DATA =
      new PartitionData(UNPARTITIONED_SPEC.partitionType());
  private static final Types.StructType PARTITION_TYPE = UNPARTITIONED_SPEC.partitionType();
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

  private static final Tracking ADDED_TRACKING = TrackingBuilder.added(SNAPSHOT_ID).build();
  private static final Tracking EXISTING_TRACKING =
      new TrackingStruct(
          EntryStatus.EXISTING, SNAPSHOT_ID, DATA_SEQ, FILE_SEQ, null, null, null, null);
  private static final Tracking DELETED_TRACKING =
      new TrackingStruct(
          EntryStatus.DELETED, SNAPSHOT_ID, DATA_SEQ, FILE_SEQ, null, null, null, null);

  private static final DataFile DATA_FILE =
      new GenericDataFile(
          UNPARTITIONED_SPEC.specId(),
          DATA_PATH,
          FileFormat.PARQUET,
          EMPTY_PARTITION_DATA,
          1024L,
          new Metrics(100L, null, null, null, null),
          null,
          ImmutableList.of(0L),
          null,
          null);
  private static final DataFile DATA_FILE_WITH_METRICS =
      new GenericDataFile(
          UNPARTITIONED_SPEC.specId(),
          DATA_PATH,
          FileFormat.PARQUET,
          EMPTY_PARTITION_DATA,
          1024L,
          METRICS_WITH_BOUNDS,
          null,
          ImmutableList.of(0L),
          null,
          null);
  private static final DeleteFile EQUALITY_DELETE_FILE =
      new GenericDeleteFile(
          UNPARTITIONED_SPEC.specId(),
          FileContent.EQUALITY_DELETES,
          DELETE_PATH,
          FileFormat.PARQUET,
          EMPTY_PARTITION_DATA,
          512L,
          new Metrics(50L, null, null, null, null),
          new int[] {1},
          null,
          null,
          null,
          null,
          null,
          null);

  private static final DeletionVector DELETION_VECTOR =
      DeletionVectorStruct.builder()
          .location(DV_LOCATION)
          .offset(128L)
          .sizeInBytes(256L)
          .cardinality(10L)
          .build();

  @Test
  void testDataFileAdapterDelegation() {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            42L,
            DATA_SEQUENCE_NUMBER,
            FILE_SEQUENCE_NUMBER,
            null,
            FIRST_ROW_ID,
            null,
            null);
    tracking.setManifestLocation(MANIFEST_LOCATION);
    tracking.set(MANIFEST_POS_ORDINAL, MANIFEST_POS);

    TrackedFile file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            PARTITION,
            100L,
            1024L,
            PARTITIONED_SPEC_ID,
            CONTENT_STATS,
            3,
            null,
            null,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(50L, 100L),
            null);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, specsById(PARTITIONED_SPEC));

    assertThat(dataFile.pos()).isEqualTo(MANIFEST_POS);
    assertThat(dataFile.specId()).isEqualTo(PARTITIONED_SPEC_ID);
    assertThat(dataFile.partition()).isSameAs(PARTITION);
    assertThat(dataFile.content()).isEqualTo(FileContent.DATA);
    assertThat(dataFile.location()).isEqualTo(DATA_FILE_LOCATION);
    assertThat(dataFile.format()).isEqualTo(FileFormat.PARQUET);
    assertThat(dataFile.recordCount()).isEqualTo(100L);
    assertThat(dataFile.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(dataFile.sortOrderId()).isEqualTo(3);
    assertThat(dataFile.dataSequenceNumber()).isEqualTo(DATA_SEQUENCE_NUMBER);
    assertThat(dataFile.fileSequenceNumber()).isEqualTo(FILE_SEQUENCE_NUMBER);
    assertThat(dataFile.firstRowId()).isEqualTo(FIRST_ROW_ID);
    assertThat(dataFile.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(dataFile.splitOffsets()).containsExactly(50L, 100L);
    assertThat(dataFile.manifestLocation()).isEqualTo(MANIFEST_LOCATION);
    assertThat(dataFile.equalityFieldIds()).isNull();
    assertThat(dataFile.columnSizes()).isNull();
    assertThat(dataFile.valueCounts()).containsOnly(Map.entry(1, 100L), Map.entry(2, 100L));
    assertThat(dataFile.nullValueCounts()).containsOnly(Map.entry(1, 5L), Map.entry(2, 10L));
    assertThat(dataFile.nanValueCounts()).containsOnly(Map.entry(2, 3L));
    assertThat(dataFile.lowerBounds())
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1)),
            Map.entry(2, Conversions.toByteBuffer(Types.FloatType.get(), 1.0f)));
    assertThat(dataFile.upperBounds())
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1000)),
            Map.entry(2, Conversions.toByteBuffer(Types.FloatType.get(), 100.0f)));
  }

  @ParameterizedTest
  @EnumSource(value = FileContent.class, mode = EnumSource.Mode.EXCLUDE, names = "DATA")
  void testDataFileAdapterRejectsNonDataContent(FileContent contentType) {
    TrackedFileStruct file = dummyTrackedFile(contentType);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for DataFile: %s", contentType);
  }

  @Test
  void testEqualityDeleteFileAdapterDelegation() {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            42L,
            DATA_SEQUENCE_NUMBER,
            FILE_SEQUENCE_NUMBER,
            null,
            FIRST_ROW_ID,
            null,
            null);
    tracking.setManifestLocation(MANIFEST_LOCATION);
    tracking.set(MANIFEST_POS_ORDINAL, MANIFEST_POS);

    TrackedFile file =
        new TrackedFileStruct(
            tracking,
            FileContent.EQUALITY_DELETES,
            FORMAT_VERSION_V4,
            "s3://bucket/eq-delete.avro",
            FileFormat.AVRO,
            PARTITION,
            50L,
            512L,
            PARTITIONED_SPEC_ID,
            CONTENT_STATS,
            5,
            null,
            null,
            ByteBuffer.wrap(new byte[] {4, 5}),
            ImmutableList.of(200L),
            ImmutableList.of(1, 2, 3));

    DeleteFile deleteFile =
        TrackedFileAdapters.asEqualityDeleteFile(file, specsById(PARTITIONED_SPEC));

    assertThat(deleteFile.pos()).isEqualTo(MANIFEST_POS);
    assertThat(deleteFile.specId()).isEqualTo(PARTITIONED_SPEC_ID);
    assertThat(deleteFile.partition()).isSameAs(PARTITION);
    assertThat(deleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(deleteFile.location()).isEqualTo("s3://bucket/eq-delete.avro");
    assertThat(deleteFile.format()).isEqualTo(FileFormat.AVRO);
    assertThat(deleteFile.recordCount()).isEqualTo(50L);
    assertThat(deleteFile.fileSizeInBytes()).isEqualTo(512L);
    assertThat(deleteFile.sortOrderId()).isEqualTo(5);
    assertThat(deleteFile.dataSequenceNumber()).isEqualTo(DATA_SEQUENCE_NUMBER);
    assertThat(deleteFile.fileSequenceNumber()).isEqualTo(FILE_SEQUENCE_NUMBER);
    assertThat(deleteFile.firstRowId()).isNull();
    assertThat(deleteFile.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {4, 5}));
    assertThat(deleteFile.splitOffsets()).containsExactly(200L);
    assertThat(deleteFile.manifestLocation()).isEqualTo(MANIFEST_LOCATION);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1, 2, 3);
    assertThat(deleteFile.columnSizes()).isNull();
    assertThat(deleteFile.valueCounts()).containsOnly(Map.entry(1, 100L), Map.entry(2, 100L));
    assertThat(deleteFile.nullValueCounts()).containsOnly(Map.entry(1, 5L), Map.entry(2, 10L));
    assertThat(deleteFile.nanValueCounts()).containsOnly(Map.entry(2, 3L));
    assertThat(deleteFile.lowerBounds())
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1)),
            Map.entry(2, Conversions.toByteBuffer(Types.FloatType.get(), 1.0f)));
    assertThat(deleteFile.upperBounds())
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1000)),
            Map.entry(2, Conversions.toByteBuffer(Types.FloatType.get(), 100.0f)));
  }

  @ParameterizedTest
  @EnumSource(value = FileContent.class, mode = EnumSource.Mode.EXCLUDE, names = "EQUALITY_DELETES")
  void testEqualityDeleteFileAdapterRejectsNonEqualityContent(FileContent contentType) {
    TrackedFileStruct file = dummyTrackedFile(contentType);

    assertThatThrownBy(() -> TrackedFileAdapters.asEqualityDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for equality delete file: %s", contentType);
  }

  @Test
  void testDVDeleteFileAdapterDelegation() {
    DeletionVector dv =
        DeletionVectorStruct.builder()
            .location(DV_LOCATION)
            .offset(128L)
            .sizeInBytes(256L)
            .cardinality(10L)
            .build();

    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            42L,
            DATA_SEQUENCE_NUMBER,
            FILE_SEQUENCE_NUMBER,
            42L,
            FIRST_ROW_ID,
            null,
            null);
    tracking.setManifestLocation(MANIFEST_LOCATION);
    tracking.set(MANIFEST_POS_ORDINAL, MANIFEST_POS);

    TrackedFile file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            PARTITION,
            100L,
            1024L,
            PARTITIONED_SPEC_ID,
            null,
            null,
            dv,
            null,
            null,
            null,
            null);

    DeleteFile dvFile = TrackedFileAdapters.asDVDeleteFile(file, specsById(PARTITIONED_SPEC));

    // DV blob metadata is surfaced through the DeleteFile DV fields.
    assertThat(dvFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(dvFile.location()).isEqualTo(DV_LOCATION);
    assertThat(dvFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(dvFile.recordCount()).isEqualTo(dv.cardinality());
    assertThat(dvFile.contentOffset()).isEqualTo(dv.offset());
    assertThat(dvFile.contentSizeInBytes()).isEqualTo(dv.sizeInBytes());
    // fileSizeInBytes reports the DV blob size, not the full Puffin file size.
    assertThat(dvFile.fileSizeInBytes()).isEqualTo(dv.sizeInBytes());
    // referencedDataFile is delegated to the tracked data file's location.
    assertThat(dvFile.referencedDataFile()).isEqualTo(DATA_FILE_LOCATION);

    // fields delegated from TrackedFile / Tracking
    assertThat(dvFile.pos()).isEqualTo(MANIFEST_POS);
    assertThat(dvFile.specId()).isEqualTo(PARTITIONED_SPEC_ID);
    assertThat(dvFile.partition()).isSameAs(PARTITION);
    assertThat(dvFile.dataSequenceNumber()).isEqualTo(DATA_SEQUENCE_NUMBER);
    assertThat(dvFile.fileSequenceNumber()).isEqualTo(FILE_SEQUENCE_NUMBER);
    assertThat(dvFile.manifestLocation()).isEqualTo(MANIFEST_LOCATION);

    // fields that are null for DVs
    assertThat(dvFile.sortOrderId()).isNull();
    assertThat(dvFile.firstRowId()).isNull();
    assertThat(dvFile.keyMetadata()).isNull();
    assertThat(dvFile.splitOffsets()).isNull();
    assertThat(dvFile.equalityFieldIds()).isNull();
    assertThat(dvFile.columnSizes()).isNull();
    assertThat(dvFile.valueCounts()).isNull();
    assertThat(dvFile.nullValueCounts()).isNull();
    assertThat(dvFile.nanValueCounts()).isNull();
    assertThat(dvFile.lowerBounds()).isNull();
    assertThat(dvFile.upperBounds()).isNull();
  }

  @ParameterizedTest
  @EnumSource(value = FileContent.class, mode = EnumSource.Mode.EXCLUDE, names = "DATA")
  void testDVDeleteFileAdapterRejectsNonDataContent(FileContent contentType) {
    TrackedFileStruct file = dummyTrackedFile(contentType);

    assertThatThrownBy(() -> TrackedFileAdapters.asDVDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for DV delete file: %s", contentType);
  }

  @Test
  void testDVDeleteFileAdapterRejectsNullDeletionVector() {
    TrackedFileStruct file = dummyTrackedFile(FileContent.DATA);

    assertThatThrownBy(() -> TrackedFileAdapters.asDVDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create DV delete file: no deletion vector");
  }

  @Test
  void testNullContentStatsReturnsNullStats() {
    TrackedFileStruct file = dummyTrackedFile(FileContent.DATA);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, UNPARTITIONED);

    assertThat(dataFile.valueCounts()).isNull();
    assertThat(dataFile.nullValueCounts()).isNull();
    assertThat(dataFile.nanValueCounts()).isNull();
    assertThat(dataFile.lowerBounds()).isNull();
    assertThat(dataFile.upperBounds()).isNull();
  }

  @Test
  void testNullTrackingReturnsNullTrackingFields() {
    // Files read before manifest inheritance have no tracking; tracking-derived fields must be
    // null rather than throwing.
    assertNullTrackingFields(
        TrackedFileAdapters.asDataFile(dummyTrackedFile(FileContent.DATA), UNPARTITIONED));
    assertNullTrackingFields(
        TrackedFileAdapters.asEqualityDeleteFile(
            dummyTrackedFile(FileContent.EQUALITY_DELETES), UNPARTITIONED));

    TrackedFileStruct fileWithDV =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            0,
            null,
            null,
            null,
            0L,
            0L,
            null,
            null,
            null,
            DELETION_VECTOR,
            null,
            null,
            null,
            null);
    assertNullTrackingFields(TrackedFileAdapters.asDVDeleteFile(fileWithDV, UNPARTITIONED));
  }

  @Test
  void testUnpartitionedFilePartitionIsEmpty() {
    TrackedFileStruct file = dummyTrackedFile(FileContent.DATA);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, UNPARTITIONED);

    assertThat(dataFile.specId()).isEqualTo(UNPARTITIONED_SPEC_ID);
    assertThat(dataFile.partition()).isEqualTo(PartitionData.EMPTY);
  }

  @Test
  void testNullSpecIdResolvesToUnpartitionedSpec() {
    PartitionSpec unpartitioned = PartitionSpec.builderFor(new Schema()).withSpecId(5).build();
    TrackedFileStruct file = dummyTrackedFile(FileContent.DATA);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, specsById(unpartitioned));

    assertThat(dataFile.specId()).isEqualTo(5);
  }

  @Test
  void testNullSpecIdThrowsWhenNoUnpartitionedSpec() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    PartitionSpec partitioned = PartitionSpec.builderFor(schema).identity("id").build();
    TrackedFileStruct file = dummyTrackedFile(FileContent.DATA);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, specsById(partitioned)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find unpartitioned spec in specs");
  }

  @Test
  void testUnknownSpecIdThrows() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            0,
            null,
            null,
            null,
            0L,
            0L,
            99,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find partition spec for spec ID");
  }

  @Test
  void testSpecIdMismatchThrows() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            0,
            null,
            null,
            null,
            0L,
            0L,
            PARTITIONED_SPEC_ID,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    int mismatchedSpecId = PARTITIONED_SPEC_ID + 1;
    PartitionSpec mismatched =
        PartitionSpec.builderFor(PARTITION_SCHEMA)
            .identity("category")
            .withSpecId(mismatchedSpecId)
            .build();

    assertThatThrownBy(
            () ->
                TrackedFileAdapters.asDataFile(
                    file, ImmutableMap.of(PARTITIONED_SPEC_ID, mismatched)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "File spec ID %s does not match partition spec %s",
            PARTITIONED_SPEC_ID, mismatchedSpecId);
  }

  private static void assertNullTrackingFields(ContentFile<?> file) {
    assertThat(file.pos()).isNull();
    assertThat(file.manifestLocation()).isNull();
    assertThat(file.dataSequenceNumber()).isNull();
    assertThat(file.fileSequenceNumber()).isNull();
    assertThat(file.firstRowId()).isNull();
  }

  private static Map<Integer, PartitionSpec> specsById(PartitionSpec spec) {
    return ImmutableMap.of(spec.specId(), spec);
  }

  // Builds a partition tuple whose struct type matches PARTITIONED_SPEC.
  private static PartitionData partition(String category) {
    PartitionData partition = new PartitionData(PARTITIONED_SPEC.partitionType());
    partition.set(0, category);
    return partition;
  }

  /** Minimal file with no tracking, used by the rejection and null-tracking tests. */
  private static TrackedFileStruct dummyTrackedFile(FileContent contentType) {
    return new TrackedFileStruct(
        null,
        contentType,
        FORMAT_VERSION_V4,
        DATA_FILE_LOCATION,
        FileFormat.PARQUET,
        null,
        1L,
        1L,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#V4_AND_ABOVE")
  void testDataFileWrapperAdded(int formatVersion) {
    DataFile file = DATA_FILE;
    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(
            formatVersion, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    TrackedFile result = wrapper.wrap(file, ADDED_TRACKING);

    assertThat(result.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(result.tracking().snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(result.tracking().dataSequenceNumber()).isNull();
    assertThat(result.tracking().fileSequenceNumber()).isNull();
    assertThat(result.tracking().firstRowId()).isNull();
    assertWriteDataFields(result, file, formatVersion);
  }

  @Test
  void testDataFileWrapperAddedWithNullSnapshotId() {
    // Staged-write ADDED entry (FastAppend writes the manifest before the commit snapshot is
    // assigned). Snapshot ID is inherited at read time from the enclosing root manifest's
    // added_snapshot_id.
    DataFile file = DATA_FILE;
    Tracking tracking =
        new TrackingStruct(EntryStatus.ADDED, null, null, null, null, null, null, null);
    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    TrackedFile result = wrapper.wrap(file, tracking);

    assertThat(result.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(result.tracking().snapshotId()).isNull();
    assertWriteDataFields(result, file, FORMAT_VERSION_V4);
  }

  @Test
  void testDataFileWrapperExisting() {
    DataFile file = DATA_FILE;
    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    TrackedFile result = wrapper.wrap(file, EXISTING_TRACKING);

    assertThat(result.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(result.tracking().snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(result.tracking().dataSequenceNumber()).isEqualTo(DATA_SEQ);
    assertThat(result.tracking().fileSequenceNumber()).isEqualTo(FILE_SEQ);
    assertWriteDataFields(result, file, FORMAT_VERSION_V4);
  }

  @Test
  void testDataFileWrapperDeleted() {
    DataFile file = DATA_FILE;
    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    TrackedFile result = wrapper.wrap(file, DELETED_TRACKING);

    assertThat(result.tracking().status()).isEqualTo(EntryStatus.DELETED);
    assertThat(result.tracking().snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(result.tracking().dataSequenceNumber()).isEqualTo(DATA_SEQ);
    assertThat(result.tracking().fileSequenceNumber()).isEqualTo(FILE_SEQ);
  }

  @Test
  void testDataFileWrapperReuse() {
    // Same wrapper instance, two different files. The second wrap() call must not leak state from
    // the first: this matches how V4Writer will hold one wrapper and rebind per row.
    DataFile file1 = DATA_FILE;
    DataFile file2 =
        new GenericDataFile(
            UNPARTITIONED_SPEC.specId(),
            "s3://bucket/data/file2.parquet",
            FileFormat.PARQUET,
            EMPTY_PARTITION_DATA,
            2048L,
            new Metrics(200L, null, null, null, null),
            null,
            ImmutableList.of(0L),
            null,
            null);

    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);

    wrapper.wrap(file1, ADDED_TRACKING);
    assertThat(wrapper.location()).isEqualTo(DATA_PATH);
    assertThat(wrapper.recordCount()).isEqualTo(100L);

    wrapper.wrap(file2, EXISTING_TRACKING);
    assertThat(wrapper.location()).isEqualTo("s3://bucket/data/file2.parquet");
    assertThat(wrapper.recordCount()).isEqualTo(200L);
    assertThat(wrapper.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(wrapper.fileSizeInBytes()).isEqualTo(2048L);

    // No state leaks from file1 after rebinding to file2.
    assertThat(wrapper.location()).isNotEqualTo(file1.location());
    assertThat(wrapper.recordCount()).isNotEqualTo(file1.recordCount());
    assertThat(wrapper.fileSizeInBytes()).isNotEqualTo(file1.fileSizeInBytes());
    assertThat(wrapper.tracking().status()).isNotEqualTo(EntryStatus.ADDED);
  }

  @Test
  void testDataFileWrapperRejectsNullFile() {
    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    assertThatThrownBy(() -> wrapper.wrap(null, ADDED_TRACKING))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid file: null");
  }

  @Test
  void testDataFileWrapperRejectsNullTracking() {
    DataFile file = DATA_FILE;
    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    assertThatThrownBy(() -> wrapper.wrap(file, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid tracking: null");
  }

  @Test
  void testEqualityDeleteFileWrapper() {
    DeleteFile file = EQUALITY_DELETE_FILE;
    TrackedFileAdapters.EqualityDeleteTrackedFile wrapper =
        TrackedFileAdapters.forEqualityDeleteFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    TrackedFile result = wrapper.wrap(file, ADDED_TRACKING);

    assertThat(result.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(result.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(result.tracking().firstRowId()).isNull();
    assertThat(result.equalityIds()).containsExactly(1);
    assertThat(result.sortOrderId()).isNull();
    assertThat(result.location()).isEqualTo(DELETE_PATH);
  }

  @Test
  void testEqualityDeleteFileWrapperRejectsV3Dv() {
    // A deletion vector (in Puffin) is not an equality delete: in v4 it is carried
    // inline on its data file's TrackedFile row rather than as a delete manifest entry.
    DeleteFile dv =
        new GenericDeleteFile(
            UNPARTITIONED_SPEC.specId(),
            FileContent.POSITION_DELETES,
            DELETE_PATH,
            FileFormat.PUFFIN,
            EMPTY_PARTITION_DATA,
            512L,
            new Metrics(10L, null, null, null, null),
            null,
            null,
            null,
            null,
            DATA_PATH,
            0L,
            512L);
    TrackedFileAdapters.EqualityDeleteTrackedFile wrapper =
        TrackedFileAdapters.forEqualityDeleteFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    assertThatThrownBy(() -> wrapper.wrap(dv, ADDED_TRACKING))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid content for delete file: POSITION_DELETES");
  }

  @Test
  void testEqualityDeleteFileWrapperRejectsV2PositionDeletes() {
    // A v2 position delete file (in Parquet) is not an equality delete and has no v4 leaf
    // representation, so the equality-delete wrapper rejects it.
    DeleteFile positionDeletes =
        new GenericDeleteFile(
            UNPARTITIONED_SPEC.specId(),
            FileContent.POSITION_DELETES,
            DELETE_PATH,
            FileFormat.PARQUET,
            EMPTY_PARTITION_DATA,
            512L,
            new Metrics(10L, null, null, null, null),
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    TrackedFileAdapters.EqualityDeleteTrackedFile wrapper =
        TrackedFileAdapters.forEqualityDeleteFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    assertThatThrownBy(() -> wrapper.wrap(positionDeletes, ADDED_TRACKING))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid content for delete file: POSITION_DELETES");
  }

  @Test
  void testDataFileWrapperContentStats() {
    DataFile file = DATA_FILE_WITH_METRICS;
    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(
            FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE);
    TrackedFile result = wrapper.wrap(file, ADDED_TRACKING);

    ContentStats stats = result.contentStats();
    assertThat(stats).isNotNull();
    assertThat(stats.fieldStats()).extracting(FieldStats::fieldId).containsExactlyInAnyOrder(1, 2);

    FieldStats<?> idStats = stats.statsFor(1);
    assertThat(idStats.valueCount()).isEqualTo(100L);
    assertThat(idStats.lowerBound()).isEqualTo(1);
    assertThat(idStats.upperBound()).isEqualTo(1000);
  }

  @Test
  void testDataFileWrapperPartitionProjection() {
    // Multi-spec carry-over: the writer spec has one field but the union partition type carries an
    // additional field from another live spec. The wrapper must place the writer's value at the
    // field-ID position and leave the extra union field null.
    PartitionSpec writerSpec = PartitionSpec.builderFor(TABLE_SCHEMA).bucket("id", 16).build();
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

    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, unionType);
    TrackedFile result = wrapper.wrap(file, ADDED_TRACKING);

    StructLike projected = result.partition();
    int writerPos = unionType.fields().indexOf(writerField);
    int extraPos = unionType.fields().indexOf(extraField);
    assertThat(projected.get(writerPos, Integer.class)).isEqualTo(7);
    assertThat(projected.get(extraPos, Integer.class)).isNull();
  }

  @Test
  void testDataFileDoubleWrapRoundTrip() {
    // DataFile -> TrackedFile (write) -> DataFile (read) round-trip preserves fields observable on
    // the DataFile API.
    DataFile source = DATA_FILE_WITH_METRICS;
    Map<Integer, PartitionSpec> specs =
        ImmutableMap.of(UNPARTITIONED_SPEC.specId(), UNPARTITIONED_SPEC);

    TrackedFile tracked =
        TrackedFileAdapters.forDataFile(
                FORMAT_VERSION_V4, TABLE_SCHEMA, METRICS_CONFIG, PARTITION_TYPE)
            .wrap(source, EXISTING_TRACKING);

    DataFile roundTripped = TrackedFileAdapters.asDataFile(tracked, specs);

    assertThat(roundTripped.location()).isEqualTo(source.location());
    assertThat(roundTripped.format()).isEqualTo(source.format());
    assertThat(roundTripped.recordCount()).isEqualTo(source.recordCount());
    assertThat(roundTripped.fileSizeInBytes()).isEqualTo(source.fileSizeInBytes());
    assertThat(roundTripped.specId()).isEqualTo(source.specId());
    assertThat(roundTripped.dataSequenceNumber()).isEqualTo(DATA_SEQ);
    assertThat(roundTripped.fileSequenceNumber()).isEqualTo(FILE_SEQ);
    assertThat(roundTripped.valueCounts()).containsAllEntriesOf(source.valueCounts());
    assertThat(roundTripped.nullValueCounts()).containsAllEntriesOf(source.nullValueCounts());
    assertThat(roundTripped.lowerBounds()).containsAllEntriesOf(source.lowerBounds());
    assertThat(roundTripped.upperBounds()).containsAllEntriesOf(source.upperBounds());
  }

  @Test
  void testManifestReferenceWrapperForV4Manifest() {
    GenericManifestFile manifest = v4WriteManifestFile(ManifestContent.DATA, 4, 6L);
    TrackedFileAdapters.ManifestTrackedFile wrapper = TrackedFileAdapters.forManifestReference();
    TrackedFile result = wrapper.wrap(manifest, EntryStatus.ADDED, 1000L);

    assertThat(result.contentType()).isEqualTo(FileContent.DATA_MANIFEST);
    assertThat(result.formatVersion()).isEqualTo(4);
    assertThat(result.location()).isEqualTo(MANIFEST_PATH);
    assertThat(result.tracking().firstRowId()).isEqualTo(1000L);
    assertThat(result.recordCount()).isEqualTo(6L);
    assertThat(result.manifestInfo()).isNotNull();
    assertThat(result.manifestInfo().addedFilesCount()).isEqualTo(2);
    assertThat(result.manifestInfo().existingFilesCount()).isEqualTo(3);
    assertThat(result.manifestInfo().deletedFilesCount()).isEqualTo(1);
    assertThat(result.manifestInfo().addedRowsCount()).isEqualTo(200L);
    assertThat(result.manifestInfo().replacedFilesCount()).isEqualTo(0);
    assertThat(result.manifestInfo().replacedRowsCount()).isEqualTo(0L);
  }

  @Test
  void testManifestReferenceWrapperForPreV4() {
    // Pre-v4 manifest has formatVersion=LEGACY_FORMAT_VERSION by default; the wrapper sums
    // per-status counts.
    ManifestFile manifest = writeManifestFile(ManifestContent.DELETES);
    TrackedFileAdapters.ManifestTrackedFile wrapper = TrackedFileAdapters.forManifestReference();
    TrackedFile result = wrapper.wrap(manifest, EntryStatus.EXISTING, null);

    assertThat(result.contentType()).isEqualTo(FileContent.DELETE_MANIFEST);
    assertThat(result.formatVersion()).isEqualTo(ManifestFile.LEGACY_FORMAT_VERSION);
    assertThat(result.recordCount()).isEqualTo(6L);
    assertThat(result.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(result.tracking().firstRowId()).isNull();
  }

  @Test
  void testManifestReferenceWrapperRejectsFirstRowIdOnDeleteManifest() {
    ManifestFile manifest = writeManifestFile(ManifestContent.DELETES);
    TrackedFileAdapters.ManifestTrackedFile wrapper = TrackedFileAdapters.forManifestReference();
    assertThatThrownBy(() -> wrapper.wrap(manifest, EntryStatus.ADDED, 100L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("firstRowId is only valid for DATA manifests");
  }

  @Test
  void testManifestReferenceWrapperRejectsUnassignedSequenceNumber() {
    ManifestFile manifest =
        writeManifestFile(ManifestContent.DATA, ManifestWriter.UNASSIGNED_SEQ, 4L);
    TrackedFileAdapters.ManifestTrackedFile wrapper = TrackedFileAdapters.forManifestReference();
    assertThatThrownBy(() -> wrapper.wrap(manifest, EntryStatus.ADDED, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sequence_number is unassigned");
  }

  @Test
  void testManifestReferenceWrapperRejectsV4WithoutRecordCount() {
    GenericManifestFile manifest =
        v4WriteManifestFile(ManifestContent.DATA, 4, null /* recordCount */);
    TrackedFileAdapters.ManifestTrackedFile wrapper = TrackedFileAdapters.forManifestReference();
    assertThatThrownBy(() -> wrapper.wrap(manifest, EntryStatus.ADDED, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("record_count must be set");
  }

  private static void assertWriteDataFields(TrackedFile result, DataFile file, int formatVersion) {
    assertThat(result.contentType()).isEqualTo(FileContent.DATA);
    assertThat(result.formatVersion()).isEqualTo(formatVersion);
    assertThat(result.location()).isEqualTo(file.location());
    assertThat(result.fileFormat()).isEqualTo(file.format());
    assertThat(result.recordCount()).isEqualTo(file.recordCount());
    assertThat(result.fileSizeInBytes()).isEqualTo(file.fileSizeInBytes());
    assertThat(result.specId()).isEqualTo(file.specId());
    assertThat(result.splitOffsets()).isEqualTo(file.splitOffsets());
  }

  private static ManifestFile writeManifestFile(ManifestContent content) {
    return writeManifestFile(content, 5L, 4L);
  }

  private static ManifestFile writeManifestFile(
      ManifestContent content, long sequenceNumber, long minSequenceNumber) {
    List<ManifestFile.PartitionFieldSummary> partitions = ImmutableList.of();
    return new GenericManifestFile(
        MANIFEST_PATH,
        2048L,
        UNPARTITIONED_SPEC.specId(),
        content,
        sequenceNumber,
        minSequenceNumber,
        SNAPSHOT_ID,
        partitions,
        null,
        2,
        200L,
        3,
        300L,
        1,
        100L,
        null);
  }

  private static GenericManifestFile v4WriteManifestFile(
      ManifestContent content, int formatVersion, Long recordCount) {
    List<ManifestFile.PartitionFieldSummary> partitions = ImmutableList.of();
    return new GenericManifestFile(
        MANIFEST_PATH,
        2048L,
        UNPARTITIONED_SPEC.specId(),
        content,
        5L,
        4L,
        SNAPSHOT_ID,
        partitions,
        null,
        2,
        200L,
        3,
        300L,
        1,
        100L,
        null,
        recordCount,
        formatVersion);
  }
}
