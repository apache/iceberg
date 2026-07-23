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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

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

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "score", Types.FloatType.get()));
  private static final Types.StructType CONTENT_STATS_TYPE =
      StatsUtil.statsReadSchema(TABLE_SCHEMA, ImmutableList.of(1, 2));
  private static final FieldStats<?> ID_STATS =
      mockFieldStats(CONTENT_STATS_TYPE.fieldType("id").asStructType(), 1, 1, 1000, 100L, 5L, null);
  private static final FieldStats<?> SCORE_STATS =
      mockFieldStats(
          CONTENT_STATS_TYPE.fieldType("score").asStructType(), 2, 1.0f, 100.0f, 100L, 10L, 3L);
  private static final ContentStatsStruct CONTENT_STATS =
      new ContentStatsStruct(CONTENT_STATS_TYPE);

  static {
    CONTENT_STATS.setStats(1, ID_STATS);
    CONTENT_STATS.setStats(2, SCORE_STATS);
  }

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
  void testMetricTrackedByNoColumnReturnsNull() {
    // an integer-only schema: no column tracks nan_value_count, so the map is null (legacy
    // contract)
    Schema schema = new Schema(optional(1, "id", Types.IntegerType.get()));
    Types.StructType statsType = StatsUtil.statsReadSchema(schema, ImmutableList.of(1));
    ContentStatsStruct stats = new ContentStatsStruct(statsType);
    stats.setStats(
        1, mockFieldStats(statsType.fieldType("id").asStructType(), 1, 1, 1000, 100L, 5L, null));

    DataFile dataFile = TrackedFileAdapters.asDataFile(dataFileWithStats(stats), UNPARTITIONED);

    assertThat(dataFile.nanValueCounts()).isNull();
    assertThat(dataFile.valueCounts()).containsOnly(Map.entry(1, 100L));
    assertThat(dataFile.nullValueCounts()).containsOnly(Map.entry(1, 5L));
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
            deletionVector(),
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

  /** A DATA file carrying the given content stats and no tracking, for the stat-map tests. */
  private static TrackedFileStruct dataFileWithStats(ContentStats contentStats) {
    return new TrackedFileStruct(
        null,
        FileContent.DATA,
        FORMAT_VERSION_V4,
        DATA_FILE_LOCATION,
        FileFormat.PARQUET,
        null,
        1L,
        1L,
        null,
        contentStats,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static DeletionVector deletionVector() {
    return DeletionVectorStruct.builder()
        .location(DV_LOCATION)
        .offset(128L)
        .sizeInBytes(256L)
        .cardinality(10L)
        .build();
  }

  // A mock FieldStats presenting a column's stats through the interface, as a reader would after
  // deserialization. A null count reports absent via has*(); type() returns the real per-column
  // struct so lower/upper bounds decode against the right types.
  @SuppressWarnings("unchecked")
  private static FieldStats<Object> mockFieldStats(
      Types.StructType type,
      int id,
      Object lower,
      Object upper,
      Long valueCount,
      Long nullCount,
      Long nanCount) {
    FieldStats<Object> stats = Mockito.mock(FieldStats.class);
    Mockito.when(stats.fieldId()).thenReturn(id);
    Mockito.when(stats.type()).thenReturn(type);
    Mockito.when(stats.lowerBound()).thenReturn(lower);
    Mockito.when(stats.upperBound()).thenReturn(upper);
    Mockito.when(stats.hasValueCount()).thenReturn(valueCount != null);
    Mockito.when(stats.hasNullValueCount()).thenReturn(nullCount != null);
    Mockito.when(stats.hasNanValueCount()).thenReturn(nanCount != null);
    if (valueCount != null) {
      Mockito.when(stats.valueCount()).thenReturn(valueCount);
    }

    if (nullCount != null) {
      Mockito.when(stats.nullValueCount()).thenReturn(nullCount);
    }

    if (nanCount != null) {
      Mockito.when(stats.nanValueCount()).thenReturn(nanCount);
    }

    return stats;
  }
}
