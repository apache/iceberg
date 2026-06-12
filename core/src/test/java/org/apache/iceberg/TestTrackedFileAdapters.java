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

class TestTrackedFileAdapters {

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

  // Passed for unpartitioned test files, where there is no partition tuple.
  private static final PartitionData NO_PARTITION = null;

  // Tracking field ordinals, looked up from the schema so the tests do not hard-code offsets.
  private static final int STATUS_ORDINAL = ordinalOf(Tracking.schema(), "status");
  private static final int SNAPSHOT_ID_ORDINAL = ordinalOf(Tracking.schema(), "snapshot_id");
  private static final int DATA_SEQUENCE_NUMBER_ORDINAL =
      ordinalOf(Tracking.schema(), "sequence_number");
  private static final int FILE_SEQUENCE_NUMBER_ORDINAL =
      ordinalOf(Tracking.schema(), "file_sequence_number");
  private static final int FIRST_ROW_ID_ORDINAL = ordinalOf(Tracking.schema(), "first_row_id");
  // manifestPos is appended after the tracking schema fields by the manifest reader.
  private static final int MANIFEST_POS_ORDINAL = Tracking.schema().fields().size();

  // TrackedFile optional field ordinals, looked up from the schema.
  private static final Types.StructType TRACKED_FILE_SCHEMA =
      TrackedFile.schemaWithContentStats(Types.StructType.of(), Types.StructType.of());
  private static final int SPEC_ID_ORDINAL = ordinalOf(TRACKED_FILE_SCHEMA, "spec_id");
  private static final int CONTENT_STATS_ORDINAL = ordinalOf(TRACKED_FILE_SCHEMA, "content_stats");
  private static final int SORT_ORDER_ID_ORDINAL = ordinalOf(TRACKED_FILE_SCHEMA, "sort_order_id");
  private static final int DELETION_VECTOR_ORDINAL =
      ordinalOf(TRACKED_FILE_SCHEMA, "deletion_vector");
  private static final int KEY_METADATA_ORDINAL = ordinalOf(TRACKED_FILE_SCHEMA, "key_metadata");
  private static final int SPLIT_OFFSETS_ORDINAL = ordinalOf(TRACKED_FILE_SCHEMA, "split_offsets");
  private static final int EQUALITY_IDS_ORDINAL = ordinalOf(TRACKED_FILE_SCHEMA, "equality_ids");

  @Test
  void testDataFileAdapterDelegation() {
    PartitionData partition = partition("books");

    TrackedFileStruct file =
        new TrackedFileStruct(
            createTracking(),
            FileContent.DATA,
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            partition,
            100L,
            1024L);
    file.set(SPEC_ID_ORDINAL, PARTITIONED_SPEC_ID);
    file.set(CONTENT_STATS_ORDINAL, createContentStats());
    file.set(SORT_ORDER_ID_ORDINAL, 3);
    file.set(KEY_METADATA_ORDINAL, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    file.set(SPLIT_OFFSETS_ORDINAL, ImmutableList.of(50L, 100L));

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, specsById(PARTITIONED_SPEC));

    assertThat(dataFile.pos()).isEqualTo(MANIFEST_POS);
    assertThat(dataFile.specId()).isEqualTo(PARTITIONED_SPEC_ID);
    assertThat(dataFile.partition()).isSameAs(partition);
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
    assertThat(dataFile.valueCounts()).containsOnly(Map.entry(1, 100L), Map.entry(2, 200L));
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
    TrackedFileStruct file = trackedFile(contentType);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for DataFile: %s", contentType);
  }

  @Test
  void testEqualityDeleteFileAdapterDelegation() {
    PartitionData partition = partition("books");

    TrackedFileStruct file =
        new TrackedFileStruct(
            createTracking(),
            FileContent.EQUALITY_DELETES,
            "s3://bucket/eq-delete.avro",
            FileFormat.AVRO,
            partition,
            50L,
            512L);
    file.set(SPEC_ID_ORDINAL, PARTITIONED_SPEC_ID);
    file.set(CONTENT_STATS_ORDINAL, createContentStats());
    file.set(SORT_ORDER_ID_ORDINAL, 5);
    file.set(KEY_METADATA_ORDINAL, ByteBuffer.wrap(new byte[] {4, 5}));
    file.set(SPLIT_OFFSETS_ORDINAL, ImmutableList.of(200L));
    file.set(EQUALITY_IDS_ORDINAL, ImmutableList.of(1, 2, 3));

    DeleteFile deleteFile =
        TrackedFileAdapters.asEqualityDeleteFile(file, specsById(PARTITIONED_SPEC));

    assertThat(deleteFile.pos()).isEqualTo(MANIFEST_POS);
    assertThat(deleteFile.specId()).isEqualTo(PARTITIONED_SPEC_ID);
    assertThat(deleteFile.partition()).isSameAs(partition);
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
    assertThat(deleteFile.valueCounts()).containsOnly(Map.entry(1, 100L), Map.entry(2, 200L));
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
    TrackedFileStruct file = trackedFile(contentType);

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

    PartitionData partition = partition("books");
    TrackedFileStruct file =
        new TrackedFileStruct(
            createTracking(),
            FileContent.DATA,
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            partition,
            100L,
            1024L);
    file.set(SPEC_ID_ORDINAL, PARTITIONED_SPEC_ID);
    file.set(DELETION_VECTOR_ORDINAL, dv);

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
    assertThat(dvFile.partition()).isSameAs(partition);
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
    TrackedFileStruct file = trackedFile(contentType);

    assertThatThrownBy(() -> TrackedFileAdapters.asDVDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for DV delete file: %s", contentType);
  }

  @Test
  void testDVDeleteFileAdapterRejectsNullDeletionVector() {
    TrackedFileStruct file = trackedFile(FileContent.DATA);

    assertThatThrownBy(() -> TrackedFileAdapters.asDVDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create DV delete file: no deletion vector");
  }

  @Test
  void testNullContentStatsReturnsNullStats() {
    TrackedFileStruct file = trackedFile(FileContent.DATA);

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
        TrackedFileAdapters.asDataFile(trackedFile(FileContent.DATA), UNPARTITIONED));
    assertNullTrackingFields(
        TrackedFileAdapters.asEqualityDeleteFile(
            trackedFile(FileContent.EQUALITY_DELETES), UNPARTITIONED));

    TrackedFileStruct dvFile = trackedFile(FileContent.DATA);
    dvFile.set(DELETION_VECTOR_ORDINAL, deletionVector());
    assertNullTrackingFields(TrackedFileAdapters.asDVDeleteFile(dvFile, UNPARTITIONED));
  }

  @Test
  void testUnpartitionedFilePartitionIsEmpty() {
    TrackedFileStruct file = trackedFile(FileContent.DATA);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, UNPARTITIONED);

    assertThat(dataFile.specId()).isEqualTo(UNPARTITIONED_SPEC_ID);
    assertThat(dataFile.partition().size()).isEqualTo(0);
  }

  @Test
  void testNullSpecIdResolvesToUnpartitionedSpec() {
    PartitionSpec unpartitioned = PartitionSpec.builderFor(new Schema()).withSpecId(5).build();
    TrackedFileStruct file = trackedFile(FileContent.DATA);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, specsById(unpartitioned));

    assertThat(dataFile.specId()).isEqualTo(5);
  }

  @Test
  void testNullSpecIdThrowsWhenNoUnpartitionedSpec() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    PartitionSpec partitioned = PartitionSpec.builderFor(schema).identity("id").build();
    TrackedFileStruct file = trackedFile(FileContent.DATA);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, specsById(partitioned)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find unpartitioned spec in specs");
  }

  @Test
  void testUnknownSpecIdThrows() {
    TrackedFileStruct file = trackedFile(FileContent.DATA);
    file.set(SPEC_ID_ORDINAL, 99);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find partition spec for spec ID");
  }

  @Test
  void testSpecIdMismatchThrows() {
    int mismatchedSpecId = PARTITIONED_SPEC_ID + 1;
    TrackedFileStruct file = trackedFile(FileContent.DATA);
    file.set(SPEC_ID_ORDINAL, PARTITIONED_SPEC_ID);
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
  private static TrackedFileStruct trackedFile(FileContent contentType) {
    return new TrackedFileStruct(
        null, contentType, "s3://bucket/file", FileFormat.PARQUET, NO_PARTITION, 1L, 1L);
  }

  private static TrackingStruct createTracking() {
    TrackingStruct tracking = new TrackingStruct();
    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());
    tracking.set(SNAPSHOT_ID_ORDINAL, 42L);
    tracking.set(DATA_SEQUENCE_NUMBER_ORDINAL, DATA_SEQUENCE_NUMBER);
    tracking.set(FILE_SEQUENCE_NUMBER_ORDINAL, FILE_SEQUENCE_NUMBER);
    tracking.set(FIRST_ROW_ID_ORDINAL, FIRST_ROW_ID);
    tracking.setManifestLocation(MANIFEST_LOCATION);
    tracking.set(MANIFEST_POS_ORDINAL, MANIFEST_POS);
    return tracking;
  }

  private static DeletionVector deletionVector() {
    return DeletionVectorStruct.builder()
        .location(DV_LOCATION)
        .offset(128L)
        .sizeInBytes(256L)
        .cardinality(10L)
        .build();
  }

  private static ContentStats createContentStats() {
    Types.StructType statsStruct =
        Types.StructType.of(
            Types.NestedField.optional(
                10000,
                "1",
                Types.StructType.of(
                    Types.NestedField.optional(10001, "value_count", Types.LongType.get()),
                    Types.NestedField.optional(10002, "null_value_count", Types.LongType.get()),
                    Types.NestedField.optional(10003, "nan_value_count", Types.LongType.get()),
                    Types.NestedField.optional(10006, "lower_bound", Types.IntegerType.get()),
                    Types.NestedField.optional(10007, "upper_bound", Types.IntegerType.get()))),
            Types.NestedField.optional(
                20000,
                "2",
                Types.StructType.of(
                    Types.NestedField.optional(20001, "value_count", Types.LongType.get()),
                    Types.NestedField.optional(20002, "null_value_count", Types.LongType.get()),
                    Types.NestedField.optional(20003, "nan_value_count", Types.LongType.get()),
                    Types.NestedField.optional(20006, "lower_bound", Types.FloatType.get()),
                    Types.NestedField.optional(20007, "upper_bound", Types.FloatType.get()))));

    List<FieldStats<?>> fieldStatsList =
        ImmutableList.of(
            BaseFieldStats.<Integer>builder()
                .fieldId(1)
                .type(Types.IntegerType.get())
                .valueCount(100L)
                .nullValueCount(5L)
                .lowerBound(1)
                .upperBound(1000)
                .build(),
            BaseFieldStats.<Float>builder()
                .fieldId(2)
                .type(Types.FloatType.get())
                .valueCount(200L)
                .nullValueCount(10L)
                .nanValueCount(3L)
                .lowerBound(1.0f)
                .upperBound(100.0f)
                .build());

    return BaseContentStats.builder()
        .withStatsStruct(statsStruct)
        .withFieldStats(fieldStatsList)
        .build();
  }

  private static int ordinalOf(Types.StructType schema, String fieldName) {
    List<Types.NestedField> fields = schema.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      if (fields.get(i).name().equals(fieldName)) {
        return i;
      }
    }

    throw new IllegalArgumentException("No such field: " + fieldName);
  }
}
