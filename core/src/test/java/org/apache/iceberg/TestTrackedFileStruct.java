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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestTrackedFileStruct {
  private static final int FORMAT_VERSION_V4 = 4;
  private static final Types.StructType PARTITION_TYPE =
      Types.StructType.of(
          Types.NestedField.optional(1000, "id_bucket", Types.IntegerType.get()),
          Types.NestedField.optional(1001, "category", Types.StringType.get()));
  private static final ColumnFile COLUMN_FILE_1 =
      ColumnFileStruct.builder()
          .fieldIds(ImmutableList.of(1, 2))
          .location("s3://bucket/data/col-1.parquet")
          .fileSizeInBytes(256L)
          .build();
  private static final ColumnFile COLUMN_FILE_2 =
      ColumnFileStruct.builder()
          .fieldIds(ImmutableList.of(3))
          .location("s3://bucket/data/col-2.parquet")
          .fileSizeInBytes(128L)
          .build();

  // Ordinals looked up from the TrackedFile schema so tests don't hard-code positions.
  private static final List<Types.NestedField> SCHEMA_FIELDS =
      TrackedFile.schemaWithContentStats(Types.StructType.of(), Types.StructType.of()).fields();
  private static final int TRACKING_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.TRACKING);
  private static final int CONTENT_TYPE_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.CONTENT_TYPE);
  private static final int FORMAT_VERSION_ORDINAL =
      SCHEMA_FIELDS.indexOf(TrackedFile.FORMAT_VERSION);
  private static final int LOCATION_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.LOCATION);
  private static final int FILE_FORMAT_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.FILE_FORMAT);
  private static final int RECORD_COUNT_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.RECORD_COUNT);
  private static final int FILE_SIZE_IN_BYTES_ORDINAL =
      SCHEMA_FIELDS.indexOf(TrackedFile.FILE_SIZE_IN_BYTES);
  private static final int SPEC_ID_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.SPEC_ID);
  // partition and content_stats are rebuilt with the supplied struct types inside
  // schemaWithContentStats, so their ordinals are looked up by field ID.
  private static final int PARTITION_ORDINAL = ordinalOf(TrackedFile.PARTITION_ID);
  private static final int CONTENT_STATS_ORDINAL = ordinalOf(TrackedFile.CONTENT_STATS_ID);
  private static final int SORT_ORDER_ID_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.SORT_ORDER_ID);
  private static final int DELETION_VECTOR_ORDINAL =
      SCHEMA_FIELDS.indexOf(TrackedFile.DELETION_VECTOR);
  private static final int MANIFEST_INFO_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.MANIFEST_INFO);
  private static final int KEY_METADATA_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.KEY_METADATA);
  private static final int SPLIT_OFFSETS_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.SPLIT_OFFSETS);
  private static final int EQUALITY_IDS_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.EQUALITY_IDS);
  private static final int COLUMN_FILES_ORDINAL = SCHEMA_FIELDS.indexOf(TrackedFile.COLUMN_FILES);

  // Ordinal of MetadataColumns.ROW_POSITION within TrackingStruct's BASE_TYPE,
  // which appends ROW_POSITION after the Tracking schema fields.
  private static final int MANIFEST_POS_ORDINAL = Tracking.schema().fields().size();

  private static int ordinalOf(int fieldId) {
    for (int i = 0; i < SCHEMA_FIELDS.size(); i++) {
      if (SCHEMA_FIELDS.get(i).fieldId() == fieldId) {
        return i;
      }
    }
    throw new IllegalArgumentException("Field not found in TrackedFile schema: " + fieldId);
  }

  @Test
  void testFieldAccess() {
    TrackedFileStruct file = new TrackedFileStruct();
    Tracking tracking = TrackingBuilder.added(42L).build();
    DeletionVectorStruct dv =
        DeletionVectorStruct.builder()
            .location("s3://bucket/dv.puffin")
            .offset(100L)
            .sizeInBytes(50L)
            .cardinality(5L)
            .build();
    ManifestInfoStruct info =
        ManifestInfoStruct.builder()
            .addedFilesCount(10)
            .existingFilesCount(20)
            .deletedFilesCount(3)
            .replacedFilesCount(2)
            .addedRowsCount(1000L)
            .existingRowsCount(2000L)
            .deletedRowsCount(300L)
            .replacedRowsCount(200L)
            .minSequenceNumber(5L)
            .build();

    file.set(TRACKING_ORDINAL, tracking);
    file.set(CONTENT_TYPE_ORDINAL, FileContent.EQUALITY_DELETES.id());
    file.set(FORMAT_VERSION_ORDINAL, FORMAT_VERSION_V4);
    file.set(LOCATION_ORDINAL, "s3://bucket/data/eq-delete.avro");
    file.set(FILE_FORMAT_ORDINAL, "avro");
    file.set(RECORD_COUNT_ORDINAL, 50L);
    file.set(FILE_SIZE_IN_BYTES_ORDINAL, 512L);
    file.set(SPEC_ID_ORDINAL, 1);
    file.set(SORT_ORDER_ID_ORDINAL, 5);
    file.set(DELETION_VECTOR_ORDINAL, dv);
    file.set(MANIFEST_INFO_ORDINAL, info);
    file.set(KEY_METADATA_ORDINAL, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    file.set(SPLIT_OFFSETS_ORDINAL, ImmutableList.of(100L, 200L));
    file.set(EQUALITY_IDS_ORDINAL, ImmutableList.of(1, 2, 3));
    file.set(COLUMN_FILES_ORDINAL, ImmutableList.of(COLUMN_FILE_1, COLUMN_FILE_2));

    assertThat(file.tracking()).isNotNull();
    assertThat(file.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(file.tracking().snapshotId()).isEqualTo(42L);
    assertThat(file.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(file.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(file.location()).isEqualTo("s3://bucket/data/eq-delete.avro");
    assertThat(file.fileFormat()).isEqualTo(FileFormat.AVRO);
    assertThat(file.recordCount()).isEqualTo(50L);
    assertThat(file.fileSizeInBytes()).isEqualTo(512L);
    assertThat(file.specId()).isEqualTo(1);
    assertThat(file.sortOrderId()).isEqualTo(5);
    assertThat(file.deletionVector()).isSameAs(dv);
    assertThat(file.manifestInfo()).isSameAs(info);
    assertThat(file.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(file.splitOffsets()).containsExactly(100L, 200L);
    assertThat(file.equalityIds()).containsExactly(1, 2, 3);
    assertThat(file.columnFiles()).hasSize(2);
    verifyColumnFile(COLUMN_FILE_1, file.columnFiles().get(0));
    verifyColumnFile(COLUMN_FILE_2, file.columnFiles().get(1));

    // should return EMPTY_PARTITION_DATA
    assertThat(file.partition()).isNotNull();
    assertThat(file.partition().size()).isEqualTo(0);
  }

  @Test
  void testReaderSideFields() {
    TrackedFileStruct file = new TrackedFileStruct();

    TrackingStruct tracking = new TrackingStruct();
    tracking.setManifestLocation("s3://bucket/metadata/manifest.avro");
    tracking.set(MANIFEST_POS_ORDINAL, 7L);

    file.set(TRACKING_ORDINAL, tracking);
    file.set(CONTENT_TYPE_ORDINAL, FileContent.DATA.id());
    file.set(LOCATION_ORDINAL, "test");
    file.set(FILE_FORMAT_ORDINAL, "parquet");
    file.set(RECORD_COUNT_ORDINAL, 0L);
    file.set(FILE_SIZE_IN_BYTES_ORDINAL, 0L);

    assertThat(file.tracking().manifestLocation()).isEqualTo("s3://bucket/metadata/manifest.avro");
    assertThat(file.tracking().manifestPos()).isEqualTo(7L);
  }

  @Test
  void projectionWithoutPartition() {
    // project only location (field ID 100) and file_size_in_bytes (field ID 104)
    Types.StructType projection =
        Types.StructType.of(TrackedFile.LOCATION, TrackedFile.FILE_SIZE_IN_BYTES);

    TrackedFileStruct file = new TrackedFileStruct(projection);
    assertThat(file.size()).isEqualTo(2);
    // should return EMPTY_PARTITION_DATA
    assertThat(file.partition()).isNotNull();
    assertThat(file.partition().size()).isEqualTo(0);
  }

  @Test
  void partitionAccess() {
    PartitionData partition = newPartition(5, "books");

    TrackedFileStruct file = new TrackedFileStruct();
    file.set(PARTITION_ORDINAL, partition);

    assertThat(file.partition()).isSameAs(partition);
    assertThat(file.partition().get(0, Integer.class)).isEqualTo(5);
    assertThat(file.partition().get(1, String.class)).isEqualTo("books");
  }

  @Test
  void partitionIsCopied() {
    PartitionData partition = newPartition(5, "books");
    TrackedFileStruct file = createFullTrackedFile();
    file.set(PARTITION_ORDINAL, partition);

    TrackedFile copy = file.copy();

    assertThat(copy.partition()).isNotNull().isNotSameAs(partition);
    assertThat(copy.partition()).isEqualTo(partition);
    assertThat(copy.partition().get(0, Integer.class)).isEqualTo(5);
    assertThat(copy.partition().get(1, String.class)).isEqualTo("books");
  }

  @Test
  void testCopy() {
    TrackedFileStruct file = createFullTrackedFile();

    TrackedFile copy = file.copy();
    assertThat(copy).isInstanceOf(TrackedFileStruct.class);

    assertThat(copy.contentType()).isEqualTo(FileContent.DATA);
    assertThat(copy.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(copy.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(copy.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(copy.tracking().snapshotId()).isEqualTo(42L);
    assertThat(copy.deletionVector().location()).isEqualTo("s3://bucket/dv.puffin");
    assertThat(copy.specId()).isEqualTo(0);
    assertThat(copy.sortOrderId()).isEqualTo(1);
    assertThat(copy.recordCount()).isEqualTo(100L);
    assertThat(copy.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(copy.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(copy.keyMetadata()).isNotNull();
    assertThat(copy.splitOffsets()).containsExactly(50L);
    assertThat(copy.equalityIds()).isNull();
    assertThat(copy.tracking().manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(copy.tracking().manifestPos()).isEqualTo(3L);
    assertThat(copy.tracking().latestColumnFileSnapshotId()).isEqualTo(42L);
    assertThat(copy.partition()).isEqualTo(newPartition(7, "music"));
    assertThat(copy.columnFiles()).hasSize(2);
    assertThat(copy.columnFiles()).isNotSameAs(file.columnFiles());
    verifyColumnFile(COLUMN_FILE_1, copy.columnFiles().get(0));
    verifyColumnFile(COLUMN_FILE_2, copy.columnFiles().get(1));
  }

  @Test
  void testCopyWithNullColumnFile() {
    TrackedFileStruct file = createFullTrackedFile();
    file.set(COLUMN_FILES_ORDINAL, Arrays.asList(COLUMN_FILE_1, null, COLUMN_FILE_2));

    TrackedFile copy = file.copy();

    assertThat(copy.columnFiles()).hasSize(2);
    verifyColumnFile(COLUMN_FILE_1, copy.columnFiles().get(0));
    verifyColumnFile(COLUMN_FILE_2, copy.columnFiles().get(1));
  }

  @Test
  void testCopyWithoutStats() {
    TrackedFileStruct file = createTrackedFileWithStats();
    assertThat(file.contentStats()).isNotNull();

    TrackedFile copy = file.copyWithoutStats();

    assertThat(copy.contentType()).isEqualTo(FileContent.DATA);
    assertThat(copy.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(copy.contentStats()).isNull();
  }

  @Test
  void testCopyWithStatsFilters() {
    TrackedFileStruct file = createTrackedFileWithStats();
    Set<Integer> keepFieldIds = ImmutableSet.of(1);

    TrackedFile copy = file.copyWithStats(keepFieldIds);

    assertThat(copy.contentStats()).isNotNull();
    ContentStats stats = copy.contentStats();
    assertThat(stats.fieldStats()).hasSize(1);
    assertThat(stats.fieldStats().get(0).fieldId()).isEqualTo(1);
  }

  @Test
  void testCopyIsDeep() {
    TrackedFileStruct file = createFullTrackedFile();

    TrackedFile copy = file.copy();

    assertThat(copy.keyMetadata()).isNotSameAs(file.keyMetadata());
  }

  @Test
  void testStructLikeSize() {
    TrackedFileStruct file = new TrackedFileStruct();
    assertThat(file.size()).isEqualTo(17);
  }

  @Test
  void testStructLikeGetSet() {
    TrackedFileStruct file = new TrackedFileStruct();

    file.set(CONTENT_TYPE_ORDINAL, FileContent.DATA.id());
    assertThat(file.get(CONTENT_TYPE_ORDINAL, Integer.class)).isEqualTo(FileContent.DATA.id());

    file.set(LOCATION_ORDINAL, "test-location");
    assertThat(file.get(LOCATION_ORDINAL, String.class)).isEqualTo("test-location");

    file.set(RECORD_COUNT_ORDINAL, 999L);
    assertThat(file.get(RECORD_COUNT_ORDINAL, Long.class)).isEqualTo(999L);

    file.set(SORT_ORDER_ID_ORDINAL, 3);
    assertThat(file.get(SORT_ORDER_ID_ORDINAL, Integer.class)).isEqualTo(3);

    file.set(COLUMN_FILES_ORDINAL, ImmutableList.of(COLUMN_FILE_1, COLUMN_FILE_2));
    @SuppressWarnings("unchecked")
    List<ColumnFile> roundTrippedColumnFiles = file.get(COLUMN_FILES_ORDINAL, List.class);
    assertThat(roundTrippedColumnFiles).hasSize(2);
    verifyColumnFile(COLUMN_FILE_1, roundTrippedColumnFiles.get(0));
    verifyColumnFile(COLUMN_FILE_2, roundTrippedColumnFiles.get(1));
  }

  @Test
  void testProjectedStructLike() {
    // project only location (field ID 100) and file_size_in_bytes (field ID 104)
    Types.StructType projection =
        Types.StructType.of(TrackedFile.LOCATION, TrackedFile.FILE_SIZE_IN_BYTES);

    TrackedFileStruct file = new TrackedFileStruct(projection);
    assertThat(file.size()).isEqualTo(2);

    // projected position 0 maps to internal position 2 (location)
    // projected position 1 maps to internal position 5 (file_size_in_bytes)
    file.set(0, "s3://bucket/file.parquet");
    file.set(1, 1024L);

    assertThat(file.location()).isEqualTo("s3://bucket/file.parquet");
    assertThat(file.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(file.get(0, String.class)).isEqualTo("s3://bucket/file.parquet");
    assertThat(file.get(1, Long.class)).isEqualTo(1024L);
  }

  @Test
  void testContentStatsReturnedWhenPresent() {
    TrackedFileStruct file = createTrackedFileWithStats();
    assertThat(file.contentStats()).isNotNull();
    assertThat(file.contentStats().fieldStats()).hasSize(2);
  }

  @Test
  void testContentStatsNullWhenNotSet() {
    TrackedFileStruct file = new TrackedFileStruct();
    file.set(CONTENT_TYPE_ORDINAL, FileContent.DATA.id());
    file.set(LOCATION_ORDINAL, "test");
    file.set(FILE_FORMAT_ORDINAL, "parquet");
    file.set(RECORD_COUNT_ORDINAL, 0L);
    file.set(FILE_SIZE_IN_BYTES_ORDINAL, 0L);
    file.set(SPEC_ID_ORDINAL, 0);

    assertThat(file.contentStats()).isNull();
  }

  @Test
  void testAllFileContentTypesSupported() {
    for (FileContent content : FileContent.values()) {
      TrackedFileStruct file = new TrackedFileStruct();
      file.set(CONTENT_TYPE_ORDINAL, content.id());
      assertThat(file.contentType()).isEqualTo(content);
    }
  }

  @Test
  void testJavaSerializationRoundTrip() throws IOException, ClassNotFoundException {
    TrackedFileStruct file = createFullTrackedFile();

    TrackedFileStruct deserialized = TestHelpers.roundTripSerialize(file);

    assertThat(deserialized.contentType()).isEqualTo(FileContent.DATA);
    assertThat(deserialized.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(deserialized.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(deserialized.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(deserialized.recordCount()).isEqualTo(100L);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(deserialized.specId()).isEqualTo(0);
    assertThat(deserialized.sortOrderId()).isEqualTo(1);
    assertThat(deserialized.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(deserialized.tracking().snapshotId()).isEqualTo(42L);
    assertThat(deserialized.deletionVector().location()).isEqualTo("s3://bucket/dv.puffin");
    assertThat(deserialized.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(deserialized.splitOffsets()).containsExactly(50L);
    assertThat(deserialized.tracking().manifestPos()).isEqualTo(3L);
    assertThat(deserialized.tracking().manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(deserialized.tracking().latestColumnFileSnapshotId()).isEqualTo(42L);
    assertThat(deserialized.partition()).isEqualTo(newPartition(7, "music"));
    assertThat(deserialized.columnFiles()).hasSize(2);
    verifyColumnFile(COLUMN_FILE_1, deserialized.columnFiles().get(0));
    verifyColumnFile(COLUMN_FILE_2, deserialized.columnFiles().get(1));
  }

  @Test
  void testKryoSerializationRoundTrip() throws IOException {
    TrackedFileStruct file = createFullTrackedFile();

    TrackedFileStruct deserialized = TestHelpers.KryoHelpers.roundTripSerialize(file);

    assertThat(deserialized.contentType()).isEqualTo(FileContent.DATA);
    assertThat(deserialized.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(deserialized.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(deserialized.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(deserialized.recordCount()).isEqualTo(100L);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(deserialized.specId()).isEqualTo(0);
    assertThat(deserialized.sortOrderId()).isEqualTo(1);
    assertThat(deserialized.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(deserialized.tracking().snapshotId()).isEqualTo(42L);
    assertThat(deserialized.deletionVector().location()).isEqualTo("s3://bucket/dv.puffin");
    assertThat(deserialized.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(deserialized.splitOffsets()).containsExactly(50L);
    assertThat(deserialized.tracking().manifestPos()).isEqualTo(3L);
    assertThat(deserialized.tracking().manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(deserialized.tracking().latestColumnFileSnapshotId()).isEqualTo(42L);
    assertThat(deserialized.partition()).isEqualTo(newPartition(7, "music"));
    assertThat(deserialized.columnFiles()).hasSize(2);
    verifyColumnFile(COLUMN_FILE_1, deserialized.columnFiles().get(0));
    verifyColumnFile(COLUMN_FILE_2, deserialized.columnFiles().get(1));
  }

  static TrackedFileStruct createFullTrackedFile() {
    DeletionVectorStruct dv =
        DeletionVectorStruct.builder()
            .location("s3://bucket/dv.puffin")
            .offset(100L)
            .sizeInBytes(50L)
            .cardinality(5L)
            .build();

    Tracking tracking = TrackingBuilder.added(42L).columnFilesUpdated().build();

    TrackedFileStruct file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            newPartition(7, "music"),
            100L,
            1024L,
            0,
            null,
            1,
            dv,
            null,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(50L),
            null,
            ImmutableList.of(COLUMN_FILE_1, COLUMN_FILE_2));

    TrackingStruct trackingStruct = (TrackingStruct) file.tracking();
    trackingStruct.setManifestLocation("s3://bucket/manifest.avro");
    trackingStruct.set(MANIFEST_POS_ORDINAL, 3L);

    return file;
  }

  private static PartitionData newPartition(int idBucket, String category) {
    PartitionData partition = new PartitionData(PARTITION_TYPE);
    partition.set(0, idBucket);
    partition.set(1, category);
    return partition;
  }

  static TrackedFileStruct createTrackedFileWithStats() {
    Types.StructType statsStruct =
        Types.StructType.of(
            Types.NestedField.optional(
                10000,
                "1",
                Types.StructType.of(
                    Types.NestedField.optional(10001, "lower_bound", Types.IntegerType.get()),
                    Types.NestedField.optional(10002, "upper_bound", Types.IntegerType.get()),
                    Types.NestedField.optional(10004, "value_count", Types.LongType.get()),
                    Types.NestedField.optional(10005, "null_value_count", Types.LongType.get()),
                    Types.NestedField.optional(10006, "nan_value_count", Types.LongType.get()))),
            Types.NestedField.optional(
                20000,
                "2",
                Types.StructType.of(
                    Types.NestedField.optional(20001, "lower_bound", Types.FloatType.get()),
                    Types.NestedField.optional(20002, "upper_bound", Types.FloatType.get()),
                    Types.NestedField.optional(20004, "value_count", Types.LongType.get()),
                    Types.NestedField.optional(20005, "null_value_count", Types.LongType.get()),
                    Types.NestedField.optional(20006, "nan_value_count", Types.LongType.get()))));

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

    BaseContentStats stats =
        BaseContentStats.builder()
            .withStatsStruct(statsStruct)
            .withFieldStats(fieldStatsList)
            .build();

    Tracking tracking = TrackingBuilder.added(0L).columnFilesUpdated().build();

    return new TrackedFileStruct(
        tracking,
        FileContent.DATA,
        FORMAT_VERSION_V4,
        "s3://bucket/data/file.parquet",
        FileFormat.PARQUET,
        new PartitionData(Types.StructType.of()),
        100L,
        1024L,
        0,
        stats,
        null,
        null,
        null,
        null,
        null,
        null,
        ImmutableList.of(COLUMN_FILE_1, COLUMN_FILE_2));
  }

  private static void verifyColumnFile(ColumnFile expected, ColumnFile actual) {
    assertThat(actual.fieldIds()).containsExactlyElementsOf(expected.fieldIds());
    assertThat(actual.location()).isEqualTo(expected.location());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
  }
}
