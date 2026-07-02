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

  private static final DeletionVectorStruct DELETION_VECTOR =
      DeletionVectorStruct.builder()
          .location("s3://bucket/dv.puffin")
          .offset(100L)
          .sizeInBytes(50L)
          .cardinality(5L)
          .build();

  private static final ManifestInfoStruct MANIFEST_INFO =
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

  private static final ContentStats CONTENT_STATS =
      BaseContentStats.builder()
          .withTableSchema(
              new Schema(
                  Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                  Types.NestedField.optional(2, "data", Types.FloatType.get())))
          .withFieldStats(BaseFieldStats.builder().fieldId(1).build())
          .withFieldStats(BaseFieldStats.builder().fieldId(2).build())
          .build();

  @Test
  void testFieldAccess() {
    Tracking tracking =
        new TrackingStruct(EntryStatus.ADDED, 42L, null, null, null, null, null, null, null, -1L);

    TrackedFileStruct file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/00000-0-file.parquet",
            FileFormat.PARQUET,
            null,
            50L,
            512L,
            1,
            null,
            5,
            DELETION_VECTOR,
            MANIFEST_INFO,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(100L, 200L),
            ImmutableList.of(1, 2, 3));

    assertThat(file.tracking()).isSameAs(tracking);
    assertThat(file.contentType()).isEqualTo(FileContent.DATA);
    assertThat(file.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(file.location()).isEqualTo("s3://bucket/data/00000-0-file.parquet");
    assertThat(file.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(file.recordCount()).isEqualTo(50L);
    assertThat(file.fileSizeInBytes()).isEqualTo(512L);
    assertThat(file.specId()).isEqualTo(1);
    assertThat(file.sortOrderId()).isEqualTo(5);
    assertThat(file.deletionVector()).isSameAs(DELETION_VECTOR);
    assertThat(file.manifestInfo()).isSameAs(MANIFEST_INFO);
    assertThat(file.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(file.splitOffsets()).containsExactly(100L, 200L);
    assertThat(file.equalityIds()).containsExactly(1, 2, 3);
    // should return EMPTY_PARTITION_DATA
    assertThat(file.partition()).isNotNull();
    assertThat(file.partition().size()).isEqualTo(0);
  }

  @Test
  void testSetByPosition() {
    Tracking tracking =
        new TrackingStruct(EntryStatus.ADDED, 42L, null, null, null, null, null, null, null, -1L);
    PartitionData partition = newPartition(7, "music");

    TrackedFileStruct file = new TrackedFileStruct();
    file.set(0, tracking);
    file.set(1, FileContent.DATA.id());
    file.set(2, FORMAT_VERSION_V4);
    file.set(3, "s3://bucket/data/00000-0-file.parquet");
    file.set(4, "parquet");
    file.set(5, 50L);
    file.set(6, 512L);
    file.set(7, 1);
    file.set(8, partition);
    file.set(9, CONTENT_STATS);
    file.set(10, 5);
    file.set(11, DELETION_VECTOR);
    file.set(12, MANIFEST_INFO);
    file.set(13, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    file.set(14, ImmutableList.of(100L, 200L));
    file.set(15, ImmutableList.of(1, 2, 3));

    assertThat(file.tracking()).isSameAs(tracking);
    assertThat(file.contentType()).isEqualTo(FileContent.DATA);
    assertThat(file.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(file.location()).isEqualTo("s3://bucket/data/00000-0-file.parquet");
    assertThat(file.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(file.recordCount()).isEqualTo(50L);
    assertThat(file.fileSizeInBytes()).isEqualTo(512L);
    assertThat(file.specId()).isEqualTo(1);
    assertThat(file.partition()).isSameAs(partition);
    assertThat(file.contentStats()).isSameAs(CONTENT_STATS);
    assertThat(file.sortOrderId()).isEqualTo(5);
    assertThat(file.deletionVector()).isSameAs(DELETION_VECTOR);
    assertThat(file.manifestInfo()).isSameAs(MANIFEST_INFO);
    assertThat(file.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(file.splitOffsets()).containsExactly(100L, 200L);
    assertThat(file.equalityIds()).containsExactly(1, 2, 3);
  }

  @Test
  void testGetByPosition() {
    Tracking tracking =
        new TrackingStruct(EntryStatus.ADDED, 42L, null, null, null, null, null, null, null, -1L);
    PartitionData partition = newPartition(7, "music");

    TrackedFileStruct file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/00000-0-file.parquet",
            FileFormat.PARQUET,
            partition,
            50L,
            512L,
            1,
            CONTENT_STATS,
            5,
            DELETION_VECTOR,
            MANIFEST_INFO,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(100L, 200L),
            ImmutableList.of(1, 2, 3));

    assertThat(file.get(0, Tracking.class)).isSameAs(tracking);
    assertThat(file.get(1, Integer.class)).isEqualTo(FileContent.DATA.id());
    assertThat(file.get(2, Integer.class)).isEqualTo(FORMAT_VERSION_V4);
    assertThat(file.get(3, String.class)).isEqualTo("s3://bucket/data/00000-0-file.parquet");
    assertThat(file.get(4, String.class)).isEqualTo(FileFormat.PARQUET.toString());
    assertThat(file.get(5, Long.class)).isEqualTo(50L);
    assertThat(file.get(6, Long.class)).isEqualTo(512L);
    assertThat(file.get(7, Integer.class)).isEqualTo(1);
    assertThat(file.get(8, PartitionData.class)).isSameAs(partition);
    assertThat(file.get(9, ContentStats.class)).isSameAs(CONTENT_STATS);
    assertThat(file.get(10, Integer.class)).isEqualTo(5);
    assertThat(file.get(11, DeletionVector.class)).isSameAs(DELETION_VECTOR);
    assertThat(file.get(12, ManifestInfo.class)).isSameAs(MANIFEST_INFO);
    assertThat(file.get(13, ByteBuffer.class)).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(file.get(14, List.class)).containsExactly(100L, 200L);
    assertThat(file.get(15, List.class)).containsExactly(1, 2, 3);
  }

  @Test
  void testReaderSideFields() {
    // manifest_location and manifest_pos are populated by readers, not written to manifests.
    // manifest_pos is ROW_POSITION, appended after the tracking schema fields (position 8).
    TrackingStruct tracking = new TrackingStruct();
    tracking.setManifestLocation("s3://bucket/metadata/manifest.avro");
    tracking.set(8, 7L);

    TrackedFileStruct file = new TrackedFileStruct();
    file.set(0, tracking);
    file.set(1, FileContent.DATA.id());
    file.set(3, "test");
    file.set(4, "parquet");
    file.set(5, 0L);
    file.set(6, 0L);

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
    file.set(8, partition);

    assertThat(file.partition()).isSameAs(partition);
    assertThat(file.partition().get(0, Integer.class)).isEqualTo(5);
    assertThat(file.partition().get(1, String.class)).isEqualTo("books");
  }

  @Test
  void partitionIsCopied() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            newPartition(7, "music"),
            100L,
            1024L,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    TrackedFile copy = file.copy();

    assertThat(copy.partition()).isNotNull().isNotSameAs(file.partition());
    assertThat(copy.partition()).isEqualTo(newPartition(7, "music"));
    assertThat(copy.partition().get(0, Integer.class)).isEqualTo(7);
    assertThat(copy.partition().get(1, String.class)).isEqualTo("music");
  }

  @Test
  void testCopy() {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            42L,
            null,
            null,
            null,
            null,
            null,
            null,
            "s3://bucket/manifest.avro",
            3L);
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
            7,
            null,
            1,
            DELETION_VECTOR,
            null,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(50L),
            null);

    TrackedFile copy = file.copy();
    assertThat(copy).isInstanceOf(TrackedFileStruct.class);

    assertThat(copy.contentType()).isEqualTo(FileContent.DATA);
    assertThat(copy.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(copy.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(copy.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(copy.tracking().snapshotId()).isEqualTo(42L);
    assertThat(copy.deletionVector().location()).isEqualTo("s3://bucket/dv.puffin");
    assertThat(copy.specId()).isEqualTo(7);
    assertThat(copy.sortOrderId()).isEqualTo(1);
    assertThat(copy.recordCount()).isEqualTo(100L);
    assertThat(copy.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(copy.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(copy.keyMetadata()).isNotNull();
    assertThat(copy.splitOffsets()).containsExactly(50L);
    assertThat(copy.equalityIds()).isNull();
    assertThat(copy.tracking().manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(copy.tracking().manifestPos()).isEqualTo(3L);
    assertThat(copy.partition()).isEqualTo(newPartition(7, "music"));
  }

  @Test
  void testCopyWithoutStats() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            new PartitionData(Types.StructType.of()),
            100L,
            1024L,
            0,
            CONTENT_STATS,
            null,
            null,
            null,
            null,
            null,
            null);
    assertThat(file.contentStats()).isNotNull();

    TrackedFile copy = file.copyWithoutStats();

    assertThat(copy.contentType()).isEqualTo(FileContent.DATA);
    assertThat(copy.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(copy.contentStats()).isNull();
  }

  @Test
  void testCopyWithStatsFilters() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            new PartitionData(Types.StructType.of()),
            100L,
            1024L,
            0,
            CONTENT_STATS,
            null,
            null,
            null,
            null,
            null,
            null);
    Set<Integer> keepFieldIds = ImmutableSet.of(1);

    TrackedFile copy = file.copyWithStats(keepFieldIds);

    assertThat(copy.contentStats()).isNotNull();
    ContentStats stats = copy.contentStats();
    assertThat(stats.fieldStats()).hasSize(1);
    assertThat(stats.fieldStats().get(0).fieldId()).isEqualTo(1);
  }

  @Test
  void testCopyIsDeep() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            null,
            100L,
            1024L,
            0,
            null,
            null,
            null,
            null,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            null,
            null);

    TrackedFile copy = file.copy();

    // keyMetadata should be a deep copy
    assertThat(copy.keyMetadata()).isNotSameAs(file.keyMetadata());
  }

  @Test
  void testStructLikeSize() {
    TrackedFileStruct file = new TrackedFileStruct();
    assertThat(file.size()).isEqualTo(16);
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
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            new PartitionData(Types.StructType.of()),
            100L,
            1024L,
            0,
            CONTENT_STATS,
            null,
            null,
            null,
            null,
            null,
            null);
    assertThat(file.contentStats()).isNotNull();
    assertThat(file.contentStats().fieldStats()).hasSize(2);
  }

  @Test
  void testContentStatsNullWhenNotSet() {
    TrackedFileStruct file = new TrackedFileStruct();
    file.set(1, FileContent.DATA.id());
    file.set(3, "test");
    file.set(4, "parquet");
    file.set(5, 0L);
    file.set(6, 0L);
    file.set(7, 0);

    assertThat(file.contentStats()).isNull();
  }

  @Test
  void testAllFileContentTypesSupported() {
    for (FileContent content : FileContent.values()) {
      TrackedFileStruct file = new TrackedFileStruct();
      file.set(1, content.id());
      assertThat(file.contentType()).isEqualTo(content);
    }
  }

  @Test
  void testJavaSerializationRoundTrip() throws IOException, ClassNotFoundException {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            42L,
            null,
            null,
            null,
            null,
            null,
            null,
            "s3://bucket/manifest.avro",
            3L);
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
            7,
            null,
            1,
            DELETION_VECTOR,
            null,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(50L),
            null);

    TrackedFileStruct deserialized = TestHelpers.roundTripSerialize(file);

    assertThat(deserialized.contentType()).isEqualTo(FileContent.DATA);
    assertThat(deserialized.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(deserialized.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(deserialized.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(deserialized.recordCount()).isEqualTo(100L);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(deserialized.specId()).isEqualTo(7);
    assertThat(deserialized.sortOrderId()).isEqualTo(1);
    assertThat(deserialized.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(deserialized.tracking().snapshotId()).isEqualTo(42L);
    assertThat(deserialized.deletionVector().location()).isEqualTo("s3://bucket/dv.puffin");
    assertThat(deserialized.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(deserialized.splitOffsets()).containsExactly(50L);
    assertThat(deserialized.tracking().manifestPos()).isEqualTo(3L);
    assertThat(deserialized.tracking().manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(deserialized.partition()).isEqualTo(newPartition(7, "music"));
  }

  @Test
  void testKryoSerializationRoundTrip() throws IOException {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            42L,
            null,
            null,
            null,
            null,
            null,
            null,
            "s3://bucket/manifest.avro",
            3L);
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
            7,
            null,
            1,
            DELETION_VECTOR,
            null,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(50L),
            null);

    TrackedFileStruct deserialized = TestHelpers.KryoHelpers.roundTripSerialize(file);

    assertThat(deserialized.contentType()).isEqualTo(FileContent.DATA);
    assertThat(deserialized.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(deserialized.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(deserialized.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(deserialized.recordCount()).isEqualTo(100L);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(deserialized.specId()).isEqualTo(7);
    assertThat(deserialized.sortOrderId()).isEqualTo(1);
    assertThat(deserialized.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(deserialized.tracking().snapshotId()).isEqualTo(42L);
    assertThat(deserialized.deletionVector().location()).isEqualTo("s3://bucket/dv.puffin");
    assertThat(deserialized.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(deserialized.splitOffsets()).containsExactly(50L);
    assertThat(deserialized.tracking().manifestPos()).isEqualTo(3L);
    assertThat(deserialized.tracking().manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(deserialized.partition()).isEqualTo(newPartition(7, "music"));
  }

  private static PartitionData newPartition(int idBucket, String category) {
    PartitionData partition = new PartitionData(PARTITION_TYPE);
    partition.set(0, idBucket);
    partition.set(1, category);
    return partition;
  }
}
