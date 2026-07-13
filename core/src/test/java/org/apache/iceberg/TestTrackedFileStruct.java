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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.TestHelpers.RoundTripSerializer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

class TestTrackedFileStruct {
  private static final int FORMAT_VERSION_V4 = 4;
  private static final Types.StructType PARTITION_TYPE =
      Types.StructType.of(
          Types.NestedField.optional(1000, "id_bucket", Types.IntegerType.get()),
          Types.NestedField.optional(1001, "category", Types.StringType.get()));

  private static final List<Types.NestedField> FIELDS =
      TrackedFile.schemaWithContentStats(PARTITION_TYPE, Types.StructType.of()).fields();

  private static final Tracking TRACKING = Mockito.mock(Tracking.class);
  private static final Tracking TRACKING_COPY = Mockito.mock(Tracking.class);

  private static final PartitionData PARTITION = Mockito.mock(PartitionData.class);
  private static final PartitionData PARTITION_COPY = Mockito.mock(PartitionData.class);

  private static final DeletionVector DELETION_VECTOR = Mockito.mock(DeletionVector.class);
  private static final DeletionVector DELETION_VECTOR_COPY = Mockito.mock(DeletionVector.class);

  private static final ManifestInfo MANIFEST_INFO = Mockito.mock(ManifestInfo.class);
  private static final ManifestInfo MANIFEST_INFO_COPY = Mockito.mock(ManifestInfo.class);

  static {
    Mockito.when(TRACKING.copy()).thenReturn(TRACKING_COPY);
    Mockito.when(PARTITION.copy()).thenReturn(PARTITION_COPY);
    Mockito.when(DELETION_VECTOR.copy()).thenReturn(DELETION_VECTOR_COPY);
    Mockito.when(MANIFEST_INFO.copy()).thenReturn(MANIFEST_INFO_COPY);
  }

  private static final ContentStats CONTENT_STATS = Mockito.mock(ContentStats.class);

  @Test
  void fieldAccess() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            TRACKING,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/00000-0-file.parquet",
            FileFormat.PARQUET,
            PARTITION,
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

    assertThat(file.tracking()).isSameAs(TRACKING);
    assertThat(file.contentType()).isEqualTo(FileContent.DATA);
    assertThat(file.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(file.location()).isEqualTo("s3://bucket/data/00000-0-file.parquet");
    assertThat(file.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(file.partition()).isSameAs(PARTITION);
    assertThat(file.recordCount()).isEqualTo(50L);
    assertThat(file.fileSizeInBytes()).isEqualTo(512L);
    assertThat(file.specId()).isEqualTo(1);
    assertThat(file.contentStats()).isSameAs(CONTENT_STATS);
    assertThat(file.sortOrderId()).isEqualTo(5);
    assertThat(file.deletionVector()).isSameAs(DELETION_VECTOR);
    assertThat(file.manifestInfo()).isSameAs(MANIFEST_INFO);
    assertThat(file.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(file.splitOffsets()).containsExactly(100L, 200L);
    assertThat(file.equalityIds()).containsExactly(1, 2, 3);
  }

  @Test
  void setByPosition() {
    TrackedFileStruct file = new TrackedFileStruct();
    file.set(pos("tracking"), TRACKING);
    file.set(pos("content_type"), FileContent.DATA.id());
    file.set(pos("format_version"), FORMAT_VERSION_V4);
    file.set(pos("location"), "s3://bucket/data/00000-0-file.parquet");
    file.set(pos("file_format"), "parquet");
    file.set(pos("record_count"), 50L);
    file.set(pos("file_size_in_bytes"), 512L);
    file.set(pos("spec_id"), 1);
    file.set(pos("partition"), PARTITION);
    file.set(pos("content_stats"), CONTENT_STATS);
    file.set(pos("sort_order_id"), 5);
    file.set(pos("deletion_vector"), DELETION_VECTOR);
    file.set(pos("manifest_info"), MANIFEST_INFO);
    file.set(pos("key_metadata"), ByteBuffer.wrap(new byte[] {1, 2, 3}));
    file.set(pos("split_offsets"), ImmutableList.of(100L, 200L));
    file.set(pos("equality_ids"), ImmutableList.of(1, 2, 3));

    assertThat(file.tracking()).isSameAs(TRACKING);
    assertThat(file.contentType()).isEqualTo(FileContent.DATA);
    assertThat(file.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(file.location()).isEqualTo("s3://bucket/data/00000-0-file.parquet");
    assertThat(file.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(file.recordCount()).isEqualTo(50L);
    assertThat(file.fileSizeInBytes()).isEqualTo(512L);
    assertThat(file.specId()).isEqualTo(1);
    assertThat(file.partition()).isSameAs(PARTITION);
    assertThat(file.contentStats()).isSameAs(CONTENT_STATS);
    assertThat(file.sortOrderId()).isEqualTo(5);
    assertThat(file.deletionVector()).isSameAs(DELETION_VECTOR);
    assertThat(file.manifestInfo()).isSameAs(MANIFEST_INFO);
    assertThat(file.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(file.splitOffsets()).containsExactly(100L, 200L);
    assertThat(file.equalityIds()).containsExactly(1, 2, 3);
  }

  @Test
  void getByPosition() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            TRACKING,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/00000-0-file.parquet",
            FileFormat.PARQUET,
            PARTITION,
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

    assertThat(file.get(pos("tracking"), Tracking.class)).isSameAs(TRACKING);
    assertThat(file.get(pos("content_type"), Integer.class)).isEqualTo(FileContent.DATA.id());
    assertThat(file.get(pos("format_version"), Integer.class)).isEqualTo(FORMAT_VERSION_V4);
    assertThat(file.get(pos("location"), String.class))
        .isEqualTo("s3://bucket/data/00000-0-file.parquet");
    assertThat(file.get(pos("file_format"), String.class)).isEqualTo(FileFormat.PARQUET.toString());
    assertThat(file.get(pos("record_count"), Long.class)).isEqualTo(50L);
    assertThat(file.get(pos("file_size_in_bytes"), Long.class)).isEqualTo(512L);
    assertThat(file.get(pos("spec_id"), Integer.class)).isEqualTo(1);
    assertThat(file.get(pos("partition"), PartitionData.class)).isSameAs(PARTITION);
    assertThat(file.get(pos("content_stats"), ContentStats.class)).isSameAs(CONTENT_STATS);
    assertThat(file.get(pos("sort_order_id"), Integer.class)).isEqualTo(5);
    assertThat(file.get(pos("deletion_vector"), DeletionVector.class)).isSameAs(DELETION_VECTOR);
    assertThat(file.get(pos("manifest_info"), ManifestInfo.class)).isSameAs(MANIFEST_INFO);
    assertThat(file.get(pos("key_metadata"), ByteBuffer.class))
        .isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(file.get(pos("split_offsets"), List.class)).containsExactly(100L, 200L);
    assertThat(file.get(pos("equality_ids"), List.class)).containsExactly(1, 2, 3);
  }

  @Test
  void copy() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            TRACKING,
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/00000-0-file.parquet",
            FileFormat.PARQUET,
            PARTITION,
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

    TrackedFile copy = file.copy();

    assertThat(copy).isInstanceOf(TrackedFileStruct.class);
    assertThat(copy.tracking()).isSameAs(TRACKING_COPY);
    assertThat(copy.contentType()).isEqualTo(FileContent.DATA);
    assertThat(copy.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(copy.location()).isEqualTo("s3://bucket/data/00000-0-file.parquet");
    assertThat(copy.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(copy.recordCount()).isEqualTo(50L);
    assertThat(copy.fileSizeInBytes()).isEqualTo(512L);
    assertThat(copy.specId()).isEqualTo(1);
    assertThat(copy.sortOrderId()).isEqualTo(5);
    assertThat(copy.deletionVector()).isSameAs(DELETION_VECTOR_COPY);
    assertThat(copy.manifestInfo()).isSameAs(MANIFEST_INFO_COPY);
    assertThat(copy.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(copy.splitOffsets()).containsExactly(100L, 200L);
    assertThat(copy.equalityIds()).containsExactly(1, 2, 3);
    assertThat(copy.partition()).isSameAs(PARTITION_COPY);

    // mutable fields are deep-copied, not shared with the original
    assertThat(copy.keyMetadata()).isNotSameAs(file.keyMetadata());
  }

  @Test
  void projectedStructLike() {
    // project only location (field ID 100) and file_size_in_bytes (field ID 104)
    Types.StructType projection =
        Types.StructType.of(TrackedFile.LOCATION, TrackedFile.FILE_SIZE_IN_BYTES);

    TrackedFileStruct file = new TrackedFileStruct(projection);
    assertThat(file.size()).isEqualTo(2);

    file.set(0, "s3://bucket/file.parquet");
    file.set(1, 1024L);

    assertThat(file.location()).isEqualTo("s3://bucket/file.parquet");
    assertThat(file.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(file.get(0, String.class)).isEqualTo("s3://bucket/file.parquet");
    assertThat(file.get(1, Long.class)).isEqualTo(1024L);
  }

  @Test
  void structLikeSize() {
    TrackedFileStruct file = new TrackedFileStruct();
    assertThat(file.size()).isEqualTo(FIELDS.size());
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  void serializationRoundTrip(RoundTripSerializer<TrackedFileStruct> serializer) throws Exception {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null, // TrackingStruct has its own serialization tests
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            null, // PartitionData has its own serialization tests
            100L,
            1024L,
            7,
            null,
            1,
            null, // DeletionVector has its own serialization tests
            null, // ManifestInfo has its own serialization tests
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(50L),
            ImmutableList.of(1, 2, 3));

    TrackedFileStruct deserialized = serializer.apply(file);

    assertThat(deserialized.tracking()).isNull();
    assertThat(deserialized.contentType()).isEqualTo(FileContent.DATA);
    assertThat(deserialized.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(deserialized.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(deserialized.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(deserialized.partition()).isNull();
    assertThat(deserialized.recordCount()).isEqualTo(100L);
    assertThat(deserialized.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(deserialized.specId()).isEqualTo(7);
    assertThat(deserialized.sortOrderId()).isEqualTo(1);
    assertThat(deserialized.deletionVector()).isNull();
    assertThat(deserialized.manifestInfo()).isNull();
    assertThat(deserialized.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(deserialized.splitOffsets()).containsExactly(50L);
    assertThat(deserialized.equalityIds()).containsExactly(1, 2, 3);
  }

  private static int pos(String fieldName) {
    for (int i = 0; i < FIELDS.size(); i += 1) {
      if (FIELDS.get(i).name().equals(fieldName)) {
        return i;
      }
    }

    throw new IllegalArgumentException("No such field in TrackedFile schema: " + fieldName);
  }
}
