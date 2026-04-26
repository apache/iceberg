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
  @Test
  void testFieldAccess() {
    TrackedFileStruct file = new TrackedFileStruct();
    TrackingStruct tracking = new TrackingStruct();
    DeletionVectorStruct dv = new DeletionVectorStruct(DeletionVector.schema());
    ManifestInfoStruct info = new ManifestInfoStruct(ManifestInfo.schema());

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);

    dv.set(0, "s3://bucket/dv.puffin");
    dv.set(1, 100L);
    dv.set(2, 50L);
    dv.set(3, 5L);

    info.set(0, 10);
    info.set(1, 20);
    info.set(2, 3);
    info.set(3, 2);
    info.set(4, 1000L);
    info.set(5, 2000L);
    info.set(6, 300L);
    info.set(7, 200L);
    info.set(8, 5L);

    file.set(0, tracking);
    file.set(1, FileContent.EQUALITY_DELETES.id());
    file.set(2, "s3://bucket/data/eq-delete.avro");
    file.set(3, "avro");
    file.set(4, 50L);
    file.set(5, 512L);
    file.set(6, 1);
    file.set(8, 5);
    file.set(9, dv);
    file.set(10, info);
    file.set(11, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    file.set(12, ImmutableList.of(100L, 200L));
    file.set(13, ImmutableList.of(1, 2, 3));

    assertThat(file.tracking()).isNotNull();
    assertThat(file.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(file.tracking().snapshotId()).isEqualTo(42L);
    assertThat(file.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
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
  }

  @Test
  void testReaderSideFields() {
    TrackedFileStruct file = new TrackedFileStruct();

    TrackingStruct tracking = new TrackingStruct();
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.setManifestLocation("s3://bucket/metadata/manifest.avro");
    tracking.set(8, 7L);

    file.set(0, tracking);
    file.set(1, FileContent.DATA.id());
    file.set(2, "test");
    file.set(3, "parquet");
    file.set(4, 0L);
    file.set(5, 0L);

    assertThat(file.tracking().manifestLocation()).isEqualTo("s3://bucket/metadata/manifest.avro");
    assertThat(file.tracking().manifestPos()).isEqualTo(7L);
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
    assertThat(copy.keyMetadata()).isNotNull();
    assertThat(copy.splitOffsets()).containsExactly(50L);
    assertThat(copy.equalityIds()).isNull();
    assertThat(copy.tracking().manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(copy.tracking().manifestPos()).isEqualTo(3L);
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

    // keyMetadata should be a deep copy
    assertThat(copy.keyMetadata()).isNotSameAs(file.keyMetadata());
  }

  @Test
  void testStructLikeSize() {
    TrackedFileStruct file = new TrackedFileStruct();
    assertThat(file.size()).isEqualTo(14);
  }

  @Test
  void testStructLikeGetSet() {
    TrackedFileStruct file = new TrackedFileStruct();

    file.set(1, FileContent.DATA.id());
    assertThat(file.get(1, Integer.class)).isEqualTo(FileContent.DATA.id());

    file.set(2, "test-location");
    assertThat(file.get(2, String.class)).isEqualTo("test-location");

    file.set(4, 999L);
    assertThat(file.get(4, Long.class)).isEqualTo(999L);
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
    file.set(1, FileContent.DATA.id());
    file.set(2, "test");
    file.set(3, "parquet");
    file.set(4, 0L);
    file.set(5, 0L);
    file.set(6, 0);

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
    TrackedFileStruct file = createFullTrackedFile();

    TrackedFileStruct deserialized = TestHelpers.roundTripSerialize(file);

    assertThat(deserialized.contentType()).isEqualTo(FileContent.DATA);
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
  }

  @Test
  void testKryoSerializationRoundTrip() throws IOException {
    TrackedFileStruct file = createFullTrackedFile();

    TrackedFileStruct deserialized = TestHelpers.KryoHelpers.roundTripSerialize(file);

    assertThat(deserialized.contentType()).isEqualTo(FileContent.DATA);
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
  }

  static TrackedFileStruct createFullTrackedFile() {
    TrackingStruct tracking = new TrackingStruct();
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);
    tracking.setManifestLocation("s3://bucket/manifest.avro");
    tracking.set(8, 3L);

    DeletionVectorStruct dv = new DeletionVectorStruct(DeletionVector.schema());
    dv.set(0, "s3://bucket/dv.puffin");
    dv.set(1, 100L);
    dv.set(2, 50L);
    dv.set(3, 5L);

    TrackedFileStruct file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            100L,
            1024L);
    file.set(6, 0);
    file.set(8, 1);
    file.set(9, dv);
    file.set(11, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    file.set(12, ImmutableList.of(50L));

    return file;
  }

  @SuppressWarnings("unchecked")
  static TrackedFileStruct createTrackedFileWithStats() {
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
            (FieldStats<?>)
                BaseFieldStats.<Integer>builder()
                    .fieldId(1)
                    .type(Types.IntegerType.get())
                    .valueCount(100L)
                    .nullValueCount(5L)
                    .lowerBound(1)
                    .upperBound(1000)
                    .build(),
            (FieldStats<?>)
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

    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            100L,
            1024L);
    file.set(6, 0);
    file.set(7, stats);

    return file;
  }
}
