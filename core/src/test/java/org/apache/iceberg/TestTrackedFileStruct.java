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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.stats.BaseContentStats;
import org.apache.iceberg.stats.BaseFieldStats;
import org.apache.iceberg.stats.ContentStats;
import org.apache.iceberg.stats.FieldStats;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestTrackedFileStruct {

  @Test
  void trackedFileStructFieldAccess() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    TrackedFileStruct.TrackingStruct tracking =
        new TrackedFileStruct.TrackingStruct(Types.StructType.of());
    TrackedFileStruct.DeletionVectorStruct dv =
        new TrackedFileStruct.DeletionVectorStruct(Types.StructType.of());
    TrackedFileStruct.ManifestInfoStruct info =
        new TrackedFileStruct.ManifestInfoStruct(Types.StructType.of());

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
    assertThat(file.deletionVector()).isNotNull();
    assertThat(file.deletionVector().location()).isEqualTo("s3://bucket/dv.puffin");
    assertThat(file.deletionVector().cardinality()).isEqualTo(5L);
    assertThat(file.manifestInfo()).isNotNull();
    assertThat(file.manifestInfo().addedFilesCount()).isEqualTo(10);
    assertThat(file.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(file.splitOffsets()).containsExactly(100L, 200L);
    assertThat(file.equalityIds()).containsExactly(1, 2, 3);
  }

  @Test
  void trackingStructFieldAccess() {
    TrackedFileStruct.TrackingStruct tracking =
        new TrackedFileStruct.TrackingStruct(Types.StructType.of());

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);
    tracking.set(3, 11L);
    tracking.set(4, 43L);
    tracking.set(5, 1000L);

    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(42L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(10L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(11L);
    assertThat(tracking.dvSnapshotId()).isEqualTo(43L);
    assertThat(tracking.firstRowId()).isEqualTo(1000L);
    assertThat(tracking.deletedPositions()).isNull();
    assertThat(tracking.replacedPositions()).isNull();
  }

  @Test
  void trackingStructReaderSideFieldsOnTrackedFile() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());

    file.set(1, FileContent.DATA.id());
    file.set(2, "test");
    file.set(3, "parquet");
    file.set(4, 0L);
    file.set(5, 0L);

    file.setManifestLocation("s3://bucket/metadata/manifest.avro");
    file.setManifestPos(7L);

    assertThat(file.manifestLocation()).isEqualTo("s3://bucket/metadata/manifest.avro");
    assertThat(file.manifestPos()).isEqualTo(7L);
  }

  @Test
  void trackingStructSetters() {
    TrackedFileStruct.TrackingStruct tracking =
        new TrackedFileStruct.TrackingStruct(Types.StructType.of());

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.setSnapshotId(100L);
    tracking.setSequenceNumber(200L);
    tracking.setFirstRowId(300L);

    assertThat(tracking.snapshotId()).isEqualTo(100L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(200L);
    assertThat(tracking.firstRowId()).isEqualTo(300L);
  }

  @Test
  void trackingStructCopy() {
    TrackedFileStruct.TrackingStruct tracking =
        new TrackedFileStruct.TrackingStruct(Types.StructType.of());

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);
    tracking.set(6, ByteBuffer.wrap(new byte[] {1, 2}));

    TrackedFileStruct.TrackingStruct copy = tracking.copy();

    assertThat(copy.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(copy.snapshotId()).isEqualTo(42L);
    assertThat(copy.dataSequenceNumber()).isEqualTo(10L);
    assertThat(copy.deletedPositions()).isNotNull();

    // verify deep copy of ByteBuffer
    assertThat(copy.deletedPositions()).isNotSameAs(tracking.deletedPositions());
  }

  @Test
  void trackingStructAllStatuses() {
    for (EntryStatus status : EntryStatus.values()) {
      TrackedFileStruct.TrackingStruct tracking =
          new TrackedFileStruct.TrackingStruct(Types.StructType.of());
      tracking.set(0, status.id());
      assertThat(tracking.status()).isEqualTo(status);
    }
  }

  @Test
  void deletionVectorStructFieldAccess() {
    TrackedFileStruct.DeletionVectorStruct dv =
        new TrackedFileStruct.DeletionVectorStruct(Types.StructType.of());

    dv.set(0, "s3://bucket/data/dv.puffin");
    dv.set(1, 256L);
    dv.set(2, 128L);
    dv.set(3, 42L);

    assertThat(dv.location()).isEqualTo("s3://bucket/data/dv.puffin");
    assertThat(dv.offset()).isEqualTo(256L);
    assertThat(dv.sizeInBytes()).isEqualTo(128L);
    assertThat(dv.cardinality()).isEqualTo(42L);
  }

  @Test
  void deletionVectorStructCopy() {
    TrackedFileStruct.DeletionVectorStruct dv =
        new TrackedFileStruct.DeletionVectorStruct(Types.StructType.of());

    dv.set(0, "s3://bucket/data/dv.puffin");
    dv.set(1, 256L);
    dv.set(2, 128L);
    dv.set(3, 42L);

    TrackedFileStruct.DeletionVectorStruct copy = dv.copy();

    assertThat(copy.location()).isEqualTo("s3://bucket/data/dv.puffin");
    assertThat(copy.offset()).isEqualTo(256L);
    assertThat(copy.sizeInBytes()).isEqualTo(128L);
    assertThat(copy.cardinality()).isEqualTo(42L);
  }

  @Test
  void deletionVectorStructSize() {
    TrackedFileStruct.DeletionVectorStruct dv =
        new TrackedFileStruct.DeletionVectorStruct(Types.StructType.of());
    assertThat(dv.size()).isEqualTo(4);
  }

  @Test
  void manifestInfoStructFieldAccess() {
    TrackedFileStruct.ManifestInfoStruct info =
        new TrackedFileStruct.ManifestInfoStruct(Types.StructType.of());

    info.set(0, 10);
    info.set(1, 20);
    info.set(2, 3);
    info.set(3, 2);
    info.set(4, 1000L);
    info.set(5, 2000L);
    info.set(6, 300L);
    info.set(7, 200L);
    info.set(8, 5L);
    info.set(9, ByteBuffer.wrap(new byte[] {0xF}));
    info.set(10, 1L);

    assertThat(info.addedFilesCount()).isEqualTo(10);
    assertThat(info.existingFilesCount()).isEqualTo(20);
    assertThat(info.deletedFilesCount()).isEqualTo(3);
    assertThat(info.replacedFilesCount()).isEqualTo(2);
    assertThat(info.addedRowsCount()).isEqualTo(1000L);
    assertThat(info.existingRowsCount()).isEqualTo(2000L);
    assertThat(info.deletedRowsCount()).isEqualTo(300L);
    assertThat(info.replacedRowsCount()).isEqualTo(200L);
    assertThat(info.minSequenceNumber()).isEqualTo(5L);
    assertThat(info.dv()).isNotNull();
    assertThat(info.dvCardinality()).isEqualTo(1L);
  }

  @Test
  void manifestInfoStructCopy() {
    TrackedFileStruct.ManifestInfoStruct info =
        new TrackedFileStruct.ManifestInfoStruct(Types.StructType.of());

    info.set(0, 10);
    info.set(1, 20);
    info.set(2, 3);
    info.set(3, 2);
    info.set(4, 1000L);
    info.set(5, 2000L);
    info.set(6, 300L);
    info.set(7, 200L);
    info.set(8, 5L);
    info.set(9, ByteBuffer.wrap(new byte[] {0xF}));
    info.set(10, 1L);

    TrackedFileStruct.ManifestInfoStruct copy = info.copy();

    assertThat(copy.addedFilesCount()).isEqualTo(10);
    assertThat(copy.existingFilesCount()).isEqualTo(20);
    assertThat(copy.deletedFilesCount()).isEqualTo(3);
    assertThat(copy.replacedFilesCount()).isEqualTo(2);
    assertThat(copy.addedRowsCount()).isEqualTo(1000L);
    assertThat(copy.existingRowsCount()).isEqualTo(2000L);
    assertThat(copy.deletedRowsCount()).isEqualTo(300L);
    assertThat(copy.replacedRowsCount()).isEqualTo(200L);
    assertThat(copy.minSequenceNumber()).isEqualTo(5L);
    assertThat(copy.dvCardinality()).isEqualTo(1L);

    // verify deep copy of dv ByteBuffer
    assertThat(copy.dv()).isNotSameAs(info.dv());
  }

  @Test
  void manifestInfoStructNullableFields() {
    TrackedFileStruct.ManifestInfoStruct info =
        new TrackedFileStruct.ManifestInfoStruct(Types.StructType.of());

    info.set(0, 0);
    info.set(1, 0);
    info.set(2, 0);
    info.set(3, 0);
    info.set(4, 0L);
    info.set(5, 0L);
    info.set(6, 0L);
    info.set(7, 0L);
    info.set(8, 0L);

    assertThat(info.dv()).isNull();
    assertThat(info.dvCardinality()).isNull();
  }

  @Test
  void trackedFileStructWithTracking() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    TrackedFileStruct.TrackingStruct tracking =
        new TrackedFileStruct.TrackingStruct(Types.StructType.of());

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);

    file.set(0, tracking);
    file.set(1, FileContent.DATA.id());
    file.set(2, "s3://bucket/data/file.parquet");
    file.set(3, "parquet");
    file.set(4, 100L);
    file.set(5, 1024L);
    file.set(6, 0);

    file.setManifestLocation("s3://bucket/manifest.avro");

    assertThat(file.tracking()).isNotNull();
    assertThat(file.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(file.tracking().snapshotId()).isEqualTo(42L);
    assertThat(file.manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(file.trackingStruct()).isSameAs(tracking);
  }

  @Test
  void trackedFileStructWithDeletionVector() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    TrackedFileStruct.DeletionVectorStruct dv =
        new TrackedFileStruct.DeletionVectorStruct(Types.StructType.of());

    dv.set(0, "s3://bucket/dv.puffin");
    dv.set(1, 100L);
    dv.set(2, 50L);
    dv.set(3, 5L);

    file.set(1, FileContent.DATA.id());
    file.set(2, "s3://bucket/data/file.parquet");
    file.set(3, "parquet");
    file.set(4, 100L);
    file.set(5, 1024L);
    file.set(6, 0);
    file.set(9, dv);

    assertThat(file.deletionVector()).isNotNull();
    assertThat(file.deletionVector().location()).isEqualTo("s3://bucket/dv.puffin");
    assertThat(file.deletionVector().offset()).isEqualTo(100L);
    assertThat(file.deletionVector().sizeInBytes()).isEqualTo(50L);
    assertThat(file.deletionVector().cardinality()).isEqualTo(5L);
  }

  @Test
  void trackedFileStructWithManifestInfo() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    TrackedFileStruct.ManifestInfoStruct info =
        new TrackedFileStruct.ManifestInfoStruct(Types.StructType.of());

    info.set(0, 5);
    info.set(1, 10);
    info.set(2, 1);
    info.set(3, 0);
    info.set(4, 500L);
    info.set(5, 1000L);
    info.set(6, 100L);
    info.set(7, 0L);
    info.set(8, 3L);

    file.set(1, FileContent.DATA_MANIFEST.id());
    file.set(2, "s3://bucket/metadata/manifest.avro");
    file.set(3, "avro");
    file.set(4, 0L);
    file.set(5, 2048L);
    file.set(6, 0);
    file.set(10, info);

    assertThat(file.manifestInfo()).isNotNull();
    assertThat(file.manifestInfo().addedFilesCount()).isEqualTo(5);
    assertThat(file.manifestInfo().existingFilesCount()).isEqualTo(10);
  }

  @Test
  void copyPreservesAllFields() {
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
    assertThat(copy.manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(copy.manifestPos()).isEqualTo(3L);
  }

  @Test
  void copyWithoutStatsDropsStats() {
    TrackedFileStruct file = createTrackedFileWithStats();
    assertThat(file.contentStats()).isNotNull();

    TrackedFile copy = file.copyWithoutStats();

    assertThat(copy.contentType()).isEqualTo(FileContent.DATA);
    assertThat(copy.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(copy.contentStats()).isNull();
  }

  @Test
  void copyWithStatsFilters() {
    TrackedFileStruct file = createTrackedFileWithStats();
    Set<Integer> keepFieldIds = ImmutableSet.of(1);

    TrackedFile copy = file.copyWithStats(keepFieldIds);

    assertThat(copy.contentStats()).isNotNull();
    ContentStats stats = copy.contentStats();
    assertThat(stats.fieldStats()).hasSize(1);
    assertThat(stats.fieldStats().get(0).fieldId()).isEqualTo(1);
  }

  @Test
  void copyIsDeep() {
    TrackedFileStruct file = createFullTrackedFile();

    TrackedFile copy = file.copy();

    // tracking should be a deep copy
    assertThat(((TrackedFileStruct) copy).trackingStruct()).isNotSameAs(file.trackingStruct());

    // keyMetadata should be a deep copy
    assertThat(copy.keyMetadata()).isNotSameAs(file.keyMetadata());
  }

  @Test
  void structLikeSize() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    assertThat(file.size()).isEqualTo(14);
  }

  @Test
  void structLikeGetSet() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());

    file.set(1, FileContent.DATA.id());
    assertThat(file.get(1, Integer.class)).isEqualTo(FileContent.DATA.id());

    file.set(2, "test-location");
    assertThat(file.get(2, String.class)).isEqualTo("test-location");

    file.set(4, 999L);
    assertThat(file.get(4, Long.class)).isEqualTo(999L);
  }

  @Test
  void structLikeInvalidOrdinalThrows() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());

    assertThatThrownBy(() -> file.get(14, Object.class))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Unknown field ordinal: 14");

    assertThatThrownBy(() -> file.set(14, "value"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Unknown field ordinal: 14");
  }

  @Test
  void contentStatsReturnedWhenPresent() {
    TrackedFileStruct file = createTrackedFileWithStats();
    assertThat(file.contentStats()).isNotNull();
    assertThat(file.contentStats().fieldStats()).hasSize(2);
  }

  @Test
  void contentStatsRejectsRawStructLike() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    StructLike rawStruct = new PartitionData(Types.StructType.of());

    assertThatThrownBy(() -> file.set(7, rawStruct))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected ContentStats but found");
  }

  @Test
  void contentStatsNullWhenNotSet() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    file.set(1, FileContent.DATA.id());
    file.set(2, "test");
    file.set(3, "parquet");
    file.set(4, 0L);
    file.set(5, 0L);
    file.set(6, 0);

    assertThat(file.contentStats()).isNull();
  }

  @Test
  void manifestLocationOnTrackedFile() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    assertThat(file.manifestLocation()).isNull();

    file.set(1, FileContent.DATA.id());
    file.set(2, "test");
    file.set(3, "parquet");
    file.set(4, 0L);
    file.set(5, 0L);
    file.set(6, 0);

    file.setManifestLocation("s3://bucket/manifest.avro");

    assertThat(file.manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
  }

  @Test
  void allFileContentTypesSupported() {
    for (FileContent content : FileContent.values()) {
      TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
      file.set(1, content.id());
      assertThat(file.contentType()).isEqualTo(content);
    }
  }

  @Test
  void isLiveDelegatesFromTracking() {
    TrackedFileStruct.TrackingStruct tracking =
        new TrackedFileStruct.TrackingStruct(Types.StructType.of());

    tracking.set(0, EntryStatus.ADDED.id());
    assertThat(tracking.isLive()).isTrue();

    tracking.set(0, EntryStatus.EXISTING.id());
    assertThat(tracking.isLive()).isTrue();

    tracking.set(0, EntryStatus.DELETED.id());
    assertThat(tracking.isLive()).isFalse();

    tracking.set(0, EntryStatus.REPLACED.id());
    assertThat(tracking.isLive()).isFalse();
  }

  private static TrackedFileStruct createFullTrackedFile() {
    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    TrackedFileStruct.TrackingStruct tracking =
        new TrackedFileStruct.TrackingStruct(Types.StructType.of());
    TrackedFileStruct.DeletionVectorStruct dv =
        new TrackedFileStruct.DeletionVectorStruct(Types.StructType.of());

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);

    dv.set(0, "s3://bucket/dv.puffin");
    dv.set(1, 100L);
    dv.set(2, 50L);
    dv.set(3, 5L);

    file.set(0, tracking);
    file.set(1, FileContent.DATA.id());
    file.set(2, "s3://bucket/data/file.parquet");
    file.set(3, "parquet");
    file.set(4, 100L);
    file.set(5, 1024L);
    file.set(6, 0);
    file.set(8, 1);
    file.set(9, dv);
    file.set(11, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    file.set(12, ImmutableList.of(50L));

    file.setManifestLocation("s3://bucket/manifest.avro");
    file.setManifestPos(3L);

    return file;
  }

  @SuppressWarnings("unchecked")
  private static TrackedFileStruct createTrackedFileWithStats() {
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

    TrackedFileStruct file = new TrackedFileStruct(Types.StructType.of());
    file.set(1, FileContent.DATA.id());
    file.set(2, "s3://bucket/data/file.parquet");
    file.set(3, "parquet");
    file.set(4, 100L);
    file.set(5, 1024L);
    file.set(6, 0);
    file.set(7, (StructLike) stats);

    return file;
  }
}
