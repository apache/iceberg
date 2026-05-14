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

class TestTrackedFileAdapters {

  private static final String MANIFEST_LOCATION = "s3://bucket/table/manifest.parquet";

  private static final Map<Integer, PartitionSpec> UNPARTITIONED =
      ImmutableMap.of(0, PartitionSpec.unpartitioned());

  private static Map<Integer, PartitionSpec> specsById(PartitionSpec spec) {
    return ImmutableMap.of(spec.specId(), spec);
  }

  @Test
  void testDataFileAdapterDelegatesAllFields() {
    TrackingStruct tracking = createTracking(3L);
    ContentStats stats = createContentStats();

    TrackedFileStruct file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            null,
            100L,
            1024L);
    file.set(6, 0);
    file.set(8, stats);
    file.set(9, 3);
    file.set(12, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    file.set(13, ImmutableList.of(50L, 100L));

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, UNPARTITIONED);

    assertThat(dataFile.pos()).isEqualTo(3L);
    assertThat(dataFile.specId()).isEqualTo(0);
    assertThat(dataFile.content()).isEqualTo(FileContent.DATA);
    assertThat(dataFile.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(dataFile.format()).isEqualTo(FileFormat.PARQUET);
    assertThat(dataFile.recordCount()).isEqualTo(100L);
    assertThat(dataFile.fileSizeInBytes()).isEqualTo(1024L);
    assertThat(dataFile.sortOrderId()).isEqualTo(3);
    assertThat(dataFile.dataSequenceNumber()).isEqualTo(10L);
    assertThat(dataFile.fileSequenceNumber()).isEqualTo(11L);
    assertThat(dataFile.firstRowId()).isEqualTo(1000L);
    assertThat(dataFile.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(dataFile.splitOffsets()).containsExactly(50L, 100L);
    assertThat(dataFile.manifestLocation()).isEqualTo(MANIFEST_LOCATION);
    assertThat(dataFile.equalityFieldIds()).isNull();
    assertThat(dataFile.columnSizes()).isNull();
    assertThat(dataFile.valueCounts()).containsOnly(entry(1, 100L), entry(2, 200L));
    assertThat(dataFile.nullValueCounts()).containsOnly(entry(1, 5L), entry(2, 10L));
    assertThat(dataFile.nanValueCounts()).containsOnly(entry(2, 3L));
    assertThat(dataFile.lowerBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1))
        .containsEntry(2, Conversions.toByteBuffer(Types.FloatType.get(), 1.0f));
    assertThat(dataFile.upperBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1000))
        .containsEntry(2, Conversions.toByteBuffer(Types.FloatType.get(), 100.0f));
  }

  @Test
  void testDataFileAdapterRejectsNonData() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.EQUALITY_DELETES,
            "s3://bucket/delete.avro",
            FileFormat.AVRO,
            null,
            50L,
            512L);
    file.set(6, 0);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for DataFile: %s", FileContent.EQUALITY_DELETES);
  }

  @Test
  void testEqualityDeleteFileAdapterDelegatesAllFields() {
    TrackingStruct tracking = createTracking(5L);
    PartitionSpec spec = PartitionSpec.builderFor(new Schema()).withSpecId(1).build();
    ContentStats stats = createContentStats();

    TrackedFileStruct file =
        new TrackedFileStruct(
            tracking,
            FileContent.EQUALITY_DELETES,
            "s3://bucket/eq-delete.avro",
            FileFormat.AVRO,
            null,
            50L,
            512L);
    file.set(6, 1);
    file.set(8, stats);
    file.set(9, 5);
    file.set(12, ByteBuffer.wrap(new byte[] {4, 5}));
    file.set(13, ImmutableList.of(200L));
    file.set(14, ImmutableList.of(1, 2, 3));

    DeleteFile deleteFile = TrackedFileAdapters.asEqualityDeleteFile(file, specsById(spec));

    assertThat(deleteFile.pos()).isEqualTo(5L);
    assertThat(deleteFile.specId()).isEqualTo(1);
    assertThat(deleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(deleteFile.location()).isEqualTo("s3://bucket/eq-delete.avro");
    assertThat(deleteFile.format()).isEqualTo(FileFormat.AVRO);
    assertThat(deleteFile.recordCount()).isEqualTo(50L);
    assertThat(deleteFile.fileSizeInBytes()).isEqualTo(512L);
    assertThat(deleteFile.sortOrderId()).isEqualTo(5);
    assertThat(deleteFile.dataSequenceNumber()).isEqualTo(10L);
    assertThat(deleteFile.fileSequenceNumber()).isEqualTo(11L);
    assertThat(deleteFile.firstRowId()).isNull();
    assertThat(deleteFile.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {4, 5}));
    assertThat(deleteFile.splitOffsets()).containsExactly(200L);
    assertThat(deleteFile.manifestLocation()).isEqualTo(MANIFEST_LOCATION);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1, 2, 3);
    assertThat(deleteFile.columnSizes()).isNull();
    assertThat(deleteFile.valueCounts()).containsOnly(entry(1, 100L), entry(2, 200L));
    assertThat(deleteFile.nullValueCounts()).containsOnly(entry(1, 5L), entry(2, 10L));
    assertThat(deleteFile.nanValueCounts()).containsOnly(entry(2, 3L));
    assertThat(deleteFile.lowerBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1))
        .containsEntry(2, Conversions.toByteBuffer(Types.FloatType.get(), 1.0f));
    assertThat(deleteFile.upperBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1000))
        .containsEntry(2, Conversions.toByteBuffer(Types.FloatType.get(), 100.0f));
  }

  @Test
  void testEqualityDeleteFileAdapterRejectsNonEqualityDeletes() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            "s3://bucket/data.parquet",
            FileFormat.PARQUET,
            null,
            100L,
            1024L);
    file.set(6, 0);

    assertThatThrownBy(() -> TrackedFileAdapters.asEqualityDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for equality delete file: %s", FileContent.DATA);
  }

  @Test
  void testDVDeleteFileAdapterDelegatesAllFields() {
    TrackingStruct tracking = createTracking(7L);
    PartitionSpec spec = PartitionSpec.builderFor(new Schema()).withSpecId(2).build();

    TrackedFileStruct file = createDataFileWithDV(tracking, 2);

    DeleteFile dvFile = TrackedFileAdapters.asDVDeleteFile(file, specsById(spec));

    // DV-specific fields from DeletionVector
    assertThat(dvFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(dvFile.location()).isEqualTo("s3://bucket/puffin/dv-file.bin");
    assertThat(dvFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(dvFile.recordCount()).isEqualTo(10L);
    assertThat(dvFile.fileSizeInBytes()).isEqualTo(256L);
    assertThat(dvFile.referencedDataFile()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(dvFile.contentOffset()).isEqualTo(128L);
    assertThat(dvFile.contentSizeInBytes()).isEqualTo(256L);

    // fields delegated from TrackedFile / Tracking
    assertThat(dvFile.pos()).isEqualTo(7L);
    assertThat(dvFile.specId()).isEqualTo(2);
    assertThat(dvFile.dataSequenceNumber()).isEqualTo(10L);
    assertThat(dvFile.fileSequenceNumber()).isEqualTo(11L);
    assertThat(dvFile.manifestLocation()).isEqualTo(MANIFEST_LOCATION);

    // fields that should be null for DVs
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

  @Test
  void testDVDeleteFileAdapterRejectsNonData() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.EQUALITY_DELETES,
            "s3://bucket/eq-delete.avro",
            FileFormat.AVRO,
            null,
            50L,
            512L);
    file.set(6, 0);
    file.set(10, createDeletionVector());

    assertThatThrownBy(() -> TrackedFileAdapters.asDVDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for DV delete file: %s", FileContent.EQUALITY_DELETES);
  }

  @Test
  void testDVDeleteFileAdapterRejectsNullDV() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            "s3://bucket/data.parquet",
            FileFormat.PARQUET,
            null,
            100L,
            1024L);
    file.set(6, 0);

    assertThatThrownBy(() -> TrackedFileAdapters.asDVDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create DV delete file: no deletion vector");
  }

  @Test
  void testNullContentStatsReturnsNullStats() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            "s3://bucket/data.parquet",
            FileFormat.PARQUET,
            null,
            100L,
            1024L);
    file.set(6, 0);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, UNPARTITIONED);

    assertThat(dataFile.valueCounts()).isNull();
    assertThat(dataFile.nullValueCounts()).isNull();
    assertThat(dataFile.nanValueCounts()).isNull();
    assertThat(dataFile.lowerBounds()).isNull();
    assertThat(dataFile.upperBounds()).isNull();
  }

  @Test
  void testNullSpecIdResolvesToUnpartitionedSpec() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            "s3://bucket/data.parquet",
            FileFormat.PARQUET,
            null,
            100L,
            1024L);

    PartitionSpec spec = PartitionSpec.builderFor(new Schema()).withSpecId(5).build();
    DataFile dataFile = TrackedFileAdapters.asDataFile(file, specsById(spec));
    assertThat(dataFile.specId()).isEqualTo(5);
  }

  @Test
  void testNullSpecIdThrowsWhenNoUnpartitionedSpec() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    PartitionSpec partitioned = PartitionSpec.builderFor(schema).identity("id").build();

    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            "s3://bucket/data.parquet",
            FileFormat.PARQUET,
            null,
            100L,
            1024L);

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
            "s3://bucket/data.parquet",
            FileFormat.PARQUET,
            null,
            100L,
            1024L);
    file.set(6, 99);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find partition spec for spec ID");
  }

  private static TrackingStruct createTracking(long manifestPos) {
    TrackingStruct tracking =
        TrackingStruct.builder()
            .status(EntryStatus.ADDED)
            .snapshotId(42L)
            .dataSequenceNumber(10L)
            .fileSequenceNumber(11L)
            .firstRowId(1000L)
            .build();
    // manifestLocation and manifestPos are set by manifest readers, not written to manifests
    tracking.setManifestLocation(MANIFEST_LOCATION);
    tracking.set(8, manifestPos);
    return tracking;
  }

  private static TrackedFileStruct createDataFileWithDV(Tracking tracking, int specId) {
    TrackedFileStruct file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            null,
            100L,
            1024L);
    file.set(6, specId);
    file.set(10, createDeletionVector());
    return file;
  }

  private static DeletionVectorStruct createDeletionVector() {
    return DeletionVectorStruct.builder()
        .location("s3://bucket/puffin/dv-file.bin")
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

  private static Map.Entry<Integer, Long> entry(int key, long value) {
    return Map.entry(key, value);
  }
}
