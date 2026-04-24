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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestTrackedFileAdapters {

  @Test
  void testAsDataFileValidatesContentType() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null, FileContent.DATA, "s3://bucket/data.parquet", FileFormat.PARQUET, 100L, 1024L);
    file.set(6, 0);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, PartitionSpec.unpartitioned());
    assertThat(dataFile).isNotNull();
    assertThat(dataFile.content()).isEqualTo(FileContent.DATA);
    assertThat(dataFile.location()).isEqualTo("s3://bucket/data.parquet");
  }

  @Test
  void testAsDataFileRejectsNonData() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.EQUALITY_DELETES,
            "s3://bucket/delete.avro",
            FileFormat.AVRO,
            50L,
            512L);
    file.set(6, 0);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, PartitionSpec.unpartitioned()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot convert tracked file to DataFile: content type is %s, not DATA",
            FileContent.EQUALITY_DELETES);
  }

  @Test
  void testAsDeleteFileValidatesContentType() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.EQUALITY_DELETES,
            "s3://bucket/eq-delete.avro",
            FileFormat.AVRO,
            50L,
            512L);
    file.set(6, 0);
    file.set(13, ImmutableList.of(1, 2));

    DeleteFile deleteFile = TrackedFileAdapters.asDeleteFile(file, PartitionSpec.unpartitioned());
    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1, 2);
  }

  @Test
  void testAsDeleteFileRejectsNonEqualityDeletes() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null, FileContent.DATA, "s3://bucket/data.parquet", FileFormat.PARQUET, 100L, 1024L);
    file.set(6, 0);

    assertThatThrownBy(() -> TrackedFileAdapters.asDeleteFile(file, PartitionSpec.unpartitioned()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot convert tracked file to DeleteFile: content type is %s, not EQUALITY_DELETES",
            FileContent.DATA);
  }

  @Test
  void testDataFileAdapterDelegatesAllFields() {
    Types.StructType trackingWithPos =
        Types.StructType.of(
            ImmutableList.<Types.NestedField>builder()
                .addAll(Tracking.schema().fields())
                .add(MetadataColumns.ROW_POSITION)
                .build());
    TrackingStruct tracking = new TrackingStruct(trackingWithPos);

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);
    tracking.set(3, 11L);
    tracking.set(5, 1000L);
    tracking.setManifestLocation("s3://bucket/manifest.avro");
    tracking.set(8, 3L);

    TrackedFileStruct file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            100L,
            1024L);
    file.set(6, 0);
    file.set(8, 3);
    file.set(11, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    file.set(12, ImmutableList.of(50L, 100L));

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, PartitionSpec.unpartitioned());

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
    assertThat(dataFile.manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(dataFile.equalityFieldIds()).isNull();
    assertThat(dataFile.columnSizes()).isNull();
  }

  @Test
  void testDeleteFileAdapterDelegatesAllFields() {
    Types.StructType trackingWithPos =
        Types.StructType.of(
            ImmutableList.<Types.NestedField>builder()
                .addAll(Tracking.schema().fields())
                .add(MetadataColumns.ROW_POSITION)
                .build());
    TrackingStruct tracking = new TrackingStruct(trackingWithPos);

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);
    tracking.set(3, 11L);
    tracking.set(5, 1000L);
    tracking.setManifestLocation("s3://bucket/manifest.avro");
    tracking.set(8, 5L);

    TrackedFileStruct file =
        new TrackedFileStruct(
            tracking,
            FileContent.EQUALITY_DELETES,
            "s3://bucket/eq-delete.avro",
            FileFormat.AVRO,
            50L,
            512L);
    file.set(6, 1);
    file.set(8, 5);
    file.set(11, ByteBuffer.wrap(new byte[] {4, 5}));
    file.set(12, ImmutableList.of(200L));
    file.set(13, ImmutableList.of(1, 2, 3));

    DeleteFile deleteFile = TrackedFileAdapters.asDeleteFile(file, PartitionSpec.unpartitioned());

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
    assertThat(deleteFile.firstRowId()).isEqualTo(1000L);
    assertThat(deleteFile.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {4, 5}));
    assertThat(deleteFile.splitOffsets()).containsExactly(200L);
    assertThat(deleteFile.manifestLocation()).isEqualTo("s3://bucket/manifest.avro");
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1, 2, 3);
    assertThat(deleteFile.columnSizes()).isNull();
  }

  @Test
  void testAdapterDelegatesNullTracking() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null, FileContent.DATA, "s3://bucket/data.parquet", FileFormat.PARQUET, 100L, 1024L);
    file.set(6, 0);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, PartitionSpec.unpartitioned());

    assertThat(dataFile.dataSequenceNumber()).isNull();
    assertThat(dataFile.fileSequenceNumber()).isNull();
    assertThat(dataFile.firstRowId()).isNull();
    assertThat(dataFile.manifestLocation()).isNull();
    assertThat(dataFile.pos()).isNull();
  }

  @Test
  void testDataFileAdapterStatsFromContentStats() {
    TrackedFileStruct file = createTrackedFileWithStats();
    DataFile dataFile = TrackedFileAdapters.asDataFile(file, PartitionSpec.unpartitioned());

    assertThat(dataFile.valueCounts()).containsOnly(entry(1, 100L), entry(2, 200L));
    assertThat(dataFile.nullValueCounts()).containsOnly(entry(1, 5L), entry(2, 10L));
    assertThat(dataFile.nanValueCounts()).containsOnly(entry(2, 3L));
    assertThat(dataFile.lowerBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1))
        .containsEntry(2, Conversions.toByteBuffer(Types.FloatType.get(), 1.0f));
    assertThat(dataFile.upperBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1000))
        .containsEntry(2, Conversions.toByteBuffer(Types.FloatType.get(), 100.0f));
    assertThat(dataFile.columnSizes()).isNull();
  }

  @Test
  void testDeleteFileAdapterStatsFromContentStats() {
    TrackedFileStruct file = createTrackedFileWithStats();
    file.set(1, FileContent.EQUALITY_DELETES.id());
    file.set(13, ImmutableList.of(1));

    DeleteFile deleteFile = TrackedFileAdapters.asDeleteFile(file, PartitionSpec.unpartitioned());

    assertThat(deleteFile.valueCounts()).containsOnly(entry(1, 100L), entry(2, 200L));
    assertThat(deleteFile.nullValueCounts()).containsOnly(entry(1, 5L), entry(2, 10L));
    assertThat(deleteFile.nanValueCounts()).containsOnly(entry(2, 3L));
    assertThat(deleteFile.lowerBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1))
        .containsEntry(2, Conversions.toByteBuffer(Types.FloatType.get(), 1.0f));
    assertThat(deleteFile.upperBounds())
        .containsEntry(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1000))
        .containsEntry(2, Conversions.toByteBuffer(Types.FloatType.get(), 100.0f));
    assertThat(deleteFile.columnSizes()).isNull();
  }

  @Test
  void testDataFileAdapterStatsNullWhenNoContentStats() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null, FileContent.DATA, "s3://bucket/data.parquet", FileFormat.PARQUET, 100L, 1024L);
    file.set(6, 0);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, PartitionSpec.unpartitioned());

    assertThat(dataFile.valueCounts()).isNull();
    assertThat(dataFile.nullValueCounts()).isNull();
    assertThat(dataFile.nanValueCounts()).isNull();
    assertThat(dataFile.lowerBounds()).isNull();
    assertThat(dataFile.upperBounds()).isNull();
  }

  @Test
  void testPartitionExtractedFromContentStatsWithIdentityTransform() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "category", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

    TrackedFileStruct file = createTrackedFileWithPartitionStats(spec);
    DataFile dataFile = TrackedFileAdapters.asDataFile(file, spec);

    StructLike partition = dataFile.partition();
    assertThat(partition).isNotNull();
    assertThat(partition.get(0, CharSequence.class).toString()).isEqualTo("electronics");
  }

  @Test
  void testPartitionExtractedWithYearTransform() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "ts", Types.DateType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).year("ts").build();

    // date value 20546 = 2026-04-03 (days since epoch)
    TrackedFileStruct file = createTrackedFileWithFieldStats(2, Types.DateType.get(), 20546);
    DataFile dataFile = TrackedFileAdapters.asDataFile(file, spec);

    StructLike partition = dataFile.partition();
    assertThat(partition).isNotNull();
    assertThat(partition.get(0, Integer.class)).isEqualTo(56);
  }

  @Test
  void testPartitionExtractedWithBucketTransform() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "value", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 16).build();

    TrackedFileStruct file = createTrackedFileWithFieldStats(2, Types.IntegerType.get(), 42);
    DataFile dataFile = TrackedFileAdapters.asDataFile(file, spec);

    StructLike partition = dataFile.partition();
    assertThat(partition).isNotNull();

    // verify the bucket value is a valid bucket (0-15)
    int bucket = partition.get(0, Integer.class);
    assertThat(bucket).isBetween(0, 15);
  }

  @Test
  void testPartitionNullWhenNoContentStats() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();

    TrackedFileStruct file =
        new TrackedFileStruct(
            null, FileContent.DATA, "s3://bucket/data.parquet", FileFormat.PARQUET, 100L, 1024L);
    file.set(6, spec.specId());

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, spec);
    assertThat(dataFile.partition()).isNull();
  }

  @Test
  void testPartitionNullWhenNullSpec() {
    TrackedFileStruct file = createTrackedFileWithStats();
    DataFile dataFile = TrackedFileAdapters.asDataFile(file, null);
    assertThat(dataFile.partition()).isNull();
  }

  @Test
  void testPartitionNullForUnpartitioned() {
    PartitionSpec spec = PartitionSpec.unpartitioned();

    TrackedFileStruct file = createTrackedFileWithStats();
    DataFile dataFile = TrackedFileAdapters.asDataFile(file, spec);
    assertThat(dataFile.partition()).isNull();
  }

  @Test
  void testPartitionWithMultipleFields() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "category", Types.StringType.get()));

    PartitionSpec spec =
        PartitionSpec.builderFor(schema).identity("id").identity("category").build();

    Types.StructType statsStruct =
        Types.StructType.of(
            Types.NestedField.optional(
                10000,
                "1",
                Types.StructType.of(
                    Types.NestedField.optional(10006, "lower_bound", Types.IntegerType.get()),
                    Types.NestedField.optional(10007, "upper_bound", Types.IntegerType.get()))),
            Types.NestedField.optional(
                20000,
                "2",
                Types.StructType.of(
                    Types.NestedField.optional(20006, "lower_bound", Types.StringType.get()),
                    Types.NestedField.optional(20007, "upper_bound", Types.StringType.get()))));

    @SuppressWarnings("unchecked")
    List<FieldStats<?>> fieldStatsList =
        ImmutableList.of(
            (FieldStats<?>)
                BaseFieldStats.<Integer>builder()
                    .fieldId(1)
                    .type(Types.IntegerType.get())
                    .lowerBound(42)
                    .upperBound(42)
                    .build(),
            (FieldStats<?>)
                BaseFieldStats.<CharSequence>builder()
                    .fieldId(2)
                    .type(Types.StringType.get())
                    .lowerBound("electronics")
                    .upperBound("electronics")
                    .build());

    BaseContentStats stats =
        BaseContentStats.builder()
            .withStatsStruct(statsStruct)
            .withFieldStats(fieldStatsList)
            .build();

    TrackedFileStruct file =
        new TrackedFileStruct(
            null, FileContent.DATA, "s3://bucket/data.parquet", FileFormat.PARQUET, 100L, 1024L);
    file.set(6, spec.specId());
    file.set(7, stats);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, spec);

    StructLike partition = dataFile.partition();
    assertThat(partition).isNotNull();
    assertThat(partition.get(0, Integer.class)).isEqualTo(42);
    assertThat(partition.get(1, CharSequence.class).toString()).isEqualTo("electronics");
  }

  @Test
  void testPartitionWithVoidTransform() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").alwaysNull("data").build();

    TrackedFileStruct file = createTrackedFileWithFieldStats(1, Types.IntegerType.get(), 42);
    DataFile dataFile = TrackedFileAdapters.asDataFile(file, spec);

    StructLike partition = dataFile.partition();
    assertThat(partition).isNotNull();
    assertThat(partition.get(0, Integer.class)).isEqualTo(42);
    assertThat(partition.get(1, CharSequence.class)).isNull();
  }

  @Test
  void testDeleteFilePartitionExtracted() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "category", Types.StringType.get()));

    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("category").build();

    TrackedFileStruct file = createTrackedFileWithPartitionStats(spec);
    file.set(1, FileContent.EQUALITY_DELETES.id());
    file.set(13, ImmutableList.of(1));

    DeleteFile deleteFile = TrackedFileAdapters.asDeleteFile(file, spec);

    StructLike partition = deleteFile.partition();
    assertThat(partition).isNotNull();
    assertThat(partition.get(0, CharSequence.class).toString()).isEqualTo("electronics");
  }

  @Test
  void testSpecIdDefaultsToZeroWhenNull() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null, FileContent.DATA, "s3://bucket/data.parquet", FileFormat.PARQUET, 100L, 1024L);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, PartitionSpec.unpartitioned());
    assertThat(dataFile.specId()).isEqualTo(0);
  }

  private static java.util.Map.Entry<Integer, Long> entry(int key, long value) {
    return java.util.Map.entry(key, value);
  }

  @SuppressWarnings("unchecked")
  private static TrackedFileStruct createTrackedFileWithPartitionStats(PartitionSpec spec) {
    Types.StructType statsStruct =
        Types.StructType.of(
            Types.NestedField.optional(
                20000,
                "2",
                Types.StructType.of(
                    Types.NestedField.optional(20006, "lower_bound", Types.StringType.get()),
                    Types.NestedField.optional(20007, "upper_bound", Types.StringType.get()))));

    List<FieldStats<?>> fieldStatsList =
        ImmutableList.of(
            (FieldStats<?>)
                BaseFieldStats.<CharSequence>builder()
                    .fieldId(2)
                    .type(Types.StringType.get())
                    .lowerBound("electronics")
                    .upperBound("electronics")
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
    file.set(6, spec.specId());
    file.set(7, stats);

    return file;
  }

  @SuppressWarnings("unchecked")
  private static <T> TrackedFileStruct createTrackedFileWithFieldStats(
      int fieldId, Type type, T value) {
    int statsFieldId = fieldId * 10000;
    Types.StructType statsStruct =
        Types.StructType.of(
            Types.NestedField.optional(
                statsFieldId,
                Integer.toString(fieldId),
                Types.StructType.of(
                    Types.NestedField.optional(statsFieldId + 6, "lower_bound", type),
                    Types.NestedField.optional(statsFieldId + 7, "upper_bound", type))));

    List<FieldStats<?>> fieldStatsList =
        ImmutableList.of(
            (FieldStats<?>)
                BaseFieldStats.<T>builder()
                    .fieldId(fieldId)
                    .type(type)
                    .lowerBound(value)
                    .upperBound(value)
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
