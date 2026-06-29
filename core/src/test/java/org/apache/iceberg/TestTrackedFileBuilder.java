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
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTrackedFileBuilder {
  private static final int FORMAT_VERSION_V4 = 4;
  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final Types.StructType PARTITION_TYPE =
      PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").build().partitionType();
  private static final PartitionData PARTITION_DATA = new PartitionData(PARTITION_TYPE);
  private static final ManifestInfo MANIFEST_INFO =
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
  private static final DeletionVector DELETION_VECTOR =
      DeletionVectorStruct.builder()
          .location("s3://bucket/data/dv.puffin")
          .offset(0L)
          .sizeInBytes(128L)
          .cardinality(10L)
          .build();
  private static final ContentStats CONTENT_STATS =
      BaseContentStats.builder()
          .withTableSchema(TABLE_SCHEMA)
          .withFieldStats(
              BaseFieldStats.<Integer>builder()
                  .fieldId(1)
                  .type(Types.IntegerType.get())
                  .valueCount(2000L)
                  .nullValueCount(0L)
                  .lowerBound(1)
                  .upperBound(1000)
                  .build())
          .withFieldStats(
              BaseFieldStats.<String>builder()
                  .fieldId(2)
                  .type(Types.StringType.get())
                  .valueCount(2000L)
                  .nullValueCount(5L)
                  .lowerBound("a")
                  .upperBound("z")
                  .build())
          .build();
  private static final ByteBuffer KEY_METADATA = ByteBuffer.wrap(new byte[] {1, 2, 3});
  private static final ImmutableList<Long> SPLIT_OFFSETS = ImmutableList.of(0L, 4096L, 8192L);
  private static final ByteBuffer DELETED_POSITIONS = ByteBuffer.wrap(new byte[] {10, 11, 12});
  private static final ByteBuffer REPLACED_POSITIONS = ByteBuffer.wrap(new byte[] {20, 21, 22});

  private static Stream<Arguments> missingRequiredFieldCases() {
    return Stream.of(
        Arguments.of("formatVersion", "Missing required field: format version"),
        Arguments.of("location", "Missing required field: location"),
        Arguments.of("fileFormat", "Missing required field: file format"),
        Arguments.of("recordCount", "Missing required field: record count"),
        Arguments.of("fileSizeInBytes", "Missing required field: file size in bytes"),
        Arguments.of("partition", "Missing required field: partition data"));
  }

  @ParameterizedTest
  @MethodSource("missingRequiredFieldCases")
  public void missingRequiredFields(String missingField, String expectedMessage) {
    TrackedFileBuilder dataBuilder =
        builderWithMissingRequiredField(TrackedFileBuilder.data(50L), missingField);
    TrackedFileBuilder equalityDeleteBuilder =
        builderWithMissingRequiredField(TrackedFileBuilder.equalityDelete(50L), missingField);
    TrackedFileBuilder dataManifestBuilder =
        builderWithMissingRequiredField(TrackedFileBuilder.dataManifest(50L), missingField);
    TrackedFileBuilder deleteManifestBuilder =
        builderWithMissingRequiredField(TrackedFileBuilder.deleteManifest(50L), missingField);

    assertThatThrownBy(dataBuilder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
    assertThatThrownBy(equalityDeleteBuilder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
    assertThatThrownBy(dataManifestBuilder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
    assertThatThrownBy(deleteManifestBuilder::build)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  private TrackedFileBuilder builderWithMissingRequiredField(
      TrackedFileBuilder builder, String missingField) {
    if (!"formatVersion".equals(missingField)) {
      builder.formatVersion(FORMAT_VERSION_V4);
    }
    if (!"location".equals(missingField)) {
      builder.location("s3://bucket/data/file");
    }
    if (!"fileFormat".equals(missingField)) {
      builder.fileFormat(FileFormat.PARQUET);
    }
    if (!"recordCount".equals(missingField)) {
      builder.recordCount(2000L);
    }
    if (!"fileSizeInBytes".equals(missingField)) {
      builder.fileSizeInBytes(12345L);
    }
    if (!"partition".equals(missingField)) {
      builder.partition(PARTITION_DATA);
    }
    return builder;
  }

  @ParameterizedTest
  @MethodSource("manifestBuilders")
  public void missingFieldsForManifests(TrackedFileBuilder builder, FileContent contentType) {
    assertThatThrownBy(
            () ->
                builder
                    .formatVersion(FORMAT_VERSION_V4)
                    .location("s3://bucket/data/manifest.avro")
                    .fileFormat(FileFormat.AVRO)
                    .recordCount(420L)
                    .fileSizeInBytes(556L)
                    .partition(PARTITION_DATA)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required field: manifest info");
  }

  @Test
  public void missingEqualityIdsForEqualityDeletes() {
    assertThatThrownBy(
            () ->
                TrackedFileBuilder.equalityDelete(50L)
                    .formatVersion(FORMAT_VERSION_V4)
                    .location("s3://bucket/data/eq_delete.parquet")
                    .fileFormat(FileFormat.PARQUET)
                    .recordCount(2000L)
                    .fileSizeInBytes(12345L)
                    .partition(PARTITION_DATA)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing required field: equality IDs");
  }

  private static Stream<Arguments> nonEqualityDeleteBuilders() {
    return Stream.of(
        Arguments.of(TrackedFileBuilder.data(10L), FileContent.DATA),
        Arguments.of(TrackedFileBuilder.dataManifest(10L), FileContent.DATA_MANIFEST),
        Arguments.of(TrackedFileBuilder.deleteManifest(10L), FileContent.DELETE_MANIFEST),
        Arguments.of(TrackedFileBuilder.from(sourceData(12L), 20L), FileContent.DATA),
        Arguments.of(
            TrackedFileBuilder.from(sourceDataManifest(21L), 25L), FileContent.DATA_MANIFEST),
        Arguments.of(
            TrackedFileBuilder.from(sourceDeleteManifest(12L), 20L), FileContent.DELETE_MANIFEST));
  }

  @ParameterizedTest
  @MethodSource("nonEqualityDeleteBuilders")
  public void invalidEqualityIdsForContentType(
      TrackedFileBuilder builder, FileContent contentType) {
    assertThatThrownBy(() -> builder.equalityIds(ImmutableList.of(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Equality IDs can only be added to EQUALITY_DELETES entries, but entry type is: "
                + contentType);
  }

  private static Stream<Arguments> nonDataBuilders() {
    return Stream.of(
        Arguments.of(TrackedFileBuilder.equalityDelete(10L), FileContent.EQUALITY_DELETES),
        Arguments.of(TrackedFileBuilder.dataManifest(10L), FileContent.DATA_MANIFEST),
        Arguments.of(TrackedFileBuilder.deleteManifest(10L), FileContent.DELETE_MANIFEST),
        Arguments.of(
            TrackedFileBuilder.from(sourceEqualityDelete(12L), 20L), FileContent.EQUALITY_DELETES),
        Arguments.of(
            TrackedFileBuilder.from(sourceDataManifest(21L), 25L), FileContent.DATA_MANIFEST),
        Arguments.of(
            TrackedFileBuilder.from(sourceDeleteManifest(12L), 20L), FileContent.DELETE_MANIFEST));
  }

  @ParameterizedTest
  @MethodSource("nonDataBuilders")
  public void invalidDeletionVectorForContentType(
      TrackedFileBuilder builder, FileContent contentType) {
    assertThatThrownBy(() -> builder.deletionVector(DELETION_VECTOR))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Deletion vector can only be added to DATA entries, but entry type is: " + contentType);
  }

  @ParameterizedTest
  @MethodSource("manifestBuilders")
  public void invalidSortOrderIdForContentType(
      TrackedFileBuilder builder, FileContent contentType) {
    assertThatThrownBy(() -> builder.sortOrderId(1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Sort order ID cannot be added to manifest entries, but entry type is: " + contentType);
  }

  @ParameterizedTest
  @MethodSource("manifestBuilders")
  public void invalidSplitOffsetsForContentType(
      TrackedFileBuilder builder, FileContent contentType) {
    assertThatThrownBy(() -> builder.splitOffsets(SPLIT_OFFSETS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Split offsets cannot be added to manifest entries, but entry type is: " + contentType);
  }

  private static Stream<Arguments> nonManifestBuilders() {
    return Stream.of(
        Arguments.of(TrackedFileBuilder.data(10L), FileContent.DATA),
        Arguments.of(TrackedFileBuilder.equalityDelete(10L), FileContent.EQUALITY_DELETES),
        Arguments.of(TrackedFileBuilder.from(sourceData(12L), 20L), FileContent.DATA),
        Arguments.of(
            TrackedFileBuilder.from(sourceEqualityDelete(12L), 20L), FileContent.EQUALITY_DELETES));
  }

  @ParameterizedTest
  @MethodSource("nonManifestBuilders")
  public void invalidManifestInfoForContentType(
      TrackedFileBuilder builder, FileContent contentType) {
    assertThatThrownBy(() -> builder.manifestInfo(MANIFEST_INFO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Manifest info can only be added to manifests, but entry type is: " + contentType);
  }

  @ParameterizedTest
  @MethodSource("nonManifestBuilders")
  public void invalidDeletedPositionsForContentType(
      TrackedFileBuilder builder, FileContent contentType) {
    assertThatThrownBy(() -> builder.deletedPositions(DELETED_POSITIONS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Deleted positions can only be added to manifest entries, but entry type is: "
                + contentType);
  }

  @ParameterizedTest
  @MethodSource("nonManifestBuilders")
  public void invalidReplacedPositionsForContentType(
      TrackedFileBuilder builder, FileContent contentType) {
    assertThatThrownBy(() -> builder.replacedPositions(REPLACED_POSITIONS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Replaced positions can only be added to manifest entries, but entry type is: "
                + contentType);
  }

  @Test
  public void invalidNullInputs() {
    assertThatThrownBy(() -> TrackedFileBuilder.data(30L).location(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: null");

    assertThatThrownBy(() -> TrackedFileBuilder.data(30L).fileFormat(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file format: null");

    assertThatThrownBy(() -> TrackedFileBuilder.data(30L).partition(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid partition: null");

    assertThatThrownBy(() -> TrackedFileBuilder.data(30L).contentStats(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content stats: null");

    assertThatThrownBy(() -> TrackedFileBuilder.data(30L).deletionVector(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid deletion vector: null");

    assertThatThrownBy(() -> TrackedFileBuilder.dataManifest(30L).manifestInfo(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid manifest info: null");

    assertThatThrownBy(() -> TrackedFileBuilder.data(30L).keyMetadata(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key metadata: null");

    assertThatThrownBy(() -> TrackedFileBuilder.data(30L).splitOffsets(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid split offsets: null");

    assertThatThrownBy(() -> TrackedFileBuilder.equalityDelete(30L).equalityIds(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid equality IDs: null");

    assertThatThrownBy(() -> TrackedFileBuilder.from(null, 20L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid source: null");

    assertThatThrownBy(
            () ->
                TrackedFileBuilder.from(
                        entryWithInheritedSeqNums(sourceDataManifest(10L), 15L), 20L)
                    .deletedPositions(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid deleted positions: null");

    assertThatThrownBy(
            () ->
                TrackedFileBuilder.from(
                        entryWithInheritedSeqNums(sourceDeleteManifest(100L), 150L), 200L)
                    .replacedPositions(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid replaced positions: null");

    assertThatThrownBy(() -> TrackedFileBuilder.deleted(null, 20L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid source: null");

    assertThatThrownBy(() -> TrackedFileBuilder.replaced(null, 20L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid source: null");
  }

  @Test
  public void invalidNegativeInputs() {
    assertThatThrownBy(() -> TrackedFileBuilder.dataManifest(40L).formatVersion(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid format version: -1 (must be >= 0)");

    assertThatThrownBy(() -> TrackedFileBuilder.dataManifest(40L).recordCount(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid record count: -1 (must be >= 0)");

    assertThatThrownBy(() -> TrackedFileBuilder.dataManifest(40L).fileSizeInBytes(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file size in bytes: -1 (must be >= 0)");

    assertThatThrownBy(() -> TrackedFileBuilder.dataManifest(40L).specId(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid spec ID: -1 (must be >= 0)");

    assertThatThrownBy(() -> TrackedFileBuilder.data(40L).sortOrderId(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid sort order ID: -1 (must be >= 0)");
  }

  @Test
  public void buildDataFileWithRequiredFieldsOnly() {
    TrackedFile trackedFile =
        TrackedFileBuilder.data(50L)
            .formatVersion(FORMAT_VERSION_V4)
            .location("s3://bucket/data/file.parquet")
            .fileFormat(FileFormat.PARQUET)
            .recordCount(2000L)
            .fileSizeInBytes(12345L)
            .partition(PARTITION_DATA)
            .build();

    assertThat(trackedFile.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(trackedFile.contentType()).isEqualTo(FileContent.DATA);
    assertThat(trackedFile.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(trackedFile.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(trackedFile.recordCount()).isEqualTo(2000L);
    assertThat(trackedFile.fileSizeInBytes()).isEqualTo(12345L);
    assertThat(trackedFile.partition()).isSameAs(PARTITION_DATA);

    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(trackedFile.tracking().snapshotId()).isEqualTo(50L);
    assertThat(trackedFile.tracking().dvSnapshotId()).isNull();

    assertThat(trackedFile.specId()).isNull();
    assertThat(trackedFile.contentStats()).isNull();
    assertThat(trackedFile.sortOrderId()).isNull();
    assertThat(trackedFile.deletionVector()).isNull();
    assertThat(trackedFile.manifestInfo()).isNull();
    assertThat(trackedFile.keyMetadata()).isNull();
    assertThat(trackedFile.splitOffsets()).isNull();
    assertThat(trackedFile.equalityIds()).isNull();
  }

  @Test
  public void buildDataFileWithAllFields() {
    TrackedFile trackedFile =
        TrackedFileBuilder.data(50L)
            .formatVersion(FORMAT_VERSION_V4)
            .location("s3://bucket/data/file.parquet")
            .fileFormat(FileFormat.PARQUET)
            .recordCount(2000L)
            .fileSizeInBytes(12345L)
            .specId(7)
            .partition(PARTITION_DATA)
            .contentStats(CONTENT_STATS)
            .sortOrderId(3)
            .deletionVector(DELETION_VECTOR)
            .keyMetadata(KEY_METADATA)
            .splitOffsets(SPLIT_OFFSETS)
            .build();

    assertThat(trackedFile.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(trackedFile.contentType()).isEqualTo(FileContent.DATA);
    assertThat(trackedFile.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(trackedFile.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(trackedFile.recordCount()).isEqualTo(2000L);
    assertThat(trackedFile.fileSizeInBytes()).isEqualTo(12345L);
    assertThat(trackedFile.specId()).isEqualTo(7);
    assertThat(trackedFile.partition()).isSameAs(PARTITION_DATA);
    assertThat(trackedFile.contentStats()).isSameAs(CONTENT_STATS);
    assertThat(trackedFile.sortOrderId()).isEqualTo(3);
    assertThat(trackedFile.deletionVector()).isSameAs(DELETION_VECTOR);
    assertThat(trackedFile.keyMetadata()).isEqualTo(KEY_METADATA);
    assertThat(trackedFile.splitOffsets()).isEqualTo(SPLIT_OFFSETS);

    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(trackedFile.tracking().snapshotId()).isEqualTo(50L);
    assertThat(trackedFile.tracking().dvSnapshotId()).isEqualTo(50L);

    // Unsupported fields for data files
    assertThat(trackedFile.manifestInfo()).isNull();
    assertThat(trackedFile.equalityIds()).isNull();
  }

  @Test
  public void buildEqualityDeleteFileWithRequiredFieldsOnly() {
    TrackedFile trackedFile =
        TrackedFileBuilder.equalityDelete(50L)
            .formatVersion(FORMAT_VERSION_V4)
            .location("s3://bucket/data/eq_delete.parquet")
            .fileFormat(FileFormat.PARQUET)
            .recordCount(2000L)
            .fileSizeInBytes(12345L)
            .partition(PARTITION_DATA)
            .equalityIds(ImmutableList.of(1))
            .build();

    assertThat(trackedFile.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(trackedFile.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(trackedFile.location()).isEqualTo("s3://bucket/data/eq_delete.parquet");
    assertThat(trackedFile.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(trackedFile.recordCount()).isEqualTo(2000L);
    assertThat(trackedFile.fileSizeInBytes()).isEqualTo(12345L);
    assertThat(trackedFile.partition()).isSameAs(PARTITION_DATA);
    assertThat(trackedFile.equalityIds()).containsExactly(1);

    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(trackedFile.tracking().snapshotId()).isEqualTo(50L);

    assertThat(trackedFile.specId()).isNull();
    assertThat(trackedFile.contentStats()).isNull();
    assertThat(trackedFile.sortOrderId()).isNull();
    assertThat(trackedFile.deletionVector()).isNull();
    assertThat(trackedFile.manifestInfo()).isNull();
    assertThat(trackedFile.keyMetadata()).isNull();
    assertThat(trackedFile.splitOffsets()).isNull();
  }

  @Test
  public void buildEqualityDeleteFileWithAllFields() {
    TrackedFile trackedFile =
        TrackedFileBuilder.equalityDelete(50L)
            .formatVersion(FORMAT_VERSION_V4)
            .location("s3://bucket/data/eq_delete.parquet")
            .fileFormat(FileFormat.PARQUET)
            .recordCount(2000L)
            .fileSizeInBytes(12345L)
            .specId(7)
            .partition(PARTITION_DATA)
            .contentStats(CONTENT_STATS)
            .keyMetadata(KEY_METADATA)
            .splitOffsets(SPLIT_OFFSETS)
            .sortOrderId(3)
            .equalityIds(ImmutableList.of(1, 2))
            .build();

    assertThat(trackedFile.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(trackedFile.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(trackedFile.location()).isEqualTo("s3://bucket/data/eq_delete.parquet");
    assertThat(trackedFile.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(trackedFile.recordCount()).isEqualTo(2000L);
    assertThat(trackedFile.fileSizeInBytes()).isEqualTo(12345L);
    assertThat(trackedFile.specId()).isEqualTo(7);
    assertThat(trackedFile.partition()).isSameAs(PARTITION_DATA);
    assertThat(trackedFile.contentStats()).isSameAs(CONTENT_STATS);
    assertThat(trackedFile.keyMetadata()).isEqualTo(KEY_METADATA);
    assertThat(trackedFile.splitOffsets()).isEqualTo(SPLIT_OFFSETS);
    assertThat(trackedFile.sortOrderId()).isEqualTo(3);
    assertThat(trackedFile.equalityIds()).containsExactly(1, 2);

    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(trackedFile.tracking().snapshotId()).isEqualTo(50L);

    // Unsupported fields for equality delete files
    assertThat(trackedFile.deletionVector()).isNull();
    assertThat(trackedFile.manifestInfo()).isNull();
  }

  private static Stream<Arguments> manifestBuilders() {
    return Stream.of(
        Arguments.of(TrackedFileBuilder.dataManifest(50L), FileContent.DATA_MANIFEST),
        Arguments.of(TrackedFileBuilder.deleteManifest(50L), FileContent.DELETE_MANIFEST));
  }

  @ParameterizedTest
  @MethodSource("manifestBuilders")
  public void buildManifestWithRequiredFieldsOnly(
      TrackedFileBuilder builder, FileContent contentType) {
    TrackedFile trackedFile =
        builder
            .formatVersion(FORMAT_VERSION_V4)
            .location("s3://bucket/data/manifest.avro")
            .fileFormat(FileFormat.AVRO)
            .recordCount(420L)
            .fileSizeInBytes(556L)
            .partition(PARTITION_DATA)
            .manifestInfo(MANIFEST_INFO)
            .build();

    assertThat(trackedFile.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(trackedFile.contentType()).isEqualTo(contentType);
    assertThat(trackedFile.location()).isEqualTo("s3://bucket/data/manifest.avro");
    assertThat(trackedFile.fileFormat()).isEqualTo(FileFormat.AVRO);
    assertThat(trackedFile.recordCount()).isEqualTo(420L);
    assertThat(trackedFile.fileSizeInBytes()).isEqualTo(556L);
    assertThat(trackedFile.partition()).isSameAs(PARTITION_DATA);
    assertThat(trackedFile.manifestInfo()).isSameAs(MANIFEST_INFO);

    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(trackedFile.tracking().snapshotId()).isEqualTo(50L);

    assertThat(trackedFile.specId()).isNull();
    assertThat(trackedFile.contentStats()).isNull();
    assertThat(trackedFile.sortOrderId()).isNull();
    assertThat(trackedFile.deletionVector()).isNull();
    assertThat(trackedFile.keyMetadata()).isNull();
    assertThat(trackedFile.splitOffsets()).isNull();
    assertThat(trackedFile.equalityIds()).isNull();
  }

  @ParameterizedTest
  @MethodSource("manifestBuilders")
  public void buildManifestWithAllFields(TrackedFileBuilder builder, FileContent contentType) {
    TrackedFile trackedFile =
        builder
            .formatVersion(FORMAT_VERSION_V4)
            .location("s3://bucket/data/manifest.avro")
            .fileFormat(FileFormat.AVRO)
            .recordCount(420L)
            .fileSizeInBytes(556L)
            .specId(7)
            .partition(PARTITION_DATA)
            .contentStats(CONTENT_STATS)
            .keyMetadata(KEY_METADATA)
            .manifestInfo(MANIFEST_INFO)
            .build();

    assertThat(trackedFile.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(trackedFile.contentType()).isEqualTo(contentType);
    assertThat(trackedFile.location()).isEqualTo("s3://bucket/data/manifest.avro");
    assertThat(trackedFile.fileFormat()).isEqualTo(FileFormat.AVRO);
    assertThat(trackedFile.recordCount()).isEqualTo(420L);
    assertThat(trackedFile.fileSizeInBytes()).isEqualTo(556L);
    assertThat(trackedFile.specId()).isEqualTo(7);
    assertThat(trackedFile.partition()).isSameAs(PARTITION_DATA);
    assertThat(trackedFile.contentStats()).isSameAs(CONTENT_STATS);
    assertThat(trackedFile.keyMetadata()).isEqualTo(KEY_METADATA);
    assertThat(trackedFile.manifestInfo()).isSameAs(MANIFEST_INFO);

    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(trackedFile.tracking().snapshotId()).isEqualTo(50L);

    // Unsupported fields for manifests
    assertThat(trackedFile.sortOrderId()).isNull();
    assertThat(trackedFile.deletionVector()).isNull();
    assertThat(trackedFile.splitOffsets()).isNull();
    assertThat(trackedFile.equalityIds()).isNull();
  }

  private static Stream<Arguments> manifestSources() {
    return Stream.of(
        Arguments.of(sourceDataManifest(10L), FileContent.DATA_MANIFEST),
        Arguments.of(sourceDeleteManifest(10L), FileContent.DELETE_MANIFEST));
  }

  @ParameterizedTest
  @MethodSource("manifestSources")
  public void buildManifestFromSourceWithDeletedPositions(
      TrackedFile source, FileContent contentType) {
    entryWithInheritedSeqNums(source, 7L);

    TrackedFile trackedFile =
        TrackedFileBuilder.from(source, 20L).deletedPositions(DELETED_POSITIONS).build();

    assertThat(trackedFile.contentType()).isEqualTo(contentType);
    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.MODIFIED);
    assertThat(trackedFile.tracking().dvSnapshotId()).isEqualTo(20L);
    assertThat(trackedFile.tracking().deletedPositions()).isEqualTo(DELETED_POSITIONS);
    assertThat(trackedFile.tracking().replacedPositions()).isNull();
  }

  @ParameterizedTest
  @MethodSource("manifestSources")
  public void buildManifestFromSourceWithReplacedPositions(
      TrackedFile source, FileContent contentType) {
    entryWithInheritedSeqNums(source, 7L);

    TrackedFile trackedFile =
        TrackedFileBuilder.from(source, 20L).replacedPositions(REPLACED_POSITIONS).build();

    assertThat(trackedFile.contentType()).isEqualTo(contentType);
    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.MODIFIED);
    assertThat(trackedFile.tracking().dvSnapshotId()).isEqualTo(20L);
    assertThat(trackedFile.tracking().deletedPositions()).isNull();
    assertThat(trackedFile.tracking().replacedPositions()).isEqualTo(REPLACED_POSITIONS);
  }

  @ParameterizedTest
  @MethodSource("manifestSources")
  public void buildManifestFromSourceClearsPositions(TrackedFile source, FileContent contentType) {
    entryWithInheritedSeqNums(source, 7L);

    TrackedFile sourceWithPositions =
        TrackedFileBuilder.from(source, 15L)
            .deletedPositions(DELETED_POSITIONS)
            .replacedPositions(REPLACED_POSITIONS)
            .build();

    TrackedFile newEntry = TrackedFileBuilder.from(sourceWithPositions, 20L).build();

    // Building a new entry from this source should not carry the positions over
    assertThat(newEntry.contentType()).isEqualTo(contentType);
    assertThat(newEntry.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(newEntry.tracking().deletedPositions()).isNull();
    assertThat(newEntry.tracking().replacedPositions()).isNull();
    assertThat(newEntry.tracking().dvSnapshotId()).isEqualTo(15L);
  }

  @Test
  public void buildDataFileFromSource() {
    TrackedFile source = entryWithInheritedSeqNums(sourceData(10L), 45L);

    TrackedFile trackedFile = TrackedFileBuilder.from(source, 20L).build();

    assertThat(trackedFile.contentType()).isEqualTo(FileContent.DATA);
    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(trackedFile.tracking().snapshotId()).isEqualTo(source.tracking().snapshotId());
    verifyFieldsAreFromSource(trackedFile, source);
  }

  @Test
  public void updateDVWhenBuildingDataFileFromSource() {
    TrackedFile source = entryWithInheritedSeqNums(sourceData(10L), 45L);

    DeletionVector dv =
        DeletionVectorStruct.builder()
            .location("s3://bucket/data/new_dv.puffin")
            .offset(5L)
            .sizeInBytes(256L)
            .cardinality(40L)
            .build();

    TrackedFile trackedFile = TrackedFileBuilder.from(source, 20L).deletionVector(dv).build();

    assertThat(trackedFile.deletionVector()).isNotSameAs(source.deletionVector()).isSameAs(dv);
    assertThat(trackedFile.tracking().status()).isEqualTo(EntryStatus.MODIFIED);
    assertThat(trackedFile.tracking().snapshotId()).isEqualTo(10L);
    assertThat(trackedFile.tracking().dataSequenceNumber()).isEqualTo(45L);
    assertThat(trackedFile.tracking().fileSequenceNumber()).isEqualTo(45L);
    assertThat(trackedFile.tracking().dvSnapshotId()).isEqualTo(20L);
  }

  @Test
  public void addingSameDeletionVectorFails() {
    TrackedFile source = entryWithInheritedSeqNums(sourceData(10L), 45L);

    DeletionVector dv =
        DeletionVectorStruct.builder()
            .location("s3://bucket/data/new_dv.puffin")
            .offset(5L)
            .sizeInBytes(256L)
            .cardinality(40L)
            .build();

    DeletionVector dvCopy = dv.copy();

    TrackedFile trackedFile = TrackedFileBuilder.from(source, 20L).deletionVector(dv).build();

    assertThatThrownBy(() -> TrackedFileBuilder.from(trackedFile, 30L).deletionVector(dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The same deletion vector already added");
    assertThatThrownBy(() -> TrackedFileBuilder.from(trackedFile, 30L).deletionVector(dvCopy))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The same deletion vector already added");
  }

  private static Stream<Arguments> nonManifestSources() {
    return Stream.of(
        Arguments.of(sourceData(10L), FileContent.DATA),
        Arguments.of(sourceEqualityDelete(10L), FileContent.EQUALITY_DELETES));
  }

  private static Stream<Arguments> allSources() {
    return Stream.concat(nonManifestSources(), manifestSources());
  }

  @ParameterizedTest
  @MethodSource("allSources")
  public void deletedFromSource(TrackedFile source, FileContent contentType) {
    entryWithInheritedSeqNums(source, 15L);

    TrackedFile deleted = TrackedFileBuilder.deleted(source, 20L);

    assertThat(deleted.contentType()).isEqualTo(contentType);
    assertThat(deleted.tracking().status()).isEqualTo(EntryStatus.DELETED);
    assertThat(deleted.tracking().snapshotId()).isEqualTo(20L);
    verifyFieldsAreFromSource(deleted, source);
  }

  @ParameterizedTest
  @MethodSource("nonManifestSources")
  public void replacedFromNonManifestSource(TrackedFile source, FileContent contentType) {
    entryWithInheritedSeqNums(source, 15L);

    TrackedFile replaced = TrackedFileBuilder.replaced(source, 20L);

    assertThat(replaced.contentType()).isEqualTo(contentType);
    assertThat(replaced.tracking().status()).isEqualTo(EntryStatus.REPLACED);
    assertThat(replaced.tracking().snapshotId()).isEqualTo(20L);
    verifyFieldsAreFromSource(replaced, source);
  }

  @ParameterizedTest
  @MethodSource("manifestSources")
  public void replacedFromManifestSourceFails(TrackedFile source, FileContent contentType) {
    entryWithInheritedSeqNums(source, 15L);

    assertThatThrownBy(() -> TrackedFileBuilder.replaced(source, 20L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Manifest entries cannot transition to REPLACED, but entry type is: " + contentType);
  }

  private static TrackedFile sourceData(long snapshotId) {
    return TrackedFileBuilder.data(snapshotId)
        .formatVersion(FORMAT_VERSION_V4)
        .location("s3://bucket/data/file.parquet")
        .fileFormat(FileFormat.PARQUET)
        .recordCount(2000L)
        .fileSizeInBytes(12345L)
        .partition(PARTITION_DATA)
        .specId(7)
        .contentStats(CONTENT_STATS)
        .sortOrderId(3)
        .deletionVector(DELETION_VECTOR)
        .keyMetadata(KEY_METADATA)
        .splitOffsets(SPLIT_OFFSETS)
        .build();
  }

  private static TrackedFile sourceEqualityDelete(long snapshotId) {
    return TrackedFileBuilder.equalityDelete(snapshotId)
        .formatVersion(FORMAT_VERSION_V4)
        .location("s3://bucket/data/eq_delete.parquet")
        .fileFormat(FileFormat.PARQUET)
        .recordCount(2000L)
        .fileSizeInBytes(12345L)
        .partition(PARTITION_DATA)
        .equalityIds(ImmutableList.of(1))
        .build();
  }

  private static TrackedFile sourceDataManifest(long snapshotId) {
    return TrackedFileBuilder.dataManifest(snapshotId)
        .formatVersion(FORMAT_VERSION_V4)
        .location("s3://bucket/data/data_manifest.parquet")
        .fileFormat(FileFormat.PARQUET)
        .recordCount(420L)
        .fileSizeInBytes(556L)
        .partition(PARTITION_DATA)
        .manifestInfo(MANIFEST_INFO)
        .build();
  }

  private static TrackedFile sourceDeleteManifest(long snapshotId) {
    return TrackedFileBuilder.deleteManifest(snapshotId)
        .formatVersion(FORMAT_VERSION_V4)
        .location("s3://bucket/data/delete_manifest.parquet")
        .fileFormat(FileFormat.PARQUET)
        .recordCount(100L)
        .fileSizeInBytes(543L)
        .partition(PARTITION_DATA)
        .manifestInfo(MANIFEST_INFO)
        .build();
  }

  private static TrackedFile entryWithInheritedSeqNums(TrackedFile entry, long sequenceNumber) {
    Tracking manifestTrackingToInheritFrom =
        new TrackingStruct(
            EntryStatus.EXISTING, 123L, sequenceNumber, sequenceNumber, null, null, null, null);

    ((TrackingStruct) entry.tracking()).inheritFrom(manifestTrackingToInheritFrom);
    return entry;
  }

  /**
   * Verifies that fields in entry are the same as in source. Note, snapshot ID can't be verified
   * here, because based on the entry's status it is either carried over or not.
   */
  private static void verifyFieldsAreFromSource(TrackedFile entry, TrackedFile source) {
    assertThat(entry.formatVersion()).isEqualTo(source.formatVersion());
    assertThat(entry.location()).isEqualTo(source.location());
    assertThat(entry.fileFormat()).isEqualTo(source.fileFormat());
    assertThat(entry.recordCount()).isEqualTo(source.recordCount());
    assertThat(entry.fileSizeInBytes()).isEqualTo(source.fileSizeInBytes());
    assertThat(entry.specId()).isEqualTo(source.specId());
    assertThat(entry.partition()).isSameAs(source.partition());
    assertThat(entry.contentStats()).isSameAs(source.contentStats());
    assertThat(entry.sortOrderId()).isEqualTo(source.sortOrderId());
    assertThat(entry.deletionVector()).isSameAs(source.deletionVector());
    assertThat(entry.keyMetadata()).isEqualTo(source.keyMetadata());
    assertThat(entry.splitOffsets()).isEqualTo(source.splitOffsets());
    assertThat(entry.manifestInfo()).isSameAs(source.manifestInfo());
    assertThat(entry.equalityIds()).isEqualTo(source.equalityIds());

    assertThat(entry.tracking().dataSequenceNumber())
        .isEqualTo(source.tracking().dataSequenceNumber());
    assertThat(entry.tracking().fileSequenceNumber())
        .isEqualTo(source.tracking().fileSequenceNumber());
    assertThat(entry.tracking().dvSnapshotId()).isEqualTo(source.tracking().dvSnapshotId());
    assertThat(entry.tracking().firstRowId()).isEqualTo(source.tracking().firstRowId());
    assertThat(entry.tracking().deletedPositions()).isEqualTo(source.tracking().deletedPositions());
    assertThat(entry.tracking().replacedPositions())
        .isEqualTo(source.tracking().replacedPositions());
  }
}
