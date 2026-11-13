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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.mumbling.MumblingTestUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UnicodeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

class TestTrackedFileAdapters {

  private static final int FORMAT_VERSION_V4 = 4;
  private static final String MANIFEST_LOCATION = "s3://bucket/table/manifest.parquet";
  private static final String DATA_FILE_LOCATION = "s3://bucket/data/file.parquet";
  private static final String DV_LOCATION = "s3://bucket/puffin/dv-file.bin";
  private static final long MANIFEST_FILE_SIZE = 2048L;

  // Tracking values that the delegation tests validate.
  private static final long MANIFEST_POS = 3L;
  private static final long SNAPSHOT_ID = 42L;
  private static final long DATA_SEQUENCE_NUMBER = 10L;
  private static final long FILE_SEQUENCE_NUMBER = 11L;
  private static final long FIRST_ROW_ID = 1000L;
  private static final int SORT_ORDER_ID = 3;

  private static final long MANIFEST_SEQUENCE_NUMBER = 5L;
  private static final long MANIFEST_MIN_SEQUENCE_NUMBER = 4L;
  private static final int ADDED_FILES_COUNT = 2;
  private static final long ADDED_ROWS_COUNT = 200L;
  private static final int EXISTING_FILES_COUNT = 3;
  private static final long EXISTING_ROWS_COUNT = 300L;
  private static final int DELETED_FILES_COUNT = 1;
  private static final long DELETED_ROWS_COUNT = 100L;
  private static final int NON_ZERO_FILE_COUNT = 1;

  private static final int UNPARTITIONED_SPEC_ID = PartitionSpec.unpartitioned().specId();
  private static final Map<Integer, PartitionSpec> UNPARTITIONED =
      ImmutableMap.of(UNPARTITIONED_SPEC_ID, PartitionSpec.unpartitioned());

  private static final ByteBuffer KEY_METADATA = ByteBuffer.wrap(new byte[] {1, 2, 3});

  private static final Schema PARTITION_SCHEMA =
      new Schema(Types.NestedField.required(1, "category", Types.StringType.get()));
  private static final int PARTITIONED_SPEC_ID = 1;
  private static final PartitionSpec PARTITIONED_SPEC =
      PartitionSpec.builderFor(PARTITION_SCHEMA)
          .identity("category")
          .withSpecId(PARTITIONED_SPEC_ID)
          .build();
  private static final PartitionData PARTITION = partition("books");

  // these are populated by readers using the setter with the position of the field.
  private static final int MANIFEST_LOCATION_ORDINAL = Tracking.schema().fields().size();
  private static final int MANIFEST_POSITION_ORDINAL = Tracking.schema().fields().size() + 1;
  // row_position follows the data file schema and stores the manifest position.
  private static final int DATA_FILE_POS_ORDINAL =
      DataFile.getType(Types.StructType.of()).fields().size();

  private static final int ID_FIELD_ID = 1;
  private static final int SCORE_FIELD_ID = 2;
  private static final int TS_FIELD_ID = 4;
  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "score", Types.FloatType.get()),
          optional(3, "geom", Types.GeometryType.crs84()));
  private static final Schema SCHEMA_WITH_TS =
      new Schema(
          ImmutableList.<Types.NestedField>builder()
              .addAll(TABLE_SCHEMA.columns())
              .add(optional(TS_FIELD_ID, "ts", Types.TimestampType.withZone()))
              .build());
  private static final long RECORD_COUNT = 100L;
  private static final Types.StructType CONTENT_STATS_TYPE =
      StatsUtil.statsReadSchema(TABLE_SCHEMA, ImmutableList.of(1, 2, 3));
  private static final FieldStats<?> ID_STATS =
      StatsTestUtil.mockFieldStats(
          CONTENT_STATS_TYPE.fieldType("id").asStructType(), 1, 1, 1000, 100L, 5L, null);
  private static final FieldStats<?> SCORE_STATS =
      StatsTestUtil.mockFieldStats(
          CONTENT_STATS_TYPE.fieldType("score").asStructType(), 2, 1.0f, 100.0f, 100L, 10L, 3L);
  private static final FieldStats<?> GEOM_STATS =
      StatsTestUtil.mockFieldStats(
          CONTENT_STATS_TYPE.fieldType("geom").asStructType(), 3, null, null, 100L, 20L, null, 12L);
  private static final ContentStatsStruct CONTENT_STATS =
      new ContentStatsStruct(CONTENT_STATS_TYPE);

  static {
    CONTENT_STATS.setStats(1, ID_STATS);
    CONTENT_STATS.setStats(2, SCORE_STATS);
    CONTENT_STATS.setStats(3, GEOM_STATS);
  }

  private static final PartitionSpec UNPARTITIONED_SPEC = PartitionSpec.unpartitioned();

  private static final Tracking MANIFEST_TRACKING =
      new TrackingStruct(
          EntryStatus.ADDED,
          SNAPSHOT_ID,
          DATA_SEQUENCE_NUMBER,
          FILE_SEQUENCE_NUMBER,
          null, // modifiedSnapshotId
          FIRST_ROW_ID,
          null, // deletedPositions
          null); // replacedPositions

  private static final ByteBuffer MANIFEST_KEY_METADATA = ByteBuffer.wrap(new byte[] {7, 8, 9});

  private static final ManifestInfo MANIFEST_INFO =
      ManifestInfoStruct.builder()
          .addedFilesCount(3)
          .existingFilesCount(5)
          .deletedFilesCount(2)
          .replacedFilesCount(4)
          .modifiedFilesCount(1)
          .addedRowsCount(300L)
          .existingRowsCount(500L)
          .deletedRowsCount(200L)
          .replacedRowsCount(40L)
          .modifiedRowsCount(10L)
          .minSequenceNumber(7L)
          .dv(ByteBuffer.wrap(MumblingTestUtil.onlyFirstBitSetBytes()))
          .formatVersion(FORMAT_VERSION_V4)
          .build();

  private static final Metrics METRICS_WITH_BOUNDS =
      new Metrics(
          100L,
          ImmutableMap.of(1, 16L, 2, 64L),
          ImmutableMap.of(1, 100L, 2, 100L),
          ImmutableMap.of(1, 0L, 2, 5L),
          ImmutableMap.of(),
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1)),
          ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1000)));

  private static final DataFile DATA_FILE =
      new GenericDataFile(
          UNPARTITIONED_SPEC.specId(),
          DATA_FILE_LOCATION,
          FileFormat.PARQUET,
          PartitionData.EMPTY,
          1024L,
          new Metrics(100L),
          KEY_METADATA,
          ImmutableList.of(0L),
          SORT_ORDER_ID,
          FIRST_ROW_ID);
  private static final DataFile DATA_FILE_WITH_METRICS =
      new GenericDataFile(
          UNPARTITIONED_SPEC.specId(),
          DATA_FILE_LOCATION,
          FileFormat.PARQUET,
          PartitionData.EMPTY,
          1024L,
          METRICS_WITH_BOUNDS,
          KEY_METADATA,
          ImmutableList.of(0L),
          SORT_ORDER_ID,
          FIRST_ROW_ID);

  static {
    assignManifestPosition(DATA_FILE, MANIFEST_LOCATION, MANIFEST_POS);
    assignManifestPosition(DATA_FILE_WITH_METRICS, MANIFEST_LOCATION, MANIFEST_POS);
  }

  @Test
  void dataFileAdapterDelegation() {
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
    tracking.set(MANIFEST_LOCATION_ORDINAL, MANIFEST_LOCATION);
    tracking.set(MANIFEST_POSITION_ORDINAL, MANIFEST_POS);

    DeletionVector dv = mock(DeletionVector.class);
    TrackedFile file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            100L,
            1024L,
            PARTITIONED_SPEC_ID,
            PARTITION,
            CONTENT_STATS,
            true,
            3,
            dv,
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
    assertThat(dataFile.deletionVector()).isSameAs(dv);
    assertThat(dataFile.tightStats()).isTrue();
    assertThat(dataFile.equalityFieldIds()).isNull();
    assertThat(dataFile.columnSizes()).isNull();
    assertThat(dataFile.valueCounts())
        .containsOnly(Map.entry(1, 100L), Map.entry(2, 100L), Map.entry(3, 100L));
    assertThat(dataFile.nullValueCounts())
        .containsOnly(Map.entry(1, 5L), Map.entry(2, 10L), Map.entry(3, 20L));
    assertThat(dataFile.nanValueCounts()).containsOnly(Map.entry(2, 3L));
    assertThat(dataFile.totalBytes()).containsOnly(Map.entry(3, 12L));
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
  void dataFileAdapterRejectsNonDataContent(FileContent contentType) {
    TrackedFileStruct file = trackedFile(contentType);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for DataFile: %s", contentType);
  }

  @Test
  void dvDeleteFileAdapterDelegation() {
    DeletionVector dv =
        DeletionVectorStruct.builder()
            .location(DV_LOCATION)
            .offset(128L)
            .sizeInBytes(256L)
            .cardinality(10L)
            .keyMetadata(KEY_METADATA)
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
    tracking.set(MANIFEST_LOCATION_ORDINAL, MANIFEST_LOCATION);
    tracking.set(MANIFEST_POSITION_ORDINAL, MANIFEST_POS);

    TrackedFile file =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            100L,
            1024L,
            PARTITIONED_SPEC_ID,
            PARTITION,
            null,
            true,
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
    assertThat(dvFile.keyMetadata()).isEqualTo(KEY_METADATA);
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
    assertThat(dvFile.splitOffsets()).isNull();
    assertThat(dvFile.equalityFieldIds()).isNull();
    assertThat(dvFile.columnSizes()).isNull();
    assertThat(dvFile.valueCounts()).isNull();
    assertThat(dvFile.nullValueCounts()).isNull();
    assertThat(dvFile.nanValueCounts()).isNull();
    assertThat(dvFile.lowerBounds()).isNull();
    assertThat(dvFile.upperBounds()).isNull();
    assertThat(dvFile.tightStats()).isFalse();
  }

  @Test
  void dataFileAdapterIsSerializable() throws Exception {
    DataFile dataFile =
        TrackedFileAdapters.asDataFile(serializableDataEntry(), specsById(PARTITIONED_SPEC));

    assertSameDataFile(TestHelpers.roundTripSerialize(dataFile), dataFile);
    assertSameDataFile(TestHelpers.KryoHelpers.roundTripSerialize(dataFile), dataFile);
  }

  @Test
  void dvDeleteFileAdapterIsSerializable() throws Exception {
    DeleteFile dvFile =
        TrackedFileAdapters.asDVDeleteFile(serializableDataEntry(), specsById(PARTITIONED_SPEC));

    assertSameDeleteFile(TestHelpers.roundTripSerialize(dvFile), dvFile);
    assertSameDeleteFile(TestHelpers.KryoHelpers.roundTripSerialize(dvFile), dvFile);
  }

  private static void assertSameDataFile(DataFile actual, DataFile expected) {
    assertThat(actual.content()).isEqualTo(expected.content());
    assertThat(actual.location()).isEqualTo(expected.location());
    assertThat(actual.format()).isEqualTo(expected.format());
    assertThat(actual.specId()).isEqualTo(expected.specId());
    assertThat(actual.partition().get(0, String.class))
        .isEqualTo(expected.partition().get(0, String.class));
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    assertThat(actual.sortOrderId()).isEqualTo(expected.sortOrderId());
    assertThat(actual.dataSequenceNumber()).isEqualTo(expected.dataSequenceNumber());
    assertThat(actual.fileSequenceNumber()).isEqualTo(expected.fileSequenceNumber());
    assertThat(actual.firstRowId()).isEqualTo(expected.firstRowId());
    assertThat(actual.pos()).isEqualTo(expected.pos());
    assertThat(actual.manifestLocation()).isEqualTo(expected.manifestLocation());
    assertThat(actual.keyMetadata()).isEqualTo(expected.keyMetadata());
    assertThat(actual.splitOffsets()).isEqualTo(expected.splitOffsets());
  }

  private static void assertSameDeleteFile(DeleteFile actual, DeleteFile expected) {
    assertThat(actual.content()).isEqualTo(expected.content());
    assertThat(actual.location()).isEqualTo(expected.location());
    assertThat(actual.format()).isEqualTo(expected.format());
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.contentOffset()).isEqualTo(expected.contentOffset());
    assertThat(actual.contentSizeInBytes()).isEqualTo(expected.contentSizeInBytes());
    assertThat(actual.keyMetadata()).isEqualTo(expected.keyMetadata());
    assertThat(actual.referencedDataFile()).isEqualTo(expected.referencedDataFile());
    assertThat(actual.specId()).isEqualTo(expected.specId());
    assertThat(actual.partition().get(0, String.class))
        .isEqualTo(expected.partition().get(0, String.class));
    assertThat(actual.dataSequenceNumber()).isEqualTo(expected.dataSequenceNumber());
    assertThat(actual.fileSequenceNumber()).isEqualTo(expected.fileSequenceNumber());
    assertThat(actual.pos()).isEqualTo(expected.pos());
    assertThat(actual.manifestLocation()).isEqualTo(expected.manifestLocation());
  }

  private static TrackedFile serializableDataEntry() {
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
    tracking.set(MANIFEST_LOCATION_ORDINAL, MANIFEST_LOCATION);
    tracking.set(MANIFEST_POSITION_ORDINAL, MANIFEST_POS);

    DeletionVector dv =
        DeletionVectorStruct.builder()
            .location(DV_LOCATION)
            .offset(128L)
            .sizeInBytes(256L)
            .cardinality(10L)
            .keyMetadata(KEY_METADATA)
            .build();

    return new TrackedFileStruct(
        tracking,
        FileContent.DATA,
        DATA_FILE_LOCATION,
        FileFormat.PARQUET,
        100L, // recordCount
        1024L, // fileSizeInBytes
        PARTITIONED_SPEC_ID,
        PARTITION,
        null, // contentStats
        false, // tightStats
        3, // sortOrderId
        dv, // deletionVector
        null, // manifestInfo
        KEY_METADATA,
        ImmutableList.of(50L, 100L), // splitOffsets
        null); // columnFiles
  }

  @ParameterizedTest
  @EnumSource(value = FileContent.class, mode = EnumSource.Mode.EXCLUDE, names = "DATA")
  void dvDeleteFileAdapterRejectsNonDataContent(FileContent contentType) {
    TrackedFileStruct file = trackedFile(contentType);

    assertThatThrownBy(() -> TrackedFileAdapters.asDVDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for DV delete file: %s", contentType);
  }

  @Test
  void dvDeleteFileAdapterRejectsNullDeletionVector() {
    TrackedFileStruct file = trackedFile(FileContent.DATA);

    assertThatThrownBy(() -> TrackedFileAdapters.asDVDeleteFile(file, UNPARTITIONED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create DV delete file: no deletion vector");
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"DATA_MANIFEST", "DELETE_MANIFEST"})
  void manifestFileAdapterDelegation(FileContent contentType) {
    TrackedFile file =
        new TrackedFileStruct(
            MANIFEST_TRACKING,
            contentType,
            MANIFEST_LOCATION,
            FileFormat.PARQUET,
            10L, // recordCount
            MANIFEST_FILE_SIZE,
            null, // specId
            null, // partition
            null, // contentStats
            false, // tightStats
            null, // sortOrderId
            null, // deletionVector
            MANIFEST_INFO,
            MANIFEST_KEY_METADATA,
            null, // splitOffsets
            null); // columnFiles

    ManifestFile manifest = TrackedFileAdapters.asManifestFile(file);

    ManifestContent expectedContent =
        contentType == FileContent.DATA_MANIFEST ? ManifestContent.DATA : ManifestContent.DELETES;
    assertThat(manifest.path()).isEqualTo(MANIFEST_LOCATION);
    assertThat(manifest.length()).isEqualTo(MANIFEST_FILE_SIZE);
    assertThat(manifest.content()).isEqualTo(expectedContent);
    assertThat(manifest.sequenceNumber()).isEqualTo(DATA_SEQUENCE_NUMBER);
    assertThat(manifest.minSequenceNumber()).isEqualTo(MANIFEST_INFO.minSequenceNumber());
    assertThat(manifest.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(manifest.addedFilesCount()).isEqualTo(MANIFEST_INFO.addedFilesCount());
    assertThat(manifest.addedRowsCount()).isEqualTo(MANIFEST_INFO.addedRowsCount());
    assertThat(manifest.existingFilesCount()).isEqualTo(MANIFEST_INFO.existingFilesCount());
    assertThat(manifest.existingRowsCount()).isEqualTo(MANIFEST_INFO.existingRowsCount());
    assertThat(manifest.deletedFilesCount()).isEqualTo(MANIFEST_INFO.deletedFilesCount());
    assertThat(manifest.deletedRowsCount()).isEqualTo(MANIFEST_INFO.deletedRowsCount());
    assertThat(manifest.replacedFilesCount()).isEqualTo(MANIFEST_INFO.replacedFilesCount());
    assertThat(manifest.replacedRowsCount()).isEqualTo(MANIFEST_INFO.replacedRowsCount());
    assertThat(manifest.modifiedFilesCount()).isEqualTo(MANIFEST_INFO.modifiedFilesCount());
    assertThat(manifest.modifiedRowsCount()).isEqualTo(MANIFEST_INFO.modifiedRowsCount());
    assertThat(manifest.firstRowId()).isEqualTo(FIRST_ROW_ID);
    assertThat(manifest.keyMetadata()).isEqualTo(MANIFEST_KEY_METADATA);
    assertThat(manifest.manifestDeletionVector().buffer())
        .isEqualTo(ByteBuffer.wrap(MumblingTestUtil.onlyFirstBitSetBytes()));
    assertThat(manifest.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(manifest.partitions()).isNull();
    assertThatThrownBy(manifest::partitionSpecId)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("v4 manifests are not bound to a single partition spec");
  }

  @Test
  void manifestFileAdapterCopy() {
    TrackedFile file = Mockito.mock(TrackedFile.class);
    TrackedFile fileCopy = Mockito.mock(TrackedFile.class);
    Mockito.when(file.contentType()).thenReturn(FileContent.DATA_MANIFEST);
    Mockito.when(file.copy()).thenReturn(fileCopy);
    Mockito.when(fileCopy.location()).thenReturn(MANIFEST_LOCATION);

    ManifestFile copy = TrackedFileAdapters.asManifestFile(file).copy();

    // copy() delegates to the tracked file's copy(), which deep-copies the nested structs.
    Mockito.verify(file).copy();
    assertThat(copy.path()).isEqualTo(MANIFEST_LOCATION);
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"DATA_MANIFEST", "DELETE_MANIFEST"})
  void manifestFileAdapterRejectsNonManifestContent(FileContent contentType) {
    TrackedFileStruct file = trackedFile(contentType);

    assertThatThrownBy(() -> TrackedFileAdapters.asManifestFile(file))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for ManifestFile: %s", contentType);
  }

  @Test
  void dataFileWithoutDeletionVectorReturnsNull() {
    TrackedFile fileWithoutDv = mock(TrackedFile.class);
    when(fileWithoutDv.contentType()).thenReturn(FileContent.DATA);
    when(fileWithoutDv.deletionVector()).thenReturn(null);

    assertThat(TrackedFileAdapters.asDataFile(fileWithoutDv, UNPARTITIONED).deletionVector())
        .isNull();
  }

  @Test
  void nullContentStatsReturnsNullStats() {
    TrackedFileStruct file = trackedFile(FileContent.DATA);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, UNPARTITIONED);

    assertThat(dataFile.valueCounts()).isNull();
    assertThat(dataFile.nullValueCounts()).isNull();
    assertThat(dataFile.nanValueCounts()).isNull();
    assertThat(dataFile.lowerBounds()).isNull();
    assertThat(dataFile.upperBounds()).isNull();
  }

  @Test
  void nullTrackingReturnsNullTrackingFields() {
    // Files read before manifest inheritance have no tracking; tracking-derived fields must be
    // null rather than throwing.
    assertNullTrackingFields(
        TrackedFileAdapters.asDataFile(trackedFile(FileContent.DATA), UNPARTITIONED));

    TrackedFileStruct fileWithDV =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            null,
            null,
            0L,
            0L,
            null,
            null,
            null,
            false,
            null,
            deletionVector(),
            null,
            null,
            null,
            null);
    assertNullTrackingFields(TrackedFileAdapters.asDVDeleteFile(fileWithDV, UNPARTITIONED));
  }

  @Test
  void unpartitionedFilePartitionIsEmpty() {
    TrackedFileStruct file = trackedFile(FileContent.DATA);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, UNPARTITIONED);

    assertThat(dataFile.specId()).isEqualTo(UNPARTITIONED_SPEC_ID);
    assertThat(dataFile.partition()).isEqualTo(PartitionData.EMPTY);
  }

  @Test
  void nullSpecIdResolvesToUnpartitionedSpec() {
    PartitionSpec unpartitioned = PartitionSpec.builderFor(new Schema()).withSpecId(5).build();
    TrackedFileStruct file = trackedFile(FileContent.DATA);

    DataFile dataFile = TrackedFileAdapters.asDataFile(file, specsById(unpartitioned));

    assertThat(dataFile.specId()).isEqualTo(5);
  }

  @Test
  void nullSpecIdThrowsWhenNoUnpartitionedSpec() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    PartitionSpec partitioned = PartitionSpec.builderFor(schema).identity("id").build();
    TrackedFileStruct file = trackedFile(FileContent.DATA);

    assertThatThrownBy(() -> TrackedFileAdapters.asDataFile(file, specsById(partitioned)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find unpartitioned spec in specs");
  }

  @Test
  void unknownSpecIdThrows() {
    TrackedFileStruct file =
        new TrackedFileStruct(
            null,
            FileContent.DATA,
            null,
            null,
            0L,
            0L,
            99,
            null,
            null,
            false,
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
  void dataTrackedFileAdapterFromDataFile() {
    TrackedFile result =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, UNPARTITIONED).wrap(DATA_FILE);

    assertThat(result.tracking()).isNull();
    assertWrappedDataFileMatchesFileFields(result, DATA_FILE);
  }

  @Test
  void dataTrackedFileAdapterFromExistingManifestEntry() {
    TrackedFile result =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, UNPARTITIONED)
            .wrap(
                new GenericManifestEntry<DataFile>(
                        ManifestEntry.getSchema(UNPARTITIONED_SPEC.partitionType()).asStruct())
                    .wrapExisting(
                        SNAPSHOT_ID, DATA_SEQUENCE_NUMBER, FILE_SEQUENCE_NUMBER, DATA_FILE));

    assertThat(result.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(result.tracking().snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(result.tracking().dataSequenceNumber()).isEqualTo(DATA_SEQUENCE_NUMBER);
    assertThat(result.tracking().fileSequenceNumber()).isEqualTo(FILE_SEQUENCE_NUMBER);
    assertThat(result.tracking().firstRowId()).isEqualTo(FIRST_ROW_ID);
    assertManifestPosition(result.tracking(), DATA_FILE);
    assertWrappedDataFileMatchesFileFields(result, DATA_FILE);
  }

  @Test
  void dataTrackedFileAdapterReuse() {
    TrackedFileAdapters.DataTrackedFile adapter =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, UNPARTITIONED);

    adapter.wrap(DATA_FILE);
    assertWrappedDataFileMatchesFileFields(adapter, DATA_FILE);
    assertThat(adapter.tracking()).isNull();

    DataFile file2 =
        new GenericDataFile(
            UNPARTITIONED_SPEC.specId(),
            "s3://bucket/data/file2.parquet",
            FileFormat.PARQUET,
            PartitionData.EMPTY,
            2048L,
            new Metrics(200L),
            null,
            ImmutableList.of(0L),
            null,
            null);
    assignManifestPosition(file2, "s3://bucket/table/manifest-2.parquet", 8L);
    adapter.wrap(
        new GenericManifestEntry<DataFile>(
                ManifestEntry.getSchema(UNPARTITIONED_SPEC.partitionType()).asStruct())
            .wrapExisting(SNAPSHOT_ID, DATA_SEQUENCE_NUMBER, FILE_SEQUENCE_NUMBER, file2));
    assertWrappedDataFileMatchesFileFields(adapter, file2);
    assertThat(adapter.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertManifestPosition(adapter.tracking(), file2);
  }

  @Test
  void dataTrackedFileAdapterRejectsNullFile() {
    TrackedFileAdapters.DataTrackedFile adapter =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, UNPARTITIONED);
    assertThatThrownBy(() -> adapter.wrap((DataFile) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid file: null");
  }

  @Test
  void dataTrackedFileAdapterUnwrapsToOriginalTrackedFile() {
    TrackedFile original = trackedFile(FileContent.DATA);
    DataFile adapted = TrackedFileAdapters.asDataFile(original, UNPARTITIONED);
    TrackedFile roundTripped =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, UNPARTITIONED).wrap(adapted);
    assertThat(roundTripped).isSameAs(original);
  }

  @Test
  void dataTrackedFileAdapterContentStats() {
    TrackedFile result =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, UNPARTITIONED).wrap(DATA_FILE_WITH_METRICS);

    ContentStats stats = result.contentStats();
    assertThat(stats).isNotNull();
    assertThat(stats.fieldStats()).extracting(FieldStats::fieldId).containsExactlyInAnyOrder(1, 2);

    FieldStats<?> idStats = stats.statsFor(1);
    assertThat(idStats.valueCount()).isEqualTo(100L);
    assertThat(idStats.lowerBound()).isEqualTo(1);
    assertThat(idStats.upperBound()).isEqualTo(1000);
  }

  @Test
  void dataTrackedFileAdapterWithoutMetricsHasNoContentStats() {
    TrackedFile result =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, UNPARTITIONED).wrap(DATA_FILE);

    assertThat(result.contentStats()).isNull();
  }

  @Test
  void dataTrackedFileAdapterKeepsPartitionTuple() {
    DataFile partitioned =
        new GenericDataFile(
            PARTITIONED_SPEC.specId(),
            DATA_FILE_LOCATION,
            FileFormat.PARQUET,
            PARTITION,
            1024L,
            new Metrics(100L),
            null,
            ImmutableList.of(0L),
            null,
            null);
    TrackedFile result =
        TrackedFileAdapters.forDataFile(PARTITION_SCHEMA, specsById(PARTITIONED_SPEC))
            .wrap(partitioned);

    assertThat(result.partition())
        .usingComparator(Comparators.forType(PARTITIONED_SPEC.partitionType()))
        .isEqualTo(PARTITION);
  }

  @Test
  void manifestTrackedFileAdapterUnwrapsToOriginalTrackedFile() {
    TrackedFile original = trackedFile(FileContent.DATA_MANIFEST);
    ManifestFile adapted = TrackedFileAdapters.asManifestFile(original);
    TrackedFile roundTripped = TrackedFileAdapters.forManifestFile().wrap(adapted);
    assertThat(roundTripped).isSameAs(original);
  }

  @ParameterizedTest
  @EnumSource(ManifestContent.class)
  void manifestTrackedFileAdapter(ManifestContent content) {
    FileContent expectedContent =
        content == ManifestContent.DATA ? FileContent.DATA_MANIFEST : FileContent.DELETE_MANIFEST;
    ManifestFile manifest =
        new GenericManifestFile(
            MANIFEST_LOCATION,
            MANIFEST_FILE_SIZE,
            UNPARTITIONED_SPEC.specId(),
            content,
            MANIFEST_SEQUENCE_NUMBER,
            MANIFEST_MIN_SEQUENCE_NUMBER,
            SNAPSHOT_ID,
            ImmutableList.of(), // partitions
            null, // key metadata
            ADDED_FILES_COUNT,
            ADDED_ROWS_COUNT,
            EXISTING_FILES_COUNT,
            EXISTING_ROWS_COUNT,
            DELETED_FILES_COUNT,
            DELETED_ROWS_COUNT,
            content == ManifestContent.DATA ? FIRST_ROW_ID : null); // first row id
    TrackedFile result = TrackedFileAdapters.forManifestFile().wrap(manifest);

    assertThat(result.contentType()).isEqualTo(expectedContent);
    assertThat(result.manifestInfo().formatVersion()).isZero();
    assertThat(result.location()).isEqualTo(MANIFEST_LOCATION);
    assertThat(result.fileFormat()).isEqualTo(FileFormat.AVRO);
    assertThat(result.tracking().status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(result.tracking().snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(result.tracking().firstRowId())
        .isEqualTo(content == ManifestContent.DATA ? FIRST_ROW_ID : null);
    assertThat(result.recordCount())
        .isEqualTo(ADDED_FILES_COUNT + EXISTING_FILES_COUNT + DELETED_FILES_COUNT);
    assertThat(result.manifestInfo()).isNotNull();
    assertThat(result.manifestInfo().addedFilesCount()).isEqualTo(ADDED_FILES_COUNT);
    assertThat(result.manifestInfo().existingFilesCount()).isEqualTo(EXISTING_FILES_COUNT);
    assertThat(result.manifestInfo().deletedFilesCount()).isEqualTo(DELETED_FILES_COUNT);
    assertThat(result.manifestInfo().addedRowsCount()).isEqualTo(ADDED_ROWS_COUNT);
    assertThat(result.manifestInfo().existingRowsCount()).isEqualTo(EXISTING_ROWS_COUNT);
    assertThat(result.manifestInfo().deletedRowsCount()).isEqualTo(DELETED_ROWS_COUNT);
    assertThat(result.manifestInfo().replacedFilesCount()).isEqualTo(0);
    assertThat(result.manifestInfo().replacedRowsCount()).isEqualTo(0L);
    assertThat(result.manifestInfo().modifiedFilesCount()).isEqualTo(0);
    assertThat(result.manifestInfo().modifiedRowsCount()).isEqualTo(0L);
  }

  @ParameterizedTest
  @EnumSource(InvalidCount.class)
  void manifestTrackedFileAdapterRejectsInvalidFilesCount(InvalidCount count) {
    ManifestFile manifest = newManifestFileWithInvalidCount(count);
    assertThatThrownBy(() -> TrackedFileAdapters.forManifestFile().wrap(manifest))
        .isInstanceOf(count.exceptionType())
        .hasMessage("Cannot convert manifest %s: %s", MANIFEST_LOCATION, count.message());
  }

  @Test
  void dataTrackedFileAdapterFillsStatsFromIdentityPartition() {
    PartitionSpec spec = PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").build();
    DataFile file = dataFile(spec, Row.of(17), new Metrics(RECORD_COUNT));

    ContentStats stats =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, specsById(spec)).wrap(file).contentStats();

    FieldStats<?> idStats = stats.statsFor(ID_FIELD_ID);
    assertThat(idStats.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(idStats.nullValueCount()).isZero();
    assertThat(idStats.hasNanValueCount()).isFalse();
    assertBounds(idStats, 17, 17);
  }

  @Test
  void dataTrackedFileAdapterFillsStatsFromNullPartition() {
    PartitionSpec spec = PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").build();
    DataFile file = dataFile(spec, Row.of((Object) null), new Metrics(RECORD_COUNT));

    ContentStats stats =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, specsById(spec)).wrap(file).contentStats();

    FieldStats<?> idStats = stats.statsFor(ID_FIELD_ID);
    assertThat(idStats.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(idStats.nullValueCount()).isEqualTo(RECORD_COUNT);
    assertBounds(idStats, null, null);
  }

  private static Stream<Arguments> floatingPointPartitionValues() {
    return Stream.of(
        Arguments.of(Types.FloatType.get(), Float.NaN, RECORD_COUNT, null),
        Arguments.of(Types.FloatType.get(), 17.0f, 0L, 17.0f),
        Arguments.of(Types.DoubleType.get(), Double.NaN, RECORD_COUNT, null),
        Arguments.of(Types.DoubleType.get(), 17.0d, 0L, 17.0d));
  }

  @ParameterizedTest(name = "{0} partition value {1}")
  @MethodSource("floatingPointPartitionValues")
  void dataTrackedFileAdapterFillsStatsFromFloatingPointPartition(
      Type type, Object value, long nanValueCount, Object bound) {
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("value").build();
    DataFile file = dataFile(spec, Row.of(value), new Metrics(RECORD_COUNT));

    ContentStats stats =
        TrackedFileAdapters.forDataFile(schema, specsById(spec)).wrap(file).contentStats();

    FieldStats<?> valueStats = stats.statsFor(1);
    assertThat(valueStats.nullValueCount()).isZero();
    assertThat(valueStats.nanValueCount()).isEqualTo(nanValueCount);
    assertBounds(valueStats, bound, bound);
  }

  @Test
  void dataTrackedFileAdapterKeepsColumnBounds() {
    PartitionSpec spec = PartitionSpec.builderFor(TABLE_SCHEMA).truncate("id", 10).build();
    Metrics columnBounds =
        new Metrics(
            RECORD_COUNT,
            null, // columnSizes
            ImmutableMap.of(ID_FIELD_ID, RECORD_COUNT),
            ImmutableMap.of(ID_FIELD_ID, 0L),
            null, // nanValueCounts
            ImmutableMap.of(ID_FIELD_ID, Conversions.toByteBuffer(Types.IntegerType.get(), 12)),
            ImmutableMap.of(ID_FIELD_ID, Conversions.toByteBuffer(Types.IntegerType.get(), 15)));
    DataFile file = dataFile(spec, Row.of(10), columnBounds);

    ContentStats stats =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, specsById(spec)).wrap(file).contentStats();

    assertBounds(stats.statsFor(ID_FIELD_ID), 12, 15);
  }

  private static Stream<Arguments> truncatedColumnBounds() {
    String value = "books-and-magazines";
    ByteBuffer binaryValue = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    return Stream.of(
        Arguments.of(
            Types.StringType.get(),
            value,
            UnicodeUtil.truncateStringMin(value, 16),
            UnicodeUtil.truncateStringMax(value, 16)),
        Arguments.of(
            Types.BinaryType.get(),
            binaryValue,
            BinaryUtil.truncateBinaryMin(binaryValue, 16),
            BinaryUtil.truncateBinaryMax(binaryValue, 16)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("truncatedColumnBounds")
  void dataTrackedFileAdapterFillsBoundsFromPartitionWhenColumnBoundsMayBeTruncated(
      Type type, Object value, Object truncatedLower, Object truncatedUpper) {
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("value").build();
    Metrics truncatedBounds =
        new Metrics(
            RECORD_COUNT,
            null, // columnSizes
            ImmutableMap.of(1, RECORD_COUNT),
            ImmutableMap.of(1, 0L),
            null, // nanValueCounts
            ImmutableMap.of(1, Conversions.toByteBuffer(type, truncatedLower)),
            ImmutableMap.of(1, Conversions.toByteBuffer(type, truncatedUpper)));
    DataFile file = dataFile(spec, Row.of(value), truncatedBounds);

    ContentStats stats =
        TrackedFileAdapters.forDataFile(schema, specsById(spec)).wrap(file).contentStats();

    FieldStats<Object> valueStats = stats.statsFor(1);
    Comparator<Object> comparator = Comparators.forType(type.asPrimitiveType());
    assertThat(valueStats.lowerBound()).usingComparator(comparator).isEqualTo(value);
    assertThat(valueStats.upperBound()).usingComparator(comparator).isEqualTo(value);
  }

  @Test
  void dataTrackedFileAdapterKeepsStatsForOtherColumns() {
    PartitionSpec spec = PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").build();
    Metrics scoreStats =
        new Metrics(
            RECORD_COUNT,
            null, // columnSizes
            ImmutableMap.of(SCORE_FIELD_ID, RECORD_COUNT),
            ImmutableMap.of(SCORE_FIELD_ID, 5L),
            ImmutableMap.of(SCORE_FIELD_ID, 3L),
            ImmutableMap.of(SCORE_FIELD_ID, Conversions.toByteBuffer(Types.FloatType.get(), 1.0f)),
            ImmutableMap.of(
                SCORE_FIELD_ID, Conversions.toByteBuffer(Types.FloatType.get(), 100.0f)));
    DataFile file = dataFile(spec, Row.of(17), scoreStats);

    ContentStats stats =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, specsById(spec)).wrap(file).contentStats();

    assertThat(stats.type())
        .isEqualTo(
            StatsUtil.statsReadSchema(TABLE_SCHEMA, ImmutableList.of(ID_FIELD_ID, SCORE_FIELD_ID)));
    assertBounds(stats.statsFor(ID_FIELD_ID), 17, 17);
    FieldStats<?> adaptedScoreStats = stats.statsFor(SCORE_FIELD_ID);
    assertThat(adaptedScoreStats.nullValueCount()).isEqualTo(5L);
    assertThat(adaptedScoreStats.nanValueCount()).isEqualTo(3L);
    assertBounds(adaptedScoreStats, 1.0f, 100.0f);
  }

  @Test
  void dataTrackedFileAdapterIgnoresVoidPartition() {
    PartitionSpec spec = PartitionSpec.builderFor(TABLE_SCHEMA).alwaysNull("id").build();
    DataFile file = dataFile(spec, Row.of((Object) null), new Metrics(RECORD_COUNT));

    TrackedFile result = TrackedFileAdapters.forDataFile(TABLE_SCHEMA, specsById(spec)).wrap(file);

    assertThat(result.contentStats()).isNull();
  }

  @Test
  void dataTrackedFileAdapterRejectsDroppedPartitionSource() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_WITH_TS).identity("ts").build();
    DataFile file =
        dataFile(
            spec,
            Row.of(DateTimeUtil.isoTimestamptzToMicros("2024-03-05T10:15:30.000000+00:00")),
            new Metrics(RECORD_COUNT));
    TrackedFileAdapters.DataTrackedFile adapter =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, specsById(spec));

    assertThatThrownBy(() -> adapter.wrap(file))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot convert data file %s: partition source column was dropped: 1000: ts: identity(4)",
            DATA_FILE_LOCATION);
  }

  private static Stream<Arguments> transformsThatDoNotPreserveOrder() {
    return Stream.of(
        Arguments.of(Transforms.bucket(16), "bucket[16]"),
        Arguments.of(Transforms.fromString("unknown"), "unknown"));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("transformsThatDoNotPreserveOrder")
  void dataTrackedFileAdapterRejectsTransformThatDoesNotPreserveOrder(
      Transform<?, ?> transform, String transformName) {
    PartitionSpec spec =
        PartitionSpec.builderFor(TABLE_SCHEMA).add(ID_FIELD_ID, 1000, "id_part", transform).build();
    DataFile file = dataFile(spec, Row.of(3), new Metrics(RECORD_COUNT));
    TrackedFileAdapters.DataTrackedFile adapter =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, specsById(spec));

    assertThatThrownBy(() -> adapter.wrap(file))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot convert data file %s: partition field does not preserve order: "
                + "1000: id_part: %s(1)",
            DATA_FILE_LOCATION, transformName);
  }

  @Test
  void dataTrackedFileAdapterNarrowestBoundsForTruncate() {
    PartitionSpec spec =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .truncate("id", 10)
            .add(ID_FIELD_ID, 1001, "id_trunc_4", Transforms.truncate(4))
            .build();
    // the partition of id = 11
    DataFile file = dataFile(spec, Row.of(10, 8), new Metrics(RECORD_COUNT));

    ContentStats stats =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, specsById(spec)).wrap(file).contentStats();

    assertBounds(stats.statsFor(ID_FIELD_ID), 10, 11);
  }

  @Test
  void dataTrackedFileAdapterNarrowestBoundsForIdentityAndDay() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_WITH_TS).identity("ts").day("ts").build();
    long ts = DateTimeUtil.isoTimestamptzToMicros("2024-03-05T10:15:30.000000+00:00");
    DataFile file =
        dataFile(
            spec, Row.of(ts, DateTimeUtil.isoDateToDays("2024-03-05")), new Metrics(RECORD_COUNT));

    ContentStats stats =
        TrackedFileAdapters.forDataFile(SCHEMA_WITH_TS, specsById(spec)).wrap(file).contentStats();

    assertBounds(stats.statsFor(TS_FIELD_ID), ts, ts);
  }

  @Test
  void dataTrackedFileAdapterNarrowestBoundsSkipsMissingUpperBound() {
    PartitionSpec spec =
        PartitionSpec.builderFor(PARTITION_SCHEMA)
            .identity("category")
            .truncate("category", 1)
            .build();
    // truncate(1) of the max code point has no higher prefix, so it has no upper bound
    String maxCodePoint = new String(Character.toChars(Character.MAX_CODE_POINT));
    DataFile file = dataFile(spec, Row.of(maxCodePoint, maxCodePoint), new Metrics(RECORD_COUNT));
    int categoryId = PARTITION_SCHEMA.findField("category").fieldId();

    ContentStats stats =
        TrackedFileAdapters.forDataFile(PARTITION_SCHEMA, specsById(spec))
            .wrap(file)
            .contentStats();

    FieldStats<CharSequence> categoryStats = stats.statsFor(categoryId);
    assertThat(categoryStats.upperBound())
        .usingComparator(Comparators.charSequences())
        .isEqualTo(maxCodePoint);
  }

  @Test
  void dataTrackedFileAdapterRejectsMissingSpec() {
    PartitionSpec spec =
        PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").withSpecId(1).build();
    DataFile file = dataFile(spec, Row.of(17), new Metrics(RECORD_COUNT));
    TrackedFileAdapters.DataTrackedFile adapter =
        TrackedFileAdapters.forDataFile(TABLE_SCHEMA, ImmutableMap.of());

    assertThatThrownBy(() -> adapter.wrap(file))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find partition spec for spec ID: 1");
  }

  @Test
  void dataTrackedFileAdapterReuseAcrossSpecs() {
    PartitionSpec idSpec =
        PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").withSpecId(0).build();
    PartitionSpec scoreSpec =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .add(SCORE_FIELD_ID, 1001, "score", Transforms.identity())
            .withSpecId(1)
            .build();
    TrackedFileAdapters.DataTrackedFile adapter =
        TrackedFileAdapters.forDataFile(
            TABLE_SCHEMA, ImmutableMap.of(idSpec.specId(), idSpec, scoreSpec.specId(), scoreSpec));

    ContentStats idFileStats =
        adapter.wrap(dataFile(idSpec, Row.of(17), new Metrics(RECORD_COUNT))).contentStats();
    assertBounds(idFileStats.statsFor(ID_FIELD_ID), 17, 17);

    ContentStats scoreFileStats =
        adapter.wrap(dataFile(scoreSpec, Row.of(17.0f), new Metrics(RECORD_COUNT))).contentStats();
    assertThat(scoreFileStats.statsFor(ID_FIELD_ID)).isNull();
    assertBounds(scoreFileStats.statsFor(SCORE_FIELD_ID), 17.0f, 17.0f);
  }

  private static void assertBounds(FieldStats<?> stats, Object lowerBound, Object upperBound) {
    assertThat(stats.lowerBound()).isEqualTo(lowerBound);
    assertThat(stats.upperBound()).isEqualTo(upperBound);
  }

  private static void assertWrappedDataFileMatchesFileFields(TrackedFile result, DataFile file) {
    assertThat(result.contentType()).isEqualTo(FileContent.DATA);
    assertThat(result.location()).isEqualTo(file.location());
    assertThat(result.fileFormat()).isEqualTo(file.format());
    assertThat(result.recordCount()).isEqualTo(file.recordCount());
    assertThat(result.fileSizeInBytes()).isEqualTo(file.fileSizeInBytes());
    assertThat(result.specId()).isEqualTo(file.specId());
    assertThat(result.tightStats()).isEqualTo(file.tightStats());
    assertThat(result.sortOrderId()).isEqualTo(file.sortOrderId());
    assertThat(result.keyMetadata()).isEqualTo(file.keyMetadata());
    assertThat(result.splitOffsets()).isEqualTo(file.splitOffsets());
    assertThat(result.manifestInfo()).isNull();
    assertThat(result.deletionVector()).isNull();
  }

  private static ManifestFile newManifestFileWithInvalidCount(InvalidCount count) {
    ManifestFile manifest = mock(ManifestFile.class);
    when(manifest.path()).thenReturn(MANIFEST_LOCATION);
    when(manifest.content()).thenReturn(ManifestContent.DATA);
    when(manifest.addedFilesCount()).thenReturn(ADDED_FILES_COUNT);
    when(manifest.existingFilesCount()).thenReturn(EXISTING_FILES_COUNT);
    when(manifest.deletedFilesCount()).thenReturn(DELETED_FILES_COUNT);
    when(manifest.addedRowsCount()).thenReturn(ADDED_ROWS_COUNT);
    when(manifest.existingRowsCount()).thenReturn(EXISTING_ROWS_COUNT);
    when(manifest.deletedRowsCount()).thenReturn(DELETED_ROWS_COUNT);
    when(manifest.replacedFilesCount()).thenReturn(0);
    when(manifest.modifiedFilesCount()).thenReturn(0);
    when(manifest.replacedRowsCount()).thenReturn(0L);
    when(manifest.modifiedRowsCount()).thenReturn(0L);
    switch (count) {
      case ADDED_FILES -> when(manifest.addedFilesCount()).thenReturn(null);
      case EXISTING_FILES -> when(manifest.existingFilesCount()).thenReturn(null);
      case DELETED_FILES -> when(manifest.deletedFilesCount()).thenReturn(null);
      case REPLACED_FILES_NULL -> when(manifest.replacedFilesCount()).thenReturn(null);
      case MODIFIED_FILES_NULL -> when(manifest.modifiedFilesCount()).thenReturn(null);
      case REPLACED_FILES_NON_ZERO ->
          when(manifest.replacedFilesCount()).thenReturn(NON_ZERO_FILE_COUNT);
      case MODIFIED_FILES_NON_ZERO ->
          when(manifest.modifiedFilesCount()).thenReturn(NON_ZERO_FILE_COUNT);
    }
    return manifest;
  }

  private enum InvalidCount {
    ADDED_FILES(NullPointerException.class, "missing added files count"),
    EXISTING_FILES(NullPointerException.class, "missing existing files count"),
    DELETED_FILES(NullPointerException.class, "missing deleted files count"),
    REPLACED_FILES_NULL(IllegalArgumentException.class, "Invalid replaced file count: null"),
    REPLACED_FILES_NON_ZERO(
        IllegalArgumentException.class, "Invalid replaced file count: " + NON_ZERO_FILE_COUNT),
    MODIFIED_FILES_NULL(IllegalArgumentException.class, "Invalid modified file count: null"),
    MODIFIED_FILES_NON_ZERO(
        IllegalArgumentException.class, "Invalid modified file count: " + NON_ZERO_FILE_COUNT);

    private final Class<? extends RuntimeException> exceptionType;
    private final String message;

    InvalidCount(Class<? extends RuntimeException> exceptionType, String message) {
      this.exceptionType = exceptionType;
      this.message = message;
    }

    private Class<? extends RuntimeException> exceptionType() {
      return exceptionType;
    }

    private String message() {
      return message;
    }
  }

  private static void assignManifestPosition(DataFile file, String location, long manifestPos) {
    GenericDataFile dataFile = (GenericDataFile) file;
    dataFile.setManifestLocation(location);
    dataFile.set(DATA_FILE_POS_ORDINAL, manifestPos);
  }

  private static void assertManifestPosition(Tracking tracking, DataFile file) {
    assertThat(tracking.manifestLocation()).isEqualTo(file.manifestLocation());
    assertThat(tracking.manifestPos()).isEqualTo(file.pos());
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

  /** Minimal file for the rejection and null-tracking tests. */
  private static TrackedFileStruct trackedFile(FileContent contentType) {
    return new TrackedFileStruct(
        null,
        contentType,
        DATA_FILE_LOCATION,
        FileFormat.PARQUET,
        1L,
        1L,
        null,
        null,
        null,
        false,
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

  private static DataFile dataFile(PartitionSpec spec, StructLike partition, Metrics metrics) {
    return DataFiles.builder(spec)
        .withPath(DATA_FILE_LOCATION)
        .withFileSizeInBytes(1024L)
        .withPartition(partition)
        .withMetrics(metrics)
        .build();
  }
}
