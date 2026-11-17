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

import static org.apache.iceberg.V4TestHelpers.partition;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.V4Metadata.LeafTrackedFileWrapper;
import org.apache.iceberg.V4Metadata.RootTrackedFileWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestV4TrackedFileWrappers {
  private static final long COMMIT_SNAPSHOT_ID = 42L;
  private static final long COMMIT_SEQ = 34L;
  private static final long PREVIOUS_SNAPSHOT_ID = 7L;
  private static final long PREVIOUS_MODIFIED_SNAPSHOT_ID = 8L;
  private static final long PREVIOUS_DATA_SEQ = 6L;
  private static final long PREVIOUS_FILE_SEQ = 5L;
  private static final long PREVIOUS_FIRST_ROW_ID = 500L;
  private static final long FIRST_ROW_ID = 1_000L;
  private static final String DATA_LOCATION = "s3://bucket/table/file.parquet";
  private static final String MANIFEST_LOCATION = "s3://bucket/table/manifest.parquet";

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "measure", Types.FloatType.get()));
  private static final int ID_PARTITION_FIELD_ID = 1000;
  private static final int DATA_PARTITION_FIELD_ID = 1001;
  private static final PartitionSpec ID_SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA)
          .withSpecId(1)
          .add(1 /* sourceId */, ID_PARTITION_FIELD_ID, "id", Transforms.identity())
          .build();
  private static final PartitionSpec DATA_SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA)
          .withSpecId(2)
          .add(2 /* sourceId */, DATA_PARTITION_FIELD_ID, "data", Transforms.identity())
          .build();
  private static final Types.StructType UNPARTITIONED_TYPE = Types.StructType.of();
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());

  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(
          TABLE_SCHEMA, MetricsTestUtil.from(ImmutableMap.of(), TABLE_SCHEMA));
  private static final Types.StructType NO_STATS_TYPE = Types.StructType.of();
  private static final Schema TRACKED_FILE_SCHEMA =
      TrackedFile.schema(Types.StructType.of(), Types.StructType.of());

  private static Stream<PartitionSpec> unpartitionedSpecs() {
    return Stream.of(
        PartitionSpec.unpartitioned(),
        PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(3).alwaysNull("id").build());
  }

  @ParameterizedTest
  @MethodSource("unpartitionedSpecs")
  void leafWritesNullSpecIdAndPartitionForUnpartitionedFile(PartitionSpec spec) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.specId()).thenReturn(spec.specId());
    when(file.partition()).thenReturn(mock(StructLike.class));

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            ImmutableMap.of(spec.specId(), spec),
            spec.partitionType(),
            NO_STATS_TYPE);
    StructLike row = wrapper.wrap(file, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build());

    assertThat(get(row, TRACKED_FILE_SCHEMA, TrackedFile.SPEC_ID)).isNull();
    assertThat(get(row, TRACKED_FILE_SCHEMA, TrackedFile.PARTITION_ID)).isNull();
  }

  @Test
  void leafProjectsPartitionToUnionType() {
    Map<Integer, PartitionSpec> specs =
        ImmutableMap.of(ID_SPEC.specId(), ID_SPEC, DATA_SPEC.specId(), DATA_SPEC);
    Types.StructType unionType = Partitioning.unionPartitionTypes(specs.values());
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.specId()).thenReturn(DATA_SPEC.specId());
    when(file.partition()).thenReturn(partition(DATA_SPEC, "a"));

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(COMMIT_SNAPSHOT_ID, specs, unionType, NO_STATS_TYPE);
    StructLike row = wrapper.wrap(file, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build());

    Schema schema = TrackedFile.schema(unionType, NO_STATS_TYPE);
    assertThat(get(row, schema, DATA_PARTITION_FIELD_ID)).isEqualTo("a");
    assertThat(get(row, schema, ID_PARTITION_FIELD_ID)).isNull();
  }

  @Test
  void leafWritesNullForMissingFieldStats() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.contentStats()).thenReturn(mock(ContentStats.class));

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, STATS_TYPE);
    StructLike row = wrapper.wrap(file, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build());

    StructLike stats = (StructLike) get(row, TRACKED_FILE_SCHEMA, TrackedFile.CONTENT_STATS_ID);
    for (int pos = 0; pos < STATS_TYPE.fields().size(); pos += 1) {
      assertThat(stats.get(pos, Object.class)).isNull();
    }
  }

  @Test
  void leafWritesNullForMissingStatsCounts() {
    ContentStats contentStats = mock(ContentStats.class);
    for (Types.NestedField column : TABLE_SCHEMA.columns()) {
      doReturn(mock(FieldStats.class)).when(contentStats).statsFor(column.fieldId());
    }

    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.contentStats()).thenReturn(contentStats);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, STATS_TYPE);
    StructLike row = wrapper.wrap(file, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build());

    Schema fileSchema = TrackedFile.schema(UNPARTITIONED_TYPE, STATS_TYPE);
    for (Types.NestedField column : TABLE_SCHEMA.columns()) {
      int baseId = StatsUtil.toBaseId(column.fieldId());
      assertThat(get(row, fileSchema, baseId + StatsUtil.VALUE_COUNT_OFFSET)).isNull();
      assertThat(get(row, fileSchema, baseId + StatsUtil.NULL_VALUE_COUNT_OFFSET)).isNull();
      if (column.type().typeId() == Type.TypeID.STRING) {
        assertThat(get(row, fileSchema, baseId + StatsUtil.TOTAL_BYTES_OFFSET)).isNull();
      }

      if (column.type().typeId() == Type.TypeID.FLOAT) {
        assertThat(get(row, fileSchema, baseId + StatsUtil.NAN_VALUE_COUNT_OFFSET)).isNull();
      }
    }
  }

  @Test
  void rootWritesCommitMinSequenceNumberForNewManifest() {
    ManifestInfo info = mock(ManifestInfo.class);
    when(info.minSequenceNumber()).thenReturn(ManifestWriter.UNASSIGNED_SEQ);
    TrackedFile manifest = mock(TrackedFile.class);
    when(manifest.contentType()).thenReturn(FileContent.DATA_MANIFEST);
    when(manifest.location()).thenReturn(MANIFEST_LOCATION);
    when(manifest.manifestInfo()).thenReturn(info);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);
    StructLike row =
        wrapper.wrap(manifest, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build(), FIRST_ROW_ID);

    assertThat(get(row, TRACKED_FILE_SCHEMA, ManifestInfo.MIN_SEQUENCE_NUMBER))
        .isEqualTo(COMMIT_SEQ);
  }

  @Test
  void leafWritesNullInheritedFieldsForAddedFile() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);
    StructLike row = wrapper.wrap(file, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build());

    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.SEQUENCE_NUMBER)).isNull();
    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.FILE_SEQUENCE_NUMBER)).isNull();
    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.FIRST_ROW_ID)).isNull();
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"DATA", "DATA_MANIFEST"})
  void rootWritesCommitValuesForAddedFile(FileContent content) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(content);
    when(file.location()).thenReturn(DATA_LOCATION);
    if (content == FileContent.DATA_MANIFEST) {
      when(file.manifestInfo()).thenReturn(mock(ManifestInfo.class));
    }

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);
    StructLike row =
        wrapper.wrap(file, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build(), FIRST_ROW_ID);

    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.SEQUENCE_NUMBER)).isEqualTo(COMMIT_SEQ);
    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.FILE_SEQUENCE_NUMBER)).isEqualTo(COMMIT_SEQ);
    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.FIRST_ROW_ID)).isEqualTo(FIRST_ROW_ID);
  }

  @Test
  void leafWritesNullDataSequenceNumberForFileModifiedInCommit() {
    Tracking source = mock(Tracking.class);
    when(source.status()).thenReturn(EntryStatus.EXISTING);
    when(source.dataSequenceNumber()).thenReturn(PREVIOUS_DATA_SEQ);
    when(source.fileSequenceNumber()).thenReturn(PREVIOUS_FILE_SEQ);

    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.tracking()).thenReturn(source);

    Tracking tracking =
        TrackingBuilder.from(file.tracking(), COMMIT_SNAPSHOT_ID).columnFilesUpdated().build();

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);
    StructLike row = wrapper.wrap(file, tracking);

    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.SEQUENCE_NUMBER)).isNull();
    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.FILE_SEQUENCE_NUMBER))
        .isEqualTo(PREVIOUS_FILE_SEQ);
  }

  @Test
  void rootWritesCommitDataSequenceNumberForFileModifiedInCommit() {
    Tracking source = mock(Tracking.class);
    when(source.status()).thenReturn(EntryStatus.EXISTING);
    when(source.dataSequenceNumber()).thenReturn(PREVIOUS_DATA_SEQ);
    when(source.fileSequenceNumber()).thenReturn(PREVIOUS_FILE_SEQ);

    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.tracking()).thenReturn(source);

    Tracking tracking =
        TrackingBuilder.from(file.tracking(), COMMIT_SNAPSHOT_ID).columnFilesUpdated().build();

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);
    StructLike row = wrapper.wrap(file, tracking, null /* assignedFirstRowId */);

    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.SEQUENCE_NUMBER)).isEqualTo(COMMIT_SEQ);
    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.FILE_SEQUENCE_NUMBER))
        .isEqualTo(PREVIOUS_FILE_SEQ);
  }

  @Test
  void leafRejectsPartitionTypeMissingSpecField() {
    Map<Integer, PartitionSpec> specs =
        ImmutableMap.of(ID_SPEC.specId(), ID_SPEC, DATA_SPEC.specId(), DATA_SPEC);

    assertThatThrownBy(
            () ->
                new LeafTrackedFileWrapper(
                    COMMIT_SNAPSHOT_ID, specs, ID_SPEC.partitionType(), NO_STATS_TYPE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid union partition type, missing field %s from spec %s: %s",
            DATA_PARTITION_FIELD_ID, DATA_SPEC.specId(), ID_SPEC.partitionType());
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"DATA_MANIFEST", "DELETE_MANIFEST"})
  void leafRejectsManifest(FileContent content) {
    TrackedFile manifest = mock(TrackedFile.class);
    when(manifest.contentType()).thenReturn(content);
    when(manifest.location()).thenReturn(MANIFEST_LOCATION);
    when(manifest.manifestInfo()).thenReturn(mock(ManifestInfo.class));

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(
            () -> wrapper.wrap(manifest, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for leaf manifest: %s", content);
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"POSITION_DELETES", "EQUALITY_DELETES"})
  void leafRejectsDeleteFile(FileContent content) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(content);
    when(file.location()).thenReturn(DATA_LOCATION);
    Tracking tracking = TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build();

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for leaf manifest: %s", content);
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"POSITION_DELETES", "EQUALITY_DELETES"})
  void rootRejectsDeleteFile(FileContent content) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(content);
    when(file.location()).thenReturn(DATA_LOCATION);
    Tracking tracking = TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build();

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking, null /* assignedFirstRowId */))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for root manifest: %s", content);
  }

  @Test
  void leafRejectsDataFileWithManifestInfo() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.manifestInfo()).thenReturn(mock(ManifestInfo.class));
    Tracking tracking = TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build();

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid data file, manifest info must be null: %s", file.location());
  }

  @Test
  void rootRejectsDataFileWithManifestInfo() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.manifestInfo()).thenReturn(mock(ManifestInfo.class));
    Tracking tracking = TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build();

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking, FIRST_ROW_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid data file, manifest info must be null: %s", file.location());
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"DATA_MANIFEST", "DELETE_MANIFEST"})
  void rootRejectsManifestWithoutManifestInfo(FileContent content) {
    TrackedFile manifest = mock(TrackedFile.class);
    when(manifest.contentType()).thenReturn(content);
    when(manifest.location()).thenReturn(MANIFEST_LOCATION);
    when(manifest.manifestInfo()).thenReturn(null);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    Long firstRowId = content == FileContent.DATA_MANIFEST ? FIRST_ROW_ID : null;

    assertThatThrownBy(
            () ->
                wrapper.wrap(
                    manifest, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build(), firstRowId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid manifest, missing manifest info: %s", manifest.location());
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"DATA_MANIFEST", "DELETE_MANIFEST"})
  void rootRejectsManifestWithDeletionVector(FileContent content) {
    TrackedFile manifest = mock(TrackedFile.class);
    when(manifest.contentType()).thenReturn(content);
    when(manifest.location()).thenReturn(MANIFEST_LOCATION);
    when(manifest.manifestInfo()).thenReturn(mock(ManifestInfo.class));
    when(manifest.deletionVector()).thenReturn(mock(DeletionVector.class));

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    Long firstRowId = content == FileContent.DATA_MANIFEST ? FIRST_ROW_ID : null;

    assertThatThrownBy(
            () ->
                wrapper.wrap(
                    manifest, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build(), firstRowId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid manifest, deletion vector must be null: %s", manifest.location());
  }

  @Test
  void leafRejectsUnknownSpecId() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.specId()).thenReturn(ID_SPEC.specId());
    Tracking tracking = TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build();

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find partition spec for spec ID: %s", ID_SPEC.specId());
  }

  @Test
  void rootRejectsUnknownSpecId() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);
    when(file.specId()).thenReturn(ID_SPEC.specId());
    Tracking tracking = TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build();

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking, FIRST_ROW_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find partition spec for spec ID: %s", ID_SPEC.specId());
  }

  @Test
  void rootRejectsMissingSnapshotId() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(EntryStatus.ADDED);
    when(tracking.snapshotId()).thenReturn(null);
    when(tracking.firstRowId()).thenReturn(null);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking, FIRST_ROW_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing snapshot ID for file: %s", file.location());
  }

  @Test
  void leafRejectsFirstRowIdForAddedFile() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(EntryStatus.ADDED);
    when(tracking.firstRowId()).thenReturn(PREVIOUS_FIRST_ROW_ID);
    when(tracking.snapshotId()).thenReturn(COMMIT_SNAPSHOT_ID);
    when(tracking.dataSequenceNumber()).thenReturn(null);
    when(tracking.fileSequenceNumber()).thenReturn(null);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid first-row-id for ADDED file (must be null): %s", file.location());
  }

  @Test
  void rootRejectsFirstRowIdForAddedFile() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(EntryStatus.ADDED);
    when(tracking.firstRowId()).thenReturn(PREVIOUS_FIRST_ROW_ID);
    when(tracking.snapshotId()).thenReturn(COMMIT_SNAPSHOT_ID);
    when(tracking.dataSequenceNumber()).thenReturn(null);
    when(tracking.fileSequenceNumber()).thenReturn(null);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking, null /* assignedFirstRowId */))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid first-row-id for ADDED file (must be null): %s", file.location());
  }

  @ParameterizedTest
  @EnumSource(value = EntryStatus.class, mode = EnumSource.Mode.EXCLUDE, names = "ADDED")
  void leafRejectsMissingFirstRowIdForNonAddedFile(EntryStatus status) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(status);
    when(tracking.firstRowId()).thenReturn(null);
    when(tracking.modifiedSnapshotId()).thenReturn(COMMIT_SNAPSHOT_ID);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing first-row-id for %s file: %s", status, file.location());
  }

  @ParameterizedTest
  @EnumSource(value = EntryStatus.class, mode = EnumSource.Mode.EXCLUDE, names = "ADDED")
  void rootRejectsMissingFirstRowIdForNonAddedFile(EntryStatus status) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(status);
    when(tracking.firstRowId()).thenReturn(null);
    when(tracking.modifiedSnapshotId()).thenReturn(COMMIT_SNAPSHOT_ID);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking, null /* assignedFirstRowId */))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing first-row-id for %s file: %s", status, file.location());
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"DATA", "DATA_MANIFEST"})
  void rootRejectsMissingFirstRowIdAssignmentForAddedFile(FileContent content) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(content);
    when(file.location()).thenReturn(DATA_LOCATION);
    if (content == FileContent.DATA_MANIFEST) {
      when(file.manifestInfo()).thenReturn(mock(ManifestInfo.class));
    }

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(
            () ->
                wrapper.wrap(
                    file,
                    TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build(),
                    null /* assignedFirstRowId */))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing first-row-id assignment for ADDED file: %s", file.location());
  }

  @ParameterizedTest
  @EnumSource(value = EntryStatus.class, mode = EnumSource.Mode.EXCLUDE, names = "ADDED")
  void rootRejectsFirstRowIdAssignmentForNonAddedFile(EntryStatus status) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(status);
    when(tracking.firstRowId()).thenReturn(PREVIOUS_FIRST_ROW_ID);
    when(tracking.modifiedSnapshotId()).thenReturn(COMMIT_SNAPSHOT_ID);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking, FIRST_ROW_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot assign first-row-id to %s file: %s", status, file.location());
  }

  @Test
  void rootRejectsFirstRowIdAssignmentForDeleteManifest() {
    TrackedFile manifest = mock(TrackedFile.class);
    when(manifest.contentType()).thenReturn(FileContent.DELETE_MANIFEST);
    when(manifest.location()).thenReturn(MANIFEST_LOCATION);
    when(manifest.manifestInfo()).thenReturn(mock(ManifestInfo.class));

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, COMMIT_SEQ, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(
            () ->
                wrapper.wrap(
                    manifest, TrackingBuilder.added(COMMIT_SNAPSHOT_ID).build(), FIRST_ROW_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot assign first-row-id to delete manifest: %s", manifest.location());
  }

  @ParameterizedTest
  @EnumSource(
      value = EntryStatus.class,
      names = {"EXISTING", "DELETED", "REPLACED"})
  void leafRejectsNullDataSequenceNumberForStatus(EntryStatus status) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(status);
    when(tracking.dataSequenceNumber()).thenReturn(null);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid data sequence number (null) for %s file", status);
  }

  @Test
  void leafRejectsNullDataSequenceNumberForFileAddedInAnotherSnapshot() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(EntryStatus.ADDED);
    when(tracking.snapshotId()).thenReturn(PREVIOUS_SNAPSHOT_ID);
    when(tracking.dataSequenceNumber()).thenReturn(null);
    when(tracking.firstRowId()).thenReturn(null);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Found invalid snapshot ID for ADDED file: %s (committing %s)",
            PREVIOUS_SNAPSHOT_ID, COMMIT_SNAPSHOT_ID);
  }

  @Test
  void leafRejectsNullDataSequenceNumberForFileModifiedInAnotherSnapshot() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(EntryStatus.MODIFIED);
    when(tracking.modifiedSnapshotId()).thenReturn(PREVIOUS_MODIFIED_SNAPSHOT_ID);
    when(tracking.dataSequenceNumber()).thenReturn(null);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);

    assertThatThrownBy(() -> wrapper.wrap(file, tracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Found invalid snapshot ID for MODIFIED file: %s (committing %s)",
            PREVIOUS_MODIFIED_SNAPSHOT_ID, COMMIT_SNAPSHOT_ID);
  }

  @Test
  void leafAcceptsAddedFileWithoutCommitSnapshotId() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn(DATA_LOCATION);

    Tracking tracking = mock(Tracking.class);
    when(tracking.status()).thenReturn(EntryStatus.ADDED);
    when(tracking.snapshotId()).thenReturn(null);
    when(tracking.dataSequenceNumber()).thenReturn(null);
    when(tracking.fileSequenceNumber()).thenReturn(null);
    when(tracking.firstRowId()).thenReturn(null);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            null /* snapshotId */, UNPARTITIONED_SPECS, UNPARTITIONED_TYPE, NO_STATS_TYPE);
    StructLike row = wrapper.wrap(file, tracking);

    assertThat(get(row, TRACKED_FILE_SCHEMA, Tracking.SNAPSHOT_ID)).isNull();
  }

  private static Object get(StructLike row, Schema schema, Types.NestedField field) {
    return get(row, schema, field.fieldId());
  }

  private static Object get(StructLike row, Schema schema, int fieldId) {
    return schema.accessorForField(fieldId).get(row);
  }
}
