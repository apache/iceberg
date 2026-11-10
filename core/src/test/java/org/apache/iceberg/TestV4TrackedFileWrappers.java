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

import static org.apache.iceberg.V4TestHelpers.ADDED_TRACKING;
import static org.apache.iceberg.V4TestHelpers.columnFile;
import static org.apache.iceberg.V4TestHelpers.copyWithTracking;
import static org.apache.iceberg.V4TestHelpers.dataFile;
import static org.apache.iceberg.V4TestHelpers.dataFileWithColumnFiles;
import static org.apache.iceberg.V4TestHelpers.dataFileWithStats;
import static org.apache.iceberg.V4TestHelpers.deletionVector;
import static org.apache.iceberg.V4TestHelpers.manifestRef;
import static org.apache.iceberg.V4TestHelpers.partition;
import static org.apache.iceberg.V4TestHelpers.readTrackedFiles;
import static org.apache.iceberg.V4TestHelpers.writeRows;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.V4Metadata.LeafTrackedFileWrapper;
import org.apache.iceberg.V4Metadata.RootTrackedFileWrapper;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestV4TrackedFileWrappers {
  private static final long COMMIT_SNAPSHOT_ID = 42L;
  private static final long COMMIT_SEQUENCE_NUMBER = 34L;
  private static final long PREVIOUS_SNAPSHOT_ID = 7L;
  private static final long PREVIOUS_SEQUENCE_NUMBER = 5L;
  private static final long PREVIOUS_FIRST_ROW_ID = 500L;
  private static final long FIRST_ROW_ID = 1_000L;

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec ID_SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(1).identity("id").build();
  private static final PartitionSpec DATA_SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA)
          .withSpecId(2)
          .add(2 /* sourceId */, 1001 /* fieldId */, "data", Transforms.identity())
          .build();
  private static final Map<Integer, PartitionSpec> ID_SPECS =
      ImmutableMap.of(ID_SPEC.specId(), ID_SPEC);
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());

  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(
          TABLE_SCHEMA, MetricsTestUtil.from(ImmutableMap.of(), TABLE_SCHEMA));
  private static final FieldStats<Integer> ID_STATS =
      new FieldStatsStruct<>(
          STATS_TYPE.fieldType("id").asStructType(),
          1 /* lowerBound */,
          99 /* upperBound */,
          true /* tightBounds */,
          100L /* valueCount */,
          0L /* nullValueCount */,
          0L /* nanValueCount */,
          0L /* totalBytes */);
  private static final FieldStats<String> DATA_STATS =
      new FieldStatsStruct<>(
          STATS_TYPE.fieldType("data").asStructType(),
          "a",
          "z",
          false /* tightBounds */,
          100L /* valueCount */,
          10L /* nullValueCount */,
          0L /* nanValueCount */,
          500L /* totalBytes */);

  private static final Tracking EXISTING_TRACKING =
      new TrackingStruct(
          EntryStatus.EXISTING,
          PREVIOUS_SNAPSHOT_ID,
          PREVIOUS_SEQUENCE_NUMBER,
          PREVIOUS_SEQUENCE_NUMBER,
          null /* modifiedSnapshotId */,
          PREVIOUS_FIRST_ROW_ID,
          null /* deletedPositions */,
          null /* replacedPositions */);
  private static final ManifestInfo MANIFEST_INFO =
      new ManifestInfoStruct(
          49 /* addedFilesCount */,
          51 /* existingFilesCount */,
          0 /* deletedFilesCount */,
          0 /* replacedFilesCount */,
          0 /* modifiedFilesCount */,
          4_900L /* addedRowsCount */,
          5_100L /* existingRowsCount */,
          0L /* deletedRowsCount */,
          0L /* replacedRowsCount */,
          0L /* modifiedRowsCount */,
          PREVIOUS_SEQUENCE_NUMBER,
          null /* dv */,
          4 /* formatVersion */);

  private final InMemoryFileIO io = new InMemoryFileIO();

  @Test
  void leafWritesDataFile() throws IOException {
    TrackedFile file =
        dataFile(
            "s3://bucket/table/id=1/file.parquet",
            ID_SPEC.specId(),
            partition(ID_SPEC, 1),
            null /* stats */,
            deletionVector("s3://bucket/table/dv.puffin"),
            ImmutableList.of(
                columnFile("s3://bucket/table/id=1/file-cols-1.parquet", ImmutableSet.of(1)),
                columnFile(
                    "s3://bucket/table/id=1/file-cols-2.parquet",
                    ImmutableSet.of(2),
                    ByteBuffer.wrap(new byte[] {4, 5}))));
    TrackedFile expected = copyWithTracking(file, EXISTING_TRACKING);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            ID_SPECS, ID_SPEC.partitionType(), Types.StructType.of() /* contentStatsType */);
    TrackedFile actual =
        writeAndRead(ID_SPEC.partitionType(), wrapper.wrap(file, EXISTING_TRACKING));

    assertThat(actual)
        .usingComparator(V4TestComparators.trackedFile(ID_SPEC.partitionType()))
        .isEqualTo(expected);
  }

  @Test
  void leafWritesAdaptedDataFile() throws IOException {
    DataFile dataFile =
        DataFiles.builder(ID_SPEC)
            .withPath("s3://bucket/table/id=1/file.parquet")
            .withPartition(partition(ID_SPEC, 1))
            .withFileSizeInBytes(1024L)
            .withMetrics(
                new Metrics(
                    100L /* rowCount */,
                    null /* columnSizes */,
                    ImmutableMap.of(1, 100L, 2, 100L),
                    ImmutableMap.of(1, 0L, 2, 10L),
                    null /* nanValueCounts */,
                    ImmutableMap.of(
                        1, Conversions.toByteBuffer(Types.IntegerType.get(), 1),
                        2, Conversions.toByteBuffer(Types.StringType.get(), "a")),
                    ImmutableMap.of(
                        1, Conversions.toByteBuffer(Types.IntegerType.get(), 99),
                        2, Conversions.toByteBuffer(Types.StringType.get(), "z"))))
            .withSortOrder(SortOrder.builderFor(TABLE_SCHEMA).withOrderId(1).asc("id").build())
            .withEncryptionKeyMetadata(ByteBuffer.wrap(new byte[] {1, 2, 3}))
            .withSplitOffsets(ImmutableList.of(4L, 512L))
            .build();
    TrackedFile file = TrackedFileAdapters.forDataFile(TABLE_SCHEMA).wrap(dataFile);
    TrackedFile expected = copyWithTracking(file, ADDED_TRACKING);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(ID_SPECS, ID_SPEC.partitionType(), STATS_TYPE);
    TrackedFile actual =
        writeAndRead(ID_SPEC.partitionType(), STATS_TYPE, wrapper.wrap(file, ADDED_TRACKING));

    assertThat(actual)
        .usingComparator(V4TestComparators.trackedFile(ID_SPEC.partitionType()))
        .isEqualTo(expected);
  }

  @Test
  void leafWritesNullSpecIdForUnpartitionedFile() throws IOException {
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("s3://bucket/table/file.parquet")
            .withFileSizeInBytes(1024L)
            .withRecordCount(100L)
            .build();
    TrackedFile file = TrackedFileAdapters.forDataFile(TABLE_SCHEMA).wrap(dataFile);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    TrackedFile actual = writeAndRead(Types.StructType.of(), wrapper.wrap(file, ADDED_TRACKING));

    assertThat(actual.specId()).isNull();
    assertThat(actual.partition()).isNull();
  }

  @Test
  void leafProjectsPartitionToUnionType() throws IOException {
    Map<Integer, PartitionSpec> specs =
        ImmutableMap.of(ID_SPEC.specId(), ID_SPEC, DATA_SPEC.specId(), DATA_SPEC);
    Types.StructType unionType = Partitioning.unionPartitionTypes(specs.values());
    TrackedFile file =
        dataFile(
            "s3://bucket/table/data=a/file.parquet", DATA_SPEC.specId(), partition(DATA_SPEC, "a"));

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(specs, unionType, Types.StructType.of() /* contentStatsType */);
    TrackedFile actual = writeAndRead(unionType, wrapper.wrap(file, file.tracking()));

    PartitionData expected = new PartitionData(unionType);
    expected.set(unionType.fields().indexOf(unionType.field("data")), "a");
    assertThat(actual.partition())
        .usingComparator(Comparators.forType(unionType))
        .isEqualTo(expected);
  }

  @Test
  void leafWritesStatsUsingManifestStatsType() throws IOException {
    Types.StructType reorderedType =
        Types.StructType.of(STATS_TYPE.field("data"), STATS_TYPE.field("id"));
    ContentStats stats = StatsTestUtil.contentStats(reorderedType, ID_STATS, DATA_STATS);
    TrackedFile file = dataFileWithStats("s3://bucket/table/file.parquet", stats);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            UNPARTITIONED_SPECS, Types.StructType.of() /* partitionType */, STATS_TYPE);
    TrackedFile actual =
        writeAndRead(Types.StructType.of(), STATS_TYPE, wrapper.wrap(file, file.tracking()));

    assertThat(actual.contentStats())
        .usingComparator(V4TestComparators.CONTENT_STATS)
        .isEqualTo(stats);
  }

  @Test
  void leafWritesNullSpecIdForVoidTransformFile() throws IOException {
    PartitionSpec voidSpec =
        PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(3).alwaysNull("id").build();
    TrackedFile file =
        dataFile("s3://bucket/table/file.parquet", voidSpec.specId(), partition(voidSpec));

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            ImmutableMap.of(voidSpec.specId(), voidSpec),
            voidSpec.partitionType(),
            Types.StructType.of() /* contentStatsType */);
    TrackedFile actual = writeAndRead(voidSpec.partitionType(), wrapper.wrap(file, ADDED_TRACKING));

    assertThat(actual.specId()).isNull();
    assertThat(actual.partition()).isNull();
  }

  @Test
  void leafRejectsPartitionTypeMissingSpecField() {
    Map<Integer, PartitionSpec> specs =
        ImmutableMap.of(ID_SPEC.specId(), ID_SPEC, DATA_SPEC.specId(), DATA_SPEC);
    int dataFieldId = DATA_SPEC.fields().get(0).fieldId();

    assertThatThrownBy(
            () ->
                new LeafTrackedFileWrapper(
                    specs, ID_SPEC.partitionType(), Types.StructType.of() /* contentStatsType */))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid union partition type, missing field %s from spec %s: %s",
            dataFieldId, DATA_SPEC.specId(), ID_SPEC.partitionType());
  }

  @Test
  void leafRejectsUnassignedDataSequenceNumberForExistingFile() {
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.EXISTING,
            COMMIT_SNAPSHOT_ID,
            null /* dataSequenceNumber */,
            null /* fileSequenceNumber */,
            null /* modifiedSnapshotId */,
            null /* firstRowId */,
            null /* deletedPositions */,
            null /* replacedPositions */);
    TrackedFile file = dataFile(tracking, "s3://bucket/table/file.parquet");

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, file.tracking()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot write null data sequence number for status %s (must be ADDED or MODIFIED): %s",
            EntryStatus.EXISTING, file.location());
  }

  @Test
  void leafRejectsUnassignedFileSequenceNumberForModifiedFile() {
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.MODIFIED,
            PREVIOUS_SNAPSHOT_ID,
            null /* dataSequenceNumber */,
            null /* fileSequenceNumber */,
            COMMIT_SNAPSHOT_ID /* modifiedSnapshotId */,
            PREVIOUS_FIRST_ROW_ID,
            null /* deletedPositions */,
            null /* replacedPositions */);
    TrackedFile file = dataFile(tracking, "s3://bucket/table/file.parquet");

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, file.tracking()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot write null file sequence number for status %s (must be ADDED): %s",
            EntryStatus.MODIFIED, file.location());
  }

  @Test
  void leafAllowsUnassignedDataSequenceNumberForModifiedFile() throws IOException {
    Tracking tracking =
        TrackingBuilder.from(EXISTING_TRACKING, COMMIT_SNAPSHOT_ID).columnFilesUpdated().build();
    TrackedFile file =
        dataFileWithColumnFiles(
            tracking,
            "s3://bucket/table/file.parquet",
            ImmutableList.of(
                columnFile("s3://bucket/table/file-cols.parquet", ImmutableSet.of(2))));

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    TrackedFile actual = writeAndRead(Types.StructType.of(), wrapper.wrap(file, file.tracking()));

    assertThat(actual.tracking().dataSequenceNumber()).isNull();
  }

  @ParameterizedTest
  @EnumSource(value = FileContent.class, mode = EnumSource.Mode.EXCLUDE, names = "DATA")
  void leafRejectsNonDataFile(FileContent content) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(content);

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, ADDED_TRACKING))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for leaf manifest: %s", content);
  }

  @Test
  void leafRejectsUnknownSpecId() {
    TrackedFile file =
        dataFile("s3://bucket/table/id=1/file.parquet", ID_SPEC.specId(), partition(ID_SPEC, 1));

    LeafTrackedFileWrapper wrapper =
        new LeafTrackedFileWrapper(
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, ADDED_TRACKING))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find partition spec for spec ID: %s", ID_SPEC.specId());
  }

  @Test
  void rootWritesDataFile() throws IOException {
    DataFile dataFile =
        DataFiles.builder(ID_SPEC)
            .withPath("s3://bucket/table/id=1/file.parquet")
            .withPartition(partition(ID_SPEC, 1))
            .withFileSizeInBytes(1024L)
            .withRecordCount(100L)
            .build();
    TrackedFile file = TrackedFileAdapters.forDataFile(TABLE_SCHEMA).wrap(dataFile);
    TrackedFile expected =
        copyWithTracking(
            file,
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                COMMIT_SEQUENCE_NUMBER,
                COMMIT_SEQUENCE_NUMBER,
                null /* modifiedSnapshotId */,
                FIRST_ROW_ID,
                null /* deletedPositions */,
                null /* replacedPositions */));

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            ID_SPECS,
            ID_SPEC.partitionType(),
            Types.StructType.of() /* contentStatsType */);
    TrackedFile actual =
        writeAndRead(ID_SPEC.partitionType(), wrapper.wrap(file, ADDED_TRACKING, FIRST_ROW_ID));

    assertThat(actual)
        .usingComparator(V4TestComparators.trackedFile(ID_SPEC.partitionType()))
        .isEqualTo(expected);
  }

  @Test
  void rootAssignsCommitDataSequenceNumberToModifiedDataFile() throws IOException {
    Tracking tracking =
        TrackingBuilder.from(EXISTING_TRACKING, COMMIT_SNAPSHOT_ID).columnFilesUpdated().build();
    TrackedFile file =
        dataFileWithColumnFiles(
            tracking,
            "s3://bucket/table/file.parquet",
            ImmutableList.of(
                columnFile("s3://bucket/table/file-cols.parquet", ImmutableSet.of(2))));

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(),
            wrapper.wrap(file, file.tracking(), null /* assignedFirstRowId */));

    assertThat(actual.tracking().dataSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
    assertThat(actual.tracking().fileSequenceNumber()).isEqualTo(PREVIOUS_SEQUENCE_NUMBER);
  }

  @Test
  void rootRejectsNullSnapshotIdForDataFile() {
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            null /* snapshotId */,
            null /* dataSequenceNumber */,
            null /* fileSequenceNumber */,
            null /* modifiedSnapshotId */,
            null /* firstRowId */,
            null /* deletedPositions */,
            null /* replacedPositions */);
    TrackedFile file = dataFile(tracking, "s3://bucket/table/file.parquet");

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, file.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing snapshot ID for file: %s", file.location());
  }

  @Test
  void rootRejectsUnassignedFirstRowIdForDataFile() {
    TrackedFile file = dataFile(ADDED_TRACKING, "s3://bucket/table/file.parquet");

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, file.tracking(), null /* assignedFirstRowId */))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Missing first-row-id for file: %s", file.location());
  }

  @ParameterizedTest
  @EnumSource(
      value = EntryStatus.class,
      names = {"DELETED", "REPLACED"})
  void rootRejectsFirstRowIdAssignmentForNonLiveDataFile(EntryStatus status) {
    Tracking tracking =
        new TrackingStruct(
            status,
            COMMIT_SNAPSHOT_ID,
            PREVIOUS_SEQUENCE_NUMBER,
            PREVIOUS_SEQUENCE_NUMBER,
            null /* modifiedSnapshotId */,
            null /* firstRowId */,
            null /* deletedPositions */,
            null /* replacedPositions */);
    TrackedFile file = dataFile(tracking, "s3://bucket/table/file.parquet");

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, file.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot assign first-row-id to file with status %s: %s", status, file.location());
  }

  @ParameterizedTest
  @EnumSource(
      value = EntryStatus.class,
      names = {"EXISTING", "MODIFIED"})
  void rootRejectsFirstRowIdAssignmentForAssignedDataFile(EntryStatus status) {
    Tracking tracking =
        new TrackingStruct(
            status,
            PREVIOUS_SNAPSHOT_ID,
            PREVIOUS_SEQUENCE_NUMBER,
            PREVIOUS_SEQUENCE_NUMBER,
            null /* modifiedSnapshotId */,
            PREVIOUS_FIRST_ROW_ID,
            null /* deletedPositions */,
            null /* replacedPositions */);
    TrackedFile file = dataFile(tracking, "s3://bucket/table/file.parquet");

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, file.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot assign first-row-id to file with existing first-row-id %s: %s",
            PREVIOUS_FIRST_ROW_ID, file.location());
  }

  @Test
  void rootWritesManifest() throws IOException {
    TrackedFile manifest =
        manifestRef(FileContent.DATA_MANIFEST, "s3://bucket/table/leaf.parquet", MANIFEST_INFO);
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.MODIFIED,
            PREVIOUS_SNAPSHOT_ID,
            PREVIOUS_SEQUENCE_NUMBER,
            PREVIOUS_SEQUENCE_NUMBER,
            COMMIT_SNAPSHOT_ID /* modifiedSnapshotId */,
            PREVIOUS_FIRST_ROW_ID,
            new byte[] {1} /* deletedPositions */,
            new byte[] {2} /* replacedPositions */);
    TrackedFile expected = copyWithTracking(manifest, tracking);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(), wrapper.wrap(manifest, tracking, null /* assignedFirstRowId */));

    assertThat(actual)
        .usingComparator(V4TestComparators.trackedFile(Types.StructType.of()))
        .isEqualTo(expected);
  }

  @Test
  void rootAssignsCommitSequenceNumberToNewManifest() throws IOException {
    ManifestFile manifest = newManifest(COMMIT_SNAPSHOT_ID);
    TrackedFile file = TrackedFileAdapters.forManifestFile().wrap(manifest);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    TrackedFile actual =
        writeAndRead(Types.StructType.of(), wrapper.wrap(file, file.tracking(), FIRST_ROW_ID));

    assertThat(actual.tracking().dataSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
    assertThat(actual.tracking().fileSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
    assertThat(actual.manifestInfo().minSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
  }

  @Test
  void rootReturnsNullFirstRowIdForDeleteManifest() throws IOException {
    TrackedFile manifest =
        manifestRef(FileContent.DELETE_MANIFEST, "s3://bucket/table/deletes.avro", MANIFEST_INFO);
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.EXISTING,
            PREVIOUS_SNAPSHOT_ID,
            PREVIOUS_SEQUENCE_NUMBER,
            PREVIOUS_SEQUENCE_NUMBER,
            null /* modifiedSnapshotId */,
            PREVIOUS_FIRST_ROW_ID /* invalid for delete manifests */,
            null /* deletedPositions */,
            null /* replacedPositions */);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(), wrapper.wrap(manifest, tracking, null /* assignedFirstRowId */));

    // a stray first-row-id on a delete manifest is dropped rather than rejected, matching v3
    assertThat(actual.tracking().firstRowId()).isNull();
  }

  @Test
  void rootRejectsUnassignedSequenceNumberForManifestFromAnotherSnapshot() {
    ManifestFile manifest = newManifest(PREVIOUS_SNAPSHOT_ID);
    TrackedFile file = TrackedFileAdapters.forManifestFile().wrap(manifest);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, file.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot assign sequence number to file from snapshot %s (committing snapshot %s): %s",
            PREVIOUS_SNAPSHOT_ID, COMMIT_SNAPSHOT_ID, manifest.path());
  }

  @Test
  void rootRejectsFirstRowIdAssignmentForDeleteManifest() {
    TrackedFile manifest =
        manifestRef(FileContent.DELETE_MANIFEST, "s3://bucket/table/deletes.avro", MANIFEST_INFO);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(manifest, manifest.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot assign first-row-id to delete manifest: %s", manifest.location());
  }

  @ParameterizedTest
  @EnumSource(
      value = FileContent.class,
      names = {"POSITION_DELETES", "EQUALITY_DELETES"})
  void rootRejectsDeleteFile(FileContent content) {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(content);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, ADDED_TRACKING, null /* assignedFirstRowId */))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for root manifest: %s", content);
  }

  @Test
  void rootRejectsManifestWithoutManifestInfo() {
    TrackedFile manifest =
        manifestRef(FileContent.DATA_MANIFEST, "s3://bucket/table/leaf.parquet", null);

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(manifest, manifest.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid manifest, missing manifest info: %s", manifest.location());
  }

  @Test
  void rootRejectsDataFileWithManifestInfo() {
    TrackedFile file = mock(TrackedFile.class);
    when(file.contentType()).thenReturn(FileContent.DATA);
    when(file.location()).thenReturn("s3://bucket/table/file.parquet");
    when(file.manifestInfo()).thenReturn(mock(ManifestInfo.class));

    RootTrackedFileWrapper wrapper =
        new RootTrackedFileWrapper(
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            UNPARTITIONED_SPECS,
            Types.StructType.of() /* partitionType */,
            Types.StructType.of() /* contentStatsType */);
    assertThatThrownBy(() -> wrapper.wrap(file, ADDED_TRACKING, FIRST_ROW_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid data file, manifest info must be null: %s", file.location());
  }

  private static ManifestFile newManifest(long snapshotId) {
    return new GenericManifestFile(
        "s3://bucket/table/manifest.avro",
        1024L /* length */,
        PartitionSpec.unpartitioned().specId(),
        ManifestContent.DATA,
        ManifestWriter.UNASSIGNED_SEQ /* sequenceNumber */,
        ManifestWriter.UNASSIGNED_SEQ /* minSequenceNumber */,
        snapshotId,
        ImmutableList.of(),
        null /* keyMetadata */,
        1 /* addedFilesCount */,
        100L /* addedRowsCount */,
        0 /* existingFilesCount */,
        0L /* existingRowsCount */,
        0 /* deletedFilesCount */,
        0L /* deletedRowsCount */,
        null /* firstRowId */);
  }

  private TrackedFile writeAndRead(Types.StructType partitionType, StructLike row)
      throws IOException {
    return writeAndRead(partitionType, Types.StructType.of(), row);
  }

  private TrackedFile writeAndRead(
      Types.StructType partitionType, Types.StructType statsType, StructLike row)
      throws IOException {
    OutputFile out =
        writeRows(io, FileFormat.PARQUET, partitionType, statsType, ImmutableList.of(row));
    return Iterables.getOnlyElement(
        readTrackedFiles(out.toInputFile(), FileFormat.PARQUET, partitionType, statsType));
  }
}
