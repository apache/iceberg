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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestV4ManifestReader {
  private static final long SNAPSHOT_ID = 42L;
  private static final int FORMAT_VERSION_V4 = 4;
  private static final long RECORD_COUNT = 100L;
  private static final long FILE_SIZE_IN_BYTES = 1024L;
  private static final int SORT_ORDER_ID = 1;
  private static final String DV_LOCATION = "s3://bucket/dv.puffin";
  private static final long DV_OFFSET = 100L;
  private static final long DV_SIZE_IN_BYTES = 50L;
  private static final long DV_CARDINALITY = 5L;
  private static final DeletionVector DV =
      DeletionVectorStruct.builder()
          .location(DV_LOCATION)
          .offset(DV_OFFSET)
          .sizeInBytes(DV_SIZE_IN_BYTES)
          .cardinality(DV_CARDINALITY)
          .build();

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec ID_PARTITIONING =
      PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").build();
  private static final Types.StructType PARTITION_TYPE = ID_PARTITIONING.partitionType();
  private static final Types.StructType EMPTY_PARTITION = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_PARTITION);
  private static final Map<Integer, PartitionSpec> PARTITIONED_SPECS =
      ImmutableMap.of(ID_PARTITIONING.specId(), ID_PARTITIONING);
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());

  private static final List<FileFormat> FORMATS =
      ImmutableList.of(FileFormat.AVRO, FileFormat.PARQUET);

  // a data file whose tracking carries every inheritable and change-tracking value set
  private static final TrackedFile FILE_WITH_FULL_TRACKING =
      new TrackedFileStruct(
          new TrackingStruct(
              EntryStatus.ADDED,
              SNAPSHOT_ID,
              5L, // data sequence number
              6L, // file sequence number
              7L, // dv snapshot id
              8L, // first row id
              new byte[] {1, 2}, // deleted positions
              new byte[] {3, 4}), // replaced positions
          FileContent.DATA,
          FORMAT_VERSION_V4,
          "s3://bucket/file.parquet",
          FileFormat.PARQUET,
          RECORD_COUNT,
          FILE_SIZE_IN_BYTES,
          0,
          EMPTY_PARTITION_DATA,
          null,
          null,
          null,
          null,
          null,
          null,
          null);

  @TempDir private Path tempDir;

  private final FileIO fileIO = new TestTables.LocalFileIO();

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testReadsWrittenFile(FileFormat format) throws IOException {
    TrackedFile file =
        new TrackedFileStruct(
            addedTracking(),
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            ID_PARTITIONING.specId(),
            partition(7),
            null,
            SORT_ORDER_ID,
            DV,
            null,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(50L, 100L),
            null);

    InputFile manifest = writeManifest(format, PARTITION_TYPE, ImmutableList.of(file));

    TrackedFile actual = Iterables.getOnlyElement(read(manifest, PARTITIONED_SPECS));

    // compare with tracking reduced to status: the reader fills status-independent tracking
    // fields (row position, sequence numbers via inheritance) that the written file does not have
    Types.StructType comparisonType =
        TypeUtil.replaceFieldTypes(
                TrackedFile.schema(PARTITION_TYPE, Types.StructType.of()),
                ImmutableMap.of(
                    TrackedFile.TRACKING.fieldId(), Types.StructType.of(Tracking.STATUS)))
            .asStruct();
    assertThat((StructLike) actual)
        .usingComparator(Comparators.forType(comparisonType))
        .isEqualTo((StructLike) file);
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testEqualityDeleteRoundTrip(FileFormat format) throws IOException {
    TrackedFile delete =
        new TrackedFileStruct(
            addedTracking(),
            FileContent.EQUALITY_DELETES,
            FORMAT_VERSION_V4,
            "s3://bucket/eq-delete.parquet",
            FileFormat.PARQUET,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            0,
            EMPTY_PARTITION_DATA,
            null,
            null,
            null,
            null,
            null,
            null,
            ImmutableList.of(1, 2));

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(delete));

    TrackedFile actual = Iterables.getOnlyElement(read(manifest, UNPARTITIONED_SPECS));
    assertThat(actual.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(actual.equalityIds()).containsExactly(1, 2);
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testStatusFiltering(FileFormat format) throws IOException {
    List<TrackedFile> files =
        ImmutableList.of(
            fileWithStatus(EntryStatus.ADDED, "s3://bucket/added.parquet"),
            fileWithStatus(EntryStatus.EXISTING, "s3://bucket/existing.parquet"),
            fileWithStatus(EntryStatus.MODIFIED, "s3://bucket/modified.parquet"),
            fileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"),
            fileWithStatus(EntryStatus.REPLACED, "s3://bucket/replaced.parquet"));

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, files);

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS).build()) {
      assertThat(reader)
          .extracting(file -> file.tracking().status())
          .containsExactly(EntryStatus.ADDED, EntryStatus.EXISTING, EntryStatus.MODIFIED);
    }

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS).includeAll().build()) {
      assertThat(reader)
          .extracting(file -> file.tracking().status())
          .containsExactly(
              EntryStatus.ADDED,
              EntryStatus.EXISTING,
              EntryStatus.MODIFIED,
              EntryStatus.DELETED,
              EntryStatus.REPLACED);
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testManifestLocationAndPosition(FileFormat format) throws IOException {
    List<TrackedFile> files =
        ImmutableList.of(
            dataFile("s3://bucket/a.parquet", EMPTY_PARTITION_DATA),
            dataFile("s3://bucket/b.parquet", EMPTY_PARTITION_DATA),
            dataFile("s3://bucket/c.parquet", EMPTY_PARTITION_DATA));

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, files);

    List<TrackedFile> read = read(manifest, UNPARTITIONED_SPECS);
    assertThat(read)
        .allSatisfy(
            file -> assertThat(file.tracking().manifestLocation()).isEqualTo(manifest.location()));
    assertThat(read).extracting(file -> file.tracking().manifestPos()).containsExactly(0L, 1L, 2L);
  }

  @ParameterizedTest(name = "{0} / {2}")
  @MethodSource("restrictedReadModes")
  public void testRestrictedReadReturnsOnlyRequestedFields(
      FileFormat format, Consumer<V4ManifestReader.Builder> configureRead, String description)
      throws IOException {
    List<TrackedFile> files =
        ImmutableList.of(
            dataFile("s3://bucket/live.parquet", EMPTY_PARTITION_DATA),
            fileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"),
            fileWithStatus(EntryStatus.REPLACED, "s3://bucket/replaced.parquet"));

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, files);

    V4ManifestReader.Builder builder = V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS);
    configureRead.accept(builder);
    try (V4ManifestReader reader = builder.build()) {
      // content_type and status are projected for liveness filtering, so only the live entry
      // survives even though the caller requested only location
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isEqualTo("s3://bucket/live.parquet");
      // fields the caller did not request are not read
      assertThat(actual.fileFormat()).isNull();
      assertThat(actual.specId()).isNull();
      assertThat(actual.sortOrderId()).isNull();
    }
  }

  private static Stream<Arguments> restrictedReadModes() {
    Map<String, Consumer<V4ManifestReader.Builder>> modes =
        ImmutableMap.of(
            "project",
            builder -> builder.project(new Schema(TrackedFile.LOCATION)),
            "select",
            builder -> builder.select("location"),
            "case-insensitive select",
            builder -> builder.select("LOCATION").caseSensitive(false));
    return FORMATS.stream()
        .flatMap(
            format ->
                modes.entrySet().stream()
                    .map(mode -> Arguments.of(format, mode.getValue(), mode.getKey())));
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testRowFilterForcesRecordCount(FileFormat format) throws IOException {
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(file));

    // record_count is read when evaluating a row filter against file metrics, so it is projected
    // even though the caller selected only location
    Schema projection = new Schema(TrackedFile.LOCATION);
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS)
            .project(projection)
            .filter(Expressions.equal("id", 1))
            .build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isEqualTo(file.location());
      assertThat(actual.recordCount()).isEqualTo(RECORD_COUNT);
    }
  }

  @Test
  public void testProjectionModesAreMutuallyExclusive() {
    InputFile manifest = fileIO.newInputFile(tempDir.resolve("manifest.avro").toString());

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS)
                    .select("location")
                    .project(new Schema(TrackedFile.LOCATION)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot select columns using both select(Collection<String>) and project(Schema)");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS)
                    .project(new Schema(TrackedFile.LOCATION))
                    .select("location"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot select columns using both select(Collection<String>) and project(Schema)");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS)
                    .forScanPlanning()
                    .select("location"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use select(Collection<String>) with forScanPlanning()");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS)
                    .select("location")
                    .forScanPlanning())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot use forScanPlanning() with select(Collection<String>) or project(Schema)");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS)
                    .forScanPlanning()
                    .project(new Schema(TrackedFile.LOCATION)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use project(Schema) with forScanPlanning()");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS)
                    .project(new Schema(TrackedFile.LOCATION))
                    .forScanPlanning())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot use forScanPlanning() with select(Collection<String>) or project(Schema)");
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testProjectionPreservesNarrowTrackingProjection(FileFormat format)
      throws IOException {
    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(FILE_WITH_FULL_TRACKING));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS).select("tracking.status").build()) {
      Tracking actual = Iterables.getOnlyElement(reader).tracking();
      assertThat(actual.status()).isEqualTo(EntryStatus.ADDED);
      // the narrow tracking projection is not widened to the full tracking type
      assertThat(actual.snapshotId()).isNull();
      assertThat(actual.dvSnapshotId()).isNull();
      assertThat(actual.deletedPositions()).isNull();
      assertThat(actual.replacedPositions()).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testForScanPlanningOmitsChangeTrackingFields(FileFormat format) throws IOException {
    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(FILE_WITH_FULL_TRACKING));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS).forScanPlanning().build()) {
      Tracking actual = Iterables.getOnlyElement(reader).tracking();
      // scan-relevant tracking fields are projected
      assertThat(actual.status()).isEqualTo(EntryStatus.ADDED);
      assertThat(actual.snapshotId()).isEqualTo(SNAPSHOT_ID);
      assertThat(actual.dataSequenceNumber()).isEqualTo(5L);
      assertThat(actual.fileSequenceNumber()).isEqualTo(6L);
      assertThat(actual.firstRowId()).isEqualTo(8L);
      // change-tracking fields are omitted from the scan projection
      assertThat(actual.dvSnapshotId()).isNull();
      assertThat(actual.deletedPositions()).isNull();
      assertThat(actual.replacedPositions()).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testDefaultReadsFullTracking(FileFormat format) throws IOException {
    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(FILE_WITH_FULL_TRACKING));

    // without scanPlanning, select, or project, the reader returns the full schema for copying to
    // other manifests, including the change-tracking fields
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS).build()) {
      Tracking actual = Iterables.getOnlyElement(reader).tracking();
      assertThat(actual.status()).isEqualTo(EntryStatus.ADDED);
      assertThat(actual.snapshotId()).isEqualTo(SNAPSHOT_ID);
      assertThat(actual.dataSequenceNumber()).isEqualTo(5L);
      assertThat(actual.fileSequenceNumber()).isEqualTo(6L);
      assertThat(actual.firstRowId()).isEqualTo(8L);
      assertThat(actual.dvSnapshotId()).isEqualTo(7L);
      assertThat(actual.deletedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2}));
      assertThat(actual.replacedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {3, 4}));
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testPartitionFilterForceProjectsFilterFields(FileFormat format) throws IOException {
    TrackedFile keep = dataFile("keep.parquet", partition(1));
    TrackedFile prune = dataFile("prune.parquet", partition(2));

    InputFile manifest = writeManifest(format, PARTITION_TYPE, ImmutableList.of(keep, prune));

    // the caller projects only location; the reader must still project the fields the partition
    // filter reads (content_type, spec_id, partition) or every row would be pruned
    Schema projection = new Schema(TrackedFile.LOCATION);
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, PARTITIONED_SPECS)
            .project(projection)
            .filter(Expressions.equal("id", 1))
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(keep.location());
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testSelectWithPartitionFilterProjectsFilterFields(FileFormat format)
      throws IOException {
    TrackedFile keep = dataFile("keep.parquet", partition(1));
    TrackedFile prune = dataFile("prune.parquet", partition(2));

    InputFile manifest = writeManifest(format, PARTITION_TYPE, ImmutableList.of(keep, prune));

    // the caller selects only location; the reader must still project spec_id and partition
    // for the partition filter or every row would be pruned
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, PARTITIONED_SPECS)
            .select("location")
            .filter(Expressions.equal("id", 1))
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(keep.location());
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testUnpartitioned(FileFormat format) throws IOException {
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(file));

    TrackedFile actual = Iterables.getOnlyElement(read(manifest, UNPARTITIONED_SPECS));
    // unpartitioned manifests omit the partition field, which is read as null
    assertThat(actual.partition()).isNull();
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testPartitionFilterPrunesFilesAndCountsSkips(FileFormat format) throws IOException {
    // one data file and one delete file match the filter; their counterparts are pruned; manifest
    // references have no partition and are always kept
    TrackedFile keepData = dataFile("keep-data.parquet", partition(1));
    TrackedFile pruneData = dataFile("prune-data.parquet", partition(2));
    TrackedFile keepDelete = deleteFile("keep-delete.parquet", partition(1));
    TrackedFile pruneDelete = deleteFile("prune-delete.parquet", partition(2));
    TrackedFile dataManifestRef = manifestRef(FileContent.DATA_MANIFEST, "data-leaf.parquet");
    TrackedFile deleteManifestRef = manifestRef(FileContent.DELETE_MANIFEST, "delete-leaf.parquet");

    InputFile manifest =
        writeManifest(
            format,
            PARTITION_TYPE,
            ImmutableList.of(
                keepData, pruneData, keepDelete, pruneDelete, dataManifestRef, deleteManifestRef));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, PARTITIONED_SPECS)
            .filter(Expressions.equal("id", 1))
            .scanMetrics(metrics)
            .build()) {
      assertThat(reader)
          .extracting(TrackedFile::location)
          .containsExactlyInAnyOrder(
              keepData.location(),
              keepDelete.location(),
              dataManifestRef.location(),
              deleteManifestRef.location());
    }

    assertThat(metrics.skippedDataFiles().value())
        .as("one data file is pruned by the partition filter")
        .isEqualTo(1L);
    assertThat(metrics.skippedDeleteFiles().value())
        .as("one delete file is pruned by the partition filter")
        .isEqualTo(1L);
    assertThat(metrics.skippedDataManifests().value())
        .as("manifests have no partition and are not pruned")
        .isEqualTo(0L);
    assertThat(metrics.skippedDeleteManifests().value())
        .as("manifests have no partition and are not pruned")
        .isEqualTo(0L);
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testRowFilterKeepsFilesWithoutStats(FileFormat format) throws IOException {
    // with no content stats to evaluate, the row filter cannot prune any file
    TrackedFile file1 = dataFile("s3://bucket/a.parquet", EMPTY_PARTITION_DATA);
    TrackedFile file2 = dataFile("s3://bucket/b.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(file1, file2));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS)
            .filter(Expressions.equal("id", 1))
            .scanMetrics(metrics)
            .build()) {
      assertThat(reader)
          .extracting(TrackedFile::location)
          .containsExactly(file1.location(), file2.location());
    }

    assertThat(metrics.skippedDataFiles().value()).isEqualTo(0L);
    assertThat(metrics.skippedDeleteFiles().value()).isEqualTo(0L);
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testCaseInsensitivePartitionFilter(FileFormat format) throws IOException {
    TrackedFile keep = dataFile("keep.parquet", partition(1));
    TrackedFile prune = dataFile("prune.parquet", partition(2));

    InputFile manifest = writeManifest(format, PARTITION_TYPE, ImmutableList.of(keep, prune));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, PARTITIONED_SPECS)
            .filter(Expressions.equal("ID", 1))
            .caseSensitive(false)
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(keep.location());
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testMultiSpecPartitionPruning(FileFormat format) throws IOException {
    PartitionSpec spec0 =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .withSpecId(0)
            .add(1, 1000, "id", Transforms.identity())
            .build();
    PartitionSpec spec1 =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .withSpecId(1)
            .add(2, 1001, "data", Transforms.identity())
            .build();
    Map<Integer, PartitionSpec> specsById =
        ImmutableMap.of(spec0.specId(), spec0, spec1.specId(), spec1);
    Types.StructType unionType = Partitioning.unionPartitionTypes(specsById.values());

    TrackedFile keepById =
        dataFile("spec0-id1.parquet", spec0.specId(), unionPartition(unionType, 1, null));
    TrackedFile prunedById =
        dataFile("spec0-id2.parquet", spec0.specId(), unionPartition(unionType, 2, null));
    TrackedFile keptOtherSpec =
        dataFile("spec1-data.parquet", spec1.specId(), unionPartition(unionType, null, "x"));

    InputFile manifest =
        writeManifest(format, unionType, ImmutableList.of(keepById, prunedById, keptOtherSpec));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, specsById).filter(Expressions.equal("id", 1)).build()) {
      // spec0 entries are pruned by id; the spec1 entry is not partitioned by id so it survives
      assertThat(reader)
          .extracting(TrackedFile::location)
          .containsExactlyInAnyOrder(keepById.location(), keptOtherSpec.location());
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testPartitionFilterKeepsFileWithUnknownSpec(FileFormat format) throws IOException {
    // spec ID 5 is not in PARTITIONED_SPECS, so no partition filter applies to this file
    TrackedFile file = dataFile("orphan.parquet", 5, partition(1));

    InputFile manifest = writeManifest(format, PARTITION_TYPE, ImmutableList.of(file));

    // the filter would prune partition id=1 under spec 0, but cannot be applied to spec 5
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, PARTITIONED_SPECS)
            .filter(Expressions.equal("id", 2))
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(file.location());
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testPartitionFilterKeepsFileWithNullSpecId(FileFormat format) throws IOException {
    TrackedFile file = dataFile("no-spec.parquet", null, null);

    InputFile manifest = writeManifest(format, PARTITION_TYPE, ImmutableList.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, PARTITIONED_SPECS)
            .filter(Expressions.equal("id", 2))
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(file.location());
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testIteratorReturnsLiveCopies(FileFormat format) throws IOException {
    TrackedFile added1 = dataFile("s3://bucket/added-1.parquet", EMPTY_PARTITION_DATA);
    TrackedFile added2 = dataFile("s3://bucket/added-2.parquet", EMPTY_PARTITION_DATA);
    List<TrackedFile> files =
        ImmutableList.of(
            added1, added2, fileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"));

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, files);

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS).build()) {
      List<TrackedFile> read = Lists.newArrayList(reader);
      assertThat(read)
          .hasSize(2)
          .extracting(TrackedFile::location)
          .containsExactly(added1.location(), added2.location());
      assertThat(read.get(0))
          .as("iterator() should copy each entry rather than yield one reused container")
          .isNotSameAs(read.get(1));
    }
  }

  @ParameterizedTest
  @FieldSource("FORMATS")
  public void testUnknownManifestFormatThrows(FileFormat format) throws IOException {
    InputFile badFile =
        fileIO.newInputFile(tempDir.resolve("manifest-" + System.nanoTime() + ".txt").toString());

    try (V4ManifestReader reader = V4ManifestReader.builder(badFile, UNPARTITIONED_SPECS).build()) {
      assertThatThrownBy(reader::iterator)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot determine format of manifest");
    }
  }

  @Test
  public void testInvalidBuilderArguments() {
    InputFile manifest = fileIO.newInputFile(tempDir.resolve("manifest.avro").toString());

    assertThatThrownBy(() -> V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS).filter(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid filter: null");

    assertThatThrownBy(
            () -> V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS).scanMetrics(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan metrics: null");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS)
                    .select((Collection<String>) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid columns: null");
  }

  private static TrackedFile dataFile(String location, PartitionData partition) {
    return dataFile(location, 0, partition);
  }

  private static TrackedFile dataFile(String location, Integer specId, PartitionData partition) {
    return new TrackedFileStruct(
        addedTracking(),
        FileContent.DATA,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        specId,
        partition,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static TrackedFile deleteFile(String location, PartitionData partition) {
    return new TrackedFileStruct(
        addedTracking(),
        FileContent.EQUALITY_DELETES,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        0,
        partition,
        null,
        null,
        null,
        null,
        null,
        null,
        ImmutableList.of(1));
  }

  private static TrackedFile manifestRef(FileContent content, String location) {
    ManifestInfo info = new ManifestInfoStruct(1, 0, 0, 0, 1L, 0L, 0L, 0L, 1L, null, null);
    return new TrackedFileStruct(
        addedTracking(),
        content,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        null, // spec_id: a manifest reference has no spec
        null, // partition: a manifest reference has no partition tuple
        null, // content_stats
        null, // sort_order_id
        null, // deletion_vector
        info,
        null, // key_metadata
        null, // split_offsets
        null); // equality_ids
  }

  private static TrackedFile fileWithStatus(EntryStatus status, String location) {
    Tracking tracking =
        new TrackingStruct(
            status,
            SNAPSHOT_ID,
            3L, // data sequence number
            3L, // file sequence number
            null, // dv snapshot id
            null, // first row id
            null, // deleted positions
            null); // replaced positions
    return new TrackedFileStruct(
        tracking,
        FileContent.DATA,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        0,
        EMPTY_PARTITION_DATA,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static Tracking addedTracking() {
    return new TrackingStruct(EntryStatus.ADDED, SNAPSHOT_ID, null, null, null, null, null, null);
  }

  private static PartitionData partition(int id) {
    PartitionData partition = new PartitionData(PARTITION_TYPE);
    partition.set(0, id);
    return partition;
  }

  private static PartitionData unionPartition(Types.StructType unionType, Integer id, String data) {
    PartitionData partition = new PartitionData(unionType);
    partition.set(0, id);
    partition.set(1, data);
    return partition;
  }

  private InputFile writeManifest(
      FileFormat format, Types.StructType partitionType, Iterable<TrackedFile> files)
      throws IOException {
    Schema writeSchema = TrackedFile.schema(partitionType, Types.StructType.of());
    OutputFile out = new InMemoryOutputFile("manifest." + format.name().toLowerCase(Locale.ROOT));
    try (FileAppender<StructLike> appender =
        InternalData.write(format, out).schema(writeSchema).named("tracked_file").build()) {
      for (TrackedFile file : files) {
        appender.add((StructLike) file);
      }
    }

    return out.toInputFile();
  }

  private List<TrackedFile> read(InputFile manifest, Map<Integer, PartitionSpec> specsById)
      throws IOException {
    try (V4ManifestReader reader = V4ManifestReader.builder(manifest, specsById).build()) {
      return Lists.newArrayList(reader);
    }
  }
}
