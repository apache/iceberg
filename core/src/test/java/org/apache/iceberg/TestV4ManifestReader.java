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
import org.apache.iceberg.exceptions.ValidationException;
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
import org.apache.iceberg.util.LocationUtil;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestV4ManifestReader {
  private static final long SNAPSHOT_ID = 42L;
  private static final int FORMAT_VERSION_V4 = 4;
  private static final long RECORD_COUNT = 100L;
  private static final long FILE_SIZE_IN_BYTES = 1024L;
  private static final String TABLE_LOCATION = "s3://bucket/db/table";
  private static final DeletionVector DV = dv("s3://bucket/dv.puffin");

  private static final int ID_FIELD_ID = 1;
  private static final int DATA_FIELD_ID = 2;
  private static final int MEASURE_FIELD_ID = 3;
  // the field types cover the stats that are tracked conditionally: null counts for optional
  // fields, NaN counts for floating point fields, and avg value size for variable-length fields
  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(ID_FIELD_ID, "id", Types.IntegerType.get()),
          optional(DATA_FIELD_ID, "data", Types.StringType.get()),
          optional(MEASURE_FIELD_ID, "measure", Types.DoubleType.get()));
  private static final Types.StructType CONTENT_STATS_TYPE =
      StatsUtil.statsReadSchema(
          TABLE_SCHEMA, ImmutableList.of(ID_FIELD_ID, DATA_FIELD_ID, MEASURE_FIELD_ID));
  private static final FieldStats<Integer> ID_STATS =
      new FieldStatsStruct<>(
          CONTENT_STATS_TYPE.fieldType("id").asStructType(), 1, 100, true, 26L, 2L, 0L, null);
  private static final FieldStats<String> DATA_STATS =
      new FieldStatsStruct<>(
          CONTENT_STATS_TYPE.fieldType("data").asStructType(), "a", "z", true, 26L, 0L, 0L, 4);
  private static final FieldStats<Double> MEASURE_STATS =
      new FieldStatsStruct<>(
          CONTENT_STATS_TYPE.fieldType("measure").asStructType(),
          1.5,
          9.5,
          false,
          26L,
          1L,
          3L,
          null);
  private static final PartitionSpec ID_PARTITIONING =
      PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").build();
  private static final Types.StructType ID_PARTITION_TYPE = ID_PARTITIONING.partitionType();
  private static final Types.StructType EMPTY_PARTITION = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_PARTITION);
  private static final Map<Integer, PartitionSpec> ID_PARTITIONING_SPECS =
      ImmutableMap.of(ID_PARTITIONING.specId(), ID_PARTITIONING);
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());

  private static final List<FileFormat> MANIFEST_FORMATS =
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

  // shared data files: FILE_A is in partition id=1, FILE_B in partition id=2. Locations are stored
  // relative to the table location (the default), so the reader resolves them against the table
  private static final TrackedFile FILE_A = dataFile("data-a.parquet", partition(1));
  private static final TrackedFile FILE_B = dataFile("data-b.parquet", partition(2));
  private static final TrackedFile EQ_DELETES_A = deleteFile("eq-deletes-a.parquet", partition(1));
  private static final TrackedFile EQ_DELETES_B = deleteFile("eq-deletes-b.parquet", partition(2));
  private static final TrackedFile DATA_MANIFEST_REF =
      manifestRef(FileContent.DATA_MANIFEST, "data-leaf.parquet");
  private static final TrackedFile DELETE_MANIFEST_REF =
      manifestRef(FileContent.DELETE_MANIFEST, "delete-leaf.parquet");

  @TempDir private Path tempDir;

  private final FileIO fileIO = new TestTables.LocalFileIO();

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void readsWrittenFile(FileFormat format) throws IOException {
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
            1, // sort order id
            DV,
            null,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(50L, 100L),
            null);

    InputFile manifest = writeManifest(format, ID_PARTITION_TYPE, ImmutableList.of(file));

    TrackedFile actual = Iterables.getOnlyElement(read(manifest, ID_PARTITIONING_SPECS));

    // compare with tracking reduced to status: the reader fills status-independent tracking
    // fields (row position, sequence numbers via inheritance) that the written file does not have
    Types.StructType comparisonType =
        TypeUtil.replaceFieldTypes(
                TrackedFile.schema(ID_PARTITION_TYPE, Types.StructType.of()),
                ImmutableMap.of(
                    TrackedFile.TRACKING.fieldId(), Types.StructType.of(Tracking.STATUS)))
            .asStruct();
    assertThat((StructLike) actual)
        .usingComparator(Comparators.forType(comparisonType))
        .isEqualTo(file);
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void equalityDeleteRoundTrip(FileFormat format) throws IOException {
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
  @FieldSource("MANIFEST_FORMATS")
  public void statusFiltering(FileFormat format) throws IOException {
    List<TrackedFile> files =
        ImmutableList.of(
            fileWithStatus(EntryStatus.ADDED, "s3://bucket/added.parquet"),
            fileWithStatus(EntryStatus.EXISTING, "s3://bucket/existing.parquet"),
            fileWithStatus(EntryStatus.MODIFIED, "s3://bucket/modified.parquet"),
            fileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"),
            fileWithStatus(EntryStatus.REPLACED, "s3://bucket/replaced.parquet"));

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, files);

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
      assertThat(reader)
          .extracting(file -> file.tracking().status())
          .containsExactly(EntryStatus.ADDED, EntryStatus.EXISTING, EntryStatus.MODIFIED);
    }

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .includeAll()
            .build()) {
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
  @FieldSource("MANIFEST_FORMATS")
  public void manifestLocationAndPosition(FileFormat format) throws IOException {
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

  @ParameterizedTest(name = "{0} / {1}")
  @MethodSource("selectiveReadModes")
  public void selectiveReadReturnsOnlyRequestedFields(
      FileFormat format, Consumer<V4ManifestReader.Builder> configureRead) throws IOException {
    List<TrackedFile> files =
        ImmutableList.of(
            dataFile("s3://bucket/live.parquet", EMPTY_PARTITION_DATA),
            fileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"),
            fileWithStatus(EntryStatus.REPLACED, "s3://bucket/replaced.parquet"));

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, files);

    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION);
    configureRead.accept(builder);
    try (V4ManifestReader reader = builder.build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);

      // the requested field is read
      assertThat(actual.location()).isEqualTo("s3://bucket/live.parquet");

      // the reader always projects the fields it consumes internally, even though the caller
      // selected only location: content type and status (liveness filtering keeps only the live
      // entry), and manifest position (from row_position)
      assertThat(actual.contentType()).isEqualTo(FileContent.DATA);
      assertThat(actual.tracking().status()).isEqualTo(EntryStatus.ADDED);
      assertThat(actual.tracking().manifestPos()).isEqualTo(0L);

      // every field the caller did not request and the reader does not require is omitted;
      // content stats in particular (the largest projection) is not read
      assertThat(actual.contentStats()).isNull();
      assertThat(actual.fileFormat()).isNull();
      assertThat(actual.recordCount()).isEqualTo(-1L);
      assertThat(actual.fileSizeInBytes()).isEqualTo(-1L);
      assertThat(actual.specId()).isNull();
      assertThat(actual.partition()).isNull();
      assertThat(actual.sortOrderId()).isNull();
      assertThat(actual.deletionVector()).isNull();
      assertThat(actual.keyMetadata()).isNull();
      assertThat(actual.splitOffsets()).isNull();
      assertThat(actual.equalityIds()).isNull();
    }
  }

  private static Stream<Arguments> selectiveReadModes() {
    Map<String, Consumer<V4ManifestReader.Builder>> modes =
        ImmutableMap.of(
            "project",
            builder -> builder.project(new Schema(TrackedFile.LOCATION)),
            "select",
            builder -> builder.select("location"),
            "case-insensitive select",
            builder -> builder.select("LOCATION").caseSensitive(false));
    return MANIFEST_FORMATS.stream()
        .flatMap(
            format ->
                modes.entrySet().stream()
                    .map(mode -> Arguments.of(format, Named.of(mode.getKey(), mode.getValue()))));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void rowFilterForcesRecordCount(FileFormat format) throws IOException {
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(file));

    // record_count is read when evaluating a row filter against file metrics, so it is projected
    // even though the caller selected only location
    Schema projection = new Schema(TrackedFile.LOCATION);
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .project(projection)
            .filter(Expressions.equal("id", 1))
            .build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isEqualTo(file.location());
      assertThat(actual.recordCount()).isEqualTo(RECORD_COUNT);
    }
  }

  @Test
  public void projectionModesAreMutuallyExclusive() {
    InputFile manifest = fileIO.newInputFile(tempDir.resolve("manifest.avro").toString());

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .select("location")
                    .project(new Schema(TrackedFile.LOCATION)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use project(Schema) with select(Collection<String>)");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .project(new Schema(TrackedFile.LOCATION))
                    .select("location"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use select(Collection<String>) with project(Schema)");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .forScanPlanning()
                    .select("location"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use select(Collection<String>) with forScanPlanning()");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .select("location")
                    .forScanPlanning())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot use forScanPlanning() with select(Collection<String>) or project(Schema)");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .forScanPlanning()
                    .project(new Schema(TrackedFile.LOCATION)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use project(Schema) with forScanPlanning()");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .project(new Schema(TrackedFile.LOCATION))
                    .forScanPlanning())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot use forScanPlanning() with select(Collection<String>) or project(Schema)");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void projectionPreservesNarrowTrackingProjection(FileFormat format) throws IOException {
    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(FILE_WITH_FULL_TRACKING));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .select("tracking.status")
            .build()) {
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
  @FieldSource("MANIFEST_FORMATS")
  public void forScanPlanningOmitsChangeTrackingFields(FileFormat format) throws IOException {
    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(FILE_WITH_FULL_TRACKING));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .forScanPlanning()
            .build()) {
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
  @FieldSource("MANIFEST_FORMATS")
  public void defaultReadsFullTracking(FileFormat format) throws IOException {
    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(FILE_WITH_FULL_TRACKING));

    // without scanPlanning, select, or project, the reader returns the full schema for copying to
    // other manifests, including the change-tracking fields
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
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
  @FieldSource("MANIFEST_FORMATS")
  public void projectNullReadsFullSchema(FileFormat format) throws IOException {
    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, ImmutableList.of(FILE_WITH_FULL_TRACKING));

    // project(null) clears the projection and reads the full schema, like no projection at all
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .project(null)
            .build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isEqualTo("s3://bucket/file.parquet");
      assertThat(actual.fileFormat()).isEqualTo(FileFormat.PARQUET);
      assertThat(actual.recordCount()).isEqualTo(RECORD_COUNT);
      assertThat(actual.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterForceProjectsFilterFields(FileFormat format) throws IOException {
    InputFile manifest = writeManifest(format, ID_PARTITION_TYPE, ImmutableList.of(FILE_A, FILE_B));

    // the caller projects only location; the reader must still project the fields the partition
    // filter reads (spec_id, partition) or every row would be pruned
    Schema projection = new Schema(TrackedFile.LOCATION);
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, ID_PARTITIONING_SPECS, TABLE_LOCATION)
            .project(projection)
            .filter(Expressions.equal("id", 1))
            .build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isEqualTo(resolved(FILE_A));
      assertThat(actual.specId()).isEqualTo(ID_PARTITIONING.specId());
      assertThat(actual.partition().get(0, Integer.class)).isEqualTo(1);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void selectWithPartitionFilterProjectsFilterFields(FileFormat format) throws IOException {
    InputFile manifest = writeManifest(format, ID_PARTITION_TYPE, ImmutableList.of(FILE_A, FILE_B));

    // the caller selects only location; the reader must still project spec_id and partition
    // for the partition filter or every row would be pruned
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, ID_PARTITIONING_SPECS, TABLE_LOCATION)
            .select("location")
            .filter(Expressions.equal("id", 1))
            .build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isEqualTo(resolved(FILE_A));
      assertThat(actual.specId()).isEqualTo(ID_PARTITIONING.specId());
      assertThat(actual.partition().get(0, Integer.class)).isEqualTo(1);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void unpartitionedProducesNullPartitionValue(FileFormat format) throws IOException {
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(file));

    TrackedFile actual = Iterables.getOnlyElement(read(manifest, UNPARTITIONED_SPECS));
    // unpartitioned manifests omit the partition field, which is read as null
    assertThat(actual.partition()).isNull();
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterPrunesFilesAndCountsSkips(FileFormat format) throws IOException {
    // FILE_A and EQ_DELETES_A match the filter; FILE_B and EQ_DELETES_B are pruned; manifest
    // references have no partition and are always kept
    InputFile manifest =
        writeManifest(
            format,
            ID_PARTITION_TYPE,
            ImmutableList.of(
                FILE_A,
                FILE_B,
                EQ_DELETES_A,
                EQ_DELETES_B,
                DATA_MANIFEST_REF,
                DELETE_MANIFEST_REF));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, ID_PARTITIONING_SPECS, TABLE_LOCATION)
            .filter(Expressions.equal("id", 1))
            .scanMetrics(metrics)
            .build()) {
      assertThat(reader)
          .extracting(TrackedFile::location)
          .containsExactlyInAnyOrder(
              resolved(FILE_A),
              resolved(EQ_DELETES_A),
              resolved(DATA_MANIFEST_REF),
              resolved(DELETE_MANIFEST_REF));
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
  @FieldSource("MANIFEST_FORMATS")
  public void rowFilterKeepsFilesWithoutStats(FileFormat format) throws IOException {
    // with no content stats to evaluate, the row filter cannot prune any file
    TrackedFile file1 = dataFile("s3://bucket/a.parquet", EMPTY_PARTITION_DATA);
    TrackedFile file2 = dataFile("s3://bucket/b.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(file1, file2));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
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
  @FieldSource("MANIFEST_FORMATS")
  public void caseInsensitivePartitionFilter(FileFormat format) throws IOException {
    InputFile manifest = writeManifest(format, ID_PARTITION_TYPE, ImmutableList.of(FILE_A, FILE_B));

    // a case-insensitive filter binds the mismatched-case "ID" reference and prunes FILE_B
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, ID_PARTITIONING_SPECS, TABLE_LOCATION)
            .filter(Expressions.equal("ID", 1))
            .caseSensitive(false)
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(resolved(FILE_A));
    }

    // the same filter is case-sensitive by default, so "ID" fails to bind to the "id" field
    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, ID_PARTITIONING_SPECS, TABLE_LOCATION)
                    .filter(Expressions.equal("ID", 1))
                    .build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'ID'");
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void filterMatchesFilesAcrossDisjointSpecs(FileFormat format) throws IOException {
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
        V4ManifestReader.builder(manifest, specsById, TABLE_LOCATION)
            .filter(Expressions.equal("id", 1))
            .build()) {
      // spec0 entries are pruned by id; the spec1 entry is not partitioned by id so it survives
      assertThat(reader)
          .extracting(TrackedFile::location)
          .containsExactlyInAnyOrder(resolved(keepById), resolved(keptOtherSpec));
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partialFilterStillPrunesOnCompatibleField(FileFormat format) throws IOException {
    // the spec partitions on id only; a filter of id = 1 AND data = 'z' should still prune by id
    // even though data is not a partition source
    TrackedFile keep = dataFile("id1.parquet", partition(1));
    TrackedFile prune = dataFile("id2.parquet", partition(2));

    InputFile manifest = writeManifest(format, ID_PARTITION_TYPE, ImmutableList.of(keep, prune));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, ID_PARTITIONING_SPECS, TABLE_LOCATION)
            .filter(Expressions.and(Expressions.equal("id", 1), Expressions.equal("data", "z")))
            .build()) {
      assertThat(reader)
          .extracting(TrackedFile::location)
          .as("the id predicate prunes even though data is not a partition field")
          .containsExactly(resolved(keep));
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterKeepsFileWithUnknownSpec(FileFormat format) throws IOException {
    // spec ID 5 is not in ID_PARTITIONING_SPECS, so no partition filter applies to this file
    TrackedFile file = dataFile("orphan.parquet", 5, partition(1));

    InputFile manifest = writeManifest(format, ID_PARTITION_TYPE, ImmutableList.of(file));

    // the filter would prune partition id=1 under spec 0, but cannot be applied to spec 5
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, ID_PARTITIONING_SPECS, TABLE_LOCATION)
            .filter(Expressions.equal("id", 2))
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(resolved(file));
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void partitionFilterKeepsFileWithNullSpecId(FileFormat format) throws IOException {
    TrackedFile file = dataFile("no-spec.parquet", (Integer) null, null);

    InputFile manifest = writeManifest(format, ID_PARTITION_TYPE, ImmutableList.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, ID_PARTITIONING_SPECS, TABLE_LOCATION)
            .filter(Expressions.equal("id", 2))
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(resolved(file));
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void iteratorReturnsLiveCopies(FileFormat format) throws IOException {
    TrackedFile added1 = dataFile("s3://bucket/added-1.parquet", EMPTY_PARTITION_DATA);
    TrackedFile added2 = dataFile("s3://bucket/added-2.parquet", EMPTY_PARTITION_DATA);
    List<TrackedFile> files =
        ImmutableList.of(
            added1, added2, fileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"));

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, files);

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
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

  @Test
  public void unknownManifestFormatThrows() throws IOException {
    InputFile badFile =
        fileIO.newInputFile(tempDir.resolve("manifest-" + System.nanoTime() + ".txt").toString());

    try (V4ManifestReader reader =
        V4ManifestReader.builder(badFile, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
      assertThatThrownBy(reader::iterator)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot determine format of manifest");
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void resolvesRelativeDataFileLocation(FileFormat format) throws IOException {
    TrackedFile file = dataFile(format.addExtension("data/00000-0"), EMPTY_PARTITION_DATA);
    verifyLocationAfterWriteReadRoundTrip(format, file, resolved(file));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void resolvesRelativeDeletionVectorLocation(FileFormat format) throws IOException {
    TrackedFile file =
        dataFile(format.addExtension("data/00000-0"), EMPTY_PARTITION_DATA, dv("data/dv.puffin"));

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isEqualTo(resolved(file));
      assertThat(actual.deletionVector().location())
          .isEqualTo(LocationUtil.resolveLocation(TABLE_LOCATION, "data/dv.puffin"));
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void resolvesLeafManifestLocation(FileFormat format) throws IOException {
    TrackedFile leaf = manifestRef(FileContent.DATA_MANIFEST, "metadata/leaf.avro");
    verifyLocationAfterWriteReadRoundTrip(format, leaf, resolved(leaf));
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void mixedAbsoluteAndRelativePathWithDataFileAndDV(FileFormat format) throws IOException {
    // absolute data file paired with a relative DV, and relative data file paired with an absolute
    // DV: each location's scheme is evaluated on its own
    TrackedFile absoluteFileRelativeDv =
        dataFile("s3://other/abs.parquet", EMPTY_PARTITION_DATA, dv("data/dv.puffin"));
    TrackedFile relativeFileAbsoluteDv =
        dataFile("data/rel.parquet", EMPTY_PARTITION_DATA, dv("s3://other/abs-dv.puffin"));

    InputFile manifest =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(absoluteFileRelativeDv, relativeFileAbsoluteDv));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
      List<TrackedFile> actual = Lists.newArrayList(reader);
      // absolute locations pass through unchanged; relative ones resolve against the table location
      assertThat(actual.get(0).location()).isEqualTo("s3://other/abs.parquet");
      assertThat(actual.get(0).deletionVector().location())
          .isEqualTo(LocationUtil.resolveLocation(TABLE_LOCATION, "data/dv.puffin"));
      assertThat(actual.get(1).location())
          .isEqualTo(LocationUtil.resolveLocation(TABLE_LOCATION, "data/rel.parquet"));
      assertThat(actual.get(1).deletionVector().location()).isEqualTo("s3://other/abs-dv.puffin");
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void resolutionSkippedWhenLocationNotProjected(FileFormat format) throws IOException {
    TrackedFile file = dataFile(format.addExtension("data/00000-0"), EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(file));

    // location is not projected, so there is nothing to resolve even though it is relative
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .select("tracking.status")
            .build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isNull();
    }
  }

  @Test
  public void invalidBuilderArguments() {
    InputFile manifest = fileIO.newInputFile(tempDir.resolve("manifest.avro").toString());

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .filter(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid filter: null");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .scanMetrics(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan metrics: null");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .select((Collection<String>) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid columns: null");

    assertThatThrownBy(() -> V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table location: null");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .projectStats(TABLE_SCHEMA, (Collection<Integer>) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field IDs: null");

    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .projectStats(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table schema: null");

    // empty field IDs would silently read no stats; projectStats(Schema) is the way to read all
    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .projectStats(TABLE_SCHEMA, new int[0]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field IDs: empty, use projectStats(Schema) instead");
  }

  // the location a relative fixture resolves to once read against TABLE_LOCATION
  private static String resolved(TrackedFile file) {
    return LocationUtil.resolveLocation(TABLE_LOCATION, file.location());
  }

  // writes a single tracked file, reads it back against TABLE_LOCATION, and checks its location
  private void verifyLocationAfterWriteReadRoundTrip(
      FileFormat format, TrackedFile file, String expectedLocation) throws IOException {
    InputFile manifest = writeManifest(format, EMPTY_PARTITION, ImmutableList.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
      TrackedFile actual = Iterables.getOnlyElement(reader);
      assertThat(actual.location()).isEqualTo(expectedLocation);
    }
  }

  private static DeletionVector dv(String location) {
    return DeletionVectorStruct.builder()
        .location(location)
        .offset(100L)
        .sizeInBytes(50L)
        .cardinality(5L)
        .build();
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void contentStatsRoundTrip(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());

    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, CONTENT_STATS_TYPE, ImmutableList.of(file));

    // given a table schema, the default read carries stats for every field so that entries can be
    // copied to a new manifest
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(TABLE_SCHEMA)
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertThat(stats).isNotNull();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertFieldStats(stats.statsFor(MEASURE_FIELD_ID), MEASURE_STATS);
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void statsAreOmittedWithoutProjectStats(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());

    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, CONTENT_STATS_TYPE, ImmutableList.of(file));

    // stats field IDs and types are derived from the table schema, so none are read without it
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION).build()) {
      assertThat(Iterables.getOnlyElement(reader).contentStats()).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void projectStatsReadsOnlyRequestedColumns(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());

    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, CONTENT_STATS_TYPE, ImmutableList.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(TABLE_SCHEMA, ID_FIELD_ID)
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertThat(stats.statsFor(DATA_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void filterStatsAreProjectedWhenOmittedByCaller(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());

    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, CONTENT_STATS_TYPE, ImmutableList.of(file));

    // the filter references data, so its stats are read even though the projection omits them
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(TABLE_SCHEMA)
            .project(new Schema(TrackedFile.LOCATION))
            .filter(Expressions.equal("data", "m"))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(DATA_FIELD_ID), DATA_STATS);
      assertThat(stats.statsFor(ID_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void filterOnMissingColumnFails(FileFormat format) throws IOException {
    InputFile manifest =
        writeManifest(
            format,
            EMPTY_PARTITION,
            ImmutableList.of(dataFile("data-a.parquet", EMPTY_PARTITION_DATA)));

    // stats for the filter's columns are resolved against the table schema when the reader is built
    assertThatThrownBy(
            () ->
                V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
                    .projectStats(TABLE_SCHEMA)
                    .filter(Expressions.equal("missing", 34))
                    .build())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'missing' in struct: %s", TABLE_SCHEMA.asStruct());
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void forScanPlanningReadsOnlyFilterStats(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());

    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, CONTENT_STATS_TYPE, ImmutableList.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(TABLE_SCHEMA)
            .forScanPlanning()
            .filter(Expressions.equal("id", 1))
            .build()) {
      ContentStats stats = Iterables.getOnlyElement(reader).contentStats();
      assertFieldStats(stats.statsFor(ID_FIELD_ID), ID_STATS);
      assertThat(stats.statsFor(DATA_FIELD_ID)).isNull();
      assertThat(stats.statsFor(MEASURE_FIELD_ID)).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void forScanPlanningOmitsStatsWithoutFilter(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());

    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, CONTENT_STATS_TYPE, ImmutableList.of(file));

    // scan planning without a filter has no stats to evaluate, so none are read
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(TABLE_SCHEMA)
            .forScanPlanning()
            .build()) {
      assertThat(Iterables.getOnlyElement(reader).contentStats()).isNull();
    }
  }

  @ParameterizedTest
  @FieldSource("MANIFEST_FORMATS")
  public void selectWithoutStatsOmitsStats(FileFormat format) throws IOException {
    TrackedFile file = fileWithStats("s3://bucket/file.parquet", contentStats());

    InputFile manifest =
        writeManifest(format, EMPTY_PARTITION, CONTENT_STATS_TYPE, ImmutableList.of(file));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStats(TABLE_SCHEMA)
            .select("location")
            .build()) {
      assertThat(Iterables.getOnlyElement(reader).contentStats()).isNull();
    }
  }

  private static void assertFieldStats(FieldStats<?> actual, FieldStats<?> expected) {
    assertThat(actual).isNotNull();
    assertThat(actual.fieldId()).isEqualTo(expected.fieldId());
    assertThat(actual.type()).isEqualTo(expected.type());
    assertThat(actual.lowerBound()).isEqualTo(expected.lowerBound());
    assertThat(actual.upperBound()).isEqualTo(expected.upperBound());
    assertThat(actual.tightBounds()).isEqualTo(expected.tightBounds());
    assertThat(actual.avgValueSizeInBytes()).isEqualTo(expected.avgValueSizeInBytes());

    // the count accessors unbox a nullable Long, so they throw for stats the field does not track
    Types.StructType statsType = expected.type();
    assertThat(statsType.field("value_count")).isNotNull();
    assertThat(actual.valueCount()).isEqualTo(expected.valueCount());

    if (statsType.field("null_value_count") != null) {
      assertThat(actual.nullValueCount()).isEqualTo(expected.nullValueCount());
    }

    if (statsType.field("nan_value_count") != null) {
      assertThat(actual.nanValueCount()).isEqualTo(expected.nanValueCount());
    }
  }

  private static TrackedFile dataFile(String location, PartitionData partition) {
    return dataFile(location, 0, partition);
  }

  private static TrackedFile dataFile(String location, PartitionData partition, DeletionVector dv) {
    return new TrackedFileStruct(
        addedTracking(),
        FileContent.DATA,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        0, // spec_id
        partition,
        null, // content_stats
        null, // sort_order_id
        dv,
        null, // manifest_info
        null, // key_metadata
        null, // split_offsets
        null); // equality_ids
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
        null, // content_stats
        null, // sort_order_id
        null, // deletion_vector
        null, // manifest_info
        null, // key_metadata
        null, // split_offsets
        null); // equality_ids
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
        0, // spec_id
        partition,
        null, // content_stats
        null, // sort_order_id
        null, // deletion_vector
        null, // manifest_info
        null, // key_metadata
        null, // split_offsets
        ImmutableList.of(1)); // equality_ids
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
    PartitionData partition = new PartitionData(ID_PARTITION_TYPE);
    partition.set(0, id);
    return partition;
  }

  private static PartitionData unionPartition(Types.StructType unionType, Integer id, String data) {
    PartitionData partition = new PartitionData(unionType);
    partition.set(0, id);
    partition.set(1, data);
    return partition;
  }

  /** Returns stats for both table columns, backed by the full content stats type. */
  private static ContentStats contentStats() {
    ContentStatsStruct stats = new ContentStatsStruct(CONTENT_STATS_TYPE);
    stats.setStats(ID_FIELD_ID, ID_STATS);
    stats.setStats(DATA_FIELD_ID, DATA_STATS);
    stats.setStats(MEASURE_FIELD_ID, MEASURE_STATS);
    return stats;
  }

  private static TrackedFile fileWithStats(String location, ContentStats stats) {
    return new TrackedFileStruct(
        addedTracking(),
        FileContent.DATA,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        0,
        EMPTY_PARTITION_DATA,
        stats,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private InputFile writeManifest(
      FileFormat format, Types.StructType partitionType, Iterable<TrackedFile> files)
      throws IOException {
    return writeManifest(format, partitionType, Types.StructType.of(), files);
  }

  private InputFile writeManifest(
      FileFormat format,
      Types.StructType partitionType,
      Types.StructType contentStatsType,
      Iterable<TrackedFile> files)
      throws IOException {
    Schema writeSchema = TrackedFile.schema(partitionType, contentStatsType);
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
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, specsById, TABLE_LOCATION)
            .projectStats(TABLE_SCHEMA)
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}
