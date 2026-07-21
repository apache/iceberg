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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
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

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA).identity("id").build();
  private static final Types.StructType PARTITION_TYPE = SPEC.partitionType();
  private static final Types.StructType EMPTY_PARTITION = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_PARTITION);
  private static final Map<Integer, PartitionSpec> PARTITIONED_SPECS =
      ImmutableMap.of(SPEC.specId(), SPEC);
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());

  @Parameter private FileFormat format;

  @Parameters(name = "format = {0}")
  protected static List<FileFormat> parameters() {
    return ImmutableList.of(FileFormat.AVRO, FileFormat.PARQUET);
  }

  @TempDir private Path tempDir;

  private final FileIO fileIO = new TestTables.LocalFileIO();

  @TestTemplate
  public void testRoundTrip() throws IOException {
    DeletionVector dv = deletionVector(DV_LOCATION, DV_OFFSET, DV_SIZE_IN_BYTES, DV_CARDINALITY);

    TrackedFile file =
        new TrackedFileStruct(
            addedTracking(),
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            partition(7),
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            0,
            null,
            SORT_ORDER_ID,
            dv,
            null,
            ByteBuffer.wrap(new byte[] {1, 2, 3}),
            ImmutableList.of(50L, 100L),
            null);

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(file));

    List<TrackedFile> read = read(manifest, PARTITIONED_SPECS);
    assertThat(read).hasSize(1);
    TrackedFile actual = read.get(0);

    assertThat(actual.contentType()).isEqualTo(FileContent.DATA);
    assertThat(actual.formatVersion()).isEqualTo(FORMAT_VERSION_V4);
    assertThat(actual.location()).isEqualTo("s3://bucket/data/file.parquet");
    assertThat(actual.fileFormat()).isEqualTo(FileFormat.PARQUET);
    assertThat(actual.recordCount()).isEqualTo(RECORD_COUNT);
    assertThat(actual.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(actual.specId()).isEqualTo(0);
    assertThat(actual.sortOrderId()).isEqualTo(SORT_ORDER_ID);
    assertThat(actual.keyMetadata()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    assertThat(actual.splitOffsets()).containsExactly(50L, 100L);
    assertThat(actual.partition().get(0, Integer.class)).isEqualTo(7);

    assertThat(actual.tracking()).isNotNull();
    assertThat(actual.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(actual.tracking().snapshotId()).isEqualTo(SNAPSHOT_ID);

    assertThat(actual.deletionVector()).isNotNull();
    assertThat(actual.deletionVector().location()).isEqualTo(DV_LOCATION);
    assertThat(actual.deletionVector().offset()).isEqualTo(DV_OFFSET);
    assertThat(actual.deletionVector().sizeInBytes()).isEqualTo(DV_SIZE_IN_BYTES);
    assertThat(actual.deletionVector().cardinality()).isEqualTo(DV_CARDINALITY);
  }

  @TestTemplate
  public void testEqualityDeleteRoundTrip() throws IOException {
    TrackedFile delete =
        new TrackedFileStruct(
            addedTracking(),
            FileContent.EQUALITY_DELETES,
            FORMAT_VERSION_V4,
            "s3://bucket/eq-delete.parquet",
            FileFormat.PARQUET,
            EMPTY_PARTITION_DATA,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            ImmutableList.of(1, 2));

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(delete));

    TrackedFile actual = read(manifest, UNPARTITIONED_SPECS).get(0);
    assertThat(actual.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(actual.equalityIds()).containsExactly(1, 2);
  }

  @TestTemplate
  public void testLiveFilesExcludesDeletedAndReplaced() throws IOException {
    List<TrackedFile> files =
        ImmutableList.of(
            fileWithStatus(EntryStatus.ADDED, "s3://bucket/added.parquet"),
            fileWithStatus(EntryStatus.EXISTING, "s3://bucket/existing.parquet"),
            fileWithStatus(EntryStatus.MODIFIED, "s3://bucket/modified.parquet"),
            fileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"),
            fileWithStatus(EntryStatus.REPLACED, "s3://bucket/replaced.parquet"));

    InputFile manifest = writeManifest(EMPTY_PARTITION, files);

    try (V4ManifestReader reader = newReader(manifest, UNPARTITIONED_SPECS).build()) {
      assertThat(reader)
          .extracting(file -> file.tracking().status())
          .containsExactly(EntryStatus.ADDED, EntryStatus.EXISTING, EntryStatus.MODIFIED);
    }

    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS).includeTombstones().build()) {
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

  @TestTemplate
  public void testManifestLocationAndPosition() throws IOException {
    List<TrackedFile> files =
        ImmutableList.of(
            dataFile("s3://bucket/a.parquet", EMPTY_PARTITION_DATA),
            dataFile("s3://bucket/b.parquet", EMPTY_PARTITION_DATA),
            dataFile("s3://bucket/c.parquet", EMPTY_PARTITION_DATA));

    InputFile manifest = writeManifest(EMPTY_PARTITION, files);

    List<TrackedFile> read = read(manifest, UNPARTITIONED_SPECS);
    assertThat(read)
        .allSatisfy(
            file -> assertThat(file.tracking().manifestLocation()).isEqualTo(manifest.location()));
    assertThat(read).extracting(file -> file.tracking().manifestPos()).containsExactly(0L, 1L, 2L);
  }

  @TestTemplate
  public void testProjectionRestrictsFields() throws IOException {
    TrackedFile file =
        new TrackedFileStruct(
            addedTracking(),
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/file.parquet",
            FileFormat.PARQUET,
            EMPTY_PARTITION_DATA,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            0,
            null,
            SORT_ORDER_ID,
            null,
            null,
            null,
            null,
            null);

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file));

    Schema projection = new Schema(TrackedFile.LOCATION);
    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS).project(projection).build()) {
      TrackedFile actual = Lists.newArrayList(reader).get(0);
      assertThat(actual.location()).isEqualTo(file.location());
      // tracking and content_type are always projected, even though the caller omitted them
      assertThat(actual.tracking()).isNotNull();
      assertThat(actual.tracking().status()).isEqualTo(EntryStatus.ADDED);
      assertThat(actual.contentType()).isEqualTo(FileContent.DATA);
      // sort_order_id, file_format, and spec_id are null because they were not projected
      assertThat(actual.sortOrderId()).isNull();
      assertThat(actual.fileFormat()).isNull();
      assertThat(actual.specId()).isNull();
    }
  }

  @TestTemplate
  public void testSelectRestrictsFields() throws IOException {
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file));

    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS).select(ImmutableList.of("location")).build()) {
      TrackedFile actual = Lists.newArrayList(reader).get(0);
      assertThat(actual.location()).isEqualTo(file.location());
      // tracking status and content_type are joined in even though not selected
      assertThat(actual.tracking()).isNotNull();
      assertThat(actual.tracking().status()).isEqualTo(EntryStatus.ADDED);
      assertThat(actual.contentType()).isEqualTo(FileContent.DATA);
      // only status is joined from tracking, not the other tracking fields
      assertThat(actual.tracking().snapshotId()).isNull();
      // file_format and spec_id are null because they were not selected
      assertThat(actual.fileFormat()).isNull();
      assertThat(actual.specId()).isNull();
    }
  }

  @TestTemplate
  public void testCaseInsensitiveSelect() throws IOException {
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file));

    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS)
            .select(ImmutableList.of("LOCATION"))
            .caseSensitive(false)
            .build()) {
      TrackedFile actual = Lists.newArrayList(reader).get(0);
      assertThat(actual.location()).isEqualTo(file.location());
      assertThat(actual.fileFormat()).isNull();
    }
  }

  @TestTemplate
  public void testProjectionModesAreMutuallyExclusive() {
    InputFile manifest = fileIO.newInputFile(tempDir.resolve("manifest.avro").toString());

    assertThatThrownBy(
            () ->
                newReader(manifest, UNPARTITIONED_SPECS)
                    .select(ImmutableList.of("location"))
                    .project(new Schema(TrackedFile.LOCATION)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot select columns using both select(Collection<String>) and project(Schema)");

    assertThatThrownBy(
            () ->
                newReader(manifest, UNPARTITIONED_SPECS)
                    .project(new Schema(TrackedFile.LOCATION))
                    .select(ImmutableList.of("location")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot select columns using both select(Collection<String>) and project(Schema)");

    assertThatThrownBy(
            () ->
                newReader(manifest, UNPARTITIONED_SPECS)
                    .forScanPlanning()
                    .select(ImmutableList.of("location")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use select(Collection<String>) with forScanPlanning()");

    assertThatThrownBy(
            () ->
                newReader(manifest, UNPARTITIONED_SPECS)
                    .select(ImmutableList.of("location"))
                    .forScanPlanning())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot use forScanPlanning() with select(Collection<String>) or project(Schema)");

    assertThatThrownBy(
            () ->
                newReader(manifest, UNPARTITIONED_SPECS)
                    .forScanPlanning()
                    .project(new Schema(TrackedFile.LOCATION)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot use project(Schema) with forScanPlanning()");

    assertThatThrownBy(
            () ->
                newReader(manifest, UNPARTITIONED_SPECS)
                    .project(new Schema(TrackedFile.LOCATION))
                    .forScanPlanning())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot use forScanPlanning() with select(Collection<String>) or project(Schema)");
  }

  @TestTemplate
  public void testProjectionPreservesNarrowTrackingProjection() throws IOException {
    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(fileWithFullTracking()));

    Schema projection =
        new Schema(
            Types.NestedField.required(
                TrackedFile.TRACKING.fieldId(), "tracking", Types.StructType.of(Tracking.STATUS)));

    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS).project(projection).build()) {
      Tracking actual = Lists.newArrayList(reader).get(0).tracking();
      assertThat(actual.status()).isEqualTo(EntryStatus.ADDED);
      // the narrow tracking projection is not widened to the full tracking type
      assertThat(actual.snapshotId()).isNull();
      assertThat(actual.dvSnapshotId()).isNull();
      assertThat(actual.deletedPositions()).isNull();
      assertThat(actual.replacedPositions()).isNull();
    }
  }

  @TestTemplate
  public void testSelectListColumn() throws IOException {
    TrackedFile file =
        new TrackedFileStruct(
            addedTracking(),
            FileContent.DATA,
            FORMAT_VERSION_V4,
            "s3://bucket/file.parquet",
            FileFormat.PARQUET,
            EMPTY_PARTITION_DATA,
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            0,
            null,
            null,
            null,
            null,
            null,
            ImmutableList.of(50L, 100L),
            null);

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file));

    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS)
            .select(ImmutableList.of("split_offsets"))
            .build()) {
      TrackedFile actual = Lists.newArrayList(reader).get(0);
      assertThat(actual.splitOffsets()).containsExactly(50L, 100L);
      assertThat(actual.location()).isNull();
    }
  }

  @TestTemplate
  public void testSelectWithPartitionFilterProjectsFilterFields() throws IOException {
    TrackedFile keep = dataFile("keep.parquet", partition(1));
    TrackedFile prune = dataFile("prune.parquet", partition(2));

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(keep, prune));

    // the caller selects only location; the reader must still project spec_id and partition
    // for the partition filter or every row would be pruned
    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS)
            .select(ImmutableList.of("location"))
            .filterRows(Expressions.equal("id", 1))
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(keep.location());
    }
  }

  @TestTemplate
  public void testForScanPlanningOmitsChangeTrackingFields() throws IOException {
    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(fileWithFullTracking()));

    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS).forScanPlanning().build()) {
      Tracking actual = Lists.newArrayList(reader).get(0).tracking();
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

  @TestTemplate
  public void testDefaultReadsFullTracking() throws IOException {
    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(fileWithFullTracking()));

    // without a projection, the reader returns the full schema for copying to other manifests,
    // including the change-tracking fields
    Tracking actual = read(manifest, UNPARTITIONED_SPECS).get(0).tracking();
    assertThat(actual.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(actual.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(actual.dataSequenceNumber()).isEqualTo(5L);
    assertThat(actual.fileSequenceNumber()).isEqualTo(6L);
    assertThat(actual.firstRowId()).isEqualTo(8L);
    assertThat(actual.dvSnapshotId()).isEqualTo(7L);
    assertThat(actual.deletedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2}));
    assertThat(actual.replacedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {3, 4}));
  }

  @TestTemplate
  public void testPartitionFilterForceProjectsFilterFields() throws IOException {
    TrackedFile keep = dataFile("keep.parquet", partition(1));
    TrackedFile prune = dataFile("prune.parquet", partition(2));

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(keep, prune));

    // the caller projects only location; the reader must still project the fields the partition
    // filter reads (content_type, spec_id, partition) or every row would be pruned
    Schema projection = new Schema(TrackedFile.LOCATION);
    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS)
            .project(projection)
            .filterRows(Expressions.equal("id", 1))
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(keep.location());
    }
  }

  @TestTemplate
  public void testUnpartitioned() throws IOException {
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file));

    TrackedFile actual = read(manifest, UNPARTITIONED_SPECS).get(0);
    // unpartitioned manifests omit the partition field, which is read as null
    assertThat(actual.partition()).isNull();
  }

  @TestTemplate
  public void testPartitionFilterPrunesNonMatchingFiles() throws IOException {
    TrackedFile keep = dataFile("keep.parquet", partition(1));
    TrackedFile prune = dataFile("prune.parquet", partition(2));

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(keep, prune));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS)
            .filterRows(Expressions.equal("id", 1))
            .scanMetrics(metrics)
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(keep.location());
    }

    assertThat(metrics.skippedDataFiles().value()).isEqualTo(1L);
  }

  @TestTemplate
  public void testPartitionFilterCountsSkippedDeleteFiles() throws IOException {
    TrackedFile delete =
        new TrackedFileStruct(
            addedTracking(),
            FileContent.EQUALITY_DELETES,
            FORMAT_VERSION_V4,
            "delete.parquet",
            FileFormat.PARQUET,
            partition(2),
            RECORD_COUNT,
            FILE_SIZE_IN_BYTES,
            0,
            null,
            null,
            null,
            null,
            null,
            null,
            ImmutableList.of(1));

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(delete));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS)
            .filterRows(Expressions.equal("id", 1))
            .scanMetrics(metrics)
            .build()) {
      assertThat(reader).isEmpty();
    }

    assertThat(metrics.skippedDeleteFiles().value()).isEqualTo(1L);
    assertThat(metrics.skippedDataFiles().value()).isEqualTo(0L);
  }

  @TestTemplate
  public void testPartitionFilterKeepsManifestReferences() throws IOException {
    TrackedFile keep = dataFile("data-1.parquet", partition(1));
    TrackedFile prune = dataFile("data-2.parquet", partition(2));
    // a real manifest reference has a null spec_id and no partition tuple; these refs carry a
    // spec and a tuple that fails the filter so that pruning would be detected if the manifest
    // passthrough broke
    TrackedFile dataManifestRef = manifestRef(FileContent.DATA_MANIFEST, "data-leaf.parquet");
    TrackedFile deleteManifestRef = manifestRef(FileContent.DELETE_MANIFEST, "delete-leaf.parquet");

    InputFile manifest =
        writeManifest(
            PARTITION_TYPE, ImmutableList.of(keep, prune, dataManifestRef, deleteManifestRef));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS)
            .filterRows(Expressions.equal("id", 1))
            .scanMetrics(metrics)
            .build()) {
      assertThat(reader)
          .extracting(TrackedFile::location)
          .containsExactlyInAnyOrder(
              keep.location(), dataManifestRef.location(), deleteManifestRef.location());
    }

    // the manifest references bypass the filter instead of being evaluated and skipped
    assertThat(metrics.skippedDataFiles().value()).isEqualTo(1L);
    assertThat(metrics.skippedDataManifests().value()).isEqualTo(0L);
    assertThat(metrics.skippedDeleteManifests().value()).isEqualTo(0L);
  }

  @TestTemplate
  public void testRowFilterOnUnpartitionedTableKeepsAllFiles() throws IOException {
    TrackedFile file1 = dataFile("s3://bucket/a.parquet", EMPTY_PARTITION_DATA);
    TrackedFile file2 = dataFile("s3://bucket/b.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file1, file2));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS)
            .filterRows(Expressions.equal("id", 1))
            .scanMetrics(metrics)
            .build()) {
      assertThat(reader)
          .extracting(TrackedFile::location)
          .containsExactly(file1.location(), file2.location());
    }

    assertThat(metrics.skippedDataFiles().value()).isEqualTo(0L);
    assertThat(metrics.skippedDeleteFiles().value()).isEqualTo(0L);
  }

  @TestTemplate
  public void testInvalidBuilderArguments() {
    InputFile manifest = fileIO.newInputFile(tempDir.resolve("manifest.avro").toString());

    assertThatThrownBy(() -> newReader(manifest, UNPARTITIONED_SPECS).filterRows(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid row filter: null");

    assertThatThrownBy(() -> newReader(manifest, UNPARTITIONED_SPECS).scanMetrics(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid scan metrics: null");

    assertThatThrownBy(() -> newReader(manifest, UNPARTITIONED_SPECS).select(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid columns: null");
  }

  @TestTemplate
  public void testCaseInsensitivePartitionFilter() throws IOException {
    TrackedFile keep = dataFile("keep.parquet", partition(1));
    TrackedFile prune = dataFile("prune.parquet", partition(2));

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(keep, prune));

    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS)
            .filterRows(Expressions.equal("ID", 1))
            .caseSensitive(false)
            .build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(keep.location());
    }
  }

  @TestTemplate
  public void testMultiSpecPartitionPruning() throws IOException {
    PartitionSpec spec0 =
        PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(0).identity("id").build();
    PartitionSpec spec1 =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .withSpecId(1)
            .add(2, 1001, "data", Transforms.identity())
            .build();
    Map<Integer, PartitionSpec> specsById = ImmutableMap.of(0, spec0, 1, spec1);
    Types.StructType unionType = Partitioning.unionPartitionTypes(specsById.values());

    TrackedFile keepById = dataFile("spec0-id1.parquet", unionPartition(unionType, 1, null), 0);
    TrackedFile prunedById = dataFile("spec0-id2.parquet", unionPartition(unionType, 2, null), 0);
    TrackedFile keptOtherSpec =
        dataFile("spec1-data.parquet", unionPartition(unionType, null, "x"), 1);

    InputFile manifest =
        writeManifest(unionType, ImmutableList.of(keepById, prunedById, keptOtherSpec));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, specsById)
            .filterRows(Expressions.equal("id", 1))
            .build()) {
      // spec0 entries are pruned by id; the spec1 entry is not partitioned by id so it survives
      assertThat(reader)
          .extracting(TrackedFile::location)
          .containsExactlyInAnyOrder(keepById.location(), keptOtherSpec.location());
    }
  }

  @TestTemplate
  public void testIteratorReturnsLiveCopies() throws IOException {
    TrackedFile added1 = dataFile("s3://bucket/added-1.parquet", EMPTY_PARTITION_DATA);
    TrackedFile added2 = dataFile("s3://bucket/added-2.parquet", EMPTY_PARTITION_DATA);
    List<TrackedFile> files =
        ImmutableList.of(
            added1, added2, fileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"));

    InputFile manifest = writeManifest(EMPTY_PARTITION, files);

    try (V4ManifestReader reader = newReader(manifest, UNPARTITIONED_SPECS).build()) {
      List<TrackedFile> read = Lists.newArrayList(reader);
      assertThat(read)
          .hasSize(2)
          .extracting(TrackedFile::location)
          .containsExactly(added1.location(), added2.location());
      // iterator() copies each entry, so the collected instances are independent of the reused
      // container (they would be the same object if iterator() did not copy)
      assertThat(read.get(0)).isNotSameAs(read.get(1));
    }
  }

  @TestTemplate
  public void testUnknownManifestFormatThrows() throws IOException {
    InputFile badFile =
        fileIO.newInputFile(tempDir.resolve("manifest-" + System.nanoTime() + ".txt").toString());

    try (V4ManifestReader reader = newReader(badFile, UNPARTITIONED_SPECS).build()) {
      assertThatThrownBy(reader::iterator)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot determine format of manifest");
    }
  }

  @TestTemplate
  public void testPartitionFilterKeepsFileWithUnknownSpec() throws IOException {
    // spec ID 5 is not in PARTITIONED_SPECS, so no partition filter applies to this file
    TrackedFile file = dataFile("orphan.parquet", partition(1), 5);

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(file));

    // the filter would prune partition id=1 under spec 0, but cannot be applied to spec 5
    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS).filterRows(Expressions.equal("id", 2)).build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(file.location());
    }
  }

  @TestTemplate
  public void testPartitionFilterKeepsFileWithNullSpecId() throws IOException {
    TrackedFile file = dataFile("no-spec.parquet", null, null);

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(file));

    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS).filterRows(Expressions.equal("id", 2)).build()) {
      assertThat(reader).extracting(TrackedFile::location).containsExactly(file.location());
    }
  }

  private static TrackedFile dataFile(String location, PartitionData partition) {
    return dataFile(location, partition, 0);
  }

  private static TrackedFile dataFile(String location, PartitionData partition, Integer specId) {
    return new TrackedFileStruct(
        addedTracking(),
        FileContent.DATA,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        partition,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        specId,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static TrackedFile fileWithFullTracking() {
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            SNAPSHOT_ID,
            5L, // data sequence number
            6L, // file sequence number
            7L, // dv snapshot id
            8L, // first row id
            new byte[] {1, 2}, // deleted positions
            new byte[] {3, 4}); // replaced positions
    return new TrackedFileStruct(
        tracking,
        FileContent.DATA,
        FORMAT_VERSION_V4,
        "s3://bucket/file.parquet",
        FileFormat.PARQUET,
        EMPTY_PARTITION_DATA,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static TrackedFile manifestRef(FileContent content, String location) {
    ManifestInfo info = new ManifestInfoStruct(1, 0, 0, 0, 1L, 0L, 0L, 0L, 1L, null, null);
    return new TrackedFileStruct(
        addedTracking(),
        content,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        partition(2),
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        0,
        null,
        null,
        null,
        info,
        null,
        null,
        null);
  }

  private static TrackedFile fileWithStatus(EntryStatus status, String location) {
    Tracking tracking = new TrackingStruct(status, SNAPSHOT_ID, 3L, 3L, null, null, null, null);
    return new TrackedFileStruct(
        tracking,
        FileContent.DATA,
        FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        EMPTY_PARTITION_DATA,
        RECORD_COUNT,
        FILE_SIZE_IN_BYTES,
        0,
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

  private static DeletionVector deletionVector(
      String location, long offset, long sizeInBytes, long cardinality) {
    DeletionVectorStruct dv = new DeletionVectorStruct(DeletionVector.schema());
    dv.set(0, location);
    dv.set(1, offset);
    dv.set(2, sizeInBytes);
    dv.set(3, cardinality);
    return dv;
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

  private InputFile writeManifest(Types.StructType partitionType, Iterable<TrackedFile> files)
      throws IOException {
    Schema writeSchema =
        new Schema(TrackedFile.schema(partitionType, Types.StructType.of()).fields());
    OutputFile out =
        fileIO.newOutputFile(
            tempDir
                .resolve(
                    "manifest-" + System.nanoTime() + "." + format.name().toLowerCase(Locale.ROOT))
                .toString());
    try (FileAppender<StructLike> appender =
        InternalData.write(format, out).schema(writeSchema).named("tracked_file").build()) {
      for (TrackedFile file : files) {
        appender.add((StructLike) file);
      }
    }

    return fileIO.newInputFile(out.location());
  }

  private V4ManifestReader.Builder newReader(
      InputFile manifest, Map<Integer, PartitionSpec> specsById) {
    return V4ManifestReader.builder(manifest, specsById);
  }

  private List<TrackedFile> read(InputFile manifest, Map<Integer, PartitionSpec> specsById)
      throws IOException {
    try (V4ManifestReader reader = newReader(manifest, specsById).build()) {
      return Lists.newArrayList(reader);
    }
  }
}
