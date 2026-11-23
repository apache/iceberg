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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mumbling.MumblingBitmaps;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestV4ManifestWriter {
  private static final long COMMIT_SNAPSHOT_ID = 42L;
  private static final long COMMIT_SEQUENCE_NUMBER = 50L;
  private static final long COMMIT_FIRST_ROW_ID = 1_000L;
  private static final long RECORD_COUNT = 100L;
  private static final Schema SCHEMA =
      new Schema(
          1,
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()));

  private static final PartitionSpec TRUNCATE_SPEC =
      PartitionSpec.builderFor(SCHEMA).truncate("id", 100).build();
  private static final Map<Integer, PartitionSpec> SPECS =
      Map.of(TRUNCATE_SPEC.specId(), TRUNCATE_SPEC);

  private static final MetricsConfig METRICS_CONFIG =
      MetricsTestUtil.from(ImmutableMap.of(), SCHEMA);

  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(SCHEMA, METRICS_CONFIG);

  private static final FieldStats<Integer> ID_STATS_0_99 =
      StatsTestUtil.fieldStats(STATS_TYPE, 1, 0, 99, RECORD_COUNT, 0L, null, null);
  private static final FieldStats<Integer> ID_STATS_100_199 =
      StatsTestUtil.fieldStats(STATS_TYPE, 1, 100, 199, RECORD_COUNT, 0L, null, null);
  private static final FieldStats<String> DATA_STATS =
      StatsTestUtil.fieldStats(STATS_TYPE, 2, "a", "z", RECORD_COUNT, 20L, null, 2L);

  private static final ContentStatsStruct CONTENT_STATS_A = new ContentStatsStruct(STATS_TYPE);
  private static final ContentStatsStruct CONTENT_STATS_B = new ContentStatsStruct(STATS_TYPE);

  static {
    CONTENT_STATS_A.setStats(1, ID_STATS_0_99);
    CONTENT_STATS_A.setStats(2, DATA_STATS);
    CONTENT_STATS_B.setStats(1, ID_STATS_100_199);
    CONTENT_STATS_B.setStats(2, DATA_STATS);
  }

  // Tracking used for manifests produced in tests
  private static final Tracking COMMIT_TRACKING =
      new TrackingStruct(
          EntryStatus.ADDED,
          COMMIT_SNAPSHOT_ID,
          COMMIT_SEQUENCE_NUMBER,
          COMMIT_SEQUENCE_NUMBER,
          null, // not modified
          COMMIT_FIRST_ROW_ID,
          null, // no change bitmaps
          null);

  private static final DeletionVector DV_A =
      DeletionVectorStruct.builder()
          .location("s3://bucket/db/table/data/file-a-dv.puffin")
          .offset(0)
          .sizeInBytes(100)
          .cardinality(10)
          .build();

  private static final ColumnFile COLUMN_FILE_A =
      new ColumnFileStruct(
          "s3://bucket/db/table/data/file-a-new-columns.parquet",
          Set.of(2),
          FileFormat.PARQUET,
          500L,
          null); // key metadata

  private static final TrackedFile FILE_A =
      new TrackedFileStruct(
          new TrackingStruct(EntryStatus.EXISTING, 40L, 35L, 34L, null, 500L, null, null),
          FileContent.DATA,
          "s3://bucket/db/table/data/file-a.parquet",
          FileFormat.PARQUET,
          100,
          1024,
          null, // unpartitioned
          null, // unpartitioned
          CONTENT_STATS_A,
          false,
          SortOrder.unsorted().orderId(),
          null, // no DV
          null, // manifest info
          null, // key metadata,
          List.of(4L),
          List.of(COLUMN_FILE_A));

  private static final TrackedFile FILE_A_WITH_DV =
      new TrackedFileStruct(
          new TrackingStruct(EntryStatus.EXISTING, 40L, 35L, 34L, 41L, 500L, null, null),
          FileContent.DATA,
          "s3://bucket/db/table/data/file-a.parquet",
          FileFormat.PARQUET,
          100,
          1024,
          null,
          null,
          CONTENT_STATS_A,
          false,
          SortOrder.unsorted().orderId(),
          DV_A,
          null, // manifest info
          null, // key metadata,
          List.of(4L),
          List.of(COLUMN_FILE_A));

  private static final TrackedFile FILE_B =
      new TrackedFileStruct(
          new TrackingStruct(EntryStatus.EXISTING, 41L, 36L, 36L, null, 600L, null, null),
          FileContent.DATA,
          "s3://bucket/db/table/data/file-b.parquet",
          FileFormat.PARQUET,
          150,
          1024,
          TRUNCATE_SPEC.specId(),
          V4TestHelpers.partition(TRUNCATE_SPEC, 100),
          CONTENT_STATS_B,
          false,
          SortOrder.unsorted().orderId(),
          null, // no DV
          null, // manifest info
          null, // key metadata,
          List.of(4L),
          List.of(COLUMN_FILE_A));

  private static final ColumnFile REPLACE_DV_COLUMNS =
      new ColumnFileStruct(
          "s3://bucket/db/table/data/leaf-40-1-dv-update.parquet",
          Set.of(TrackedFile.DELETION_VECTOR.fieldId(), TrackedFile.TIGHT_STATS.fieldId()),
          FileFormat.PARQUET,
          512,
          null);

  // a generic manifest info; row count = 100 * file count
  private static final ManifestInfo MANIFEST_INFO =
      new ManifestInfoStruct(2, 10, 2, 4, 4, 200, 1_000, 200, 400, 400, 34L, null /* MDV */, 4);

  private static final ManifestBitmap MDV = MumblingBitmaps.newMutableBitmap(4, 8, 12);

  private static final ManifestInfo MANIFEST_INFO_WITH_MDV =
      new ManifestInfoStruct(2, 10, 2, 4, 4, 200, 1_000, 200, 400, 400, 34L, MDV, 4);

  private static final TrackedFile LEAF_MANIFEST =
      new TrackedFileStruct(
          new TrackingStruct(EntryStatus.EXISTING, 40L, 35L, 35L, null, 500L, null, null),
          FileContent.DATA_MANIFEST,
          "s3://bucket/db/table/metadata/leaf-40-1.parquet",
          FileFormat.PARQUET,
          100,
          1024,
          null, // unpartitioned
          null, // unpartitioned
          CONTENT_STATS_A,
          false,
          null, // no sort order for manifests
          null, // no external DV for manifests
          MANIFEST_INFO,
          null, // key metadata
          List.of(4L),
          List.of(REPLACE_DV_COLUMNS));

  private static final TrackedFile LEAF_MANIFEST_WITH_MDV =
      new TrackedFileStruct(
          new TrackingStruct(EntryStatus.EXISTING, 40L, 35L, 35L, 41L, 500L, null, null),
          FileContent.DATA_MANIFEST,
          "s3://bucket/db/table/metadata/leaf-40-1.parquet",
          FileFormat.PARQUET,
          100,
          1024,
          null, // unpartitioned
          null, // unpartitioned
          CONTENT_STATS_A,
          false,
          null, // no sort order for manifests
          null, // no external DV for manifests
          MANIFEST_INFO_WITH_MDV,
          null, // key metadata
          List.of(4L),
          List.of(REPLACE_DV_COLUMNS));

  private static final TrackedFile DELETE_MANIFEST =
      new TrackedFileStruct(
          new TrackingStruct(EntryStatus.EXISTING, 40L, 35L, 35L, 41L, null, null, null),
          FileContent.DELETE_MANIFEST,
          "s3://bucket/db/table/metadata/leaf-40-1-deletes.parquet",
          FileFormat.PARQUET,
          100,
          1024,
          null, // unpartitioned
          null, // unpartitioned
          null, // no stats
          false,
          null, // no sort order for manifests
          null, // no external DV for manifests
          MANIFEST_INFO_WITH_MDV,
          null, // key metadata
          List.of(4L),
          List.of(REPLACE_DV_COLUMNS));

  @Test
  public void rootDataFileAppend() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.append(FILE_A);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_A.recordCount());

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(FILE_A);
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the commit metadata")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID,
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootDataFileAppendAtDataSeq() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.appendAtSeq(FILE_A, 33L);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_A.recordCount());

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(FILE_A);
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the commit metadata")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  33L,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID,
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootDataFileWithDVAppend() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.append(FILE_A_WITH_DV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_A.recordCount());

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(FILE_A_WITH_DV);
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the commit metadata")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // added, not modified
                  COMMIT_FIRST_ROW_ID,
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootDataFileWithDVModify() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.modify(FILE_A, DV_A);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0);

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(FILE_A_WITH_DV);
      Tracking originalTracking = FILE_A.tracking();
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the original metadata, sets modified snapshot ID")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.MODIFIED,
                  originalTracking.snapshotId(),
                  originalTracking.dataSequenceNumber(),
                  originalTracking.fileSequenceNumber(),
                  COMMIT_SNAPSHOT_ID,
                  originalTracking.firstRowId(),
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootDataFileExisting() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.existing(FILE_A);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0);

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(FILE_A);
      Tracking originalTracking = FILE_A.tracking();
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the original metadata")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.EXISTING,
                  originalTracking.snapshotId(),
                  originalTracking.dataSequenceNumber(),
                  originalTracking.fileSequenceNumber(),
                  originalTracking.modifiedSnapshotId(),
                  originalTracking.firstRowId(),
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootDataFileDelete() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.delete(FILE_A_WITH_DV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0);

    assertThat(read(writer, output.toInputFile())).isEmpty();

    TrackedFile file = rawReadOne(output.toInputFile());
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A_WITH_DV);
    Tracking originalTracking = FILE_A_WITH_DV.tracking();
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Deleted entry preserves original metadata, sets snapshot ID")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.DELETED,
                COMMIT_SNAPSHOT_ID,
                originalTracking.dataSequenceNumber(),
                originalTracking.fileSequenceNumber(),
                originalTracking.modifiedSnapshotId(),
                originalTracking.firstRowId(),
                null, // no change bitmaps
                null));
  }

  @Test
  public void rootDataFileReplace() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.replace(FILE_A_WITH_DV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0);

    assertThat(read(writer, output.toInputFile())).isEmpty();

    TrackedFile file = rawReadOne(output.toInputFile());
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A_WITH_DV);
    Tracking originalTracking = FILE_A_WITH_DV.tracking();
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Replaced entry preserves original metadata, sets snapshot ID")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.REPLACED,
                COMMIT_SNAPSHOT_ID,
                originalTracking.dataSequenceNumber(),
                originalTracking.fileSequenceNumber(),
                originalTracking.modifiedSnapshotId(),
                originalTracking.firstRowId(),
                null, // no change bitmaps
                null));
  }

  @Test
  public void rootMixedPartitionDataFiles() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.existing(FILE_A);
      writer.append(FILE_B);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_B.recordCount());

    List<TrackedFile> files = read(writer, output.toInputFile());
    assertThat(files)
        .usingComparatorForType(
            V4TestComparators.trackedFileWithoutStatus(TRUNCATE_SPEC.partitionType()),
            TrackedFile.class)
        .containsExactly(FILE_A, FILE_B);
    assertThat(files.get(0).tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking should preserve FILE_A metadata")
        .isEqualTo(FILE_A.tracking());
    assertThat(files.get(1).tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking uses the commit metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                COMMIT_SEQUENCE_NUMBER,
                COMMIT_SEQUENCE_NUMBER,
                null, // not modified
                COMMIT_FIRST_ROW_ID,
                null, // no change bitmaps
                null));
  }

  @Test
  public void rootMissingPartitionSpec() throws IOException {
    V4ManifestWriter root =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(new InMemoryOutputFile("root.parquet")),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);

    try (root) {
      assertThatThrownBy(() -> root.append(FILE_B))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cannot find partition spec for spec ID: " + TRUNCATE_SPEC.specId());
    }
  }

  @Test
  public void rootDataFileFirstRowIdAssignment() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.append(FILE_A);
      writer.append(FILE_B);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_A.recordCount() + FILE_B.recordCount());

    InputFile in = output.toInputFile();
    for (List<TrackedFile> files : List.of(read(writer, in), rawRead(output.toInputFile()))) {
      assertThat(files)
          .usingComparatorForType(
              V4TestComparators.trackedFileWithoutStatus(TRUNCATE_SPEC.partitionType()),
              TrackedFile.class)
          .containsExactly(FILE_A, FILE_B);
      assertThat(files.get(0).tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID,
                  null, // no change bitmaps
                  null));
      assertThat(files.get(1).tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID + FILE_A.recordCount(),
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootManifestFileAppend() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.append(LEAF_MANIFEST);
    }

    ManifestInfo summary = LEAF_MANIFEST.manifestInfo();
    long leafRowsAssigned =
        summary.addedRowsCount() + summary.existingRowsCount() + summary.modifiedRowsCount();
    assertThat(writer.assignedRowIds()).isEqualTo(leafRowsAssigned);

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(LEAF_MANIFEST);
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the commit metadata")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID,
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootManifestFileWithMDVAppend() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.append(LEAF_MANIFEST_WITH_MDV);
    }

    ManifestInfo summary = LEAF_MANIFEST_WITH_MDV.manifestInfo();
    long leafRowsAssigned =
        summary.addedRowsCount() + summary.existingRowsCount() + summary.modifiedRowsCount();
    assertThat(writer.assignedRowIds()).isEqualTo(leafRowsAssigned);

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(LEAF_MANIFEST_WITH_MDV);
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the commit metadata")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID,
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootManifestFileExisting() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.existing(LEAF_MANIFEST_WITH_MDV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(LEAF_MANIFEST_WITH_MDV);
      Tracking originalTracking = LEAF_MANIFEST_WITH_MDV.tracking();
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the original metadata")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.EXISTING,
                  originalTracking.snapshotId(),
                  originalTracking.dataSequenceNumber(),
                  originalTracking.fileSequenceNumber(),
                  originalTracking.modifiedSnapshotId(),
                  originalTracking.firstRowId(),
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootManifestFileDelete() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.delete(LEAF_MANIFEST);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    assertThat(read(writer, output.toInputFile())).isEmpty();

    TrackedFile file = rawReadOne(output.toInputFile());
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(LEAF_MANIFEST);
    Tracking originalTracking = LEAF_MANIFEST.tracking();
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Deleted entry preserves original metadata, sets snapshot ID")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.DELETED,
                COMMIT_SNAPSHOT_ID,
                originalTracking.dataSequenceNumber(),
                originalTracking.fileSequenceNumber(),
                originalTracking.modifiedSnapshotId(),
                originalTracking.firstRowId(),
                null, // no change bitmaps
                null));
  }

  @Test
  public void rootManifestFileReplace() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.replace(LEAF_MANIFEST_WITH_MDV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    assertThat(read(writer, output.toInputFile())).isEmpty();

    TrackedFile file = rawReadOne(output.toInputFile());
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(LEAF_MANIFEST_WITH_MDV);
    Tracking originalTracking = LEAF_MANIFEST_WITH_MDV.tracking();
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Replaced entry preserves original metadata, sets snapshot ID")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.REPLACED,
                COMMIT_SNAPSHOT_ID,
                originalTracking.dataSequenceNumber(),
                originalTracking.fileSequenceNumber(),
                originalTracking.modifiedSnapshotId(),
                originalTracking.firstRowId(),
                null, // no change bitmaps
                null));
  }

  @Test
  public void rootDeleteManifestExisting() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.existing(DELETE_MANIFEST);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(DELETE_MANIFEST);
      Tracking originalTracking = DELETE_MANIFEST.tracking();
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the original metadata")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.EXISTING,
                  originalTracking.snapshotId(),
                  originalTracking.dataSequenceNumber(),
                  originalTracking.fileSequenceNumber(),
                  originalTracking.modifiedSnapshotId(),
                  null,
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void rootDeleteManifestDelete() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.delete(DELETE_MANIFEST);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    assertThat(read(writer, output.toInputFile())).isEmpty();

    TrackedFile file = rawReadOne(output.toInputFile());
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(DELETE_MANIFEST);
    Tracking originalTracking = DELETE_MANIFEST.tracking();
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking uses the original metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.DELETED,
                COMMIT_SNAPSHOT_ID,
                originalTracking.dataSequenceNumber(),
                originalTracking.fileSequenceNumber(),
                originalTracking.modifiedSnapshotId(),
                null,
                null, // no change bitmaps
                null));
  }

  @Test
  public void rootManifestFileAppendAtSeq() throws IOException {
    V4ManifestWriter root =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(new InMemoryOutputFile("root.parquet")),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);

    try (root) {
      assertThatThrownBy(() -> root.appendAtSeq(LEAF_MANIFEST, 33L))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot append DATA_MANIFEST at data sequence number 33");
    }
  }

  @Test
  public void rootManifestFileWithDeletionVector() throws IOException {
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(new InMemoryOutputFile("manifest.parquet")),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);

    try (writer) {
      assertThatThrownBy(() -> writer.modify(LEAF_MANIFEST, DV_A))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot modify DATA_MANIFEST file with a data DV");
    }
  }

  @Test
  public void rootDeleteManifestAppend() throws IOException {
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(new InMemoryOutputFile("manifest.parquet")),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);

    try (writer) {
      assertThatThrownBy(() -> writer.append(DELETE_MANIFEST))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot append DELETE_MANIFEST file");
    }
  }

  @Test
  public void rootDeleteManifestAppendAtSeq() throws IOException {
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(new InMemoryOutputFile("manifest.parquet")),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);

    try (writer) {
      assertThatThrownBy(() -> writer.appendAtSeq(DELETE_MANIFEST, 33L))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot append DELETE_MANIFEST at data sequence number 33");
    }
  }

  @Test
  public void rootDeleteManifestModify() throws IOException {
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(new InMemoryOutputFile("manifest.parquet")),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);

    try (writer) {
      assertThatThrownBy(() -> writer.modify(DELETE_MANIFEST, DV_A))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot modify DELETE_MANIFEST file with a data DV");
    }
  }

  @Test
  public void rootDeleteManifestReplace() throws IOException {
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(new InMemoryOutputFile("manifest.parquet")),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);

    try (writer) {
      assertThatThrownBy(() -> writer.replace(DELETE_MANIFEST))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot replace DELETE_MANIFEST file");
    }
  }

  @Test
  public void rootMixedDataAndManifestFiles() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.existing(FILE_A);
      writer.existing(DELETE_MANIFEST);
      writer.append(FILE_B);
      writer.existing(LEAF_MANIFEST_WITH_MDV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_B.recordCount());

    InputFile in = output.toInputFile();
    for (List<TrackedFile> files : List.of(read(writer, in), rawRead(in))) {
      assertThat(files)
          .usingComparatorForType(
              V4TestComparators.trackedFileWithoutStatus(TRUNCATE_SPEC.partitionType()),
              TrackedFile.class)
          .containsExactly(FILE_A, DELETE_MANIFEST, FILE_B, LEAF_MANIFEST_WITH_MDV);
      assertThat(files.get(0).tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking should preserve FILE_A metadata")
          .isEqualTo(FILE_A.tracking());
      assertThat(files.get(1).tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the commit metadata for FILE_B")
          .isEqualTo(DELETE_MANIFEST.tracking());
      assertThat(files.get(2).tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the commit metadata for FILE_B")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID,
                  null, // no change bitmaps
                  null));
      assertThat(files.get(3).tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking should preserve LEAF_MANIFEST_WITH_DV metadata")
          .isEqualTo(LEAF_MANIFEST_WITH_MDV.tracking());
    }
  }

  @Test
  public void rootMixedDataAndManifestFilesFirstRowIdAssignment() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createRoot(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID,
            COMMIT_SEQUENCE_NUMBER,
            COMMIT_FIRST_ROW_ID);
    try (writer) {
      writer.append(FILE_A);
      writer.append(LEAF_MANIFEST);
      writer.append(FILE_B);
    }

    ManifestInfo leafSummary = LEAF_MANIFEST.manifestInfo();
    long leafRowsAssigned =
        leafSummary.addedRowsCount()
            + leafSummary.existingRowsCount()
            + leafSummary.modifiedRowsCount();

    assertThat(writer.assignedRowIds())
        .isEqualTo(FILE_A.recordCount() + leafRowsAssigned + FILE_B.recordCount());

    InputFile in = output.toInputFile();
    for (List<TrackedFile> files : List.of(read(writer, in), rawRead(output.toInputFile()))) {
      assertThat(files)
          .usingComparatorForType(
              V4TestComparators.trackedFileWithoutStatus(TRUNCATE_SPEC.partitionType()),
              TrackedFile.class)
          .containsExactly(FILE_A, LEAF_MANIFEST, FILE_B);
      assertThat(files.get(0).tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID,
                  null, // no change bitmaps
                  null));
      assertThat(files.get(1).tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID + FILE_A.recordCount(),
                  null, // no change bitmaps
                  null));
      assertThat(files.get(2).tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.ADDED,
                  COMMIT_SNAPSHOT_ID,
                  COMMIT_SEQUENCE_NUMBER,
                  COMMIT_SEQUENCE_NUMBER,
                  null, // not modified
                  COMMIT_FIRST_ROW_ID + FILE_A.recordCount() + leafRowsAssigned,
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void leafDataFileAppend() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.append(FILE_A);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_A.recordCount());

    InputFile in = output.toInputFile();
    TrackedFile file = readOne(writer, in);
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A);
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking uses the commit metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                COMMIT_SEQUENCE_NUMBER,
                COMMIT_SEQUENCE_NUMBER,
                null, // not modified
                COMMIT_FIRST_ROW_ID,
                null, // no change bitmaps
                null));

    file = rawReadOne(in);
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A);
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking will inherit metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                null, // inherit data sequence number
                null, // inherit file sequence number
                null, // not modified
                null, // inherit first row ID
                null, // no change bitmaps
                null));
  }

  @Test
  public void leafDataFileAppendAtDataSeq() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.appendAtSeq(FILE_A, 33L);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_A.recordCount());

    InputFile in = output.toInputFile();
    TrackedFile file = readOne(writer, in);
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A);
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking uses the commit metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                33L,
                COMMIT_SEQUENCE_NUMBER,
                null, // not modified
                COMMIT_FIRST_ROW_ID,
                null, // no change bitmaps
                null));

    file = rawReadOne(in);
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A);
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking will inherit metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                33L,
                null, // inherit file sequence number
                null, // not modified
                null, // inherit first row ID
                null, // no change bitmaps
                null));
  }

  @Test
  public void leafDataFileAppendWithoutSnapshotID() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            null); // snapshot ID will be inherited
    try (writer) {
      writer.append(FILE_A_WITH_DV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_A_WITH_DV.recordCount());

    InputFile in = output.toInputFile();
    TrackedFile file = readOne(writer, in);
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A_WITH_DV);
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking uses the commit metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                COMMIT_SEQUENCE_NUMBER,
                COMMIT_SEQUENCE_NUMBER,
                null, // added, not modified
                COMMIT_FIRST_ROW_ID,
                null, // no change bitmaps
                null));

    file = rawReadOne(in);
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A_WITH_DV);
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking will inherit metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                null, // inherit snapshot ID
                null, // inherit data sequence number
                null, // inherit file sequence number
                null, // not modified
                null, // inherit first row ID
                null, // no change bitmaps
                null));
  }

  @Test
  public void leafDataFileWithDVAppend() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.append(FILE_A_WITH_DV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_A_WITH_DV.recordCount());

    InputFile in = output.toInputFile();
    TrackedFile file = readOne(writer, in);
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A_WITH_DV);
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking uses the commit metadata, sets modified snapshot ID")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                COMMIT_SEQUENCE_NUMBER,
                COMMIT_SEQUENCE_NUMBER,
                null, // added, not modified
                COMMIT_FIRST_ROW_ID,
                null, // no change bitmaps
                null));

    file = rawReadOne(in);
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A_WITH_DV);
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking will inherit metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                null, // inherit data sequence number
                null, // inherit file sequence number
                null, // added, not modified
                null, // inherit first row ID
                null, // no change bitmaps
                null));
  }

  @Test
  public void leafDataFileWithDVModify() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.modify(FILE_A, DV_A);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(FILE_A_WITH_DV);
      Tracking originalTracking = FILE_A.tracking();
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the original metadata, sets modified snapshot ID")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.MODIFIED,
                  originalTracking.snapshotId(),
                  originalTracking.dataSequenceNumber(),
                  originalTracking.fileSequenceNumber(),
                  COMMIT_SNAPSHOT_ID,
                  originalTracking.firstRowId(),
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void leafDataFileExisting() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.existing(FILE_A);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    InputFile in = output.toInputFile();
    for (TrackedFile file : List.of(readOne(writer, in), rawReadOne(in))) {
      assertThat(file)
          .usingComparator(V4TestComparators.trackedFileWithoutStatus())
          .isEqualTo(FILE_A);
      Tracking originalTracking = FILE_A.tracking();
      assertThat(file.tracking())
          .usingComparator(V4TestComparators.TRACKING)
          .as("Tracking uses the original metadata")
          .isEqualTo(
              new TrackingStruct(
                  EntryStatus.EXISTING,
                  originalTracking.snapshotId(),
                  originalTracking.dataSequenceNumber(),
                  originalTracking.fileSequenceNumber(),
                  originalTracking.modifiedSnapshotId(),
                  originalTracking.firstRowId(),
                  null, // no change bitmaps
                  null));
    }
  }

  @Test
  public void leafDataFileDelete() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.delete(FILE_A_WITH_DV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    assertThat(read(writer, output.toInputFile())).isEmpty();

    TrackedFile file = rawReadOne(output.toInputFile());
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A_WITH_DV);
    Tracking originalTracking = FILE_A_WITH_DV.tracking();
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Deleted entry preserves original metadata, sets snapshot ID")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.DELETED,
                COMMIT_SNAPSHOT_ID,
                originalTracking.dataSequenceNumber(),
                originalTracking.fileSequenceNumber(),
                originalTracking.modifiedSnapshotId(),
                originalTracking.firstRowId(),
                null, // no change bitmaps
                null));
  }

  @Test
  public void leafDataFileReplace() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.replace(FILE_A_WITH_DV);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    assertThat(read(writer, output.toInputFile())).isEmpty();

    TrackedFile file = rawReadOne(output.toInputFile());
    assertThat(file)
        .usingComparator(V4TestComparators.trackedFileWithoutStatus())
        .isEqualTo(FILE_A_WITH_DV);
    Tracking originalTracking = FILE_A_WITH_DV.tracking();
    assertThat(file.tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Replaced entry preserves original metadata, sets snapshot ID")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.REPLACED,
                COMMIT_SNAPSHOT_ID,
                originalTracking.dataSequenceNumber(),
                originalTracking.fileSequenceNumber(),
                originalTracking.modifiedSnapshotId(),
                originalTracking.firstRowId(),
                null, // no change bitmaps
                null));
  }

  @Test
  public void leafMixedPartitionDataFiles() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.existing(FILE_A);
      writer.append(FILE_B);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_B.recordCount());

    List<TrackedFile> files = read(writer, output.toInputFile());
    assertThat(files)
        .usingComparatorForType(
            V4TestComparators.trackedFileWithoutStatus(TRUNCATE_SPEC.partitionType()),
            TrackedFile.class)
        .containsExactly(FILE_A, FILE_B);
    assertThat(files.get(0).tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking should preserve FILE_A metadata")
        .isEqualTo(FILE_A.tracking());
    assertThat(files.get(1).tracking())
        .usingComparator(V4TestComparators.TRACKING)
        .as("Tracking uses the commit metadata")
        .isEqualTo(
            new TrackingStruct(
                EntryStatus.ADDED,
                COMMIT_SNAPSHOT_ID,
                COMMIT_SEQUENCE_NUMBER,
                COMMIT_SEQUENCE_NUMBER,
                null, // not modified
                COMMIT_FIRST_ROW_ID,
                null, // no change bitmaps
                null));
  }

  @Test
  public void leafMissingPartitionSpec() throws IOException {
    V4ManifestWriter leaf =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(new InMemoryOutputFile("leaf.parquet")),
            Map.of(),
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);

    try (leaf) {
      assertThatThrownBy(() -> leaf.append(FILE_B))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cannot find partition spec for spec ID: " + TRUNCATE_SPEC.specId());
    }
  }

  @Test
  public void leafManifestInfoAppend() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.appendAtSeq(FILE_A_WITH_DV, 33L);
      writer.append(FILE_B);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(FILE_A.recordCount() + FILE_B.recordCount());

    ManifestInfo stats = writer.toTrackedManifest().manifestInfo();
    assertThat(stats.addedFilesCount()).isEqualTo(2);
    assertThat(stats.addedRowsCount()).isEqualTo(FILE_A.recordCount() + FILE_B.recordCount());
    assertThat(stats.existingFilesCount()).isEqualTo(0);
    assertThat(stats.existingRowsCount()).isEqualTo(0L);
    assertThat(stats.modifiedFilesCount()).isEqualTo(0);
    assertThat(stats.modifiedRowsCount()).isEqualTo(0L);
    assertThat(stats.deletedFilesCount()).isEqualTo(0);
    assertThat(stats.deletedRowsCount()).isEqualTo(0L);
    assertThat(stats.replacedFilesCount()).isEqualTo(0);
    assertThat(stats.replacedRowsCount()).isEqualTo(0L);
    assertThat(stats.minSequenceNumber()).isEqualTo(33L);
  }

  @Test
  public void leafManifestInfoExisting() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.existing(FILE_A);
      writer.existing(FILE_B);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    ManifestInfo stats = writer.toTrackedManifest().manifestInfo();
    assertThat(stats.addedFilesCount()).isEqualTo(0);
    assertThat(stats.addedRowsCount()).isEqualTo(0L);
    assertThat(stats.existingFilesCount()).isEqualTo(2);
    assertThat(stats.existingRowsCount()).isEqualTo(FILE_A.recordCount() + FILE_B.recordCount());
    assertThat(stats.modifiedFilesCount()).isEqualTo(0);
    assertThat(stats.modifiedRowsCount()).isEqualTo(0L);
    assertThat(stats.deletedFilesCount()).isEqualTo(0);
    assertThat(stats.deletedRowsCount()).isEqualTo(0L);
    assertThat(stats.replacedFilesCount()).isEqualTo(0);
    assertThat(stats.replacedRowsCount()).isEqualTo(0L);
    assertThat(stats.minSequenceNumber())
        .isEqualTo(
            Math.min(
                FILE_A.tracking().dataSequenceNumber(), FILE_B.tracking().dataSequenceNumber()));
  }

  @Test
  public void leafManifestInfoModify() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.modify(FILE_A, DV_A);
      writer.existing(FILE_B);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    ManifestInfo stats = writer.toTrackedManifest().manifestInfo();
    assertThat(stats.addedFilesCount()).isEqualTo(0);
    assertThat(stats.addedRowsCount()).isEqualTo(0L);
    assertThat(stats.existingFilesCount()).isEqualTo(1);
    assertThat(stats.existingRowsCount()).isEqualTo(FILE_B.recordCount());
    assertThat(stats.modifiedFilesCount()).isEqualTo(1);
    assertThat(stats.modifiedRowsCount()).isEqualTo(FILE_A.recordCount());
    assertThat(stats.deletedFilesCount()).isEqualTo(0);
    assertThat(stats.deletedRowsCount()).isEqualTo(0L);
    assertThat(stats.replacedFilesCount()).isEqualTo(0);
    assertThat(stats.replacedRowsCount()).isEqualTo(0L);
    assertThat(stats.minSequenceNumber())
        .isEqualTo(
            Math.min(
                FILE_A.tracking().dataSequenceNumber(), FILE_B.tracking().dataSequenceNumber()));
  }

  @Test
  public void leafManifestInfoDelete() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.delete(FILE_A);
      writer.delete(FILE_B);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    ManifestInfo stats = writer.toTrackedManifest().manifestInfo();
    assertThat(stats.addedFilesCount()).isEqualTo(0);
    assertThat(stats.addedRowsCount()).isEqualTo(0L);
    assertThat(stats.existingFilesCount()).isEqualTo(0);
    assertThat(stats.existingRowsCount()).isEqualTo(0L);
    assertThat(stats.modifiedFilesCount()).isEqualTo(0);
    assertThat(stats.modifiedRowsCount()).isEqualTo(0L);
    assertThat(stats.deletedFilesCount()).isEqualTo(2);
    assertThat(stats.deletedRowsCount()).isEqualTo(FILE_A.recordCount() + FILE_B.recordCount());
    assertThat(stats.replacedFilesCount()).isEqualTo(0);
    assertThat(stats.replacedRowsCount()).isEqualTo(0L);
    assertThat(stats.minSequenceNumber()).isEqualTo(V4ManifestWriter.UNASSIGNED_SEQ);
  }

  @Test
  public void leafManifestInfoReplace() throws IOException {
    OutputFile output = new InMemoryOutputFile("manifest.parquet");
    V4ManifestWriter writer =
        V4ManifestWriter.createLeaf(
            EncryptionUtil.plainAsEncryptedOutput(output),
            SPECS,
            STATS_TYPE,
            Map.of(),
            SCHEMA.schemaId(),
            COMMIT_SNAPSHOT_ID);
    try (writer) {
      writer.replace(FILE_A);
      writer.replace(FILE_B);
    }

    assertThat(writer.assignedRowIds()).isEqualTo(0L);

    ManifestInfo stats = writer.toTrackedManifest().manifestInfo();
    assertThat(stats.addedFilesCount()).isEqualTo(0);
    assertThat(stats.addedRowsCount()).isEqualTo(0L);
    assertThat(stats.existingFilesCount()).isEqualTo(0);
    assertThat(stats.existingRowsCount()).isEqualTo(0L);
    assertThat(stats.modifiedFilesCount()).isEqualTo(0);
    assertThat(stats.modifiedRowsCount()).isEqualTo(0L);
    assertThat(stats.deletedFilesCount()).isEqualTo(0);
    assertThat(stats.deletedRowsCount()).isEqualTo(0L);
    assertThat(stats.replacedFilesCount()).isEqualTo(2);
    assertThat(stats.replacedRowsCount()).isEqualTo(FILE_A.recordCount() + FILE_B.recordCount());
    assertThat(stats.minSequenceNumber()).isEqualTo(V4ManifestWriter.UNASSIGNED_SEQ);
  }

  private static TrackedFile readOne(V4ManifestWriter writer, InputFile file) throws IOException {
    return Iterables.getOnlyElement(read(writer, file));
  }

  private static List<TrackedFile> read(V4ManifestWriter writer, InputFile file)
      throws IOException {
    // add metadata that comes from the commit to the writer's file metadata
    TrackedFile manifest = withTracking(writer.toTrackedManifest(), COMMIT_TRACKING);
    try (CloseableIterable<TrackedFile> reader =
        V4ManifestReader.builder(TrackedFileAdapters.asManifestFile(manifest), file, SCHEMA, SPECS)
            .metricsConfig(METRICS_CONFIG)
            .projectStats(1, 2) // return all stats
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  private static TrackedFile rawReadOne(InputFile file) throws IOException {
    return Iterables.getOnlyElement(rawRead(file));
  }

  private static List<TrackedFile> rawRead(InputFile file) throws IOException {
    Schema manifestSchema =
        V4Metadata.rootManifestSchema(
            Partitioning.unionPartitionTypes(SPECS.values()),
            StatsUtil.statsWriteSchema(SCHEMA, METRICS_CONFIG));
    try (CloseableIterable<TrackedFile> reader =
        InternalData.read(FileFormat.PARQUET, file)
            .project(manifestSchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .setCustomType(TrackedFile.CONTENT_STATS_ID, ContentStatsStruct.class)
            .setCustomType(StatsUtil.toBaseId(1), FieldStatsStruct.class)
            .setCustomType(StatsUtil.toBaseId(2), FieldStatsStruct.class)
            .setCustomType(
                TrackedFile.COLUMN_FILES.type().asListType().elementId(), ColumnFileStruct.class)
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  private static TrackedFile withTracking(TrackedFile manifest, Tracking tracking) {
    return new Delegates.DelegatingTrackedFile(manifest) {
      @Override
      public Tracking tracking() {
        return tracking;
      }
    };
  }
}
