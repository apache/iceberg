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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
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

  private static final List<Types.NestedField> SCHEMA_FIELDS =
      TrackedFile.schemaWithContentStats(Types.StructType.of(), Types.StructType.of()).fields();
  private static final int SORT_ORDER_ID_ORDINAL = ordinalOf(TrackedFile.SORT_ORDER_ID.fieldId());

  @Parameter private FileFormat format;

  @Parameters(name = "format = {0}")
  protected static List<FileFormat> parameters() {
    return Arrays.asList(FileFormat.AVRO, FileFormat.PARQUET);
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
      assertThat(reader.allFiles())
          .extracting(file -> file.tracking().status())
          .containsExactly(
              EntryStatus.ADDED,
              EntryStatus.EXISTING,
              EntryStatus.MODIFIED,
              EntryStatus.DELETED,
              EntryStatus.REPLACED);

      assertThat(reader.liveFiles())
          .extracting(file -> file.tracking().status())
          .containsExactly(EntryStatus.ADDED, EntryStatus.EXISTING, EntryStatus.MODIFIED);
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
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);
    ((StructLike) file).set(SORT_ORDER_ID_ORDINAL, SORT_ORDER_ID);

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file));

    Schema projection = new Schema(TrackedFile.LOCATION);
    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS).project(projection).build()) {
      TrackedFile actual = Lists.newArrayList(reader.allFiles()).get(0);
      assertThat(actual.location()).isEqualTo(file.location());
      // a minimal status-only tracking is force-added when the caller omits tracking
      assertThat(actual.tracking()).isNotNull();
      assertThat(actual.tracking().status()).isEqualTo(EntryStatus.ADDED);
      // sort_order_id, file_format, and spec_id are null because they were not projected
      assertThat(actual.sortOrderId()).isNull();
      assertThat(actual.fileFormat()).isNull();
      assertThat(actual.specId()).isNull();
    }
  }

  @TestTemplate
  public void testUnpartitioned() throws IOException {
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file));

    TrackedFile actual = read(manifest, UNPARTITIONED_SPECS).get(0);
    assertThat(actual.partition()).isNotNull();
    assertThat(actual.partition().size()).isEqualTo(0);
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
      assertThat(reader.allFiles())
          .extracting(TrackedFile::location)
          .containsExactly(keep.location());
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
      assertThat(reader.allFiles()).isEmpty();
    }

    assertThat(metrics.skippedDeleteFiles().value()).isEqualTo(1L);
    assertThat(metrics.skippedDataFiles().value()).isEqualTo(0L);
  }

  @TestTemplate
  public void testPartitionFilterKeepsManifestReferences() throws IOException {
    TrackedFile keep = dataFile("data-1.parquet", partition(1));
    TrackedFile prune = dataFile("data-2.parquet", partition(2));
    ManifestInfo info = new ManifestInfoStruct(1, 0, 0, 0, 1L, 0L, 0L, 0L, 1L, null, null);
    TrackedFile manifestRef =
        new TrackedFileStruct(
            addedTracking(),
            FileContent.DATA_MANIFEST,
            FORMAT_VERSION_V4,
            "leaf.parquet",
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

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(keep, prune, manifestRef));

    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS).filterRows(Expressions.equal("id", 1)).build()) {
      assertThat(reader.allFiles())
          .extracting(TrackedFile::location)
          .containsExactlyInAnyOrder(keep.location(), manifestRef.location());
    }
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
      assertThat(reader.allFiles())
          .extracting(TrackedFile::location)
          .containsExactly(keep.location());
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
    Types.StructType unionType = Partitioning.partitionType(TABLE_SCHEMA, specsById.values());

    TrackedFile keepById = dataFile("spec0-id1.parquet", unionPartition(unionType, 1, null), 0);
    TrackedFile prunedById = dataFile("spec0-id2.parquet", unionPartition(unionType, 2, null), 0);
    TrackedFile keptOtherSpec =
        dataFile("spec1-data.parquet", unionPartition(unionType, null, "x"), 1);

    InputFile manifest =
        writeManifest(unionType, ImmutableList.of(keepById, prunedById, keptOtherSpec));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, specsById)
            .filterRows(Expressions.equal("id", 1))
            .build()) {
      // spec0 entries are pruned by id; the spec1 entry is not partitioned by id so it survives
      assertThat(reader.allFiles())
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
      assertThatThrownBy(reader::allFiles)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Unable to determine format of manifest");
    }
  }

  @TestTemplate
  public void testFileWithUnknownSpecThrows() throws IOException {
    // spec ID 5 is not in PARTITIONED_SPECS, so pruning cannot resolve a spec for this file
    TrackedFile file = dataFile("orphan.parquet", partition(1), 5);

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(file));

    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS).filterRows(Expressions.equal("id", 1)).build()) {
      assertThatThrownBy(() -> Lists.newArrayList(reader.allFiles()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("not one of the known specs");
    }
  }

  private static TrackedFile dataFile(String location, PartitionData partition) {
    return dataFile(location, partition, 0);
  }

  private static TrackedFile dataFile(String location, PartitionData partition, int specId) {
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
    // Parquet cannot write empty groups, so v4 writers omit the partition and content_stats fields
    // entirely when they would be empty (unpartitioned tables, no stats).
    List<Types.NestedField> writeFields = Lists.newArrayList();
    for (Types.NestedField field :
        TrackedFile.schemaWithContentStats(partitionType, Types.StructType.of()).fields()) {
      if (field.type().isStructType() && field.type().asStructType().fields().isEmpty()) {
        continue;
      }

      writeFields.add(field);
    }

    Schema writeSchema = new Schema(writeFields);
    OutputFile out =
        fileIO.newOutputFile(
            tempDir
                .resolve(
                    "manifest-" + System.nanoTime() + "." + format.name().toLowerCase(Locale.ROOT))
                .toString());
    try (FileAppender<StructLike> appender =
        InternalData.write(format, out).schema(writeSchema).named("tracked_file").build()) {
      for (TrackedFile file : files) {
        appender.add(toWriteRow(file, writeSchema));
      }
    }

    return fileIO.newInputFile(out.location());
  }

  /**
   * Adapts a fully-populated tracked file to a write schema that may omit fields (partition and
   * content_stats are omitted when empty).
   */
  private static StructLike toWriteRow(TrackedFile file, Schema writeSchema) {
    StructLike struct = (StructLike) file;
    int[] toBase = new int[writeSchema.columns().size()];
    for (int i = 0; i < writeSchema.columns().size(); i++) {
      toBase[i] = ordinalOf(writeSchema.columns().get(i).fieldId());
    }

    return new StructLike() {
      @Override
      public int size() {
        return toBase.length;
      }

      @Override
      public <T> T get(int pos, Class<T> javaClass) {
        return struct.get(toBase[pos], javaClass);
      }

      @Override
      public <T> void set(int pos, T value) {
        throw new UnsupportedOperationException("Cannot modify write row");
      }
    };
  }

  private V4ManifestReader.Builder newReader(
      InputFile manifest, Map<Integer, PartitionSpec> specsById) {
    return V4ManifestReader.builder(manifest, TABLE_SCHEMA, specsById);
  }

  private List<TrackedFile> read(InputFile manifest, Map<Integer, PartitionSpec> specsById)
      throws IOException {
    // allFiles() returns reused instances, so copy each entry before collecting.
    try (V4ManifestReader reader = newReader(manifest, specsById).build()) {
      return Lists.newArrayList(CloseableIterable.transform(reader.allFiles(), TrackedFile::copy));
    }
  }

  private static int ordinalOf(int fieldId) {
    for (int i = 0; i < SCHEMA_FIELDS.size(); i++) {
      if (SCHEMA_FIELDS.get(i).fieldId() == fieldId) {
        return i;
      }
    }

    throw new IllegalArgumentException("Field not found in TrackedFile schema: " + fieldId);
  }
}
