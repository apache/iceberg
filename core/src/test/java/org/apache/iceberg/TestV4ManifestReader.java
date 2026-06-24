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
import java.io.UncheckedIOException;
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
  private static final int WRITER_FORMAT_VERSION_V4 = 4;

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

  @Parameter private FileFormat format;

  @Parameters(name = "format = {0}")
  protected static List<FileFormat> parameters() {
    return Arrays.asList(FileFormat.AVRO, FileFormat.PARQUET);
  }

  @TempDir private Path tempDir;

  private final FileIO fileIO = new TestTables.LocalFileIO();

  @TestTemplate
  public void testRoundTrip() {
    DeletionVector dv =
        DeletionVectorStruct.builder()
            .location("s3://bucket/dv.puffin")
            .offset(100L)
            .sizeInBytes(50L)
            .cardinality(5L)
            .build();

    TrackedFile file =
        dataFileBuilder("s3://bucket/data/file.parquet", partition(7))
            .sortOrderId(1)
            .deletionVector(dv)
            .keyMetadata(ByteBuffer.wrap(new byte[] {1, 2, 3}))
            .splitOffsets(ImmutableList.of(50L, 100L))
            .build();

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(file));

    List<TrackedFile> read = read(manifest, PARTITIONED_SPECS);
    assertThat(read).hasSize(1);
    TrackedFile actual = read.get(0);

    assertThat(actual.contentType()).isEqualTo(file.contentType());
    assertThat(actual.writerFormatVersion()).isEqualTo(file.writerFormatVersion());
    assertThat(actual.location()).isEqualTo(file.location());
    assertThat(actual.fileFormat()).isEqualTo(file.fileFormat());
    assertThat(actual.recordCount()).isEqualTo(file.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(file.fileSizeInBytes());
    assertThat(actual.specId()).isEqualTo(file.specId());
    assertThat(actual.sortOrderId()).isEqualTo(file.sortOrderId());
    assertThat(actual.keyMetadata()).isEqualTo(file.keyMetadata());
    assertThat(actual.splitOffsets()).isEqualTo(file.splitOffsets());
    assertThat(actual.partition().get(0, Integer.class))
        .isEqualTo(file.partition().get(0, Integer.class));

    assertThat(actual.tracking()).isNotNull();
    assertThat(actual.tracking().status()).isEqualTo(file.tracking().status());
    assertThat(actual.tracking().snapshotId()).isEqualTo(file.tracking().snapshotId());

    assertThat(actual.deletionVector()).isNotNull();
    assertThat(actual.deletionVector().location()).isEqualTo(file.deletionVector().location());
    assertThat(actual.deletionVector().offset()).isEqualTo(file.deletionVector().offset());
    assertThat(actual.deletionVector().sizeInBytes())
        .isEqualTo(file.deletionVector().sizeInBytes());
    assertThat(actual.deletionVector().cardinality())
        .isEqualTo(file.deletionVector().cardinality());
  }

  @TestTemplate
  public void testEqualityDeleteRoundTrip() {
    TrackedFile delete =
        TrackedFileBuilder.equalityDelete(SNAPSHOT_ID)
            .writerFormatVersion(WRITER_FORMAT_VERSION_V4)
            .location("s3://bucket/eq-delete.parquet")
            .fileFormat(FileFormat.PARQUET)
            .recordCount(10L)
            .fileSizeInBytes(128L)
            .partition(EMPTY_PARTITION_DATA)
            .specId(0)
            .equalityIds(ImmutableList.of(1, 2))
            .build();

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(delete));

    TrackedFile actual = read(manifest, UNPARTITIONED_SPECS).get(0);
    assertThat(actual.contentType()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(actual.equalityIds()).containsExactly(1, 2);
  }

  @TestTemplate
  public void testLiveFilesExcludesDeletedAndReplaced() {
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @TestTemplate
  public void testManifestLocationAndPosition() {
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
  public void testProjectionRestrictsFields() {
    // sort_order_id is written but not projected below, so it must not be read back
    TrackedFile file =
        dataFileBuilder("s3://bucket/file.parquet", EMPTY_PARTITION_DATA).sortOrderId(7).build();

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file));

    Schema projection = new Schema(TrackedFile.LOCATION);
    try (V4ManifestReader reader =
        newReader(manifest, UNPARTITIONED_SPECS).project(projection).build()) {
      TrackedFile actual = Lists.newArrayList(reader.allFiles()).get(0);
      assertThat(actual.location()).isEqualTo("s3://bucket/file.parquet");
      // tracking is always projected even when the caller omits it
      assertThat(actual.tracking()).isNotNull();
      assertThat(actual.tracking().status()).isEqualTo(EntryStatus.ADDED);
      assertThat(actual.tracking().manifestPos()).isEqualTo(0L);
      // sort_order_id was written but not projected, so it should not be read
      assertThat(actual.sortOrderId()).isNull();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @TestTemplate
  public void testUnpartitioned() {
    TrackedFile file = dataFile("s3://bucket/file.parquet", EMPTY_PARTITION_DATA);

    InputFile manifest = writeManifest(EMPTY_PARTITION, ImmutableList.of(file));

    TrackedFile actual = read(manifest, UNPARTITIONED_SPECS).get(0);
    assertThat(actual.partition()).isNotNull();
    assertThat(actual.partition().size()).isEqualTo(0);
  }

  @TestTemplate
  public void testPartitionFilterPrunesNonMatchingFiles() {
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
          .containsExactly("keep.parquet");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    assertThat(metrics.skippedDataFiles().value()).isEqualTo(1L);
  }

  @TestTemplate
  public void testPartitionFilterCountsSkippedDeleteFiles() {
    TrackedFile delete =
        TrackedFileBuilder.equalityDelete(SNAPSHOT_ID)
            .writerFormatVersion(WRITER_FORMAT_VERSION_V4)
            .location("delete.parquet")
            .fileFormat(FileFormat.PARQUET)
            .recordCount(100L)
            .fileSizeInBytes(1024L)
            .partition(partition(2))
            .specId(0)
            .equalityIds(ImmutableList.of(1))
            .build();

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(delete));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS)
            .filterRows(Expressions.equal("id", 1))
            .scanMetrics(metrics)
            .build()) {
      assertThat(reader.allFiles()).isEmpty();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    assertThat(metrics.skippedDeleteFiles().value()).isEqualTo(1L);
    assertThat(metrics.skippedDataFiles().value()).isEqualTo(0L);
  }

  @TestTemplate
  public void testPartitionFilterKeepsManifestReferences() {
    TrackedFile keep = dataFile("data-1.parquet", partition(1));
    TrackedFile prune = dataFile("data-2.parquet", partition(2));
    ManifestInfo info =
        ManifestInfoStruct.builder()
            .addedFilesCount(1)
            .existingFilesCount(0)
            .deletedFilesCount(0)
            .replacedFilesCount(0)
            .addedRowsCount(1L)
            .existingRowsCount(0L)
            .deletedRowsCount(0L)
            .replacedRowsCount(0L)
            .minSequenceNumber(1L)
            .build();
    TrackedFile manifestRef =
        TrackedFileBuilder.dataManifest(SNAPSHOT_ID)
            .writerFormatVersion(WRITER_FORMAT_VERSION_V4)
            .location("leaf.parquet")
            .fileFormat(FileFormat.PARQUET)
            .recordCount(1L)
            .fileSizeInBytes(100L)
            .partition(partition(2))
            .specId(0)
            .manifestInfo(info)
            .build();

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(keep, prune, manifestRef));

    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS).filterRows(Expressions.equal("id", 1)).build()) {
      assertThat(reader.allFiles())
          .extracting(TrackedFile::location)
          .containsExactlyInAnyOrder("data-1.parquet", "leaf.parquet");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @TestTemplate
  public void testCaseInsensitivePartitionFilter() {
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
          .containsExactly("keep.parquet");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @TestTemplate
  public void testMultiSpecPartitionPruning() {
    PartitionSpec spec0 =
        PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(0).identity("id").build();
    PartitionSpec spec1 =
        PartitionSpec.builderFor(TABLE_SCHEMA)
            .withSpecId(1)
            .add(2, 1001, "data", Transforms.identity())
            .build();
    Map<Integer, PartitionSpec> specsById = ImmutableMap.of(0, spec0, 1, spec1);
    Types.StructType unionType = Partitioning.partitionType(TABLE_SCHEMA, specsById.values());

    TrackedFile keepById =
        dataFileBuilder("spec0-id1.parquet", unionPartition(unionType, 1, null)).specId(0).build();
    TrackedFile prunedById =
        dataFileBuilder("spec0-id2.parquet", unionPartition(unionType, 2, null)).specId(0).build();
    TrackedFile keptOtherSpec =
        dataFileBuilder("spec1-data.parquet", unionPartition(unionType, null, "x"))
            .specId(1)
            .build();

    InputFile manifest =
        writeManifest(unionType, ImmutableList.of(keepById, prunedById, keptOtherSpec));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, specsById)
            .filterRows(Expressions.equal("id", 1))
            .build()) {
      // spec0 entries are pruned by id; the spec1 entry is not partitioned by id so it survives
      assertThat(reader.allFiles())
          .extracting(TrackedFile::location)
          .containsExactlyInAnyOrder("spec0-id1.parquet", "spec1-data.parquet");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @TestTemplate
  public void testIteratorReturnsLiveCopies() {
    List<TrackedFile> files =
        ImmutableList.of(
            dataFile("s3://bucket/added-1.parquet", EMPTY_PARTITION_DATA),
            dataFile("s3://bucket/added-2.parquet", EMPTY_PARTITION_DATA),
            fileWithStatus(EntryStatus.DELETED, "s3://bucket/deleted.parquet"));

    InputFile manifest = writeManifest(EMPTY_PARTITION, files);

    try (V4ManifestReader reader = newReader(manifest, UNPARTITIONED_SPECS).build()) {
      List<TrackedFile> read = Lists.newArrayList(reader);
      assertThat(read)
          .hasSize(2)
          .extracting(TrackedFile::location)
          .containsExactly("s3://bucket/added-1.parquet", "s3://bucket/added-2.parquet");
      // iterator() copies each entry, so the collected instances are independent of the reused
      // container (they would be the same object if iterator() did not copy)
      assertThat(read.get(0)).isNotSameAs(read.get(1));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
    TrackedFile file = dataFileBuilder("orphan.parquet", partition(1)).specId(5).build();

    InputFile manifest = writeManifest(PARTITION_TYPE, ImmutableList.of(file));

    try (V4ManifestReader reader =
        newReader(manifest, PARTITIONED_SPECS).filterRows(Expressions.equal("id", 1)).build()) {
      assertThatThrownBy(() -> Lists.newArrayList(reader.allFiles()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("not one of the known specs");
    }
  }

  private static TrackedFile dataFile(String location, PartitionData partition) {
    return dataFileBuilder(location, partition).build();
  }

  private static TrackedFileBuilder dataFileBuilder(String location, PartitionData partition) {
    return TrackedFileBuilder.data(SNAPSHOT_ID)
        .writerFormatVersion(WRITER_FORMAT_VERSION_V4)
        .location(location)
        .fileFormat(FileFormat.PARQUET)
        .recordCount(100L)
        .fileSizeInBytes(1024L)
        .partition(partition)
        .specId(0);
  }

  private static TrackedFile fileWithStatus(EntryStatus status, String location) {
    Tracking tracking = new TrackingStruct(status, SNAPSHOT_ID, 3L, 3L, null, null, null, null);
    return new TrackedFileStruct(
        tracking,
        FileContent.DATA,
        WRITER_FORMAT_VERSION_V4,
        location,
        FileFormat.PARQUET,
        EMPTY_PARTITION_DATA,
        100L,
        1024L,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
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

  private InputFile writeManifest(Types.StructType partitionType, Iterable<TrackedFile> files) {
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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

  private List<TrackedFile> read(InputFile manifest, Map<Integer, PartitionSpec> specsById) {
    // allFiles() returns reused instances, so copy each entry before collecting.
    try (V4ManifestReader reader = newReader(manifest, specsById).build()) {
      return Lists.newArrayList(CloseableIterable.transform(reader.allFiles(), TrackedFile::copy));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
