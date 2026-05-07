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
package org.apache.iceberg.arrow.vectorized;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.PositionDeleteIndexReader;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestVectorizedPositionDeleteReader {

  private static final String FILE_A = "s3://bucket/data/file-a.parquet";
  private static final String FILE_B = "s3://bucket/data/file-b.parquet";
  private static final int SMALL_BATCH_SIZE = 64;

  @TempDir private Path tempDir;

  @Test
  void filtersByDataFilePath() throws IOException {
    File deleteFile = newDeleteFile("two-files.parquet");
    List<Long> aPositions = Lists.newArrayList(1L, 2L, 3L, 5L, 7L, 100L, 101L, 102L, 103L);
    List<Long> bPositions = Lists.newArrayList(0L, 4L, 6L, 50L, 200L);
    writeDeletesForTwoFiles(deleteFile, aPositions, bPositions);

    PositionDeleteIndex aIndex =
        VectorizedPositionDeleteReader.read(Files.localInput(deleteFile), FILE_A, null);
    assertAllPositionsDeleted(aIndex, aPositions, FILE_A);
    assertNoPositionsDeleted(aIndex, bPositions, "FILE_B positions in FILE_A-filtered index");
    assertThat(aIndex.cardinality()).as("FILE_A index cardinality").isEqualTo(aPositions.size());

    PositionDeleteIndex bIndex =
        VectorizedPositionDeleteReader.read(Files.localInput(deleteFile), FILE_B, null);
    assertAllPositionsDeleted(bIndex, bPositions, FILE_B);
    assertThat(bIndex.cardinality()).as("FILE_B index cardinality").isEqualTo(bPositions.size());
  }

  @Test
  void coalescesContiguousRuns() throws IOException {
    File deleteFile = newDeleteFile("dense.parquet");
    int count = 50_000;
    List<Long> positions = Lists.newArrayListWithExpectedSize(count);
    for (long i = 0; i < count; i++) {
      positions.add(i);
    }

    writeDeletesForFile(deleteFile, FILE_A, positions);

    PositionDeleteIndex index =
        VectorizedPositionDeleteReader.read(Files.localInput(deleteFile), FILE_A, null);
    assertThat(index.cardinality()).as("dense index cardinality").isEqualTo(count);
    assertThat(index.isDeleted(0L)).as("first position deleted").isTrue();
    assertThat(index.isDeleted(count - 1L)).as("last position deleted").isTrue();
    assertThat(index.isDeleted((long) count)).as("position past last not deleted").isFalse();
  }

  @Test
  void readsAllPositionsWhenNoFilterIsRequested() throws IOException {
    File deleteFile = newDeleteFile("two-files-no-filter.parquet");
    List<Long> aPositions = ImmutableList.of(10L, 11L, 12L, 50L, 100L);
    List<Long> bPositions = ImmutableList.of(0L, 7L, 13L, 99L);
    writeDeletesForTwoFiles(deleteFile, aPositions, bPositions);

    PositionDeleteIndex index =
        VectorizedPositionDeleteReader.read(Files.localInput(deleteFile), null, null);

    assertThat(index.cardinality())
        .as("no-filter index includes positions from both data files")
        .isEqualTo(aPositions.size() + bPositions.size());
    assertAllPositionsDeleted(index, aPositions, "no-filter (FILE_A rows)");
    assertAllPositionsDeleted(index, bPositions, "no-filter (FILE_B rows)");
  }

  @Test
  void rejectsNullInputFile() {
    assertThatThrownBy(() -> VectorizedPositionDeleteReader.read(null, FILE_A, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid input file");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void rejectsNonPositiveBatchSize(int batchSize) throws IOException {
    File deleteFile = newDeleteFile("rejects-batch-size-" + batchSize + ".parquet");
    writeDeletesForFile(deleteFile, FILE_A, ImmutableList.of(0L));

    assertThatThrownBy(
            () ->
                VectorizedPositionDeleteReader.read(
                    Files.localInput(deleteFile), FILE_A, null, batchSize))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid batch size");
  }

  @Test
  void recordsDeleteFileProvenance() throws IOException {
    File deleteFile = newDeleteFile("with-provenance.parquet");
    writeDeletesForFile(deleteFile, FILE_A, ImmutableList.of(1L, 2L, 3L));
    DeleteFile metadata = Mockito.mock(DeleteFile.class);

    PositionDeleteIndex index =
        VectorizedPositionDeleteReader.read(Files.localInput(deleteFile), FILE_A, metadata);

    assertThat(index.deleteFiles())
        .as("returned index should record the source delete file")
        .containsExactly(metadata);
    assertThat(index.cardinality()).as("filtered cardinality").isEqualTo(3L);
  }

  @Test
  void filtersDictionaryEncodedFilePath() throws IOException {
    // Many rows over only two distinct file_path values force Parquet to encode file_path
    // with RLE_DICTIONARY. This test pins that the byte-for-byte filter still matches when the
    // path column was dictionary-encoded. aPositions and bPositions are disjoint so a FILE_B row
    // leaking into the FILE_A index would surface as a never-deleted position in aIndex.
    File deleteFile = newDeleteFile("dict-encoded.parquet");
    int rowsPerFile = 4_096;
    List<Long> aPositions = Lists.newArrayListWithExpectedSize(rowsPerFile);
    List<Long> bPositions = Lists.newArrayListWithExpectedSize(rowsPerFile);
    for (long i = 0; i < rowsPerFile; i++) {
      aPositions.add(i);
      bPositions.add(i + rowsPerFile);
    }
    writeDeletesForTwoFiles(deleteFile, aPositions, bPositions);

    PositionDeleteIndex aIndex =
        VectorizedPositionDeleteReader.read(Files.localInput(deleteFile), FILE_A, null);
    assertThat(aIndex.cardinality())
        .as("dictionary-encoded path filter should keep all FILE_A rows only")
        .isEqualTo(rowsPerFile);
    assertAllPositionsDeleted(aIndex, aPositions, "FILE_A (dictionary-encoded)");
    assertNoPositionsDeleted(
        aIndex, bPositions, "FILE_B positions in dictionary-encoded FILE_A index");
  }

  @Test
  void filteredReadWithUnknownPathReturnsEmptyIndex() throws IOException {
    File deleteFile = newDeleteFile("unknown-path.parquet");
    writeDeletesForTwoFiles(deleteFile, ImmutableList.of(1L, 2L, 3L), ImmutableList.of(10L, 20L));

    PositionDeleteIndex index =
        VectorizedPositionDeleteReader.read(
            Files.localInput(deleteFile), "s3://bucket/data/file-c.parquet", null);

    assertThat(index.isEmpty())
        .as("filter for unknown data file path should yield an empty index")
        .isTrue();
    assertThat(index.cardinality()).as("empty index cardinality").isEqualTo(0L);
  }

  @Test
  void surfacesIOFailuresWithFileLocation() throws IOException {
    // An unreadable file must not silently produce an empty index; verify a recognizable
    // Parquet-side message propagates to the caller.
    File invalid = newDeleteFile("not-parquet.bin");
    java.nio.file.Files.write(invalid.toPath(), new byte[] {0, 1, 2, 3}, StandardOpenOption.CREATE);

    assertThatThrownBy(
            () -> VectorizedPositionDeleteReader.read(Files.localInput(invalid), FILE_A, null))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("not a Parquet file");
  }

  @Test
  void arrowRegistersAsParquetPositionDeleteIndexReader() throws IOException {
    // ArrowFormatModels.register() should be invoked by FormatModelRegistry's static initializer
    // on classpath. The registered reader should round-trip a parquet delete file end-to-end.
    Optional<PositionDeleteIndexReader> reader =
        FormatModelRegistry.positionDeleteIndexReader(FileFormat.PARQUET);
    assertThat(reader)
        .as("iceberg-arrow on classpath should register a parquet position delete index reader")
        .isPresent();

    File deleteFile = newDeleteFile("registry-roundtrip.parquet");
    writeDeletesForFile(deleteFile, FILE_A, ImmutableList.of(1L, 2L, 3L));

    PositionDeleteIndex single = reader.get().read(Files.localInput(deleteFile), FILE_A, null);
    assertThat(single.cardinality()).as("filtered cardinality").isEqualTo(3L);

    CharSequenceMap<PositionDeleteIndex> grouped =
        reader.get().readAll(Files.localInput(deleteFile), null);
    assertThat(grouped).hasSize(1);
    assertThat(grouped.get(FILE_A).cardinality()).as("grouped cardinality").isEqualTo(3L);
  }

  @Test
  void readAllByDataFileSplitsRowsByPath() throws IOException {
    File deleteFile = newDeleteFile("two-files-grouped.parquet");
    List<Long> aPositions = Lists.newArrayList(1L, 2L, 3L, 5L, 7L, 100L, 101L, 102L, 103L);
    List<Long> bPositions = Lists.newArrayList(0L, 4L, 6L, 50L, 200L);
    writeDeletesForTwoFiles(deleteFile, aPositions, bPositions);

    CharSequenceMap<PositionDeleteIndex> indexes =
        VectorizedPositionDeleteReader.readAllByDataFile(Files.localInput(deleteFile), null);

    assertThat(indexes).as("one entry per data file path").hasSize(2);
    assertThat(indexes.get(FILE_A))
        .as("FILE_A index must be populated")
        .isNotNull()
        .extracting(PositionDeleteIndex::cardinality)
        .isEqualTo((long) aPositions.size());
    assertThat(indexes.get(FILE_B))
        .as("FILE_B index must be populated")
        .isNotNull()
        .extracting(PositionDeleteIndex::cardinality)
        .isEqualTo((long) bPositions.size());
    assertAllPositionsDeleted(indexes.get(FILE_A), aPositions, "FILE_A (grouped)");
    assertAllPositionsDeleted(indexes.get(FILE_B), bPositions, "FILE_B (grouped)");
  }

  @Test
  void readAllByDataFileCoalescesContiguousRunsAcrossBatches() throws IOException {
    File deleteFile = newDeleteFile("dense-grouped.parquet");
    int aCount = 50_000;
    int bCount = 25_000;
    List<Long> aPositions = Lists.newArrayListWithExpectedSize(aCount);
    List<Long> bPositions = Lists.newArrayListWithExpectedSize(bCount);
    for (long i = 0; i < aCount; i++) {
      aPositions.add(i);
    }
    for (long i = 0; i < bCount; i++) {
      bPositions.add(i);
    }
    writeDeletesForTwoFiles(deleteFile, aPositions, bPositions);

    CharSequenceMap<PositionDeleteIndex> indexes =
        VectorizedPositionDeleteReader.readAllByDataFile(Files.localInput(deleteFile), null);

    assertThat(indexes).hasSize(2);
    assertThat(indexes.get(FILE_A).cardinality()).as("FILE_A cardinality").isEqualTo(aCount);
    assertThat(indexes.get(FILE_B).cardinality()).as("FILE_B cardinality").isEqualTo(bCount);
    assertThat(indexes.get(FILE_A).isDeleted(0L)).isTrue();
    assertThat(indexes.get(FILE_A).isDeleted(aCount - 1L)).isTrue();
    assertThat(indexes.get(FILE_A).isDeleted((long) aCount)).isFalse();
    assertThat(indexes.get(FILE_B).isDeleted(bCount - 1L)).isTrue();
  }

  @Test
  void readAllByDataFileRejectsNullInputFile() {
    assertThatThrownBy(() -> VectorizedPositionDeleteReader.readAllByDataFile(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid input file");
  }

  @Test
  void honorsExplicitBatchSize() throws IOException {
    File deleteFile = newDeleteFile("small-batches.parquet");
    List<Long> positions = Lists.newArrayList();
    for (long i = 0; i < 1_000; i++) {
      positions.add(i * 2L);
    }

    writeDeletesForFile(deleteFile, FILE_A, positions);

    PositionDeleteIndex small =
        VectorizedPositionDeleteReader.read(
            Files.localInput(deleteFile), FILE_A, null, SMALL_BATCH_SIZE);
    PositionDeleteIndex big =
        VectorizedPositionDeleteReader.read(
            Files.localInput(deleteFile),
            FILE_A,
            null,
            VectorizedPositionDeleteReader.DEFAULT_BATCH_SIZE);
    assertThat(small.cardinality())
        .as("small-batch index cardinality")
        .isEqualTo(big.cardinality())
        .isEqualTo(positions.size());
    assertAllPositionsDeleted(small, positions, "small batches");
    assertAllPositionsDeleted(big, positions, "default batch size");
  }

  // ---------- helpers ----------

  private File newDeleteFile(String name) {
    return tempDir.resolve(name).toFile();
  }

  private static PositionDeleteWriter<Void> openPositionDeleteWriter(File file) throws IOException {
    OutputFile out = Files.localOutput(file);
    return Parquet.writeDeletes(out)
        .createWriterFunc(GenericParquetWriter::create)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .buildPositionWriter();
  }

  private static void writeDeletesForFile(File file, String dataLocation, List<Long> positions)
      throws IOException {
    PositionDelete<Void> pd = PositionDelete.create();
    try (PositionDeleteWriter<Void> writer = openPositionDeleteWriter(file)) {
      for (long p : positions) {
        pd.set(dataLocation, p, null);
        writer.write(pd);
      }
    }
  }

  /**
   * Writes a position delete file containing two contiguous blocks of rows: all {@link #FILE_A}
   * positions first, then all {@link #FILE_B} positions. Position delete files require rows in
   * {@code (file_path, pos)} order, so true row-by-row interleaving across the two paths is not
   * representable here.
   */
  private static void writeDeletesForTwoFiles(
      File file, List<Long> aPositions, List<Long> bPositions) throws IOException {
    PositionDelete<Void> pd = PositionDelete.create();
    try (PositionDeleteWriter<Void> writer = openPositionDeleteWriter(file)) {
      for (long p : aPositions) {
        pd.set(FILE_A, p, null);
        writer.write(pd);
      }

      for (long p : bPositions) {
        pd.set(FILE_B, p, null);
        writer.write(pd);
      }
    }
  }

  private static void assertAllPositionsDeleted(
      PositionDeleteIndex index, List<Long> positions, String context) {
    for (long p : positions) {
      assertThat(index.isDeleted(p)).as("%s: position %d should be deleted", context, p).isTrue();
    }
  }

  private static void assertNoPositionsDeleted(
      PositionDeleteIndex index, List<Long> positions, String context) {
    for (long p : positions) {
      assertThat(index.isDeleted(p))
          .as("%s: position %d should NOT be deleted", context, p)
          .isFalse();
    }
  }
}
