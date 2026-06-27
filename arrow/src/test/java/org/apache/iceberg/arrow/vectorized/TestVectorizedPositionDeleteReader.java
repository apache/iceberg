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
  }

  @Test
  void surfacesParquetReadFailures() throws IOException {
    // An unreadable file must not silently produce an empty index. The actual exception from
    // parquet-mr (a RuntimeException with "not a Parquet file" in the message) is not an
    // IOException, so it propagates unchanged through the reader's catch (IOException) block.
    // This is fine -- the parquet-side message is distinctive enough for diagnosis. We
    // additionally assert it's not an IllegalArgumentException so a regression in our own
    // precondition checks does not silently satisfy this test.
    File invalid = newDeleteFile("not-parquet.bin");
    java.nio.file.Files.write(invalid.toPath(), new byte[] {0, 1, 2, 3}, StandardOpenOption.CREATE);

    assertThatThrownBy(
            () -> VectorizedPositionDeleteReader.read(Files.localInput(invalid), FILE_A, null))
        .isInstanceOf(RuntimeException.class)
        .isNotInstanceOf(IllegalArgumentException.class)
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
    assertThat(grouped).as("one entry per data file path").hasSize(1);
    assertThat(grouped.get(FILE_A).cardinality()).as("grouped cardinality").isEqualTo(3L);
  }

  @Test
  void registeredReaderRejectsNullDataLocation() throws IOException {
    // The PositionDeleteIndexReader SPI requires a non-null data file path. The direct
    // VectorizedPositionDeleteReader#read API accepts null as "no filter / union all rows",
    // but that mode is intentionally not exposed through the registry-based dispatch path.
    Optional<PositionDeleteIndexReader> reader =
        FormatModelRegistry.positionDeleteIndexReader(FileFormat.PARQUET);
    assertThat(reader).as("Arrow reader registered").isPresent();

    File deleteFile = newDeleteFile("reject-null-data-location.parquet");
    writeDeletesForFile(deleteFile, FILE_A, ImmutableList.of(1L, 2L));

    assertThatThrownBy(() -> reader.get().read(Files.localInput(deleteFile), null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid data location");
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

    assertThat(indexes).as("one entry per data file path").hasSize(2);
    assertThat(indexes.get(FILE_A).cardinality()).as("FILE_A cardinality").isEqualTo(aCount);
    assertThat(indexes.get(FILE_B).cardinality()).as("FILE_B cardinality").isEqualTo(bCount);
    assertThat(indexes.get(FILE_A).isDeleted(0L)).as("FILE_A: first position").isTrue();
    assertThat(indexes.get(FILE_A).isDeleted(aCount - 1L)).as("FILE_A: last position").isTrue();
    assertThat(indexes.get(FILE_A).isDeleted((long) aCount))
        .as("FILE_A: one past last must not be deleted")
        .isFalse();
    assertThat(indexes.get(FILE_B).isDeleted(bCount - 1L)).as("FILE_B: last position").isTrue();
  }

  @Test
  void readAllByDataFileRejectsNullInputFile() {
    assertThatThrownBy(() -> VectorizedPositionDeleteReader.readAllByDataFile(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid input file");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void readAllByDataFileRejectsNonPositiveBatchSize(int batchSize) throws IOException {
    File deleteFile = newDeleteFile("readall-rejects-batch-size-" + batchSize + ".parquet");
    writeDeletesForFile(deleteFile, FILE_A, ImmutableList.of(0L));

    assertThatThrownBy(
            () ->
                VectorizedPositionDeleteReader.readAllByDataFile(
                    Files.localInput(deleteFile), null, batchSize))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid batch size");
  }

  @Test
  void readAllByDataFileSurfacesParquetReadFailures() throws IOException {
    File invalid = newDeleteFile("readall-not-parquet.bin");
    java.nio.file.Files.write(invalid.toPath(), new byte[] {0, 1, 2, 3}, StandardOpenOption.CREATE);

    assertThatThrownBy(
            () -> VectorizedPositionDeleteReader.readAllByDataFile(Files.localInput(invalid), null))
        .isInstanceOf(RuntimeException.class)
        .isNotInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not a Parquet file");
  }

  @Test
  void readAllByDataFileHandlesDictionaryEncodedPaths() throws IOException {
    // Many rows over two distinct paths force Parquet to encode file_path with RLE_DICTIONARY.
    // The grouped path uses endOfPathRun's length+first-byte fast filter against the dictionary-
    // decoded bytes; this test pins that grouping still produces the correct per-path indexes.
    File deleteFile = newDeleteFile("readall-dict-encoded.parquet");
    int rowsPerFile = 4_096;
    List<Long> aPositions = Lists.newArrayListWithExpectedSize(rowsPerFile);
    List<Long> bPositions = Lists.newArrayListWithExpectedSize(rowsPerFile);
    for (long i = 0; i < rowsPerFile; i++) {
      aPositions.add(i);
      bPositions.add(i + rowsPerFile);
    }
    writeDeletesForTwoFiles(deleteFile, aPositions, bPositions);

    CharSequenceMap<PositionDeleteIndex> indexes =
        VectorizedPositionDeleteReader.readAllByDataFile(Files.localInput(deleteFile), null);

    assertThat(indexes).as("one entry per data file path").hasSize(2);
    assertThat(indexes.get(FILE_A).cardinality())
        .as("FILE_A cardinality with dictionary-encoded path")
        .isEqualTo(rowsPerFile);
    assertThat(indexes.get(FILE_B).cardinality())
        .as("FILE_B cardinality with dictionary-encoded path")
        .isEqualTo(rowsPerFile);
    assertAllPositionsDeleted(indexes.get(FILE_A), aPositions, "FILE_A (dict-encoded grouped)");
    assertAllPositionsDeleted(indexes.get(FILE_B), bPositions, "FILE_B (dict-encoded grouped)");
  }

  @Test
  void readAllByDataFileHonorsExplicitBatchSize() throws IOException {
    // Pin appendGroupedByPath's batch-boundary handling: cross-batch coalescing within each path
    // and a path transition that lands inside an Arrow batch must produce the same per-path
    // indexes regardless of batch size. At SMALL_BATCH_SIZE the FILE_A -> FILE_B transition
    // falls inside a batch (1_000 is not a multiple of 64), exercising endOfPathRun across the
    // boundary.
    File deleteFile = newDeleteFile("readall-small-batches.parquet");
    int aCount = 1_000;
    int bCount = 500;
    List<Long> aPositions = Lists.newArrayListWithExpectedSize(aCount);
    List<Long> bPositions = Lists.newArrayListWithExpectedSize(bCount);
    for (long i = 0; i < aCount; i++) {
      aPositions.add(i);
    }
    for (long i = 0; i < bCount; i++) {
      bPositions.add(i);
    }
    writeDeletesForTwoFiles(deleteFile, aPositions, bPositions);

    CharSequenceMap<PositionDeleteIndex> small =
        VectorizedPositionDeleteReader.readAllByDataFile(
            Files.localInput(deleteFile), null, SMALL_BATCH_SIZE);
    CharSequenceMap<PositionDeleteIndex> big =
        VectorizedPositionDeleteReader.readAllByDataFile(
            Files.localInput(deleteFile), null, VectorizedPositionDeleteReader.DEFAULT_BATCH_SIZE);

    assertThat(small).as("small-batch: one entry per data file path").hasSize(2);
    assertThat(big).as("default-batch: one entry per data file path").hasSize(2);
    assertThat(small.get(FILE_A).cardinality())
        .as("FILE_A cardinality should not depend on batch size")
        .isEqualTo(big.get(FILE_A).cardinality())
        .isEqualTo(aCount);
    assertThat(small.get(FILE_B).cardinality())
        .as("FILE_B cardinality should not depend on batch size")
        .isEqualTo(big.get(FILE_B).cardinality())
        .isEqualTo(bCount);
    assertAllPositionsDeleted(small.get(FILE_A), aPositions, "FILE_A (small batches)");
    assertAllPositionsDeleted(small.get(FILE_B), bPositions, "FILE_B (small batches)");
  }

  @Test
  void readAllByDataFileHandlesEmptyFilePath() throws IOException {
    File deleteFile = newDeleteFile("empty-path.parquet");
    List<Long> emptyPathPositions = ImmutableList.of(0L, 1L, 2L, 5L);
    List<Long> realPathPositions = ImmutableList.of(10L, 11L, 12L);
    writeDeletesForTwoFiles(deleteFile, "", emptyPathPositions, FILE_A, realPathPositions);

    CharSequenceMap<PositionDeleteIndex> indexes =
        VectorizedPositionDeleteReader.readAllByDataFile(Files.localInput(deleteFile), null);

    assertThat(indexes).as("one entry per data file path").hasSize(2);
    assertThat(indexes.get(""))
        .as("empty-path index must be populated")
        .isNotNull()
        .extracting(PositionDeleteIndex::cardinality)
        .isEqualTo((long) emptyPathPositions.size());
    assertAllPositionsDeleted(indexes.get(""), emptyPathPositions, "empty path");
    assertAllPositionsDeleted(indexes.get(FILE_A), realPathPositions, "FILE_A after empty path");
  }

  @Test
  void readFiltersByEmptyFilePath() throws IOException {
    File deleteFile = newDeleteFile("empty-path-filtered.parquet");
    List<Long> emptyPathPositions = ImmutableList.of(0L, 1L, 2L, 5L);
    List<Long> realPathPositions = ImmutableList.of(10L, 11L, 12L);
    writeDeletesForTwoFiles(deleteFile, "", emptyPathPositions, FILE_A, realPathPositions);

    PositionDeleteIndex emptyIndex =
        VectorizedPositionDeleteReader.read(Files.localInput(deleteFile), "", null);
    assertAllPositionsDeleted(emptyIndex, emptyPathPositions, "empty-path filter");
    assertNoPositionsDeleted(
        emptyIndex, realPathPositions, "FILE_A positions in empty-path-filtered index");
    assertThat(emptyIndex.cardinality())
        .as("empty-path filter cardinality")
        .isEqualTo(emptyPathPositions.size());
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
    writeDeletesForTwoFiles(file, FILE_A, aPositions, FILE_B, bPositions);
  }

  private static void writeDeletesForTwoFiles(
      File file,
      String firstPath,
      List<Long> firstPositions,
      String secondPath,
      List<Long> secondPositions)
      throws IOException {
    PositionDelete<Void> pd = PositionDelete.create();
    try (PositionDeleteWriter<Void> writer = openPositionDeleteWriter(file)) {
      for (long p : firstPositions) {
        pd.set(firstPath, p, null);
        writer.write(pd);
      }

      for (long p : secondPositions) {
        pd.set(secondPath, p, null);
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
