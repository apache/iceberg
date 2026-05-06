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
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class TestVectorizedPositionDeleteReader {

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
    assertNoPositionsDeleted(aIndex, bPositions, FILE_A);
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

  private File newDeleteFile(String name) throws IOException {
    File file = tempDir.resolve(name).toFile();
    if (file.exists()) {
      file.delete();
    }

    return file;
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
