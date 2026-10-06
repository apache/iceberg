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
package org.apache.iceberg.vortex;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that byte-range splits are approximated to row ranges: contiguous byte splits of a file
 * must map to contiguous, non-overlapping row ranges that cover every row exactly once.
 */
public class TestVortexSplits {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final int ROWS = 10_000;

  @TempDir private Path temp;

  @Test
  public void testContiguousByteSplitsCoverAllRowsExactlyOnce() throws IOException {
    InputFile file = writeRows(ROWS);
    long fileLength = file.getLength();

    List<Integer> allIds = Lists.newArrayList();
    int nonEmptySplits = 0;
    int splits = 4;
    long splitSize = fileLength / splits;
    for (int i = 0; i < splits; i++) {
      long start = i * splitSize;
      // the last split extends to the end of the file
      long length = i == splits - 1 ? fileLength - start : splitSize;
      List<Integer> splitIds = readIds(file, start, length);
      if (!splitIds.isEmpty()) {
        nonEmptySplits += 1;
      }

      allIds.addAll(splitIds);
    }

    assertThat(allIds)
        .as("Contiguous byte splits must cover every row exactly once")
        .hasSize(ROWS)
        .doesNotHaveDuplicates();
    assertThat(nonEmptySplits).as("Uniform byte splits should map to rows").isEqualTo(4);
  }

  @Test
  public void testEmptyTailSplitReadsNoRows() throws IOException {
    InputFile file = writeRows(100);

    assertThat(readIds(file, file.getLength(), 0))
        .as("A zero-length split at the end of the file must read no rows")
        .isEmpty();
  }

  @Test
  public void testFullRangeSplitReadsAllRows() throws IOException {
    InputFile file = writeRows(100);

    assertThat(readIds(file, 0, file.getLength()))
        .as("A split covering the whole file must read every row")
        .hasSize(100)
        .doesNotHaveDuplicates();
  }

  @Test
  public void testRowPositionIsFileAbsoluteWithinSplit() throws IOException {
    int rows = 1000;
    InputFile file = writeRows(rows);
    long fileLength = file.getLength();
    long half = fileLength / 2;

    Schema projection =
        new Schema(SCHEMA.columns().get(0), org.apache.iceberg.MetadataColumns.ROW_POSITION);

    List<Long> positions = Lists.newArrayList();
    List<Integer> ids = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        formatModel()
            .readBuilder(file)
            .project(projection)
            .split(half, fileLength - half)
            .build()) {
      for (Record record : reader) {
        ids.add((Integer) record.getField("id"));
        positions.add(
            (Long) record.getField(org.apache.iceberg.MetadataColumns.ROW_POSITION.name()));
      }
    }

    assertThat(positions).isNotEmpty();
    assertThat(positions.get(0)).as("_pos must be file-absolute within a split").isGreaterThan(0L);
    for (int i = 0; i < positions.size(); i++) {
      // row i was written with id == position, so file-absolute _pos must equal the id
      assertThat(positions.get(i).intValue()).isEqualTo(ids.get(i));
    }
  }

  @Test
  public void testTinySplitOfSingleRowFile() throws IOException {
    InputFile file = writeRows(1);
    long fileLength = file.getLength();

    // Split the single-row file in two: the row must be read by exactly one of the two splits.
    long firstHalf = fileLength / 2;
    List<Integer> ids = Lists.newArrayList();
    ids.addAll(readIds(file, 0, firstHalf));
    ids.addAll(readIds(file, firstHalf, fileLength - firstHalf));

    assertThat(ids).as("The single row must be read exactly once").containsExactly(0);
  }

  @Test
  public void testPersistedSplitOffsets() throws IOException {
    OutputFile outputFile =
        Files.localOutput(temp.resolve("split-offsets-" + System.nanoTime() + ".vortex").toFile());
    FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(SCHEMA)
            .set(org.apache.iceberg.TableProperties.WRITE_VORTEX_SPLIT_SIZE, "2048")
            .content(FileContent.DATA)
            .build();

    try (FileAppender<Record> closing = appender) {
      for (int i = 0; i < ROWS; i++) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("id", i);
        record.setField("data", "val-" + i);
        closing.add(record);
      }
    }

    List<Long> offsets = appender.splitOffsets();
    assertThat(offsets)
        .as("Synthetic split offsets should be persisted at the configured granularity")
        .isNotNull()
        .hasSizeGreaterThan(1)
        .isSorted()
        .doesNotHaveDuplicates()
        .startsWith(0L)
        .allSatisfy(offset -> assertThat(offset).isLessThan(appender.length()));
    assertThat(offsets.get(1)).isEqualTo(2048L);
  }

  private InputFile writeRows(int count) throws IOException {
    OutputFile outputFile =
        Files.localOutput(temp.resolve("data-" + System.nanoTime() + ".vortex").toFile());
    List<Record> records = Lists.newArrayListWithCapacity(count);
    for (int i = 0; i < count; i++) {
      GenericRecord record = GenericRecord.create(SCHEMA);
      record.setField("id", i);
      record.setField("data", "val-" + i);
      records.add(record);
    }

    try (FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(SCHEMA)
            .content(FileContent.DATA)
            .build()) {
      appender.addAll(records);
    }

    return outputFile.toInputFile();
  }

  private List<Integer> readIds(InputFile file, long start, long length) throws IOException {
    List<Integer> ids = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        formatModel().readBuilder(file).project(SCHEMA).split(start, length).build()) {
      for (Record record : reader) {
        ids.add((Integer) record.getField("id"));
      }
    }

    return ids;
  }

  private static VortexFormatModel<Record, StructType, VortexRowReader<?>> formatModel() {
    return VortexFormatModel.create(
        Record.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> GenericVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
  }
}
