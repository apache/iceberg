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
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
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

/** Verifies that position deletes are pushed into the Vortex scan and exclude deleted rows. */
public class TestVortexPositionDeletes {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private Path temp;

  @Test
  public void testPositionDeletesExcludeRows() throws IOException {
    InputFile file = writeRows(10);

    // Delete row positions 2, 5 and 7 (ids 2, 5, 7).
    PositionDeleteIndex deletes =
        Deletes.toPositionIndex(CloseableIterable.withNoopClose(Lists.newArrayList(2L, 5L, 7L)));

    assertThat(readIds(file, deletes))
        .as("Pushed-down position deletes should remove exactly the deleted rows")
        .containsExactlyInAnyOrder(0, 1, 3, 4, 6, 8, 9);
  }

  @Test
  public void testNoPositionDeletesReadsAllRows() throws IOException {
    InputFile file = writeRows(10);

    assertThat(readIds(file, null))
        .as("Without position deletes every row should be returned")
        .containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  public void testFirstAndLastPositionsDeleted() throws IOException {
    InputFile file = writeRows(5);

    PositionDeleteIndex deletes =
        Deletes.toPositionIndex(CloseableIterable.withNoopClose(Lists.newArrayList(0L, 4L)));

    assertThat(readIds(file, deletes)).containsExactlyInAnyOrder(1, 2, 3);
  }

  @Test
  public void testPositionDeletesAppliedAlongsideRowRange() throws IOException {
    // GenericReader sets a row range via split(); for the row-splittable Vortex format the planner
    // reports these in rows (file.recordCount()), so this mirrors a whole-file task [0, rowCount).
    // Positions in the bitmap are file-relative and must still hit the right rows.
    int rows = 10;
    InputFile file = writeRows(rows);

    PositionDeleteIndex deletes =
        Deletes.toPositionIndex(CloseableIterable.withNoopClose(Lists.newArrayList(2L, 5L, 7L)));

    List<Integer> ids = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        formatModel()
            .readBuilder(file)
            .project(SCHEMA)
            .split(0, rows)
            .positionDeletes(deletes)
            .build()) {
      for (Record record : reader) {
        ids.add((Integer) record.getField("id"));
      }
    }

    assertThat(ids).containsExactlyInAnyOrder(0, 1, 3, 4, 6, 8, 9);
  }

  @Test
  public void testRowPositionProjection() throws IOException {
    InputFile file = writeRows(5);

    Schema projection = new Schema(SCHEMA.columns().get(0), MetadataColumns.ROW_POSITION);

    List<Integer> ids = Lists.newArrayList();
    List<Long> positions = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        formatModel().readBuilder(file).project(projection).build()) {
      for (Record record : reader) {
        ids.add((Integer) record.getField("id"));
        positions.add((Long) record.getField(MetadataColumns.ROW_POSITION.name()));
      }
    }

    assertThat(ids).containsExactly(0, 1, 2, 3, 4);
    assertThat(positions)
        .as("_pos should resolve to file-relative row positions via row_idx")
        .containsExactly(0L, 1L, 2L, 3L, 4L);
  }

  @Test
  public void testRowPositionWithPositionDeletes() throws IOException {
    InputFile file = writeRows(5);

    // Delete positions 1 and 3; the surviving rows keep their original file positions.
    PositionDeleteIndex deletes =
        Deletes.toPositionIndex(CloseableIterable.withNoopClose(Lists.newArrayList(1L, 3L)));

    Schema projection = new Schema(SCHEMA.columns().get(0), MetadataColumns.ROW_POSITION);

    List<Long> positions = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        formatModel().readBuilder(file).project(projection).positionDeletes(deletes).build()) {
      for (Record record : reader) {
        positions.add((Long) record.getField(MetadataColumns.ROW_POSITION.name()));
      }
    }

    assertThat(positions)
        .as("row_idx reports absolute file positions even after deletes are excluded")
        .containsExactly(0L, 2L, 4L);
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

  private List<Integer> readIds(InputFile file, PositionDeleteIndex deletes) throws IOException {
    List<Integer> ids = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        formatModel().readBuilder(file).project(SCHEMA).positionDeletes(deletes).build()) {
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
