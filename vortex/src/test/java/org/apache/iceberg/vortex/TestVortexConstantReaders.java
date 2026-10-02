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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises {@code idToConstant} injection in {@link GenericVortexReader}. */
public class TestVortexConstantReaders {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "category", Types.StringType.get()),
          required(3, "measurement", Types.DoubleType.get()));

  @TempDir private Path temp;

  @Test
  public void testIdentityPartitionConstantOverridesFileColumn() throws IOException {
    // The physical "category" value differs from the partition constant. When supplied through
    // idToConstant the reader must surface the constant, not whatever is stored in the file.
    List<Record> written =
        Lists.newArrayList(record(0L, "file-value", 1.5d), record(1L, "file-value", 2.5d));
    OutputFile outputFile = writeRecords(SCHEMA, written);

    Map<Integer, ?> idToConstant = ImmutableMap.of(2, "partition-value");

    List<Record> rows = read(outputFile, SCHEMA, idToConstant);

    assertThat(rows).hasSize(2);
    for (int i = 0; i < rows.size(); i++) {
      assertThat(rows.get(i).getField("id")).isEqualTo((long) i);
      assertThat(rows.get(i).getField("category")).isEqualTo("partition-value");
      assertThat(rows.get(i).getField("measurement"))
          .isEqualTo(written.get(i).getField("measurement"));
    }
  }

  @Test
  public void testMetadataColumnConstantsInjected() throws IOException {
    List<Record> written = Lists.newArrayList(record(0L, "a", 1.5d), record(1L, "b", 2.5d));
    OutputFile outputFile = writeRecords(SCHEMA, written);

    String filePath = "s3://bucket/path/data.vortex";
    int specId = 7;

    // Projection mixes file columns with metadata columns that are not stored in the file at all.
    Schema projection =
        new Schema(
            SCHEMA.findField("id"),
            SCHEMA.findField("measurement"),
            MetadataColumns.FILE_PATH,
            MetadataColumns.SPEC_ID);

    Map<Integer, ?> idToConstant =
        ImmutableMap.of(
            MetadataColumns.FILE_PATH.fieldId(), filePath,
            MetadataColumns.SPEC_ID.fieldId(), specId);

    List<Record> rows = read(outputFile, projection, idToConstant);

    assertThat(rows).hasSize(2);
    for (int i = 0; i < rows.size(); i++) {
      assertThat(rows.get(i).getField("id")).isEqualTo((long) i);
      assertThat(rows.get(i).getField("measurement"))
          .isEqualTo(written.get(i).getField("measurement"));
      assertThat(rows.get(i).getField(MetadataColumns.FILE_PATH.name())).isEqualTo(filePath);
      assertThat(rows.get(i).getField(MetadataColumns.SPEC_ID.name())).isEqualTo(specId);
    }
  }

  @Test
  public void testReorderedProjectionWithConstant() throws IOException {
    // A projection whose order differs from the file's physical column order, with a constant
    // interleaved, must still resolve each column by name.
    List<Record> written =
        Lists.newArrayList(record(0L, "file-value", 1.5d), record(1L, "file-value", 2.5d));
    OutputFile outputFile = writeRecords(SCHEMA, written);

    Schema projection =
        new Schema(
            SCHEMA.findField("measurement"), SCHEMA.findField("category"), SCHEMA.findField("id"));

    Map<Integer, ?> idToConstant = ImmutableMap.of(2, "partition-value");

    List<Record> rows = read(outputFile, projection, idToConstant);

    assertThat(rows).hasSize(2);
    for (int i = 0; i < rows.size(); i++) {
      assertThat(rows.get(i).getField("measurement"))
          .isEqualTo(written.get(i).getField("measurement"));
      assertThat(rows.get(i).getField("category")).isEqualTo("partition-value");
      assertThat(rows.get(i).getField("id")).isEqualTo((long) i);
    }
  }

  private static Record record(long id, String category, double measurement) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("category", category);
    record.setField("measurement", measurement);
    return record;
  }

  private OutputFile writeRecords(Schema schema, List<Record> records) throws IOException {
    OutputFile outputFile =
        Files.localOutput(temp.resolve("test-" + System.nanoTime() + ".vortex").toFile());
    try (FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(schema)
            .content(FileContent.DATA)
            .build()) {
      appender.addAll(records);
    }

    return outputFile;
  }

  private List<Record> read(OutputFile outputFile, Schema projection, Map<Integer, ?> idToConstant)
      throws IOException {
    try (CloseableIterable<Record> reader =
        formatModel()
            .readBuilder(outputFile.toInputFile())
            .project(projection)
            .idToConstant(idToConstant)
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  private static VortexFormatModel<Record, StructType, VortexRowReader<?>> formatModel() {
    return VortexFormatModel.create(
        Record.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> GenericVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
  }
}
