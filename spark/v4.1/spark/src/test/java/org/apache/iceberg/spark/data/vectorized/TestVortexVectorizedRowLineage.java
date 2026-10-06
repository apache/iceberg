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
package org.apache.iceberg.spark.data.vectorized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies row lineage on the vectorized Vortex read path: {@code _row_id} prefers stored values
 * and inherits {@code firstRowId + position} for nulls, and {@code _last_updated_sequence_number}
 * substitutes the file's sequence number for nulls.
 */
public class TestVortexVectorizedRowLineage {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  private static final long FIRST_ROW_ID = 100L;
  private static final long FILE_SEQ_NUMBER = 5L;
  private static final int ROWS = 1000;

  @TempDir private Path temp;

  @Test
  public void testRowLineageInheritedFromBase() throws IOException {
    InputFile inputFile = writeRecords(SCHEMA, false);

    Schema projection =
        new Schema(
            SCHEMA.columns().get(0),
            MetadataColumns.ROW_ID,
            MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);

    long position = 0;
    for (ColumnarBatch batch : readBatches(inputFile, projection)) {
      for (int row = 0; row < batch.numRows(); row++, position++) {
        assertThat(batch.column(1).getLong(row))
            .as("_row_id should inherit firstRowId + position")
            .isEqualTo(FIRST_ROW_ID + position);
        assertThat(batch.column(2).getLong(row))
            .as("_last_updated_sequence_number should inherit the file sequence number")
            .isEqualTo(FILE_SEQ_NUMBER);
      }
    }

    assertThat(position).isEqualTo(ROWS);
  }

  @Test
  public void testRowLineagePrefersStoredValues() throws IOException {
    InputFile inputFile = writeRecords(MetadataColumns.schemaWithRowLineage(SCHEMA), true);

    Schema projection =
        new Schema(
            SCHEMA.columns().get(0),
            MetadataColumns.ROW_ID,
            MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);

    long position = 0;
    for (ColumnarBatch batch : readBatches(inputFile, projection)) {
      for (int row = 0; row < batch.numRows(); row++, position++) {
        if (position % 2 == 0) {
          assertThat(batch.column(1).getLong(row))
              .as("stored _row_id values win")
              .isEqualTo(555L + position);
          assertThat(batch.column(2).getLong(row))
              .as("stored _last_updated_sequence_number values win")
              .isEqualTo(7L);
        } else {
          assertThat(batch.column(1).getLong(row))
              .as("null _row_id values inherit firstRowId + position")
              .isEqualTo(FIRST_ROW_ID + position);
          assertThat(batch.column(2).getLong(row))
              .as("null _last_updated_sequence_number values inherit the file sequence number")
              .isEqualTo(FILE_SEQ_NUMBER);
        }
      }
    }

    assertThat(position).isEqualTo(ROWS);
  }

  private InputFile writeRecords(Schema writeSchema, boolean withStoredLineage) throws IOException {
    EncryptedOutputFile outputFile =
        EncryptedFiles.encryptedOutput(
            Files.localOutput(temp.resolve("lineage-" + System.nanoTime() + ".vortex").toFile()),
            EncryptionKeyMetadata.EMPTY);

    List<Record> records = Lists.newArrayListWithCapacity(ROWS);
    for (int i = 0; i < ROWS; i++) {
      Record record = GenericRecord.create(writeSchema);
      record.setField("id", i);
      record.setField("data", "val-" + i);
      if (withStoredLineage) {
        if (i % 2 == 0) {
          record.setField(MetadataColumns.ROW_ID.name(), 555L + i);
          record.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), 7L);
        } else {
          record.setField(MetadataColumns.ROW_ID.name(), null);
          record.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), null);
        }
      }

      records.add(record);
    }

    DataWriter<Record> writer =
        FormatModelRegistry.dataWriteBuilder(FileFormat.VORTEX, Record.class, outputFile)
            .schema(writeSchema)
            .spec(PartitionSpec.unpartitioned())
            .build();
    try (DataWriter<Record> closing = writer) {
      records.forEach(closing::write);
    }

    return outputFile.encryptingOutputFile().toInputFile();
  }

  private CloseableIterable<ColumnarBatch> readBatches(InputFile inputFile, Schema projection) {
    Map<Integer, Object> idToConstant =
        ImmutableMap.of(
            MetadataColumns.ROW_ID.fieldId(),
            FIRST_ROW_ID,
            MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
            FILE_SEQ_NUMBER);

    return FormatModelRegistry.readBuilder(FileFormat.VORTEX, ColumnarBatch.class, inputFile)
        .project(projection)
        .idToConstant(idToConstant)
        .build();
  }
}
