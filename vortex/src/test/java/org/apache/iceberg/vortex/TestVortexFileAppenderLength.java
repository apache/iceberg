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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that the length of an open Vortex appender estimates the compressed file, because
 * rolling writers compare it with the target file size.
 */
public class TestVortexFileAppenderLength {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "category", Types.IntegerType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private static final int ROWS = 2_000_000;

  @TempDir private Path temp;

  @Test
  public void testOpenLengthEstimatesTheCompressedFile() throws IOException {
    OutputFile outputFile = Files.localOutput(temp.resolve("length.vortex").toFile());
    FileAppender<Record> appender = newAppender(outputFile);

    long uncompressedBytes = 0;
    long openLength;
    try (FileAppender<Record> closing = appender) {
      for (int i = 0; i < ROWS; i++) {
        Record record = record(i);
        closing.add(record);
        uncompressedBytes +=
            Long.BYTES + Integer.BYTES + ((String) record.getField("data")).length();
      }

      openLength = closing.length();
    }

    long fileLength = appender.length();
    assertThat(fileLength)
        .as("The data must compress, or the test cannot tell the estimates apart")
        .isLessThan(uncompressedBytes / 4);
    assertThat(openLength)
        .as("The open length must estimate the compressed file, not the uncompressed rows")
        .isBetween(fileLength * 4 / 5, fileLength * 6 / 5);
  }

  @Test
  public void testOpenLengthEstimatesAPartialFile() throws IOException {
    // Rolling writers check the length early in a file, when the writer compressed little or no
    // data. Close the file at a small fraction of the rows and compare.
    OutputFile outputFile = Files.localOutput(temp.resolve("partial.vortex").toFile());
    FileAppender<Record> appender = newAppender(outputFile);

    long openLength;
    try (FileAppender<Record> closing = appender) {
      for (int i = 0; i < ROWS / 8; i++) {
        closing.add(record(i));
      }

      openLength = closing.length();
    }

    long fileLength = appender.length();
    assertThat(openLength)
        .as("The open length must estimate the compressed file early in the write")
        .isBetween(fileLength / 2, fileLength * 2);
  }

  @Test
  public void testOpenLengthGrowsWithTheRows() throws IOException {
    OutputFile outputFile = Files.localOutput(temp.resolve("growth.vortex").toFile());
    try (FileAppender<Record> appender = newAppender(outputFile)) {
      appender.add(record(0));
      assertThat(appender.length())
          .as("Rolling writers must see a length as soon as rows are buffered")
          .isPositive();
    }
  }

  private static FileAppender<Record> newAppender(OutputFile outputFile) throws IOException {
    return formatModel()
        .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
        .schema(SCHEMA)
        .content(FileContent.DATA)
        .build();
  }

  private static Record record(int id) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", (long) id);
    record.setField("category", id % 16);
    record.setField("data", "value-" + (id % 1000));
    return record;
  }

  private static VortexFormatModel<Record, StructType, VortexRowReader<?>> formatModel() {
    return VortexFormatModel.create(
        Record.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> GenericVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
  }
}
