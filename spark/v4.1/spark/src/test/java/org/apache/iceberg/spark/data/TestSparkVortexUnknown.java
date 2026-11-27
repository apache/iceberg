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
package org.apache.iceberg.spark.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexFormatModel;
import org.apache.iceberg.vortex.VortexRowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unknown columns are never stored, so the Spark writer has fewer Arrow vectors than the schema has
 * columns and the reader has to fill the column back in. This pins both halves, including a struct
 * child, where the Arrow struct has no matching field either.
 */
public class TestSparkVortexUnknown {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "u", Types.UnknownType.get()),
          required(3, "data", Types.StringType.get()),
          optional(
              4,
              "nested",
              Types.StructType.of(
                  required(5, "kept", Types.IntegerType.get()),
                  optional(6, "dropped", Types.UnknownType.get()))));

  @TempDir private Path temp;

  @Test
  public void testUnknownColumnsRoundTripAsNull() throws IOException {
    InternalRow nested = new GenericInternalRow(new Object[] {7, null});
    InternalRow row =
        new GenericInternalRow(
            new Object[] {
              1L, null, org.apache.spark.unsafe.types.UTF8String.fromString("a"), nested
            });

    OutputFile outputFile = Files.localOutput(temp.resolve("unknown.vortex").toFile());
    try (FileAppender<InternalRow> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(SCHEMA)
            .content(FileContent.DATA)
            .build()) {
      appender.add(row);
    }

    List<InternalRow> rows;
    try (CloseableIterable<InternalRow> reader =
        formatModel().readBuilder(outputFile.toInputFile()).project(SCHEMA).build()) {
      rows = Lists.newArrayList(reader);
    }

    assertThat(rows).hasSize(1);
    InternalRow read = rows.get(0);
    assertThat(read.getLong(0)).isEqualTo(1L);
    assertThat(read.isNullAt(1)).as("unknown column reads back as null").isTrue();
    assertThat(read.getUTF8String(2).toString()).isEqualTo("a");

    InternalRow readNested = read.getStruct(3, 2);
    assertThat(readNested.getInt(0)).isEqualTo(7);
    assertThat(readNested.isNullAt(1)).as("unknown struct child reads back as null").isTrue();
  }

  private static VortexFormatModel<InternalRow, StructType, VortexRowReader<?>> formatModel() {
    return VortexFormatModel.create(
        InternalRow.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> SparkVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<InternalRow>) SparkVortexReader::new);
  }
}
