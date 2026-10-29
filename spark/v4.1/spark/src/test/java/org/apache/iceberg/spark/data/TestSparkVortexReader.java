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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.vortex.VortexFormatModel;
import org.apache.iceberg.vortex.VortexRowReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

/**
 * Type and schema coverage for the Spark read path: each scenario is written as generic records and
 * read back through {@link SparkVortexReader}, so the Spark readers are exercised over the same
 * types, nested shapes and projections as the generic ones.
 */
public class TestSparkVortexReader extends AvroDataTestBase {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    writeAndValidate(writeSchema, expectedSchema, RandomGenericData.generate(writeSchema, 100, 0L));
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema, List<Record> expected)
      throws IOException {
    assumeSupported(writeSchema);
    assumeSupported(expectedSchema);

    OutputFile output =
        Files.localOutput(temp.resolve("spark-vortex-" + System.nanoTime() + ".vortex").toFile());
    try (FileAppender<Record> writer =
        genericModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(output))
            .schema(writeSchema)
            .content(FileContent.DATA)
            .build()) {
      writer.addAll(expected);
    }

    try (CloseableIterable<InternalRow> reader =
        sparkModel()
            .readBuilder(output.toInputFile())
            .project(expectedSchema)
            .idToConstant(ID_TO_CONSTANT)
            .build()) {
      Iterator<InternalRow> rows = reader.iterator();
      int pos = 0;
      for (Record record : expected) {
        assertThat(rows).as("Should have expected number of rows").hasNext();
        GenericsHelpers.assertEqualsUnsafe(
            expectedSchema.asStruct(), record, rows.next(), ID_TO_CONSTANT, pos);
        pos += 1;
      }

      assertThat(rows).as("Should not have extra rows").isExhausted();
    }
  }

  private static void assumeSupported(Schema schema) {
    // Vortex has no fixed-width binary type, so Iceberg FIXED cannot be written at all.
    assumeThat(TypeUtil.find(schema, type -> type.typeId() == Type.TypeID.FIXED))
        .as("Vortex has no fixed-width binary type")
        .isNull();
  }

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  @Override
  protected boolean supportsNestedTypes() {
    return true;
  }

  @Override
  protected boolean supportsRowLineage() {
    return true;
  }

  @Override
  protected boolean supportsVariant() {
    return true;
  }

  private static VortexFormatModel<
          Record, org.apache.iceberg.types.Types.StructType, VortexRowReader<?>>
      genericModel() {
    return VortexFormatModel.create(
        Record.class,
        org.apache.iceberg.types.Types.StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> GenericVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
  }

  private static VortexFormatModel<InternalRow, StructType, VortexRowReader<?>> sparkModel() {
    return VortexFormatModel.create(
        InternalRow.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> SparkVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<InternalRow>) SparkVortexReader::new);
  }
}
