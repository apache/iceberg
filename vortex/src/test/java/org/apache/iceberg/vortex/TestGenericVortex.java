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

import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTestBase;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.StructType;

public class TestGenericVortex extends DataTestBase {
  @Override
  protected boolean supportsUnknown() {
    return false;
  }

  @Override
  protected boolean supportsVariant() {
    return false;
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, RandomGenericData.generate(schema, 100, 1376L));
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    writeAndValidate(
        writeSchema, expectedSchema, RandomGenericData.generate(writeSchema, 100, 1376L));
  }

  @Override
  protected void writeAndValidate(Schema schema, List<Record> expected) throws IOException {
    writeAndValidate(schema, schema, expected);
  }

  private void writeAndValidate(Schema writeSchema, Schema expectedSchema, List<Record> expected)
      throws IOException {
    assumeSupported(writeSchema);
    assumeSupported(expectedSchema);

    // Needed because the current writer doesn't really support OutputFile
    OutputFile outputFile =
        Files.localOutput(temp.resolve("test-" + System.nanoTime() + ".vortex").toFile());

    try (FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(writeSchema)
            .content(FileContent.DATA)
            .build()) {
      appender.addAll(expected);
    }

    List<Record> rows;
    try (CloseableIterable<Record> reader =
        formatModel().readBuilder(outputFile.toInputFile()).project(expectedSchema).build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(expectedSchema.asStruct(), expected.get(i), rows.get(i));
    }

    try (CloseableIterable<Record> reader =
        formatModel()
            .readBuilder(outputFile.toInputFile())
            .project(expectedSchema)
            .reuseContainers()
            .build()) {
      int index = 0;
      for (Record actualRecord : reader) {
        DataTestHelpers.assertEquals(expectedSchema.asStruct(), expected.get(index), actualRecord);
        index += 1;
      }
    }
  }

  private static void assumeSupported(Schema schema) {
    assumeThat(
            TypeUtil.find(
                schema,
                type ->
                    type.typeId() == Type.TypeID.LIST
                        || type.typeId() == Type.TypeID.MAP
                        || type.typeId() == Type.TypeID.FIXED))
        .as("Vortex does not yet support lists, maps, or variants")
        .isNull();
  }

  private static VortexFormatModel<Record, StructType, VortexRowReader<?>> formatModel() {
    return VortexFormatModel.create(
        Record.class,
        StructType.class,
        (icebergSchema, fileSchema, engineSchema) -> GenericVortexWriter.buildWriter(icebergSchema),
        (VortexFormatModel.ReaderFunction<Record>) GenericVortexReader::buildReader);
  }
}
