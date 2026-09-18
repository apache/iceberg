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

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.TestReadProjection;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.data.vortex.GenericVortexWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.StructType;

public class TestGenericReadProjection extends TestReadProjection {
  @Override
  protected Record writeAndRead(String desc, Schema writeSchema, Schema readSchema, Record record)
      throws IOException {
    assumeSupported(writeSchema);
    assumeSupported(readSchema);

    File file = new File(tempDir, "junit-" + desc + "-" + System.nanoTime() + ".vortex");
    OutputFile outputFile = Files.localOutput(file);

    try (FileAppender<Record> appender =
        formatModel()
            .writeBuilder(EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(writeSchema)
            .content(FileContent.DATA)
            .build()) {
      appender.add(record);
    }

    try (CloseableIterable<Record> records =
        formatModel().readBuilder(outputFile.toInputFile()).project(readSchema).build()) {
      return Iterables.getOnlyElement(records);
    }
  }

  private static void assumeSupported(Schema schema) {
    // Vortex has no fixed-width binary type, so Iceberg FIXED cannot be written at all.
    assumeThat(TypeUtil.find(schema, type -> type.typeId() == Type.TypeID.FIXED))
        .as("Vortex has no fixed-width binary type")
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
