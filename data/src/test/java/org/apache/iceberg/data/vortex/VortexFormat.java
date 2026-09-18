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
package org.apache.iceberg.data.vortex;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileFormatTestSupport;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.vortex.VortexFiles;

/**
 * {@link FileFormatTestSupport} for Vortex.
 *
 * <p>This lives in the {@code iceberg-data} test sources rather than in {@code iceberg-vortex}
 * because the TCK classes that consume it ship in the {@code iceberg-data} test artifact. It is
 * only ever instantiated when {@code iceberg-vortex} is on the classpath -- see {@link
 * FileFormatTestSupport#all()}.
 */
public class VortexFormat implements FileFormatTestSupport {
  @Override
  public FileFormat format() {
    return FileFormat.VORTEX;
  }

  @Override
  public void writeRecordsWithoutFieldIds(
      OutputFile outputFile, Schema schema, List<Record> records) throws IOException {
    // Vortex files never carry Iceberg field ids, so the regular writer already produces the
    // id-less file this test wants: Vortex readers bind columns by name.
    DataWriter<Record> writer =
        FormatModelRegistry.<Record, Schema>dataWriteBuilder(
                FileFormat.VORTEX, Record.class, EncryptedFiles.plainAsEncryptedOutput(outputFile))
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .build();

    try (writer) {
      records.forEach(writer::write);
    }
  }

  @Override
  public Map<String, String> testPropertiesToSet() {
    throw new UnsupportedOperationException(
        "Vortex has no write properties: the writer accepts none today, though some are "
            + "planned. Tests that need one are gated on FEATURE_WRITER_PROPERTIES");
  }

  @Override
  public boolean checkTestProperties(InputFile inputFile) {
    throw new UnsupportedOperationException(
        "Vortex has no write properties: the writer accepts none today, though some are "
            + "planned. Tests that need one are gated on FEATURE_WRITER_PROPERTIES");
  }

  @Override
  public String metadataValue(InputFile inputFile, String key) throws IOException {
    byte[] value = VortexFiles.metadata(inputFile).get(key);
    return value == null ? null : new String(value, StandardCharsets.UTF_8);
  }

  @Override
  public String splitSizeProperty() {
    return TableProperties.WRITE_VORTEX_SPLIT_SIZE;
  }
}
