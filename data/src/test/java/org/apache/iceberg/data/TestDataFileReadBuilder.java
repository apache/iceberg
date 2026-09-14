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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.formats.DataFileReadBuilder;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDataFileReadBuilder {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @TempDir private Path temp;

  @Test
  void readsDataFileWithoutColumnFiles() throws IOException {
    List<Record> records = records();
    DataFile dataFile = write(records);
    Function<String, InputFile> inputFiles = Files::localInput;

    List<Record> read;
    try (CloseableIterable<Record> reader =
        DataFileReadBuilder.<Record, Object>read(dataFile, Record.class, inputFiles)
            .project(SCHEMA)
            .build()) {
      read = ImmutableList.copyOf(reader);
    }

    DataTestHelpers.assertEquals(SCHEMA.asStruct(), records, read);
  }

  @Test
  void appliesReadBuilderConfiguration() throws IOException {
    DataFile dataFile = write(records());
    Schema projection = SCHEMA.select("id");
    Function<String, InputFile> inputFiles = Files::localInput;

    try (CloseableIterable<Record> reader =
        DataFileReadBuilder.<Record, Object>read(dataFile, Record.class, inputFiles)
            .project(projection)
            .caseSensitive(false)
            .split(0, dataFile.fileSizeInBytes())
            .build()) {
      assertThat(reader).allSatisfy(record -> assertThat(record.size()).isEqualTo(1));
    }
  }

  private List<Record> records() {
    GenericRecord record = GenericRecord.create(SCHEMA);
    return ImmutableList.of(
        record.copy("id", 1, "data", "a"),
        record.copy("id", 2, "data", "b"),
        record.copy("id", 3, "data", "c"));
  }

  private DataFile write(List<Record> records) throws IOException {
    OutputFile out = Files.localOutput(new File(temp.toFile(), "data.avro"));
    DataWriter<Record> writer =
        FormatModelRegistry.<Record, Object>dataWriteBuilder(
                FileFormat.AVRO, Record.class, EncryptedFiles.plainAsEncryptedOutput(out))
            .schema(SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .build();

    try (writer) {
      records.forEach(writer::write);
    }

    return writer.toDataFile();
  }
}
