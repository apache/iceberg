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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.data.FileAccessFactoryRegistry.MULTI_THREADED;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.FileAccessFactoryRegistry;
import org.apache.iceberg.data.GenericObjectModels;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestMultiThreadedParquetDataWriter {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "binary", Types.BinaryType.get()));

  private List<Record> records;

  @TempDir private Path temp;

  @BeforeEach
  public void createRecords() {
    GenericRecord record = GenericRecord.create(SCHEMA);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(record.copy(ImmutableMap.of("id", 1L, "data", "a")));
    builder.add(record.copy(ImmutableMap.of("id", 2L, "data", "b")));
    builder.add(record.copy(ImmutableMap.of("id", 3L, "data", "c")));
    builder.add(record.copy(ImmutableMap.of("id", 4L, "data", "d")));
    builder.add(record.copy(ImmutableMap.of("id", 5L, "data", "e")));

    this.records = builder.build();
  }

  static File createTempFile(Path temp) throws IOException {
    File tmpFolder = temp.resolve("parquet").toFile();
    String filename = UUID.randomUUID().toString();
    return new File(tmpFolder, FileFormat.PARQUET.addExtension(filename));
  }

  @Test
  public void testDataWriter() throws IOException {
    File file1 = createTempFile(temp);
    File file2 = createTempFile(temp);
    EncryptedOutputFile outputFile1 =
        EncryptionUtil.plainAsEncryptedOutput(Files.localOutput(file1));
    EncryptedOutputFile outputFile2 =
        EncryptionUtil.plainAsEncryptedOutput(Files.localOutput(file2));

    FileAppender<Record> dataWriter =
        FileAccessFactoryRegistry.writeBuilder(
                FileFormat.PARQUET,
                GenericObjectModels.GENERIC_OBJECT_MODEL,
                new Pair[] {
                  Pair.of(outputFile1, new Integer[] {1, 3}),
                  Pair.of(outputFile2, new Integer[] {2})
                })
            .fileSchema(SCHEMA)
            .set(MULTI_THREADED, "true")
            .overwrite()
            .build();

    try {
      for (Record record : records) {
        dataWriter.add(record);
      }
    } finally {
      dataWriter.close();
    }

    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader =
        FileAccessFactoryRegistry.readBuilder(
                FileFormat.PARQUET,
                GenericObjectModels.GENERIC_OBJECT_MODEL,
                new Pair[] {
                  Pair.of(Files.localInput(file1), new Integer[] {1, 3}),
                  Pair.of(Files.localInput(file2), new Integer[] {2})
                })
            .project(SCHEMA)
            .set(MULTI_THREADED, "true")
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    writtenRecords = writtenRecords.stream().map(Record::copy).collect(Collectors.toList());
    assertThat(writtenRecords).as("Written records should match").isEqualTo(records);
  }
}
