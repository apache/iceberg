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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetEncryption {

  private static final String COLUMN_NAME = "intCol";
  private static final int RECORD_COUNT = 100;
  private static final ByteBuffer FILE_DEK = ByteBuffer.allocate(16);
  private static final ByteBuffer AAD_PREFIX = ByteBuffer.allocate(16);
  private static final Schema SCHEMA = new Schema(optional(1, COLUMN_NAME, IntegerType.get()));
  private static File file;

  @TempDir private Path temp;

  @BeforeEach
  public void writeEncryptedFile() throws IOException {
    List<GenericData.Record> records = Lists.newArrayListWithCapacity(RECORD_COUNT);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(SCHEMA.asStruct());
    for (int i = 1; i <= RECORD_COUNT; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put(COLUMN_NAME, i);
      records.add(record);
    }

    SecureRandom rand = new SecureRandom();
    rand.nextBytes(FILE_DEK.array());
    rand.nextBytes(AAD_PREFIX.array());

    file = createTempFile(temp);

    FileAppender<GenericData.Record> writer =
        Parquet.write(localOutput(file))
            .schema(SCHEMA)
            .withFileEncryptionKey(FILE_DEK)
            .withAADPrefix(AAD_PREFIX)
            .build();

    try (Closeable toClose = writer) {
      writer.addAll(Lists.newArrayList(records.toArray(new GenericData.Record[] {})));
    }
  }

  @Test
  public void testReadEncryptedFileWithoutKeys() throws IOException {
    assertThatThrownBy(
            () -> Parquet.read(localInput(file)).project(SCHEMA).callInit().build().iterator())
        .as("Decrypted without keys")
        .isInstanceOf(ParquetCryptoRuntimeException.class)
        .hasMessage("Trying to read file with encrypted footer. No keys available");
  }

  @Test
  public void testReadEncryptedFileWithoutAADPrefix() throws IOException {
    assertThatThrownBy(
            () ->
                Parquet.read(localInput(file))
                    .project(SCHEMA)
                    .withFileEncryptionKey(FILE_DEK)
                    .callInit()
                    .build()
                    .iterator())
        .as("Decrypted without AAD prefix")
        .isInstanceOf(ParquetCryptoRuntimeException.class)
        .hasMessage(
            "AAD prefix used for file encryption, "
                + "but not stored in file and not supplied in decryption properties");
  }

  @Test
  public void testReadEncryptedFile() throws IOException {
    try (CloseableIterator readRecords =
        Parquet.read(localInput(file))
            .withFileEncryptionKey(FILE_DEK)
            .withAADPrefix(AAD_PREFIX)
            .project(SCHEMA)
            .callInit()
            .build()
            .iterator()) {
      for (int i = 1; i <= RECORD_COUNT; i++) {
        GenericData.Record readRecord = (GenericData.Record) readRecords.next();
        assertThat(readRecord.get(COLUMN_NAME)).isEqualTo(i);
      }
    }
  }
}
