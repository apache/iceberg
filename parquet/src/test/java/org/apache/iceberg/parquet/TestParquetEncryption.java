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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.encryption.NativeFileEncryptParameters;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.crypto.ColumnDecryptionProperties;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.apache.iceberg.types.Types.NestedField.optional;

// Modelled after TestParquet.java - writing with Iceberg Parquet API, reading with Parquet API
public class TestParquetEncryption {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testEncryption() throws IOException {
    String columnName = "intCol";
    Schema schema = new Schema(
            optional(1, columnName, IntegerType.get())
    );

    int minimumRowGroupRecordCount = 100;
    int desiredRecordCount = minimumRowGroupRecordCount - 1;

    List<GenericData.Record> records = new ArrayList<>(desiredRecordCount);
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    for (int i = 1; i <= desiredRecordCount; i++) {
      GenericData.Record record = new GenericData.Record(avroSchema);
      record.put(columnName, i);
      records.add(record);
    }

    Random rand = new Random();

    byte[] fileDek = new byte[16];
    rand.nextBytes(fileDek);
    byte[] columnDek = new byte[16];
    rand.nextBytes(columnDek);
    Map<String, ByteBuffer> columnKeys = new HashMap<>();
    columnKeys.put(columnName, ByteBuffer.wrap(columnDek));

    String aadPrefix = "abcd";

    NativeFileEncryptParameters encryptionParams =
            NativeFileEncryptParameters.create(ByteBuffer.wrap(fileDek))
            .columnKeys(columnKeys)
            .aadPrefix(ByteBuffer.wrap(aadPrefix.getBytes(StandardCharsets.UTF_8)))
            .build();

    File file = createTempFile(temp);

    FileAppender<GenericData.Record> writer = Parquet.write(localOutput(file))
            .schema(schema)
            .encryption(encryptionParams)
            .build();

    try (Closeable toClose = writer) {
      writer.addAll(Lists.newArrayList(records.toArray(new GenericData.Record[]{})));
    }

    ColumnDecryptionProperties columnDecryptionProp = ColumnDecryptionProperties.builder(columnName)
            .withKey(columnDek)
            .build();
    Map<ColumnPath, ColumnDecryptionProperties> columnDecryptionPropMap = new HashMap<>();
    columnDecryptionPropMap.put(columnDecryptionProp.getPath(), columnDecryptionProp);

    FileDecryptionProperties decryptionProperties = FileDecryptionProperties.builder()
            .withFooterKey(fileDek)
            .withColumnKeys(columnDecryptionPropMap)
            .withAADPrefix(aadPrefix.getBytes(StandardCharsets.UTF_8))
            .build();

    String exceptionText = "";
    try {
      ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(file)));
    } catch (ParquetCryptoRuntimeException e) {
      exceptionText = e.toString();
    }
    String expected = "org.apache.parquet.crypto.ParquetCryptoRuntimeException: " +
            "Trying to read file with encrypted footer. No keys available";
    Assert.assertEquals(expected, exceptionText);

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(file)),
            ParquetReadOptions.builder()
                    .withDecryption(decryptionProperties)
                    .build())) {
      MessageType parquetSchema = reader.getFileMetaData().getSchema();
      Assert.assertTrue(parquetSchema.containsPath(new String[]{columnName}));
    }
  }
}
