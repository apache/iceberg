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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.crypto.ColumnDecryptionProperties;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.TagVerificationException;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestNativeParquetEncryption {
  private static final MessageType SCHEMA =
      Types.buildMessage()
          .required(INT32)
          .named("id")
          .optional(BINARY)
          .named("secret")
          .named("test_schema");
  private static final int ROWS = 500;

  private final byte[] footerKey = randomKey();
  private final byte[] columnKey = randomKey();
  private final byte[] aadPrefix = "aad-prefix".getBytes(StandardCharsets.UTF_8);

  @TempDir private File tempDir;

  @Test
  void hadoopReadsNativeEncryptedFooter() throws IOException {
    File file = writeNative(footerOnly(false));

    assertThat(readWithHadoop(file, footerDecryption())).isEqualTo(ROWS);
  }

  @Test
  void hadoopReadsNativePlaintextFooter() throws IOException {
    File file = writeNative(footerOnly(true));

    assertThat(readWithHadoop(file, footerDecryption())).isEqualTo(ROWS);
  }

  @Test
  void hadoopReadsNativeColumnKeys() throws IOException {
    File file = writeNative(columnKeys());

    assertThat(readWithHadoop(file, columnKeyDecryption())).isEqualTo(ROWS);
  }

  @Test
  void nativeReadsHadoopEncryptedFile() throws IOException {
    File file = writeWithHadoop(columnKeys());

    assertThat(readRowGroupsNatively(file, columnKeyDecryption())).isEqualTo(ROWS);
  }

  @Test
  void nativeReadsNativeEncryptedFile() throws IOException {
    File file = writeNative(columnKeys());

    assertThat(readRowGroupsNatively(file, columnKeyDecryption())).isEqualTo(ROWS);
  }

  @Test
  void nativeReadsNativePlaintextFooter() throws IOException {
    File file = writeNative(footerOnly(true));

    assertThat(readRowGroupsNatively(file, footerDecryption())).isEqualTo(ROWS);
  }

  @Test
  void nativeReaderRequiresKeysForEncryptedFooter() throws IOException {
    File file = writeNative(footerOnly(false));

    assertThatThrownBy(() -> NativeParquetFileReader.open(Files.localInput(file)))
        .isInstanceOf(ParquetCryptoRuntimeException.class)
        .hasMessageContaining("No keys available");
  }

  @Test
  void nativeReaderRejectsWrongFooterKey() throws IOException {
    File file = writeNative(footerOnly(false));
    FileDecryptionProperties wrong =
        FileDecryptionProperties.builder()
            .withFooterKey(randomKey())
            .withAADPrefix(aadPrefix)
            .build();

    assertThatThrownBy(
            () ->
                NativeParquetFileReader.open(
                    Files.localInput(file),
                    org.apache.parquet.ParquetReadOptions.builder().withDecryption(wrong).build()))
        .isInstanceOf(TagVerificationException.class)
        .hasMessage("GCM tag check failed");
  }

  private int readRowGroupsNatively(File file, FileDecryptionProperties decryption)
      throws IOException {
    int rows = 0;
    try (NativeParquetFileReader reader =
        NativeParquetFileReader.open(
            Files.localInput(file),
            org.apache.parquet.ParquetReadOptions.builder().withDecryption(decryption).build())) {
      PageReadStore store;
      while ((store = reader.readNextRowGroup()) != null) {
        for (ColumnDescriptor column : SCHEMA.getColumns()) {
          assertThat(store.getPageReader(column).readPage()).isNotNull();
        }

        rows += (int) store.getRowCount();
      }
    }

    return rows;
  }

  private int readWithHadoop(File file, FileDecryptionProperties decryption) throws IOException {
    int rows = 0;
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toURI()))
            .withDecryption(decryption)
            .build()) {
      Group group;
      while ((group = reader.read()) != null) {
        assertThat(group.getInteger("id", 0)).isEqualTo(rows);
        if (rows % 2 == 0) {
          assertThat(group.getString("secret", 0)).isEqualTo("secret-" + rows);
        }

        rows++;
      }
    }

    return rows;
  }

  private File writeNative(FileEncryptionProperties encryption) throws IOException {
    File source = writeWithHadoop(null);
    File target = new File(tempDir, "native-" + System.nanoTime() + ".parquet");
    InternalFileEncryptor encryptor = new InternalFileEncryptor(encryption);

    try (NativeParquetFileWriter writer =
            NativeParquetFileWriter.create(
                org.apache.iceberg.Files.localOutput(target),
                SCHEMA,
                CompressionCodecName.SNAPPY,
                org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE,
                0,
                0,
                false,
                encryptor);
        NativeParquetFileReader reader = NativeParquetFileReader.open(Files.localInput(source))) {
      PageReadStore store = reader.readNextRowGroup();
      writer.startBlock(store.getRowCount());
      for (ColumnDescriptor column : SCHEMA.getColumns()) {
        org.apache.parquet.column.page.PageReader pages = store.getPageReader(column);
        writer.startColumn(column, pages.getTotalValueCount(), CompressionCodecName.UNCOMPRESSED);
        org.apache.parquet.column.page.DataPage page;
        while ((page = pages.readPage()) != null) {
          org.apache.parquet.column.page.DataPageV1 v1 =
              (org.apache.parquet.column.page.DataPageV1) page;
          writer.writeDataPage(
              v1.getValueCount(),
              v1.getUncompressedSize(),
              v1.getBytes(),
              v1.getStatistics(),
              v1.getValueCount(),
              v1.getRlEncoding(),
              v1.getDlEncoding(),
              v1.getValueEncoding());
        }

        writer.endColumn();
      }

      writer.endBlock();
      writer.end(Map.of());
    }

    return target;
  }

  private File writeWithHadoop(FileEncryptionProperties encryption) throws IOException {
    File file = new File(tempDir, "hadoop-" + System.nanoTime() + ".parquet");
    ExampleParquetWriter.Builder builder =
        ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(file.toURI()))
            .withType(SCHEMA)
            .withCompressionCodec(
                encryption == null
                    ? CompressionCodecName.UNCOMPRESSED
                    : CompressionCodecName.SNAPPY)
            .withDictionaryEncoding(false);
    if (encryption != null) {
      builder = builder.withEncryption(encryption);
    }

    try (ParquetWriter<Group> writer = builder.build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
      for (int i = 0; i < ROWS; i++) {
        Group group = factory.newGroup();
        group.append("id", i);
        if (i % 2 == 0) {
          group.append("secret", "secret-" + i);
        }

        writer.write(group);
      }
    }

    return file;
  }

  private FileEncryptionProperties footerOnly(boolean plaintextFooter) {
    FileEncryptionProperties.Builder builder =
        FileEncryptionProperties.builder(footerKey).withAADPrefix(aadPrefix);
    if (plaintextFooter) {
      builder.withPlaintextFooter();
    }

    return builder.build();
  }

  private FileEncryptionProperties columnKeys() {
    Map<ColumnPath, ColumnEncryptionProperties> columns =
        Map.of(
            ColumnPath.get("secret"),
            ColumnEncryptionProperties.builder("secret").withKey(columnKey).build());
    return FileEncryptionProperties.builder(footerKey)
        .withAADPrefix(aadPrefix)
        .withEncryptedColumns(columns)
        .build();
  }

  private FileDecryptionProperties footerDecryption() {
    return FileDecryptionProperties.builder()
        .withFooterKey(footerKey)
        .withAADPrefix(aadPrefix)
        .build();
  }

  private FileDecryptionProperties columnKeyDecryption() {
    return FileDecryptionProperties.builder()
        .withFooterKey(footerKey)
        .withAADPrefix(aadPrefix)
        .withColumnKeys(
            Map.of(
                ColumnPath.get("secret"),
                ColumnDecryptionProperties.builder("secret").withKey(columnKey).build()))
        .build();
  }

  private static byte[] randomKey() {
    byte[] key = new byte[16];
    new SecureRandom().nextBytes(key);
    return key;
  }
}
