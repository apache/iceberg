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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.formats.FileWriterBuilder;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

/**
 * Tests for engines that support both reading and writing through the format model API.
 *
 * <p>Adds the engine write tests on top of the read tests in {@link ReadFormatModelTests}.
 *
 * @param <T> the engine's record type
 */
public abstract class BaseFormatModelTests<T> extends ReadFormatModelTests<T> {

  /** Write with engine type T, read with Generic Record */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testDataWriterEngineWriteGenericRead(FileFormat fileFormat, DataGenerator dataGenerator)
      throws IOException {
    Schema schema = supportedSchema(dataGenerator);
    List<Record> genericRecords = project(dataGenerator.generateRecords(), schema);
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);
    writeEngineRecords(fileFormat, schema, engineRecords, engineSchema(schema));
    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }

  /** Write with engine type T without explicit engineSchema, read with Generic Record */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testDataWriterEngineWriteWithoutEngineSchema(
      FileFormat fileFormat, DataGenerator dataGenerator) throws IOException {
    Schema schema = supportedSchema(dataGenerator);
    List<Record> genericRecords = project(dataGenerator.generateRecords(), schema);
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);
    writeEngineRecords(fileFormat, schema, engineRecords);
    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }

  /** Write with engine type T, read with engine type T */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testDataWriterEngineWriteEngineRead(FileFormat fileFormat, DataGenerator dataGenerator)
      throws IOException {
    Schema schema = supportedSchema(dataGenerator);
    List<Record> genericRecords = project(dataGenerator.generateRecords(), schema);
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);
    writeEngineRecords(fileFormat, schema, engineRecords);
    readAndAssertEngineRecords(fileFormat, schema, genericRecords, Function.identity());
  }

  /** Write with engine type T, read with Generic Record */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testEqualityDeleteWriterEngineWriteGenericRead(
      FileFormat fileFormat, DataGenerator dataGenerator) throws IOException {
    Schema schema = supportedSchema(dataGenerator);
    FileWriterBuilder<EqualityDeleteWriter<T>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, engineType(), encryptedFile);

    EqualityDeleteWriter<T> writer =
        writerBuilder
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .build();

    List<Record> genericRecords = project(dataGenerator.generateRecords(), schema);
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      engineRecords.forEach(writer::write);
    }

    DeleteFile deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(engineRecords.size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1);

    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }

  /**
   * Write equality deletes with engine type T without explicit engineSchema, read with Generic
   * Record
   */
  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testEqualityDeleteWriterEngineWriteWithoutEngineSchema(
      FileFormat fileFormat, DataGenerator dataGenerator) throws IOException {
    Schema schema = supportedSchema(dataGenerator);
    FileWriterBuilder<EqualityDeleteWriter<T>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, engineType(), encryptedFile);

    EqualityDeleteWriter<T> writer =
        writerBuilder
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .build();

    List<Record> genericRecords = project(dataGenerator.generateRecords(), schema);
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      engineRecords.forEach(writer::write);
    }

    DeleteFile deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(engineRecords.size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1);

    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterOverwrite(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    writeEngineRecords(fileFormat, schema, engineRecords);
    readAndAssertGenericRecords(fileFormat, schema, genericRecords);

    genericRecords = dataGenerator.generateRecords(20);
    writeEngineRecords(
        fileFormat, schema, convertToEngineRecords(genericRecords, schema), true /* overwrite */);
    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterNoOverwriteFailsIfFileExists(FileFormat fileFormat) throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    writeEngineRecords(fileFormat, schema, engineRecords);
    readAndAssertGenericRecords(fileFormat, schema, genericRecords);

    assertThatThrownBy(() -> writeEngineRecords(fileFormat, schema, engineRecords))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Already exists");
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterSet(FileFormat fileFormat) throws IOException {
    writeAndAssertDataWriterWithConfig(
        fileFormat,
        (writerBuilder, format) -> testPropertiesToSet(format).forEach(writerBuilder::set),
        format -> assertThat(checkTestProperties(format)).isTrue());
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterSetAll(FileFormat fileFormat) throws IOException {
    writeAndAssertDataWriterWithConfig(
        fileFormat,
        (writerBuilder, format) -> writerBuilder.setAll(testPropertiesToSet(format)),
        format -> assertThat(checkTestProperties(format)).isTrue());
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterMeta(FileFormat fileFormat) throws IOException {
    writeAndAssertDataWriterWithConfig(
        fileFormat,
        (writerBuilder, format) -> writerBuilder.meta("tck.meta.key", "tck-meta-value"),
        format ->
            assertThat(fileMetadataValue(format, "tck.meta.key")).isEqualTo("tck-meta-value"));
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterMetaMap(FileFormat fileFormat) throws IOException {
    writeAndAssertDataWriterWithConfig(
        fileFormat,
        (writerBuilder, format) ->
            writerBuilder.meta(
                Map.of("tck.meta.key", "tck-meta-value", "tck.meta.key2", "tck-meta-value2")),
        format -> {
          assertThat(fileMetadataValue(format, "tck.meta.key")).isEqualTo("tck-meta-value");
          assertThat(fileMetadataValue(format, "tck.meta.key2")).isEqualTo("tck-meta-value2");
        });
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterAesStreamEncryption(FileFormat fileFormat) throws IOException {
    assumeSupports(fileFormat, FEATURE_AES_STREAM_ENCRYPTION);

    EncryptionManager encryptionManager = EncryptionTestHelpers.createEncryptionManager();
    EncryptingFileIO encryptingFileIO = EncryptingFileIO.combine(fileIO, encryptionManager);
    EncryptedOutputFile encryptedOutputFile =
        encryptingFileIO.newEncryptingOutputFile("test-file-" + UUID.randomUUID());

    FileWriterBuilder<DataWriter<T>, ?> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, engineType(), encryptedOutputFile)
            .keyMetadata(encryptedOutputFile.keyMetadata());

    writeAndAssertEncryptedDataWriter(fileFormat, encryptingFileIO, writerBuilder);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testDataWriterNativeEncryption(FileFormat fileFormat) throws IOException {
    assumeSupports(fileFormat, FEATURE_NATIVE_ENCRYPTION);

    EncryptionManager encryptionManager = EncryptionTestHelpers.createEncryptionManager();
    EncryptingFileIO encryptingFileIO = EncryptingFileIO.combine(fileIO, encryptionManager);
    String location = "test-file-" + UUID.randomUUID();
    NativeEncryptionKeyMetadata keyMetadata =
        (NativeEncryptionKeyMetadata)
            encryptingFileIO.newEncryptingOutputFile(location).keyMetadata();

    // Use a plain encrypted output file so Parquet cannot auto-inject the native encryption key
    // and AAD prefix from the output file metadata. This ensures encryption is driven only by
    // withFileEncryptionKey and withAADPrefix below.
    EncryptedOutputFile encryptedOutputFile =
        EncryptedFiles.plainAsEncryptedOutput(fileIO.newOutputFile(location));

    // keyMetadata is mainly used for parsing during reading, so this call is required here.
    FileWriterBuilder<DataWriter<T>, ?> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, engineType(), encryptedOutputFile)
            .keyMetadata(keyMetadata)
            .withFileEncryptionKey(keyMetadata.encryptionKey().duplicate())
            .withAADPrefix(keyMetadata.aadPrefix().duplicate());

    writeAndAssertEncryptedDataWriter(fileFormat, encryptingFileIO, writerBuilder);
  }

  @SuppressWarnings("checkstyle:AssertThatThrownByWithMessageCheck")
  private void writeAndAssertEncryptedDataWriter(
      FileFormat fileFormat,
      EncryptingFileIO encryptingFileIO,
      FileWriterBuilder<DataWriter<T>, ?> writerBuilder)
      throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    DataWriter<T> writer = writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned()).build();

    try (writer) {
      engineRecords.forEach(writer::write);
    }

    DataFile dataFile = writer.toDataFile();
    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(engineRecords.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);
    assertThat(dataFile.keyMetadata()).isNotNull();

    assertThatThrownBy(
            () ->
                readAndAssertGenericRecords(
                    fileFormat, schema, genericRecords, fileIO.newInputFile(dataFile.location())))
        .isInstanceOf(RuntimeException.class);

    readAndAssertGenericRecords(
        fileFormat, schema, genericRecords, encryptingFileIO.newInputFile(dataFile));
  }

  private void readAndAssertGenericRecords(
      FileFormat fileFormat, Schema schema, List<Record> expected, InputFile inputFile)
      throws IOException {
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(schema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    DataTestHelpers.assertEquals(schema.asStruct(), expected, readRecords);
  }

  private DataFile writeEngineRecords(FileFormat fileFormat, Schema schema, List<T> records)
      throws IOException {
    return writeEngineRecords(fileFormat, schema, records, false /* overwrite */, null);
  }

  private DataFile writeEngineRecords(
      FileFormat fileFormat, Schema schema, List<T> records, Object engineSchema)
      throws IOException {
    return writeEngineRecords(fileFormat, schema, records, false /* overwrite */, engineSchema);
  }

  private DataFile writeEngineRecords(
      FileFormat fileFormat, Schema schema, List<T> records, boolean overwrite) throws IOException {
    return writeEngineRecords(fileFormat, schema, records, overwrite, null);
  }

  private DataFile writeEngineRecords(
      FileFormat fileFormat, Schema schema, List<T> records, boolean overwrite, Object engineSchema)
      throws IOException {
    FileWriterBuilder<DataWriter<T>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, engineType(), encryptedFile);

    writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned());

    if (engineSchema != null) {
      writerBuilder.engineSchema(engineSchema);
    }

    if (overwrite) {
      writerBuilder.overwrite();
    }

    DataWriter<T> writer = writerBuilder.build();

    try (writer) {
      records.forEach(writer::write);
    }

    DataFile dataFile = writer.toDataFile();
    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(records.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    return dataFile;
  }

  private static Map<String, String> testPropertiesToSet(FileFormat fileFormat) {
    return FileFormatTestSupport.forFormat(fileFormat).testPropertiesToSet();
  }

  private boolean checkTestProperties(FileFormat fileFormat) throws IOException {
    return FileFormatTestSupport.forFormat(fileFormat)
        .checkTestProperties(encryptedFile.encryptingOutputFile().toInputFile());
  }

  private String fileMetadataValue(FileFormat fileFormat, String key) throws IOException {
    return FileFormatTestSupport.forFormat(fileFormat)
        .metadataValue(encryptedFile.encryptingOutputFile().toInputFile(), key);
  }

  @FunctionalInterface
  private interface DataWriterEffectAssertion {
    void accept(FileFormat fileFormat) throws IOException;
  }

  private void writeAndAssertDataWriterWithConfig(
      FileFormat fileFormat,
      BiConsumer<FileWriterBuilder<DataWriter<T>, Object>, FileFormat> configureWriter,
      DataWriterEffectAssertion assertWriterEffect)
      throws IOException {
    DataGenerator dataGenerator = new DataGenerators.DefaultSchema();
    Schema schema = dataGenerator.schema();
    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    FileWriterBuilder<DataWriter<T>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, engineType(), encryptedFile);
    writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned());
    configureWriter.accept(writerBuilder, fileFormat);

    DataWriter<T> writer = writerBuilder.build();

    try (writer) {
      engineRecords.forEach(writer::write);
    }

    DataFile dataFile = writer.toDataFile();
    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(genericRecords.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);
    assertWriterEffect.accept(fileFormat);
    readAndAssertGenericRecords(fileFormat, schema, genericRecords);
  }
}
