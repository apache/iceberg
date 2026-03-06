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

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.formats.FileWriterBuilder;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;

public abstract class BaseFormatModelTests<T> {

  protected abstract Class<T> engineType();

  protected abstract Object engineSchema(Schema schema);

  protected abstract T convertToEngine(Record record, Schema schema);

  protected abstract void assertEquals(Schema schema, List<T> expected, List<T> actual);

  private static final FileFormat[] FILE_FORMATS =
      new FileFormat[] {FileFormat.AVRO, FileFormat.PARQUET, FileFormat.ORC};

  private static final List<Arguments> FORMAT_AND_GENERATOR =
      Arrays.stream(FILE_FORMATS)
          .flatMap(
              format ->
                  Arrays.stream(DataGenerators.ALL)
                      .map(generator -> Arguments.of(format, generator)))
          .toList();

  @TempDir protected Path temp;

  private InMemoryFileIO fileIO;
  private EncryptedOutputFile encryptedFile;

  @BeforeEach
  void before() {
    this.fileIO = new InMemoryFileIO();
    this.encryptedFile =
        EncryptedFiles.encryptedOutput(
            fileIO.newOutputFile("test-file"), EncryptionKeyMetadata.EMPTY);
  }

  @AfterEach
  void after() {
    fileIO.deleteFile(encryptedFile.encryptingOutputFile());
    this.encryptedFile = null;
    if (fileIO != null) {
      fileIO.close();
    }
  }

  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testDataWriterEngineWriteGenericRead(FileFormat fileFormat, DataGenerator dataGenerator)
      throws IOException {
    // Write with engine type T, read with Generic Record
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<DataWriter<T>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, engineType(), encryptedFile);

    DataFile dataFile;
    DataWriter<T> writer =
        writerBuilder
            .schema(schema)
            .engineSchema(engineSchema(schema))
            .spec(PartitionSpec.unpartitioned())
            .build();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      for (T record : engineRecords) {
        writer.write(record);
      }
    }

    dataFile = writer.toDataFile();

    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(engineRecords.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(schema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    DataTestHelpers.assertEquals(schema.asStruct(), genericRecords, readRecords);
  }

  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testDataWriterGenericWriteEngineRead(FileFormat fileFormat, DataGenerator dataGenerator)
      throws IOException {
    // Write with Generic Record, read with engine type T
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<DataWriter<Record>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile);

    DataFile dataFile;
    DataWriter<Record> writer =
        writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned()).build();

    List<Record> genericRecords = dataGenerator.generateRecords();

    try (writer) {
      for (Record record : genericRecords) {
        writer.write(record);
      }
    }

    dataFile = writer.toDataFile();

    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(genericRecords.size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(schema, convertToEngineRecords(genericRecords, schema), readRecords);
  }

  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testEqualityDeleteWriterEngineWriteGenericRead(
      FileFormat fileFormat, DataGenerator dataGenerator) throws IOException {
    // Write with engine type T, read with Generic Record
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<EqualityDeleteWriter<T>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, engineType(), encryptedFile);

    DeleteFile deleteFile;
    EqualityDeleteWriter<T> writer =
        writerBuilder
            .schema(schema)
            .engineSchema(engineSchema(schema))
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .build();

    List<Record> genericRecords = dataGenerator.generateRecords();
    List<T> engineRecords = convertToEngineRecords(genericRecords, schema);

    try (writer) {
      for (T record : engineRecords) {
        writer.write(record);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(engineRecords.size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(schema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    DataTestHelpers.assertEquals(schema.asStruct(), genericRecords, readRecords);
  }

  @ParameterizedTest
  @FieldSource("FORMAT_AND_GENERATOR")
  void testEqualityDeleteWriterGenericWriteEngineRead(
      FileFormat fileFormat, DataGenerator dataGenerator) throws IOException {
    // Write with Generic Record, read with engine type T
    Schema schema = dataGenerator.schema();
    FileWriterBuilder<EqualityDeleteWriter<Record>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, Record.class, encryptedFile);

    DeleteFile deleteFile;
    EqualityDeleteWriter<Record> writer =
        writerBuilder
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .build();

    List<Record> genericRecords = dataGenerator.generateRecords();

    try (writer) {
      for (Record record : genericRecords) {
        writer.write(record);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(genericRecords.size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(schema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(schema, convertToEngineRecords(genericRecords, schema), readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  void testPositionDeleteWriterEngineWriteGenericRead(FileFormat fileFormat) throws IOException {
    // Write position deletes, read with Generic Record
    Schema positionDeleteSchema = DeleteSchemaUtil.pathPosSchema();

    FileWriterBuilder<PositionDeleteWriter<T>, ?> writerBuilder =
        FormatModelRegistry.positionDeleteWriteBuilder(fileFormat, encryptedFile);

    List<PositionDelete<T>> deletes =
        ImmutableList.of(
            PositionDelete.<T>create().set("data-file-1.parquet", 0L),
            PositionDelete.<T>create().set("data-file-1.parquet", 1L));

    List<Record> records =
        deletes.stream()
            .map(
                d ->
                    GenericRecord.create(positionDeleteSchema)
                        .copy(DELETE_FILE_PATH.name(), d.path(), DELETE_FILE_POS.name(), d.pos()))
            .toList();

    DeleteFile deleteFile;
    PositionDeleteWriter<T> writer = writerBuilder.spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      for (PositionDelete<T> delete : deletes) {
        writer.write(delete);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(2);
    assertThat(deleteFile.format()).isEqualTo(fileFormat);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(positionDeleteSchema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    DataTestHelpers.assertEquals(positionDeleteSchema.asStruct(), records, readRecords);
  }

  private List<T> convertToEngineRecords(List<Record> records, Schema schema) {
    return records.stream().map(r -> convertToEngine(r, schema)).collect(Collectors.toList());
  }
}
