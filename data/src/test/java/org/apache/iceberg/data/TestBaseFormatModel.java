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
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestBase;
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
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public abstract class TestBaseFormatModel<T> {

  protected abstract Class<T> engineType();

  protected abstract Object engineSchema(Schema schema);

  protected abstract List<T> engineTestRecords();

  protected abstract void assertEqualsEngineToGeneric(
      Types.StructType struct, List<T> expected, List<Record> actual);

  protected abstract void assertEqualsGenericToEngine(
      Types.StructType struct, List<Record> expected, List<T> actual);

  protected List<Record> genericTestRecords() {
    return RandomGenericData.generate(TestBase.SCHEMA, 10, 1L);
  }

  protected List<Record> genericPositionDeletes(Schema schema) {
    return ImmutableList.of(
        GenericRecord.create(schema)
            .copy(DELETE_FILE_PATH.name(), "data-file-1.parquet", DELETE_FILE_POS.name(), 0L),
        GenericRecord.create(schema)
            .copy(DELETE_FILE_PATH.name(), "data-file-1.parquet", DELETE_FILE_POS.name(), 1L));
  }

  private static final FileFormat[] FILE_FORMATS =
      new FileFormat[] {FileFormat.AVRO, FileFormat.PARQUET, FileFormat.ORC};

  @TempDir protected Path temp;

  private InMemoryFileIO fileIO;
  private EncryptedOutputFile encryptedFile;

  @BeforeEach
  public void before() {
    this.fileIO = new InMemoryFileIO();
    this.encryptedFile =
        EncryptedFiles.encryptedOutput(
            fileIO.newOutputFile("test-file"), EncryptionKeyMetadata.EMPTY);
  }

  @AfterEach
  public void after() throws IOException {
    fileIO.deleteFile(encryptedFile.encryptingOutputFile());
    this.encryptedFile = null;
    if (fileIO != null) {
      fileIO.close();
    }
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testDataWriterEngineWriteGenericRead(FileFormat fileFormat) throws IOException {
    // Write with engine type T, read with Generic Record
    FileWriterBuilder<DataWriter<T>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, engineType(), encryptedFile);

    DataFile dataFile;
    DataWriter<T> writer =
        writerBuilder
            .schema(TestBase.SCHEMA)
            .engineSchema(engineSchema(TestBase.SCHEMA))
            .spec(PartitionSpec.unpartitioned())
            .build();
    try (writer) {
      for (T record : engineTestRecords()) {
        writer.write(record);
      }
    }

    dataFile = writer.toDataFile();

    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(engineTestRecords().size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(TestBase.SCHEMA)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEqualsEngineToGeneric(TestBase.SCHEMA.asStruct(), engineTestRecords(), readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testDataWriterGenericWriteEngineRead(FileFormat fileFormat) throws IOException {
    // Write with Generic Record, read with engine type T
    FileWriterBuilder<DataWriter<Record>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile);

    DataFile dataFile;
    DataWriter<Record> writer =
        writerBuilder.schema(TestBase.SCHEMA).spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      for (Record record : genericTestRecords()) {
        writer.write(record);
      }
    }

    dataFile = writer.toDataFile();

    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(genericTestRecords().size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(TestBase.SCHEMA)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEqualsGenericToEngine(TestBase.SCHEMA.asStruct(), genericTestRecords(), readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testEqualityDeleteWriterEngineWriteGenericRead(FileFormat fileFormat)
      throws IOException {
    // Write with engine type T, read with Generic Record

    FileWriterBuilder<EqualityDeleteWriter<T>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, engineType(), encryptedFile);

    DeleteFile deleteFile;
    EqualityDeleteWriter<T> writer =
        writerBuilder
            .schema(TestBase.SCHEMA)
            .engineSchema(engineSchema(TestBase.SCHEMA))
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(3)
            .build();
    try (writer) {
      for (T record : engineTestRecords()) {
        writer.write(record);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(engineTestRecords().size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(3);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(TestBase.SCHEMA)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEqualsEngineToGeneric(TestBase.SCHEMA.asStruct(), engineTestRecords(), readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testEqualityDeleteWriterGenericWriteEngineRead(FileFormat fileFormat)
      throws IOException {
    // Write with Generic Record, read with engine type T
    FileWriterBuilder<EqualityDeleteWriter<Record>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, Record.class, encryptedFile);

    DeleteFile deleteFile;
    EqualityDeleteWriter<Record> writer =
        writerBuilder
            .schema(TestBase.SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(3)
            .build();
    try (writer) {
      for (Record record : genericTestRecords()) {
        writer.write(record);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(genericTestRecords().size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(3);

    // Read back and verify
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<T> readRecords;
    try (CloseableIterable<T> reader =
        FormatModelRegistry.readBuilder(fileFormat, engineType(), inputFile)
            .project(TestBase.SCHEMA)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEqualsGenericToEngine(TestBase.SCHEMA.asStruct(), genericTestRecords(), readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testPositionDeleteWriterEngineWriteGenericRead(FileFormat fileFormat)
      throws IOException {
    // Write position deletes, read with Generic Record
    Schema positionDeleteSchema = new Schema(DELETE_FILE_PATH, DELETE_FILE_POS);

    FileWriterBuilder<PositionDeleteWriter<T>, ?> writerBuilder =
        FormatModelRegistry.positionDeleteWriteBuilder(fileFormat, encryptedFile);

    PositionDelete<T> delete1 = PositionDelete.create();
    delete1.set("data-file-1.parquet", 0L);

    PositionDelete<T> delete2 = PositionDelete.create();
    delete2.set("data-file-1.parquet", 1L);

    List<PositionDelete<T>> positionDeletes = ImmutableList.of(delete1, delete2);

    DeleteFile deleteFile;
    PositionDeleteWriter<T> writer = writerBuilder.spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      for (PositionDelete<T> delete : positionDeletes) {
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

    DataTestHelpers.assertEquals(
        positionDeleteSchema.asStruct(), genericPositionDeletes(positionDeleteSchema), readRecords);
  }
}
