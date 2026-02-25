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

public abstract class TestBaseFormatModel<W, R> {

  protected abstract Class<W> writeType();

  protected abstract Class<R> readType();

  protected abstract Object writeEngineSchema(Schema schema);

  protected abstract Object readEngineSchema(Schema schema);

  protected abstract List<W> testRecords();

  protected abstract void assertEquals(Types.StructType struct, List<W> expected, List<R> actual);

  protected abstract List<W> expectedPositionDeletes(Schema schema);

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
  public void testDataWriterRoundTrip(FileFormat fileFormat) throws IOException {
    FileWriterBuilder<DataWriter<W>, Object> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, writeType(), encryptedFile);

    DataFile dataFile;
    DataWriter<W> writer =
        writerBuilder
            .schema(TestBase.SCHEMA)
            .engineSchema(writeEngineSchema(TestBase.SCHEMA))
            .spec(PartitionSpec.unpartitioned())
            .build();
    try (writer) {
      for (W record : testRecords()) {
        writer.write(record);
      }
    }

    dataFile = writer.toDataFile();

    assertThat(dataFile).isNotNull();
    assertThat(dataFile.recordCount()).isEqualTo(testRecords().size());
    assertThat(dataFile.format()).isEqualTo(fileFormat);

    // Verify the file content by reading it back
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<R> readRecords;
    try (CloseableIterable<R> reader =
        FormatModelRegistry.readBuilder(fileFormat, readType(), inputFile)
            .project(TestBase.SCHEMA)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(TestBase.SCHEMA.asStruct(), testRecords(), readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testEqualityDeleteWriterRoundTrip(FileFormat fileFormat) throws IOException {
    FileWriterBuilder<EqualityDeleteWriter<W>, Object> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, writeType(), encryptedFile);

    DeleteFile deleteFile;
    EqualityDeleteWriter<W> writer =
        writerBuilder
            .schema(TestBase.SCHEMA)
            .engineSchema(writeEngineSchema(TestBase.SCHEMA))
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(3)
            .build();
    try (writer) {
      for (W record : testRecords()) {
        writer.write(record);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(testRecords().size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(3);

    // Verify the file content by reading it back
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<R> readRecords;
    try (CloseableIterable<R> reader =
        FormatModelRegistry.readBuilder(fileFormat, readType(), inputFile)
            .project(TestBase.SCHEMA)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    assertEquals(TestBase.SCHEMA.asStruct(), testRecords(), readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testPositionDeleteWriterRoundTrip(FileFormat fileFormat) throws IOException {
    Schema positionDeleteSchema = new Schema(DELETE_FILE_PATH, DELETE_FILE_POS);

    FileWriterBuilder<PositionDeleteWriter<W>, ?> writerBuilder =
        FormatModelRegistry.positionDeleteWriteBuilder(fileFormat, encryptedFile);

    PositionDelete<W> delete1 = PositionDelete.create();
    delete1.set("data-file-1.parquet", 0L);

    PositionDelete<W> delete2 = PositionDelete.create();
    delete2.set("data-file-1.parquet", 1L);

    List<PositionDelete<W>> positionDeletes = ImmutableList.of(delete1, delete2);

    DeleteFile deleteFile;
    PositionDeleteWriter<W> writer = writerBuilder.spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      for (PositionDelete<W> delete : positionDeletes) {
        writer.write(delete);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(2);
    assertThat(deleteFile.format()).isEqualTo(fileFormat);

    // Verify the file content by reading it back
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<R> readRecords;
    try (CloseableIterable<R> reader =
        FormatModelRegistry.readBuilder(fileFormat, readType(), inputFile)
            .project(positionDeleteSchema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    List<W> expected = expectedPositionDeletes(positionDeleteSchema);

    assertEquals(positionDeleteSchema.asStruct(), expected, readRecords);
  }
}
