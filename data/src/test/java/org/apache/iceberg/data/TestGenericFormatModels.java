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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestGenericFormatModels {
  private static final List<Record> TEST_RECORDS =
      RandomGenericData.generate(TestBase.SCHEMA, 10, 1L);

  private static final FileFormat[] FILE_FORMATS =
      new FileFormat[] {FileFormat.AVRO, FileFormat.PARQUET, FileFormat.ORC};

  private static final Schema PRIMITIVE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "b", Types.BooleanType.get()),
          optional(3, "i", Types.IntegerType.get()),
          optional(4, "l", Types.LongType.get()),
          optional(5, "f", Types.FloatType.get()),
          optional(6, "d", Types.DoubleType.get()),
          optional(7, "date", Types.DateType.get()),
          optional(8, "ts_tz", Types.TimestampType.withZone()),
          optional(9, "ts", Types.TimestampType.withoutZone()),
          optional(10, "s", Types.StringType.get()),
          optional(11, "fixed", Types.FixedType.ofLength(7)),
          optional(12, "bytes", Types.BinaryType.get()),
          optional(13, "dec_9_0", Types.DecimalType.of(9, 0)),
          optional(14, "dec_11_2", Types.DecimalType.of(11, 2)),
          optional(15, "dec_38_10", Types.DecimalType.of(38, 10)),
          optional(16, "time", Types.TimeType.get()),
          optional(17, "uuid", Types.UUIDType.get()));

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

  private void runRoundTrip(FileFormat fileFormat, Schema schema) throws IOException {
    List<Record> records = RandomGenericData.generate(schema, 10, 0L);
    FileWriterBuilder<DataWriter<Record>, Schema> writerBuilder =
        FormatModelRegistry.dataWriteBuilder(fileFormat, Record.class, encryptedFile);

    DataWriter<Record> writer =
        writerBuilder.schema(schema).spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      for (Record record : records) {
        writer.write(record);
      }
    }

    // Verify the file content by reading it back
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(schema)
            .build()) {
      readRecords = Lists.newArrayList(reader);
    }

    DataTestHelpers.assertEquals(schema.asStruct(), records, readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testDataWriterRoundTrip(FileFormat fileFormat) throws IOException {
    runRoundTrip(fileFormat, TestBase.SCHEMA);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testPrimitiveTypesRoundTrip(FileFormat fileFormat) throws IOException {
    runRoundTrip(fileFormat, PRIMITIVE_SCHEMA);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testEqualityDeleteWriterRoundTrip(FileFormat fileFormat) throws IOException {
    FileWriterBuilder<EqualityDeleteWriter<Record>, Schema> writerBuilder =
        FormatModelRegistry.equalityDeleteWriteBuilder(fileFormat, Record.class, encryptedFile);

    DeleteFile deleteFile;
    EqualityDeleteWriter<Record> writer =
        writerBuilder
            .schema(TestBase.SCHEMA)
            .spec(PartitionSpec.unpartitioned())
            .equalityFieldIds(3)
            .build();
    try (writer) {
      for (Record record : TEST_RECORDS) {
        writer.write(record);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(TEST_RECORDS.size());
    assertThat(deleteFile.format()).isEqualTo(fileFormat);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(3);

    // Verify the file content by reading it back
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(TestBase.SCHEMA)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    DataTestHelpers.assertEquals(TestBase.SCHEMA.asStruct(), TEST_RECORDS, readRecords);
  }

  @ParameterizedTest
  @FieldSource("FILE_FORMATS")
  public void testPositionDeleteWriterRoundTrip(FileFormat fileFormat) throws IOException {
    Schema positionDeleteSchema = new Schema(DELETE_FILE_PATH, DELETE_FILE_POS);

    FileWriterBuilder<PositionDeleteWriter<Record>, ?> writerBuilder =
        FormatModelRegistry.positionDeleteWriteBuilder(fileFormat, encryptedFile);

    PositionDelete<Record> delete1 = PositionDelete.create();
    delete1.set("data-file-1.parquet", 0L);

    PositionDelete<Record> delete2 = PositionDelete.create();
    delete2.set("data-file-1.parquet", 1L);

    List<PositionDelete<Record>> positionDeletes = ImmutableList.of(delete1, delete2);

    DeleteFile deleteFile;
    PositionDeleteWriter<Record> writer = writerBuilder.spec(PartitionSpec.unpartitioned()).build();
    try (writer) {
      for (PositionDelete<Record> delete : positionDeletes) {
        writer.write(delete);
      }
    }

    deleteFile = writer.toDeleteFile();

    assertThat(deleteFile).isNotNull();
    assertThat(deleteFile.recordCount()).isEqualTo(2);
    assertThat(deleteFile.format()).isEqualTo(fileFormat);

    // Verify the file content by reading it back
    InputFile inputFile = encryptedFile.encryptingOutputFile().toInputFile();
    List<Record> readRecords;
    try (CloseableIterable<Record> reader =
        FormatModelRegistry.readBuilder(fileFormat, Record.class, inputFile)
            .project(positionDeleteSchema)
            .build()) {
      readRecords = ImmutableList.copyOf(reader);
    }

    List<Record> expected =
        ImmutableList.of(
            GenericRecord.create(positionDeleteSchema)
                .copy(DELETE_FILE_PATH.name(), "data-file-1.parquet", DELETE_FILE_POS.name(), 0L),
            GenericRecord.create(positionDeleteSchema)
                .copy(DELETE_FILE_PATH.name(), "data-file-1.parquet", DELETE_FILE_POS.name(), 1L));

    DataTestHelpers.assertEquals(positionDeleteSchema.asStruct(), expected, readRecords);
  }
}
