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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestGenericSortedPosDeleteWriter extends TestBase {
  private static final int FORMAT_V2 = 2;

  @Parameter(index = 1)
  private FileFormat format;

  private OutputFileFactory fileFactory;
  private Record gRecord;

  @Parameters(name = "formatVersion = {0}, fileFormat = {1}")
  public static List<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {FORMAT_V2, FileFormat.AVRO},
        new Object[] {FORMAT_V2, FileFormat.ORC},
        new Object[] {FORMAT_V2, FileFormat.PARQUET});
  }

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.gRecord = GenericRecord.create(SCHEMA);

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();

    table.updateProperties().defaultFormat(format).commit();
  }

  private EncryptedOutputFile createEncryptedOutputFile() {
    return fileFactory.newOutputFile();
  }

  private DataFile prepareDataFile(FileAppenderFactory<Record> appenderFactory, List<Record> rowSet)
      throws IOException {
    DataWriter<Record> writer =
        appenderFactory.newDataWriter(createEncryptedOutputFile(), format, null);
    try (DataWriter<Record> closeableWriter = writer) {
      for (Record record : rowSet) {
        closeableWriter.write(record);
      }
    }

    return writer.toDataFile();
  }

  private Record createRow(Integer id, String data) {
    Record row = gRecord.copy();
    row.setField("id", id);
    row.setField("data", data);
    return row;
  }

  private StructLikeSet expectedRowSet(Iterable<Record> records) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.forEach(set::add);
    return set;
  }

  private StructLikeSet actualRowSet(String... columns) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }

  @TestTemplate
  public void testSortedPosDelete() throws IOException {
    List<Record> rowSet =
        Lists.newArrayList(
            createRow(0, "aaa"),
            createRow(1, "bbb"),
            createRow(2, "ccc"),
            createRow(3, "ddd"),
            createRow(4, "eee"));

    FileAppenderFactory<Record> appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), null, null, null);
    DataFile dataFile = prepareDataFile(appenderFactory, rowSet);

    SortedPosDeleteWriter<Record> writer =
        new SortedPosDeleteWriter<>(appenderFactory, fileFactory, format, null, 100);
    try (SortedPosDeleteWriter<Record> closeableWriter = writer) {
      for (int index = rowSet.size() - 1; index >= 0; index -= 2) {
        closeableWriter.delete(dataFile.path(), index);
      }
    }

    List<DeleteFile> deleteFiles = writer.complete();
    assertThat(deleteFiles).hasSize(1);
    DeleteFile deleteFile = deleteFiles.get(0);

    // Check whether the path-pos pairs are sorted as expected.
    Schema pathPosSchema = DeleteSchemaUtil.pathPosSchema();
    Record record = GenericRecord.create(pathPosSchema);
    List<Record> expectedDeletes =
        Lists.newArrayList(
            record.copy("file_path", dataFile.path(), "pos", 0L),
            record.copy("file_path", dataFile.path(), "pos", 2L),
            record.copy("file_path", dataFile.path(), "pos", 4L));
    assertThat(readRecordsAsList(pathPosSchema, deleteFile.path())).isEqualTo(expectedDeletes);

    table
        .newRowDelta()
        .addRows(dataFile)
        .addDeletes(deleteFiles.get(0))
        .validateDataFilesExist(writer.referencedDataFiles())
        .validateDeletedFiles()
        .commit();

    List<Record> expectedData = Lists.newArrayList(createRow(1, "bbb"), createRow(3, "ddd"));
    assertThat(actualRowSet("*")).isEqualTo(expectedRowSet(expectedData));
  }

  @TestTemplate
  public void testSortedPosDeleteWithSchemaAndNullRow() throws IOException {
    List<Record> rowSet =
        Lists.newArrayList(createRow(0, "aaa"), createRow(1, "bbb"), createRow(2, "ccc"));

    // Create a FileAppenderFactory which requires pos-delete row schema.
    FileAppenderFactory<Record> appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), null, null, table.schema());
    DataFile dataFile = prepareDataFile(appenderFactory, rowSet);

    // no check on the underlying error msg as it might be missing based on the JDK version
    assertThatThrownBy(
            () ->
                new SortedPosDeleteWriter<>(appenderFactory, fileFactory, format, null, 1)
                    .delete(dataFile.path(), 0L))
        .isInstanceOf(Exception.class);
  }

  @TestTemplate
  public void testSortedPosDeleteWithRow() throws IOException {
    List<Record> rowSet =
        Lists.newArrayList(
            createRow(0, "aaa"),
            createRow(1, "bbb"),
            createRow(2, "ccc"),
            createRow(3, "ddd"),
            createRow(4, "eee"));

    FileAppenderFactory<Record> appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), null, null, table.schema());
    DataFile dataFile = prepareDataFile(appenderFactory, rowSet);

    SortedPosDeleteWriter<Record> writer =
        new SortedPosDeleteWriter<>(appenderFactory, fileFactory, format, null, 100);
    try (SortedPosDeleteWriter<Record> closeableWriter = writer) {
      for (int index = rowSet.size() - 1; index >= 0; index -= 2) {
        closeableWriter.delete(
            dataFile.path(), index, rowSet.get(index)); // Write deletes with row.
      }
    }

    List<DeleteFile> deleteFiles = writer.complete();
    assertThat(deleteFiles).hasSize(1);
    DeleteFile deleteFile = deleteFiles.get(0);

    // Check whether the path-pos pairs are sorted as expected.
    Schema pathPosSchema = DeleteSchemaUtil.posDeleteSchema(table.schema());
    Record record = GenericRecord.create(pathPosSchema);
    List<Record> expectedDeletes =
        Lists.newArrayList(
            record.copy("file_path", dataFile.path(), "pos", 0L, "row", createRow(0, "aaa")),
            record.copy("file_path", dataFile.path(), "pos", 2L, "row", createRow(2, "ccc")),
            record.copy("file_path", dataFile.path(), "pos", 4L, "row", createRow(4, "eee")));
    assertThat(readRecordsAsList(pathPosSchema, deleteFile.path())).isEqualTo(expectedDeletes);

    table
        .newRowDelta()
        .addRows(dataFile)
        .addDeletes(deleteFiles.get(0))
        .validateDataFilesExist(writer.referencedDataFiles())
        .validateDeletedFiles()
        .commit();

    List<Record> expectedData = Lists.newArrayList(createRow(1, "bbb"), createRow(3, "ddd"));
    assertThat(actualRowSet("*")).isEqualTo(expectedRowSet(expectedData));
  }

  @TestTemplate
  public void testMultipleFlush() throws IOException {
    FileAppenderFactory<Record> appenderFactory =
        new GenericAppenderFactory(table.schema(), table.spec(), null, null, null);

    // It will produce 5 record lists, each list will write into a separate data file:
    // The 1th file has: <0  , val-0>   , <1  , val-1>   , ... , <99 , val-99>
    // The 2th file has: <100, val-100> , <101, val-101> , ... , <199, val-199>
    // The 3th file has: <200, val-200> , <201, val-201> , ... , <299, val-299>
    // The 4th file has: <300, val-300> , <301, val-301> , ... , <399, val-399>
    // The 5th file has: <400, val-400> , <401, val-401> , ... , <499, val-499>
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int fileIndex = 0; fileIndex < 5; fileIndex++) {
      List<Record> recordList = Lists.newLinkedList();
      for (int recordIndex = 0; recordIndex < 100; recordIndex++) {
        int id = fileIndex * 100 + recordIndex;
        recordList.add(createRow(id, String.format("val-%s", id)));
      }

      // Write the records and generate the data file.
      dataFiles.add(prepareDataFile(appenderFactory, recordList));
    }

    // Commit those data files to iceberg table.
    RowDelta rowDelta = table.newRowDelta();
    dataFiles.forEach(rowDelta::addRows);
    rowDelta.commit();

    SortedPosDeleteWriter<Record> writer =
        new SortedPosDeleteWriter<>(appenderFactory, fileFactory, format, null, 50);
    try (SortedPosDeleteWriter<Record> closeableWriter = writer) {
      for (int pos = 0; pos < 100; pos++) {
        for (int fileIndex = 4; fileIndex >= 0; fileIndex--) {
          closeableWriter.delete(dataFiles.get(fileIndex).path(), pos);
        }
      }
    }

    List<DeleteFile> deleteFiles = writer.complete();
    assertThat(deleteFiles).hasSize(10);

    Schema pathPosSchema = DeleteSchemaUtil.pathPosSchema();
    Record record = GenericRecord.create(pathPosSchema);
    for (int deleteFileIndex = 0; deleteFileIndex < 10; deleteFileIndex++) {
      List<Record> expectedDeletes = Lists.newArrayList();
      for (int dataFileIndex = 0; dataFileIndex < 5; dataFileIndex++) {
        DataFile dataFile = dataFiles.get(dataFileIndex);
        for (long pos = deleteFileIndex * 10; pos < deleteFileIndex * 10 + 10; pos++) {
          expectedDeletes.add(record.copy("file_path", dataFile.path(), "pos", pos));
        }
      }

      DeleteFile deleteFile = deleteFiles.get(deleteFileIndex);
      assertThat(readRecordsAsList(pathPosSchema, deleteFile.path())).isEqualTo(expectedDeletes);
    }

    rowDelta = table.newRowDelta();
    deleteFiles.forEach(rowDelta::addDeletes);
    rowDelta.commit();

    assertThat(actualRowSet("*")).isEqualTo(expectedRowSet(ImmutableList.of()));
  }

  private List<Record> readRecordsAsList(Schema schema, CharSequence path) throws IOException {
    CloseableIterable<Record> iterable;

    InputFile inputFile = Files.localInput(path.toString());
    switch (format) {
      case PARQUET:
        iterable =
            Parquet.read(inputFile)
                .project(schema)
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build();
        break;

      case AVRO:
        iterable =
            Avro.read(inputFile).project(schema).createReaderFunc(DataReader::create).build();
        break;

      case ORC:
        iterable =
            ORC.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
                .build();
        break;

      default:
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }

    try (CloseableIterable<Record> closeableIterable = iterable) {
      return Lists.newArrayList(closeableIterable);
    }
  }
}
