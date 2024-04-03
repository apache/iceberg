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

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_ROW_FIELD_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestFileWriterFactory<T> extends WriterTestBase<T> {
  @Parameters(name = "formatVersion = {0}, fileFormat = {1}, Partitioned = {2}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {2, FileFormat.AVRO, false},
        new Object[] {2, FileFormat.AVRO, true},
        new Object[] {2, FileFormat.PARQUET, false},
        new Object[] {2, FileFormat.PARQUET, true},
        new Object[] {2, FileFormat.ORC, false},
        new Object[] {2, FileFormat.ORC, true});
  }

  private static final String PARTITION_VALUE = "aaa";

  @Parameter(index = 1)
  private FileFormat fileFormat;

  @Parameter(index = 2)
  private boolean partitioned;

  private StructLike partition = null;
  private OutputFileFactory fileFactory = null;
  private List<T> dataRows;

  protected abstract StructLikeSet toSet(Iterable<T> records);

  protected FileFormat format() {
    return fileFormat;
  }

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue(); // created during table creation

    this.metadataDir = new File(tableDir, "metadata");

    if (partitioned) {
      this.table = create(SCHEMA, SPEC);
      this.partition = partitionKey(table.spec(), PARTITION_VALUE);
    } else {
      this.table = create(SCHEMA, PartitionSpec.unpartitioned());
      this.partition = null;
    }

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

    this.dataRows =
        ImmutableList.of(
            toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "aaa"), toRow(4, "aaa"), toRow(5, "aaa"));
  }

  @TestTemplate
  public void testDataWriter() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    DataFile dataFile = writeData(writerFactory, dataRows, table.spec(), partition);

    table.newRowDelta().addRows(dataFile).commit();

    assertThat(actualRowSet("*")).isEqualTo(toSet(dataRows));
  }

  @TestTemplate
  public void testEqualityDeleteWriter() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);

    // write a data file
    DataFile dataFile = writeData(writerFactory, dataRows, table.spec(), partition);

    // commit the written data file
    table.newRowDelta().addRows(dataFile).commit();

    // write an equality delete file
    List<T> deletes = ImmutableList.of(toRow(1, "aaa"), toRow(3, "bbb"), toRow(5, "ccc"));
    DeleteFile deleteFile = writeEqualityDeletes(writerFactory, deletes, table.spec(), partition);

    // verify the written delete file
    GenericRecord deleteRecord = GenericRecord.create(equalityDeleteRowSchema);
    List<Record> expectedDeletes =
        ImmutableList.of(
            deleteRecord.copy("id", 1), deleteRecord.copy("id", 3), deleteRecord.copy("id", 5));
    InputFile inputDeleteFile = table.io().newInputFile(deleteFile.path().toString());
    List<Record> actualDeletes = readFile(equalityDeleteRowSchema, inputDeleteFile);
    assertThat(actualDeletes).isEqualTo(expectedDeletes);

    // commit the written delete file
    table.newRowDelta().addDeletes(deleteFile).commit();

    // verify the delete file is applied correctly
    List<T> expectedRows = ImmutableList.of(toRow(2, "aaa"), toRow(4, "aaa"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testEqualityDeleteWriterWithMultipleSpecs() throws IOException {
    assumeThat(partitioned).isFalse();

    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);

    // write an unpartitioned data file
    DataFile firstDataFile = writeData(writerFactory, dataRows, table.spec(), partition);
    assertThat(firstDataFile.partition().size())
        .as("First data file must be unpartitioned")
        .isEqualTo(0);

    List<T> deletes =
        ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "aaa"), toRow(4, "aaa"));

    // write an unpartitioned delete file
    DeleteFile firstDeleteFile =
        writeEqualityDeletes(writerFactory, deletes, table.spec(), partition);
    assertThat(firstDeleteFile.partition().size())
        .as("First delete file must be unpartitioned")
        .isEqualTo(0);

    // commit the first data and delete files
    table.newAppend().appendFile(firstDataFile).commit();
    table.newRowDelta().addDeletes(firstDeleteFile).commit();

    // evolve the spec
    table.updateSpec().addField("data").commit();

    partition = partitionKey(table.spec(), PARTITION_VALUE);

    // write a partitioned data file
    DataFile secondDataFile = writeData(writerFactory, dataRows, table.spec(), partition);
    assertThat(secondDataFile.partition().size())
        .as("Second data file must be partitioned")
        .isEqualTo(1);

    // write a partitioned delete file
    DeleteFile secondDeleteFile =
        writeEqualityDeletes(writerFactory, deletes, table.spec(), partition);
    assertThat(secondDeleteFile.partition().size())
        .as("Second delete file must be partitioned")
        .isEqualTo(1);

    // commit the second data and delete files
    table.newAppend().appendFile(secondDataFile).commit();
    table.newRowDelta().addDeletes(secondDeleteFile).commit();

    // verify both delete files are applied correctly
    List<T> expectedRows = ImmutableList.of(toRow(5, "aaa"), toRow(5, "aaa"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testPositionDeleteWriter() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // write a data file
    DataFile dataFile = writeData(writerFactory, dataRows, table.spec(), partition);

    // write a position delete file
    List<PositionDelete<T>> deletes =
        ImmutableList.of(
            positionDelete(dataFile.path(), 0L, null),
            positionDelete(dataFile.path(), 2L, null),
            positionDelete(dataFile.path(), 4L, null));
    Pair<DeleteFile, CharSequenceSet> result =
        writePositionDeletes(writerFactory, deletes, table.spec(), partition);
    DeleteFile deleteFile = result.first();
    CharSequenceSet referencedDataFiles = result.second();

    if (fileFormat == FileFormat.AVRO) {
      assertThat(deleteFile.lowerBounds()).isNull();
      assertThat(deleteFile.upperBounds()).isNull();
      assertThat(deleteFile.columnSizes()).isNull();
    } else {
      assertThat(referencedDataFiles).hasSize(1);
      assertThat(deleteFile.lowerBounds()).hasSize(2).containsKey(DELETE_FILE_PATH.fieldId());
      assertThat(deleteFile.upperBounds()).hasSize(2).containsKey(DELETE_FILE_PATH.fieldId());
      assertThat(deleteFile.columnSizes()).hasSize(2);
    }

    assertThat(deleteFile.valueCounts()).isNull();
    assertThat(deleteFile.nullValueCounts()).isNull();
    assertThat(deleteFile.nanValueCounts()).isNull();

    // verify the written delete file
    GenericRecord deleteRecord = GenericRecord.create(DeleteSchemaUtil.pathPosSchema());
    List<Record> expectedDeletes =
        ImmutableList.of(
            deleteRecord.copy(DELETE_FILE_PATH.name(), dataFile.path(), DELETE_FILE_POS.name(), 0L),
            deleteRecord.copy(DELETE_FILE_PATH.name(), dataFile.path(), DELETE_FILE_POS.name(), 2L),
            deleteRecord.copy(
                DELETE_FILE_PATH.name(), dataFile.path(), DELETE_FILE_POS.name(), 4L));
    InputFile inputDeleteFile = table.io().newInputFile(deleteFile.path().toString());
    List<Record> actualDeletes = readFile(DeleteSchemaUtil.pathPosSchema(), inputDeleteFile);
    assertThat(actualDeletes).isEqualTo(expectedDeletes);

    // commit the data and delete files
    table
        .newRowDelta()
        .addRows(dataFile)
        .addDeletes(deleteFile)
        .validateDataFilesExist(referencedDataFiles)
        .validateDeletedFiles()
        .commit();

    // verify the delete file is applied correctly
    List<T> expectedRows = ImmutableList.of(toRow(2, "aaa"), toRow(4, "aaa"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testPositionDeleteWriterWithRow() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema(), table.schema());

    // write a data file
    DataFile dataFile = writeData(writerFactory, dataRows, table.spec(), partition);

    // write a position delete file and persist the deleted row
    List<PositionDelete<T>> deletes =
        ImmutableList.of(positionDelete(dataFile.path(), 0, dataRows.get(0)));
    Pair<DeleteFile, CharSequenceSet> result =
        writePositionDeletes(writerFactory, deletes, table.spec(), partition);
    DeleteFile deleteFile = result.first();
    CharSequenceSet referencedDataFiles = result.second();

    if (fileFormat == FileFormat.AVRO) {
      assertThat(deleteFile.lowerBounds()).isNull();
      assertThat(deleteFile.upperBounds()).isNull();
      assertThat(deleteFile.columnSizes()).isNull();
      assertThat(deleteFile.valueCounts()).isNull();
      assertThat(deleteFile.nullValueCounts()).isNull();
      assertThat(deleteFile.nanValueCounts()).isNull();
    } else {
      assertThat(referencedDataFiles).hasSize(1);
      assertThat(deleteFile.lowerBounds())
          .hasSize(4)
          .containsKey(DELETE_FILE_PATH.fieldId())
          .containsKey(DELETE_FILE_POS.fieldId());
      for (Types.NestedField column : table.schema().columns()) {
        assertThat(deleteFile.lowerBounds()).containsKey(column.fieldId());
      }
      assertThat(deleteFile.upperBounds())
          .hasSize(4)
          .containsKey(DELETE_FILE_PATH.fieldId())
          .containsKey(DELETE_FILE_POS.fieldId());
      for (Types.NestedField column : table.schema().columns()) {
        assertThat(deleteFile.upperBounds()).containsKey(column.fieldId());
      }
      // ORC also contains metrics for the deleted row struct, not just actual data fields
      assertThat(deleteFile.columnSizes()).hasSizeGreaterThanOrEqualTo(4);
      assertThat(deleteFile.valueCounts()).hasSizeGreaterThanOrEqualTo(2);
      assertThat(deleteFile.nullValueCounts()).hasSizeGreaterThanOrEqualTo(2);
      assertThat(deleteFile.nanValueCounts()).isNull();
    }

    // verify the written delete file
    GenericRecord deletedRow = GenericRecord.create(table.schema());
    Schema positionDeleteSchema = DeleteSchemaUtil.posDeleteSchema(table.schema());
    GenericRecord deleteRecord = GenericRecord.create(positionDeleteSchema);
    Map<String, Object> deleteRecordColumns =
        ImmutableMap.of(
            DELETE_FILE_PATH.name(),
            dataFile.path(),
            DELETE_FILE_POS.name(),
            0L,
            DELETE_FILE_ROW_FIELD_NAME,
            deletedRow.copy("id", 1, "data", "aaa"));
    List<Record> expectedDeletes = ImmutableList.of(deleteRecord.copy(deleteRecordColumns));
    InputFile inputDeleteFile = table.io().newInputFile(deleteFile.path().toString());
    List<Record> actualDeletes = readFile(positionDeleteSchema, inputDeleteFile);
    assertThat(actualDeletes).isEqualTo(expectedDeletes);

    // commit the data and delete files
    table
        .newRowDelta()
        .addRows(dataFile)
        .addDeletes(deleteFile)
        .validateDataFilesExist(referencedDataFiles)
        .validateDeletedFiles()
        .commit();

    // verify the delete file is applied correctly
    List<T> expectedRows =
        ImmutableList.of(toRow(2, "aaa"), toRow(3, "aaa"), toRow(4, "aaa"), toRow(5, "aaa"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  @TestTemplate
  public void testPositionDeleteWriterMultipleDataFiles() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // write two data files
    DataFile dataFile1 = writeData(writerFactory, dataRows, table.spec(), partition);
    DataFile dataFile2 = writeData(writerFactory, dataRows, table.spec(), partition);

    // write a position delete file referencing both
    List<PositionDelete<T>> deletes =
        ImmutableList.of(
            positionDelete(dataFile1.path(), 0L, null),
            positionDelete(dataFile1.path(), 2L, null),
            positionDelete(dataFile2.path(), 4L, null));
    Pair<DeleteFile, CharSequenceSet> result =
        writePositionDeletes(writerFactory, deletes, table.spec(), partition);
    DeleteFile deleteFile = result.first();
    CharSequenceSet referencedDataFiles = result.second();

    // verify the written delete file has NO lower and upper bounds
    assertThat(referencedDataFiles).hasSize(2);
    assertThat(deleteFile.lowerBounds()).isNull();
    assertThat(deleteFile.upperBounds()).isNull();
    assertThat(deleteFile.valueCounts()).isNull();
    assertThat(deleteFile.nullValueCounts()).isNull();
    assertThat(deleteFile.nanValueCounts()).isNull();

    if (fileFormat == FileFormat.AVRO) {
      assertThat(deleteFile.columnSizes()).isNull();
    } else {
      assertThat(deleteFile.columnSizes()).hasSize(2);
    }

    // commit the data and delete files
    table
        .newRowDelta()
        .addRows(dataFile1)
        .addRows(dataFile2)
        .addDeletes(deleteFile)
        .validateDataFilesExist(referencedDataFiles)
        .validateDeletedFiles()
        .commit();

    // verify the delete file is applied correctly
    List<T> expectedRows =
        ImmutableList.of(
            toRow(2, "aaa"),
            toRow(4, "aaa"),
            toRow(5, "aaa"),
            toRow(1, "aaa"),
            toRow(2, "aaa"),
            toRow(3, "aaa"),
            toRow(4, "aaa"));
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  private DataFile writeData(
      FileWriterFactory<T> writerFactory, List<T> rows, PartitionSpec spec, StructLike partitionKey)
      throws IOException {

    EncryptedOutputFile file = newOutputFile(spec, partitionKey);
    DataWriter<T> writer = writerFactory.newDataWriter(file, spec, partitionKey);

    try (DataWriter<T> closeableWriter = writer) {
      for (T row : rows) {
        closeableWriter.write(row);
      }
    }

    return writer.toDataFile();
  }

  private DeleteFile writeEqualityDeletes(
      FileWriterFactory<T> writerFactory,
      List<T> deletes,
      PartitionSpec spec,
      StructLike partitionKey)
      throws IOException {

    EncryptedOutputFile file = newOutputFile(spec, partitionKey);
    EqualityDeleteWriter<T> writer =
        writerFactory.newEqualityDeleteWriter(file, spec, partitionKey);

    try (EqualityDeleteWriter<T> closableWriter = writer) {
      closableWriter.write(deletes);
    }

    return writer.toDeleteFile();
  }

  private Pair<DeleteFile, CharSequenceSet> writePositionDeletes(
      FileWriterFactory<T> writerFactory,
      List<PositionDelete<T>> deletes,
      PartitionSpec spec,
      StructLike partitionKey)
      throws IOException {

    EncryptedOutputFile file = newOutputFile(spec, partitionKey);
    PositionDeleteWriter<T> writer =
        writerFactory.newPositionDeleteWriter(file, spec, partitionKey);

    PositionDelete<T> posDelete = PositionDelete.create();
    try (PositionDeleteWriter<T> closableWriter = writer) {
      for (PositionDelete<T> delete : deletes) {
        closableWriter.write(posDelete.set(delete.path(), delete.pos(), delete.row()));
      }
    }

    return Pair.of(writer.toDeleteFile(), writer.referencedDataFiles());
  }

  private List<Record> readFile(Schema schema, InputFile inputFile) throws IOException {
    switch (fileFormat) {
      case PARQUET:
        try (CloseableIterable<Record> records =
            Parquet.read(inputFile)
                .project(schema)
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build()) {

          return ImmutableList.copyOf(records);
        }

      case AVRO:
        try (CloseableIterable<Record> records =
            Avro.read(inputFile).project(schema).createReaderFunc(DataReader::create).build()) {

          return ImmutableList.copyOf(records);
        }

      case ORC:
        try (CloseableIterable<Record> records =
            ORC.read(inputFile)
                .project(schema)
                .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
                .build()) {

          return ImmutableList.copyOf(records);
        }

      default:
        throw new UnsupportedOperationException("Unsupported read file format: " + fileFormat);
    }
  }

  private EncryptedOutputFile newOutputFile(PartitionSpec spec, StructLike partitionKey) {
    return fileFactory.newOutputFile(spec, partitionKey);
  }
}
