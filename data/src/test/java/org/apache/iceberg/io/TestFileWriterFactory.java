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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
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
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class TestFileWriterFactory<T> extends WriterTestBase<T> {
  @Parameterized.Parameters(name = "FileFormat={0}, Partitioned={1}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.AVRO, false},
      new Object[] {FileFormat.AVRO, true},
      new Object[] {FileFormat.PARQUET, false},
      new Object[] {FileFormat.PARQUET, true},
      new Object[] {FileFormat.ORC, false},
      new Object[] {FileFormat.ORC, true}
    };
  }

  private static final int TABLE_FORMAT_VERSION = 2;
  private static final String PARTITION_VALUE = "aaa";

  private final FileFormat fileFormat;
  private final boolean partitioned;
  private final List<T> dataRows;

  private StructLike partition = null;
  private OutputFileFactory fileFactory = null;

  public TestFileWriterFactory(FileFormat fileFormat, boolean partitioned) {
    super(TABLE_FORMAT_VERSION);
    this.fileFormat = fileFormat;
    this.partitioned = partitioned;
    this.dataRows =
        ImmutableList.of(
            toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "aaa"), toRow(4, "aaa"), toRow(5, "aaa"));
  }

  protected abstract StructLikeSet toSet(Iterable<T> records);

  protected FileFormat format() {
    return fileFormat;
  }

  @Override
  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete()); // created during table creation

    this.metadataDir = new File(tableDir, "metadata");

    if (partitioned) {
      this.table = create(SCHEMA, SPEC);
      this.partition = partitionKey(table.spec(), PARTITION_VALUE);
    } else {
      this.table = create(SCHEMA, PartitionSpec.unpartitioned());
      this.partition = null;
    }

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
  }

  @Test
  public void testDataWriter() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    DataFile dataFile = writeData(writerFactory, dataRows, table.spec(), partition);

    table.newRowDelta().addRows(dataFile).commit();

    Assert.assertEquals("Records should match", toSet(dataRows), actualRowSet("*"));
  }

  @Test
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
    Assert.assertEquals("Delete records must match", expectedDeletes, actualDeletes);

    // commit the written delete file
    table.newRowDelta().addDeletes(deleteFile).commit();

    // verify the delete file is applied correctly
    List<T> expectedRows = ImmutableList.of(toRow(2, "aaa"), toRow(4, "aaa"));
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }

  @Test
  public void testEqualityDeleteWriterWithMultipleSpecs() throws IOException {
    Assume.assumeFalse("Table must start unpartitioned", partitioned);

    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);

    // write an unpartitioned data file
    DataFile firstDataFile = writeData(writerFactory, dataRows, table.spec(), partition);
    Assert.assertEquals(
        "First data file must be unpartitioned", 0, firstDataFile.partition().size());

    List<T> deletes =
        ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "aaa"), toRow(4, "aaa"));

    // write an unpartitioned delete file
    DeleteFile firstDeleteFile =
        writeEqualityDeletes(writerFactory, deletes, table.spec(), partition);
    Assert.assertEquals(
        "First delete file must be unpartitioned", 0, firstDeleteFile.partition().size());

    // commit the first data and delete files
    table.newAppend().appendFile(firstDataFile).commit();
    table.newRowDelta().addDeletes(firstDeleteFile).commit();

    // evolve the spec
    table.updateSpec().addField("data").commit();

    partition = partitionKey(table.spec(), PARTITION_VALUE);

    // write a partitioned data file
    DataFile secondDataFile = writeData(writerFactory, dataRows, table.spec(), partition);
    Assert.assertEquals(
        "Second data file must be partitioned", 1, secondDataFile.partition().size());

    // write a partitioned delete file
    DeleteFile secondDeleteFile =
        writeEqualityDeletes(writerFactory, deletes, table.spec(), partition);
    Assert.assertEquals(
        "Second delete file must be artitioned", 1, secondDeleteFile.partition().size());

    // commit the second data and delete files
    table.newAppend().appendFile(secondDataFile).commit();
    table.newRowDelta().addDeletes(secondDeleteFile).commit();

    // verify both delete files are applied correctly
    List<T> expectedRows = ImmutableList.of(toRow(5, "aaa"), toRow(5, "aaa"));
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }

  @Test
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
      Assert.assertNull(deleteFile.lowerBounds());
      Assert.assertNull(deleteFile.upperBounds());
      Assert.assertNull(deleteFile.columnSizes());
    } else {
      Assert.assertEquals(1, referencedDataFiles.size());
      Assert.assertEquals(2, deleteFile.lowerBounds().size());
      Assert.assertTrue(deleteFile.lowerBounds().containsKey(DELETE_FILE_PATH.fieldId()));
      Assert.assertEquals(2, deleteFile.upperBounds().size());
      Assert.assertTrue(deleteFile.upperBounds().containsKey(DELETE_FILE_PATH.fieldId()));
      Assert.assertEquals(2, deleteFile.columnSizes().size());
    }

    Assert.assertNull(deleteFile.valueCounts());
    Assert.assertNull(deleteFile.nullValueCounts());
    Assert.assertNull(deleteFile.nanValueCounts());

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
    Assert.assertEquals("Delete records must match", expectedDeletes, actualDeletes);

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
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }

  @Test
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
      Assert.assertNull(deleteFile.lowerBounds());
      Assert.assertNull(deleteFile.upperBounds());
      Assert.assertNull(deleteFile.columnSizes());
      Assert.assertNull(deleteFile.valueCounts());
      Assert.assertNull(deleteFile.nullValueCounts());
      Assert.assertNull(deleteFile.nanValueCounts());
    } else {
      Assert.assertEquals(1, referencedDataFiles.size());
      Assert.assertEquals(4, deleteFile.lowerBounds().size());
      Assert.assertTrue(deleteFile.lowerBounds().containsKey(DELETE_FILE_PATH.fieldId()));
      Assert.assertTrue(deleteFile.lowerBounds().containsKey(DELETE_FILE_POS.fieldId()));
      for (Types.NestedField column : table.schema().columns()) {
        Assert.assertTrue(deleteFile.lowerBounds().containsKey(column.fieldId()));
      }
      Assert.assertEquals(4, deleteFile.upperBounds().size());
      Assert.assertTrue(deleteFile.upperBounds().containsKey(DELETE_FILE_PATH.fieldId()));
      Assert.assertTrue(deleteFile.upperBounds().containsKey(DELETE_FILE_POS.fieldId()));
      for (Types.NestedField column : table.schema().columns()) {
        Assert.assertTrue(deleteFile.upperBounds().containsKey(column.fieldId()));
      }
      // ORC also contains metrics for the deleted row struct, not just actual data fields
      Assert.assertTrue(deleteFile.columnSizes().size() >= 4);
      Assert.assertTrue(deleteFile.valueCounts().size() >= 2);
      Assert.assertTrue(deleteFile.nullValueCounts().size() >= 2);
      Assert.assertNull(deleteFile.nanValueCounts());
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
    Assert.assertEquals("Delete records must match", expectedDeletes, actualDeletes);

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
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
  }

  @Test
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
    Assert.assertEquals(2, referencedDataFiles.size());
    Assert.assertNull(deleteFile.lowerBounds());
    Assert.assertNull(deleteFile.upperBounds());
    Assert.assertNull(deleteFile.valueCounts());
    Assert.assertNull(deleteFile.nullValueCounts());
    Assert.assertNull(deleteFile.nanValueCounts());

    if (fileFormat == FileFormat.AVRO) {
      Assert.assertNull(deleteFile.columnSizes());
    } else {
      Assert.assertEquals(2, deleteFile.columnSizes().size());
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
    Assert.assertEquals("Records should match", toSet(expectedRows), actualRowSet("*"));
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
