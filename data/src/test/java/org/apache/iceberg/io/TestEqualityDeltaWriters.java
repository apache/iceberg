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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class TestEqualityDeltaWriters<T> extends WriterTestBase<T> {

  @Parameterized.Parameters(name = "FileFormat={0}")
  public static Object[] parameters() {
    return new Object[] {
        new Object[] {FileFormat.AVRO},
        new Object[] {FileFormat.PARQUET},
        new Object[] {FileFormat.ORC}
    };
  }

  private static final int TABLE_FORMAT_VERSION = 2;
  private static final long TARGET_FILE_SIZE = 128L * 1024 * 1024;
  private static final GenericRecord RECORD = GenericRecord.create(SCHEMA);
  private static final Schema POS_DELETE_SCHEMA = DeleteSchemaUtil.pathPosSchema();
  private static final GenericRecord POS_RECORD = GenericRecord.create(DeleteSchemaUtil.pathPosSchema());

  private final FileFormat fileFormat;

  private FileIO io;
  private int idFieldId;
  private int dataFieldId;

  private OutputFileFactory fileFactory = null;

  public TestEqualityDeltaWriters(FileFormat fileFormat) {
    super(TABLE_FORMAT_VERSION);
    this.fileFormat = fileFormat;
  }

  @Override
  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete()); // created during table creation

    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.table.updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.toString().toLowerCase())
        .set(TableProperties.DELETE_DEFAULT_FILE_FORMAT, fileFormat.toString().toLowerCase())
        .commit();

    this.io = table.io();

    this.idFieldId = table.schema().findField("id").fieldId();
    this.dataFieldId = table.schema().findField("data").fieldId();

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
  }

  protected FileFormat format() {
    return fileFormat;
  }

  protected abstract T toKey(List<Integer> keyFieldIds, Integer id, String data);

  protected abstract StructLikeSet toSet(Iterable<T> records);

  public abstract StructLike asStructLike(T data);

  public abstract StructLike asStructLikeKey(List<Integer> keyFieldIds, T key);

  @Test
  public void testInsertOnly() throws IOException {
    // Commit the first row collection.
    EqualityDeltaWriter<T> deltaWriter = createEqualityWriter(table.schema(), fullKey(), false);

    List<T> rows = IntStream.range(0, 20)
        .mapToObj(i -> toRow(i, String.format("val-%d", i)))
        .collect(Collectors.toList());
    for (T row : rows) {
      deltaWriter.insert(row, PartitionSpec.unpartitioned(), null);
    }

    deltaWriter.close();
    WriteResult result = deltaWriter.result();
    Assert.assertEquals("Should only have a data file.", 1, result.dataFiles().length);
    Assert.assertEquals("Should have no delete file", 0, result.deleteFiles().length);
    commitTransaction(result);
    Assert.assertEquals("Should have expected records", toSet(rows), actualRowSet("*"));

    // Commit the second row collection.
    deltaWriter = createEqualityWriter(table.schema(), fullKey(), false);

    rows = IntStream.range(20, 40)
        .mapToObj(i -> toRow(i, String.format("val-%d", i)))
        .collect(Collectors.toList());
    for (T row : rows) {
      deltaWriter.insert(row, PartitionSpec.unpartitioned(), null);
    }

    deltaWriter.close();
    result = deltaWriter.result();
    Assert.assertEquals("Should only have a data file.", 1, result.dataFiles().length);
    Assert.assertEquals("Should have no delete file", 0, result.deleteFiles().length);
    commitTransaction(result);

    rows = IntStream.range(0, 40)
        .mapToObj(i -> toRow(i, String.format("val-%d", i)))
        .collect(Collectors.toList());
    Assert.assertEquals("Should have expected records", toSet(rows), actualRowSet("*"));
  }

  @Test
  public void testInsertDuplicatedKey() throws IOException {
    EqualityDeltaWriter<T> deltaWriter = createEqualityWriter(table.schema(), idKey(), false);
    List<T> records = ImmutableList.of(
        toRow(1, "aaa"),
        toRow(2, "bbb"),
        toRow(3, "ccc"),
        toRow(4, "ddd"),
        toRow(4, "eee"),
        toRow(3, "fff"),
        toRow(2, "ggg"),
        toRow(1, "hhh")
    );
    records.forEach(row -> deltaWriter.insert(row, PartitionSpec.unpartitioned(), null));

    deltaWriter.close();
    WriteResult result = deltaWriter.result();
    commitTransaction(result);

    Assert.assertEquals("Should have a data file.", 1, result.dataFiles().length);
    Assert.assertEquals("Should have a pos-delete file.", 1, result.deleteFiles().length);
    DeleteFile posDeleteFile = result.deleteFiles()[0];
    Assert.assertEquals("Should be a pos-delete file.", FileContent.POSITION_DELETES, posDeleteFile.content());
    Assert.assertEquals(1, result.referencedDataFiles().length);
    DataFile dataFile = result.dataFiles()[0];
    Assert.assertEquals("Should have the expected referenced data file",
        dataFile.path(), result.referencedDataFiles()[0]);
    Assert.assertEquals("Should have expected records", toSet(ImmutableList.of(
        toRow(4, "eee"),
        toRow(3, "fff"),
        toRow(2, "ggg"),
        toRow(1, "hhh")
    )), actualRowSet("*"));

    // Check records in the data file.
    List<Record> expectedRecords = ImmutableList.of(
        toRecord(1, "aaa"),
        toRecord(2, "bbb"),
        toRecord(3, "ccc"),
        toRecord(4, "ddd"),
        toRecord(4, "eee"),
        toRecord(3, "fff"),
        toRecord(2, "ggg"),
        toRecord(1, "hhh")
    );
    Assert.assertEquals(expectedRecords, readFile(fileFormat, table.schema(), dataFile.path()));

    // Check records in the pos-delete file.
    Assert.assertEquals(ImmutableList.of(
        toPosDelete(dataFile.path(), 3L),
        toPosDelete(dataFile.path(), 2L),
        toPosDelete(dataFile.path(), 1L),
        toPosDelete(dataFile.path(), 0L)
    ), readFile(fileFormat, POS_DELETE_SCHEMA, posDeleteFile.path()));
  }

  @Test
  public void testUpsertSameRow() throws IOException {
    EqualityDeltaWriter<T> deltaWriter = createEqualityWriter(table.schema(), fullKey(), false);

    T row = toRow(1, "aaa");
    deltaWriter.insert(row, PartitionSpec.unpartitioned(), null);

    // UPSERT <1, 'aaa'> to <1, 'aaa'>
    deltaWriter.delete(row, PartitionSpec.unpartitioned(), null);
    deltaWriter.insert(row, PartitionSpec.unpartitioned(), null);

    deltaWriter.close();
    WriteResult result = deltaWriter.result();
    Assert.assertEquals("Should have a data file.", 1, result.dataFiles().length);
    Assert.assertEquals("Should have a pos-delete file.", 1, result.deleteFiles().length);
    commitTransaction(result);
    Assert.assertEquals("Should have an expected record", toSet(ImmutableList.of(row)), actualRowSet("*"));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    Assert.assertEquals(
        ImmutableList.of(toRecord(1, "aaa"), toRecord(1, "aaa")),
        readFile(fileFormat, table.schema(), dataFile.path()));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[0];
    Assert.assertEquals(
        ImmutableList.of(toPosDelete(dataFile.path(), 0L)),
        readFile(fileFormat, POS_DELETE_SCHEMA, posDeleteFile.path()));

    // DELETE the row.
    deltaWriter = createEqualityWriter(table.schema(), fullKey(), false);
    deltaWriter.delete(row, PartitionSpec.unpartitioned(), null);
    deltaWriter.close();
    result = deltaWriter.result();
    Assert.assertEquals("Should have 0 data file.", 0, result.dataFiles().length);
    Assert.assertEquals("Should have 1 eq-delete file.", 1, result.deleteFiles().length);
    Assert.assertEquals(0, result.referencedDataFiles().length);
    Assert.assertEquals(FileContent.EQUALITY_DELETES, result.deleteFiles()[0].content());
    commitTransaction(result);
    Assert.assertEquals("Should have no record", toSet(ImmutableList.of()), actualRowSet("*"));
    Assert.assertEquals(
        ImmutableList.of(toRecord(1, "aaa")),
        readFile(fileFormat, table.schema(), result.deleteFiles()[0].path()));
  }

  @Test
  public void testUpsertMultipleRows() throws IOException {
    EqualityDeltaWriter<T> deltaWriter = createEqualityWriter(table.schema(), dataKey(), false);

    List<T> rows = Lists.newArrayList(
        toRow(1, "aaa"),
        toRow(2, "bbb"),
        toRow(3, "aaa"),
        toRow(3, "ccc"),
        toRow(4, "ccc")
    );
    for (T row : rows) {
      deltaWriter.insert(row, PartitionSpec.unpartitioned(), null);
    }

    // Commit the 1th transaction.
    deltaWriter.close();
    WriteResult result = deltaWriter.result();
    Assert.assertEquals("Should have a data file", 1, result.dataFiles().length);
    Assert.assertEquals("Should have a pos-delete file for deduplication purpose", 1, result.deleteFiles().length);
    Assert.assertEquals("Should be pos-delete file", FileContent.POSITION_DELETES, result.deleteFiles()[0].content());
    Assert.assertEquals(1, result.referencedDataFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have expected records", toSet(ImmutableList.of(
        toRow(2, "bbb"),
        toRow(3, "aaa"),
        toRow(4, "ccc")
    )), actualRowSet("*"));

    // Start the 2nd transaction.
    deltaWriter = createEqualityWriter(table.schema(), dataKey(), false);

    // UPSERT <3,'aaa'> to <5,'aaa'> - (by delete the key)
    deltaWriter.deleteKey(toKey(dataKey(), 3, "aaa"), PartitionSpec.unpartitioned(), null);
    deltaWriter.insert(toRow(5, "aaa"), PartitionSpec.unpartitioned(), null);

    // UPSERT <5,'aaa'> to <6,'aaa'> - (by delete the key)
    deltaWriter.deleteKey(toKey(dataKey(), 5, "aaa"), PartitionSpec.unpartitioned(), null);
    deltaWriter.insert(toRow(6, "aaa"), PartitionSpec.unpartitioned(), null);

    // UPSERT <4,'ccc'> to <7,'ccc'> - (by delete the key)
    deltaWriter.deleteKey(toKey(dataKey(), 4, "ccc"), PartitionSpec.unpartitioned(), null);
    deltaWriter.insert(toRow(7, "ccc"), PartitionSpec.unpartitioned(), null);

    // DELETE <2, 'bbb'> - (by delete the key)
    deltaWriter.deleteKey(toKey(dataKey(), 2, "bbb"), PartitionSpec.unpartitioned(), null);

    // Commit the 2nd transaction.
    deltaWriter.close();
    result = deltaWriter.result();
    Assert.assertEquals(1, result.dataFiles().length);
    Assert.assertEquals(2, result.deleteFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have expected records", toSet(ImmutableList.of(
        toRow(6, "aaa"),
        toRow(7, "ccc")
    )), actualRowSet("*"));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    Assert.assertEquals(ImmutableList.of(
        toRecord(5, "aaa"),
        toRecord(6, "aaa"),
        toRecord(7, "ccc")
    ), readFile(fileFormat, table.schema(), dataFile.path()));

    // Check records in position delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[0];
    Assert.assertEquals(FileContent.POSITION_DELETES, posDeleteFile.content());
    Assert.assertEquals(ImmutableList.of(
        toPosDelete(dataFile.path(), 0L)
    ), readFile(fileFormat, POS_DELETE_SCHEMA, posDeleteFile.path()));

    // Check records in the equality delete file.
    DeleteFile eqDeleteFile = result.deleteFiles()[1];
    Assert.assertEquals(FileContent.EQUALITY_DELETES, eqDeleteFile.content());
    Schema keySchema = TypeUtil.select(table.schema(), Sets.newHashSet(dataKey()));
    GenericRecord dataRecord = GenericRecord.create(keySchema.asStruct());
    Assert.assertEquals(ImmutableList.of(
        dataRecord.copy("data", "aaa"),
        dataRecord.copy("data", "ccc"),
        dataRecord.copy("data", "bbb")
    ), readFile(fileFormat, keySchema, eqDeleteFile.path()));
  }

  @Test
  public void testUpsertDataWithFullRowSchema() throws IOException {
    EqualityDeltaWriter<T> deltaWriter = createEqualityWriter(table.schema(), table.schema(), dataKey(), false);

    List<T> rows = ImmutableList.of(
        toRow(1, "aaa"),
        toRow(2, "bbb"),
        toRow(3, "aaa"),
        toRow(3, "ccc"),
        toRow(4, "ccc")
    );
    for (T row : rows) {
      deltaWriter.insert(row, PartitionSpec.unpartitioned(), null);
    }

    // Commit the 1th transaction.
    deltaWriter.close();
    WriteResult result = deltaWriter.result();
    Assert.assertEquals("Should have a data file.", 1, result.dataFiles().length);
    Assert.assertEquals("Should have a pos-delete file for deduplication purpose", 1, result.deleteFiles().length);
    Assert.assertEquals("Should be a pos-delete file", FileContent.POSITION_DELETES, result.deleteFiles()[0].content());
    Assert.assertEquals(1, result.referencedDataFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have expected records", toSet(ImmutableList.of(
        toRow(2, "bbb"),
        toRow(3, "aaa"),
        toRow(4, "ccc")
    )), actualRowSet("*"));

    // Start the 2nd transaction.
    deltaWriter = createEqualityWriter(table.schema(), table.schema(), dataKey(), false);

    // UPSERT <3, 'aaa'> to <5, 'aaa'> - (by delete the entire row).
    deltaWriter.delete(toRow(3, "aaa"), PartitionSpec.unpartitioned(), null);
    deltaWriter.insert(toRow(5, "aaa"), PartitionSpec.unpartitioned(), null);

    // UPSERT <5, 'aaa'> to <6, 'aaa'> - (by delete the entire row)
    deltaWriter.delete(toRow(5, "aaa"), PartitionSpec.unpartitioned(), null);
    deltaWriter.insert(toRow(6, "aaa"), PartitionSpec.unpartitioned(), null);

    // UPSERT <4, 'ccc'> to <7, 'ccc'> - (by delete the entire row)
    deltaWriter.delete(toRow(4, "ccc"), PartitionSpec.unpartitioned(), null);
    deltaWriter.insert(toRow(7, "ccc"), PartitionSpec.unpartitioned(), null);

    // DELETE <2, 'bbb'> - (by delete the entire row)
    deltaWriter.delete(toRow(2, "bbb"), PartitionSpec.unpartitioned(), null);

    // Commit the 2nd transaction.
    deltaWriter.close();
    result = deltaWriter.result();
    Assert.assertEquals(1, result.dataFiles().length);
    Assert.assertEquals(2, result.deleteFiles().length);
    Assert.assertEquals(1, result.referencedDataFiles().length);
    commitTransaction(result);

    Assert.assertEquals("Should have expected records", toSet(ImmutableList.of(
        toRow(6, "aaa"),
        toRow(7, "ccc")
    )), actualRowSet("*"));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    Assert.assertEquals(ImmutableList.of(
        toRecord(5, "aaa"),
        toRecord(6, "aaa"),
        toRecord(7, "ccc")
    ), readFile(fileFormat, table.schema(), dataFile.path()));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[0];
    Assert.assertEquals(FileContent.POSITION_DELETES, posDeleteFile.content());
    Assert.assertEquals(ImmutableList.of(
        toPosDelete(dataFile.path(), 0L)
    ), readFile(fileFormat, POS_DELETE_SCHEMA, posDeleteFile.path()));

    // Check records in the equality delete file.
    DeleteFile eqDeleteFile = result.deleteFiles()[1];
    Assert.assertEquals(FileContent.EQUALITY_DELETES, eqDeleteFile.content());
    Assert.assertEquals(ImmutableList.of(
        toRecord(3, "aaa"),
        toRecord(4, "ccc"),
        toRecord(2, "bbb")
    ), readFile(fileFormat, table.schema(), eqDeleteFile.path()));
  }

  private Record toRecord(Integer id, String data) {
    return RECORD.copy("id", id, "data", data);
  }

  private Record toPosDelete(CharSequence path, Long pos) {
    return POS_RECORD.copy("file_path", path, "pos", pos);
  }

  protected List<Integer> fullKey() {
    return ImmutableList.of(idFieldId, dataFieldId);
  }

  protected List<Integer> idKey() {
    return ImmutableList.of(idFieldId);
  }

  protected List<Integer> dataKey() {
    return ImmutableList.of(dataFieldId);
  }

  private void commitTransaction(WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

    rowDelta.validateDeletedFiles()
        .validateDataFilesExist(Lists.newArrayList(result.referencedDataFiles()))
        .commit();
  }

  private EqualityDeltaWriter<T> createEqualityWriter(
      Schema schema,
      List<Integer> equalityFieldIds,
      boolean fanoutEnabled) {
    // Select the equality fields to generate the delete schema.
    Schema deleteSchema = TypeUtil.select(schema, ImmutableSet.copyOf(equalityFieldIds));
    return createEqualityWriter(schema, deleteSchema, equalityFieldIds, fanoutEnabled);
  }

  private EqualityDeltaWriter<T> createEqualityWriter(
      Schema schema,
      Schema equalityDeleteSchema,
      List<Integer> equalityFieldIds,
      boolean fanoutEnabled) {
    FileWriterFactory<T> writerFactory = newWriterFactory(schema, equalityFieldIds, equalityDeleteSchema, null);

    PartitioningWriter<T, DataWriteResult> dataWriter;
    PartitioningWriter<T, DeleteWriteResult> eqWriter;
    PartitioningWriter<PositionDelete<T>, DeleteWriteResult> posWriter;

    if (fanoutEnabled) {
      dataWriter = new FanoutDataWriter<>(writerFactory, fileFactory, io, TARGET_FILE_SIZE);
      eqWriter = new FanoutEqualityDeleteWriter<>(writerFactory, fileFactory, io, TARGET_FILE_SIZE);
      posWriter = new FanoutPositionDeleteWriter<>(writerFactory, fileFactory, io, TARGET_FILE_SIZE);
    } else {
      dataWriter = new ClusteredDataWriter<>(writerFactory, fileFactory, io, TARGET_FILE_SIZE);
      eqWriter = new ClusteredEqualityDeleteWriter<>(writerFactory, fileFactory, io, TARGET_FILE_SIZE);
      posWriter = new ClusteredPositionDeleteWriter<>(writerFactory, fileFactory, io, TARGET_FILE_SIZE);
    }

    return new BaseEqualityDeltaWriter<>(
        dataWriter,
        eqWriter,
        posWriter,
        schema,
        TypeUtil.select(schema, ImmutableSet.copyOf(equalityFieldIds)),
        this::asStructLike,
        data -> this.asStructLikeKey(equalityFieldIds, data));
  }
}
