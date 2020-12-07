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
import java.util.Locale;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTaskDeltaWriter extends TableTestBase {
  private static final int FORMAT_V2 = 2;
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024L;

  private final FileFormat format;
  private final GenericRecord gRecord = GenericRecord.create(SCHEMA);

  private OutputFileFactory fileFactory = null;
  private FileAppenderFactory<Record> appenderFactory = null;

  @Parameterized.Parameters(name = "FileFormat = {0}")
  public static Object[][] parameters() {
    return new Object[][] {
        {"avro"},
        {"parquet"}
    };
  }

  public TestTaskDeltaWriter(String fileFormat) {
    super(FORMAT_V2);
    this.format = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void setupTable() throws IOException {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete()); // created by table create

    this.metadataDir = new File(tableDir, "metadata");

    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = new OutputFileFactory(table.spec(), format, table.locationProvider(), table.io(),
        table.encryption(), 1, 1);

    int firstFieldId = table.schema().findField("id").fieldId();
    int secondFieldId = table.schema().findField("data").fieldId();
    this.appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(),
        new int[] {firstFieldId, secondFieldId}, table.schema(), null);

    table.updateProperties()
        .defaultFormat(format)
        .commit();
  }

  private Record createRecord(Integer id, String data) {
    return gRecord.copy("id", id, "data", data);
  }

  @Test
  public void testPureInsert() throws IOException {
    GenericTaskDeltaWriter deltaWriter = createTaskWriter(TARGET_FILE_SIZE);
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < 20; i++) {
      Record record = createRecord(i, String.format("val-%d", i));
      expected.add(record);

      deltaWriter.write(record);
    }

    WriteResult result = deltaWriter.complete();
    Assert.assertEquals("Should only have a data file.", 1, result.dataFiles().length);
    Assert.assertEquals("Should have no delete file", 0, result.deleteFiles().length);
    commitTransaction(result);
    Assert.assertEquals("Should have expected records", expectedRowSet(expected), actualRowSet("*"));

    deltaWriter = createTaskWriter(TARGET_FILE_SIZE);
    for (int i = 20; i < 30; i++) {
      Record record = createRecord(i, String.format("val-%d", i));
      expected.add(record);

      deltaWriter.write(record);
    }
    result = deltaWriter.complete();
    Assert.assertEquals("Should only have a data file.", 1, result.dataFiles().length);
    Assert.assertEquals("Should have no delete file", 0, result.deleteFiles().length);
    commitTransaction(deltaWriter.complete());
    Assert.assertEquals("Should have expected records", expectedRowSet(expected), actualRowSet("*"));
  }

  @Test
  public void testUpsertSameRow() throws IOException {
    GenericTaskDeltaWriter deltaWriter = createTaskWriter(TARGET_FILE_SIZE);

    Record record = createRecord(1, "aaa");
    deltaWriter.write(record);
    deltaWriter.delete(record);
    deltaWriter.write(record);
    deltaWriter.delete(record);
    deltaWriter.write(record);

    WriteResult result = deltaWriter.complete();
    Assert.assertEquals("Should have a data file.", 1, result.dataFiles().length);
    Assert.assertEquals("Should have a pos-delete file and an eq-delete file", 2, result.deleteFiles().length);
    commitTransaction(result);
    Assert.assertEquals("Should have an expected record", expectedRowSet(ImmutableList.of(record)), actualRowSet("*"));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    Assert.assertEquals(ImmutableList.of(record, record, record), readRecordsAsList(table.schema(), dataFile.path()));

    // Check records in the eq-delete file.
    DeleteFile eqDeleteFile = result.deleteFiles()[0];
    Assert.assertEquals(ImmutableList.of(record, record), readRecordsAsList(table.schema(), eqDeleteFile.path()));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[1];
    GenericRecord posRecord = GenericRecord.create(DeleteSchemaUtil.pathPosSchema());
    Assert.assertEquals(ImmutableList.of(
        posRecord.copy("file_path", dataFile.path(), "pos", 0L),
        posRecord.copy("file_path", dataFile.path(), "pos", 1L)
    ), readRecordsAsList(DeleteSchemaUtil.pathPosSchema(), posDeleteFile.path()));

    deltaWriter = createTaskWriter(TARGET_FILE_SIZE);
    deltaWriter.delete(record);
    result = deltaWriter.complete();
    Assert.assertEquals("Should have 0 data file.", 0, result.dataFiles().length);
    Assert.assertEquals("Should have 1 eq-delete file", 1, result.deleteFiles().length);
    commitTransaction(result);
    Assert.assertEquals("Should have no record", expectedRowSet(ImmutableList.of()), actualRowSet("*"));
  }

  private void commitTransaction(WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    rowDelta.commit();
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

  private GenericTaskDeltaWriter createTaskWriter(long targetFileSize) {
    return new GenericTaskDeltaWriter(table.schema(), table.spec(), format, appenderFactory, fileFactory, table.io(),
        targetFileSize);
  }

  private static class GenericTaskDeltaWriter extends BaseTaskWriter<Record> {
    private final GenericDeltaWriter deltaWriter;

    private GenericTaskDeltaWriter(Schema eqDeleteSchema, PartitionSpec spec, FileFormat format,
                                   FileAppenderFactory<Record> appenderFactory,
                                   OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.deltaWriter = new GenericDeltaWriter(null, eqDeleteSchema);
    }

    @Override
    public void write(Record row) throws IOException {
      deltaWriter.write(row);
    }

    public void delete(Record delete) throws IOException {
      deltaWriter.delete(delete);
    }

    @Override
    public void close() throws IOException {
      deltaWriter.close();
    }

    private class GenericDeltaWriter extends BaseDeltaWriter {

      private GenericDeltaWriter(PartitionKey partition, Schema eqDeleteSchema) {
        super(partition, eqDeleteSchema);
      }

      @Override
      protected StructLike asStructLike(Record row) {
        return row;
      }

      @Override
      protected StructLike asCopiedStructLike(Record row) {
        return row.copy();
      }
    }
  }

  private List<Record> readRecordsAsList(Schema schema, CharSequence path) throws IOException {
    CloseableIterable<Record> iterable;

    InputFile inputFile = Files.localInput(path.toString());
    switch (format) {
      case PARQUET:
        iterable = Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build();
        break;

      case AVRO:
        iterable = Avro.read(inputFile)
            .project(schema)
            .createReaderFunc(DataReader::create)
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
