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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTaskEqualityDeltaWriter extends TestBase {
  private static final int FORMAT_V2 = 2;
  private static final long TARGET_FILE_SIZE = 128L;
  private final GenericRecord gRecord = GenericRecord.create(SCHEMA);
  private final GenericRecord posRecord = GenericRecord.create(DeleteSchemaUtil.pathPosSchema());

  private OutputFileFactory fileFactory = null;
  private int idFieldId;
  private int dataFieldId;

  @Parameter(index = 1)
  protected FileFormat format;

  @Parameters(name = "formatVersion = {0}, FileFormat = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {FORMAT_V2, FileFormat.AVRO},
        new Object[] {FORMAT_V2, FileFormat.ORC},
        new Object[] {FORMAT_V2, FileFormat.PARQUET});
  }

  @Override
  @BeforeEach
  public void setupTable() throws IOException {
    this.tableDir = java.nio.file.Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");

    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();

    this.idFieldId = table.schema().findField("id").fieldId();
    this.dataFieldId = table.schema().findField("data").fieldId();

    table.updateProperties().defaultFormat(format).commit();
  }

  private Record createRecord(Integer id, String data) {
    return gRecord.copy("id", id, "data", data);
  }

  @TestTemplate
  public void testPureInsert() throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(idFieldId, dataFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    List<Record> expected = Lists.newArrayList();
    for (int i = 0; i < 20; i++) {
      Record record = createRecord(i, String.format("val-%d", i));
      expected.add(record);

      deltaWriter.write(record);
    }

    WriteResult result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should only have a data file.").hasSize(1);
    assertThat(result.deleteFiles()).as("Should have no delete file").hasSize(0);
    commitTransaction(result);
    assertThat(expectedRowSet(expected))
        .as("Should have expected records")
        .isEqualTo(actualRowSet("*"));

    deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    for (int i = 20; i < 30; i++) {
      Record record = createRecord(i, String.format("val-%d", i));
      expected.add(record);

      deltaWriter.write(record);
    }
    result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should only have a data file.").hasSize(1);
    assertThat(result.deleteFiles()).as("Should have no delete file").hasSize(0);
    commitTransaction(result);
    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(expectedRowSet(expected));
  }

  @TestTemplate
  public void testInsertDuplicatedKey() throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(idFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(equalityFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    deltaWriter.write(createRecord(1, "aaa"));
    deltaWriter.write(createRecord(2, "bbb"));
    deltaWriter.write(createRecord(3, "ccc"));
    deltaWriter.write(createRecord(4, "ddd"));
    deltaWriter.write(createRecord(4, "eee"));
    deltaWriter.write(createRecord(3, "fff"));
    deltaWriter.write(createRecord(2, "ggg"));
    deltaWriter.write(createRecord(1, "hhh"));

    WriteResult result = deltaWriter.complete();
    commitTransaction(result);

    assertThat(result.dataFiles()).as("Should have a data file.").hasSize(1);
    assertThat(result.deleteFiles()).as("Should have a pos-delete file").hasSize(1);
    DeleteFile posDeleteFile = result.deleteFiles()[0];
    assertThat(posDeleteFile.content())
        .as("Should be a pos-delete file")
        .isEqualTo(FileContent.POSITION_DELETES);
    assertThat(result.referencedDataFiles()).hasSize(1);
    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(
                    createRecord(4, "eee"),
                    createRecord(3, "fff"),
                    createRecord(2, "ggg"),
                    createRecord(1, "hhh"))));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    assertThat(readRecordsAsList(table.schema(), dataFile.path()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(1, "aaa"),
                createRecord(2, "bbb"),
                createRecord(3, "ccc"),
                createRecord(4, "ddd"),
                createRecord(4, "eee"),
                createRecord(3, "fff"),
                createRecord(2, "ggg"),
                createRecord(1, "hhh")));

    // Check records in the pos-delete file.
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    assertThat(readRecordsAsList(posDeleteSchema, posDeleteFile.path()))
        .isEqualTo(
            ImmutableList.of(
                posRecord.copy("file_path", dataFile.path(), "pos", 0L),
                posRecord.copy("file_path", dataFile.path(), "pos", 1L),
                posRecord.copy("file_path", dataFile.path(), "pos", 2L),
                posRecord.copy("file_path", dataFile.path(), "pos", 3L)));
  }

  @TestTemplate
  public void testUpsertSameRow() throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(idFieldId, dataFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);

    Record record = createRecord(1, "aaa");
    deltaWriter.write(record);

    // UPSERT <1, 'aaa'> to <1, 'aaa'>
    deltaWriter.delete(record);
    deltaWriter.write(record);

    WriteResult result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should have a data file.").hasSize(1);
    assertThat(result.deleteFiles()).as("Should have a pos-delete file").hasSize(1);
    commitTransaction(result);
    assertThat(actualRowSet("*"))
        .as("Should have an expected record")
        .isEqualTo(expectedRowSet(ImmutableList.of(record)));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    assertThat(readRecordsAsList(table.schema(), dataFile.path()))
        .isEqualTo(ImmutableList.of(record, record));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[0];
    assertThat(readRecordsAsList(DeleteSchemaUtil.pathPosSchema(), posDeleteFile.path()))
        .isEqualTo(ImmutableList.of(posRecord.copy("file_path", dataFile.path(), "pos", 0L)));

    deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    deltaWriter.delete(record);
    result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should have 0 data file.").hasSize(0);
    assertThat(result.deleteFiles()).as("Should have 1 eq-delete file").hasSize(1);
    commitTransaction(result);
    assertThat(actualRowSet("*"))
        .as("Should have no record")
        .isEqualTo(expectedRowSet(ImmutableList.of()));
  }

  @TestTemplate
  public void testUpsertData() throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(dataFieldId);
    Schema eqDeleteRowSchema = table.schema().select("data");

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    deltaWriter.write(createRecord(1, "aaa"));
    deltaWriter.write(createRecord(2, "bbb"));
    deltaWriter.write(createRecord(3, "aaa"));
    deltaWriter.write(createRecord(3, "ccc"));
    deltaWriter.write(createRecord(4, "ccc"));

    // Commit the 1th transaction.
    WriteResult result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should have a data file").hasSize(1);
    assertThat(result.deleteFiles())
        .as("Should have a pos-delete file for deduplication purpose")
        .hasSize(1);
    assertThat(result.deleteFiles()[0].content())
        .as("Should be pos-delete file")
        .isEqualTo(FileContent.POSITION_DELETES);
    assertThat(result.referencedDataFiles()).hasSize(1);
    commitTransaction(result);

    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(
                    createRecord(2, "bbb"), createRecord(3, "aaa"), createRecord(4, "ccc"))));

    // Start the 2nd transaction.
    deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    GenericRecord keyRecord = GenericRecord.create(eqDeleteRowSchema);
    Function<String, Record> keyFunc = data -> keyRecord.copy("data", data);

    // UPSERT <3,'aaa'> to <5,'aaa'> - (by delete the key)
    deltaWriter.deleteKey(keyFunc.apply("aaa"));
    deltaWriter.write(createRecord(5, "aaa"));

    // UPSERT <5,'aaa'> to <6,'aaa'> - (by delete the key)
    deltaWriter.deleteKey(keyFunc.apply("aaa"));
    deltaWriter.write(createRecord(6, "aaa"));

    // UPSERT <4,'ccc'> to <7,'ccc'> - (by delete the key)
    deltaWriter.deleteKey(keyFunc.apply("ccc"));
    deltaWriter.write(createRecord(7, "ccc"));

    // DELETE <2, 'bbb'> - (by delete the key)
    deltaWriter.deleteKey(keyFunc.apply("bbb"));

    // Commit the 2nd transaction.
    result = deltaWriter.complete();
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(2);
    commitTransaction(result);

    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(ImmutableList.of(createRecord(6, "aaa"), createRecord(7, "ccc"))));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    assertThat(readRecordsAsList(table.schema(), dataFile.path()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(5, "aaa"), createRecord(6, "aaa"), createRecord(7, "ccc")));

    // Check records in the eq-delete file.
    DeleteFile eqDeleteFile = result.deleteFiles()[0];
    assertThat(eqDeleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(readRecordsAsList(eqDeleteRowSchema, eqDeleteFile.path()))
        .isEqualTo(
            ImmutableList.of(keyFunc.apply("aaa"), keyFunc.apply("ccc"), keyFunc.apply("bbb")));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[1];
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(readRecordsAsList(posDeleteSchema, posDeleteFile.path()))
        .isEqualTo(ImmutableList.of(posRecord.copy("file_path", dataFile.path(), "pos", 0L)));
  }

  @TestTemplate
  public void testUpsertDataWithFullRowSchema() throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(dataFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);
    deltaWriter.write(createRecord(1, "aaa"));
    deltaWriter.write(createRecord(2, "bbb"));
    deltaWriter.write(createRecord(3, "aaa"));
    deltaWriter.write(createRecord(3, "ccc"));
    deltaWriter.write(createRecord(4, "ccc"));

    // Commit the 1th transaction.
    WriteResult result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should have a data file").hasSize(1);
    assertThat(result.deleteFiles())
        .as("Should have a pos-delete file for deduplication purpose")
        .hasSize(1);
    assertThat(result.deleteFiles()[0].content())
        .as("Should be pos-delete file")
        .isEqualTo(FileContent.POSITION_DELETES);
    assertThat(result.referencedDataFiles()).hasSize(1);
    commitTransaction(result);

    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(
                ImmutableList.of(
                    createRecord(2, "bbb"), createRecord(3, "aaa"), createRecord(4, "ccc"))));

    // Start the 2nd transaction.
    deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, DeleteGranularity.PARTITION);

    // UPSERT <3,'aaa'> to <5,'aaa'> - (by delete the entire row)
    deltaWriter.delete(createRecord(3, "aaa"));
    deltaWriter.write(createRecord(5, "aaa"));

    // UPSERT <5,'aaa'> to <6,'aaa'> - (by delete the entire row)
    deltaWriter.delete(createRecord(5, "aaa"));
    deltaWriter.write(createRecord(6, "aaa"));

    // UPSERT <4,'ccc'> to <7,'ccc'> - (by delete the entire row)
    deltaWriter.delete(createRecord(4, "ccc"));
    deltaWriter.write(createRecord(7, "ccc"));

    // DELETE <2, 'bbb'> - (by delete the entire row)
    deltaWriter.delete(createRecord(2, "bbb"));

    // Commit the 2nd transaction.
    result = deltaWriter.complete();
    assertThat(result.dataFiles()).hasSize(1);
    assertThat(result.deleteFiles()).hasSize(2);
    assertThat(result.referencedDataFiles()).hasSize(1);
    commitTransaction(result);

    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(
            expectedRowSet(ImmutableList.of(createRecord(6, "aaa"), createRecord(7, "ccc"))));

    // Check records in the data file.
    DataFile dataFile = result.dataFiles()[0];
    assertThat(readRecordsAsList(table.schema(), dataFile.path()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(5, "aaa"), createRecord(6, "aaa"), createRecord(7, "ccc")));

    // Check records in the eq-delete file.
    DeleteFile eqDeleteFile = result.deleteFiles()[0];
    assertThat(eqDeleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(readRecordsAsList(eqDeleteRowSchema, eqDeleteFile.path()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(3, "aaa"), createRecord(4, "ccc"), createRecord(2, "bbb")));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[1];
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(readRecordsAsList(posDeleteSchema, posDeleteFile.path()))
        .isEqualTo(ImmutableList.of(posRecord.copy("file_path", dataFile.path(), "pos", 0L)));
  }

  @TestTemplate
  public void testDeleteFileGranularity() throws IOException {
    withGranularity(DeleteGranularity.FILE);
  }

  @TestTemplate
  public void testDeletePartitionGranularity() throws IOException {
    withGranularity(DeleteGranularity.PARTITION);
  }

  private void withGranularity(DeleteGranularity granularity) throws IOException {
    List<Integer> eqDeleteFieldIds = Lists.newArrayList(idFieldId, dataFieldId);
    Schema eqDeleteRowSchema = table.schema();

    GenericTaskDeltaWriter deltaWriter =
        createTaskWriter(eqDeleteFieldIds, eqDeleteRowSchema, granularity);

    Map<Integer, Record> expected = Maps.newHashMapWithExpectedSize(2000);
    // Create enough records, so we have multiple files
    for (int i = 0; i < 2000; ++i) {
      Record record = createRecord(i, "aaa" + i);
      deltaWriter.write(record);
      if (i % 5 == 10) {
        deltaWriter.delete(record);
      } else {
        expected.put(i, record);
      }
    }

    // Add some deletes in the end
    for (int i = 0; i < 199; ++i) {
      int id = i * 10 + 1;
      Record record = createRecord(id, "aaa" + id);
      deltaWriter.delete(record);
      expected.remove(id);
    }

    WriteResult result = deltaWriter.complete();
    assertThat(result.dataFiles()).as("Should have 2 data files.").hasSize(2);
    assertThat(result.deleteFiles())
        .as("Should have correct number of pos-delete files")
        .hasSize(granularity.equals(DeleteGranularity.FILE) ? 2 : 1);

    commitTransaction(result);
    assertThat(actualRowSet("*"))
        .as("Should have an expected record")
        .isEqualTo(expectedRowSet(expected.values()));
  }

  private void commitTransaction(WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

    rowDelta
        .validateDeletedFiles()
        .validateDataFilesExist(Lists.newArrayList(result.referencedDataFiles()))
        .commit();
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

  /**
   * Create a generic task equality delta writer.
   *
   * @param equalityFieldIds defines the equality field ids.
   * @param eqDeleteRowSchema defines the schema of rows that eq-delete writer will write, it could
   *     be the entire fields of the table schema.
   */
  private GenericTaskDeltaWriter createTaskWriter(
      List<Integer> equalityFieldIds,
      Schema eqDeleteRowSchema,
      DeleteGranularity deleteGranularity) {
    FileAppenderFactory<Record> appenderFactory =
        new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            ArrayUtil.toIntArray(equalityFieldIds),
            eqDeleteRowSchema,
            null);

    List<String> columns = Lists.newArrayList();
    for (Integer fieldId : equalityFieldIds) {
      columns.add(table.schema().findField(fieldId).name());
    }
    Schema deleteSchema = table.schema().select(columns);

    return new GenericTaskDeltaWriter(
        table.schema(),
        deleteSchema,
        table.spec(),
        format,
        appenderFactory,
        fileFactory,
        table.io(),
        TARGET_FILE_SIZE,
        deleteGranularity);
  }

  private static class GenericTaskDeltaWriter extends BaseTaskWriter<Record> {
    private final GenericEqualityDeltaWriter deltaWriter;

    private GenericTaskDeltaWriter(
        Schema schema,
        Schema deleteSchema,
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<Record> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        DeleteGranularity deleteGranularity) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.deltaWriter =
          new GenericEqualityDeltaWriter(null, schema, deleteSchema, deleteGranularity);
    }

    @Override
    public void write(Record row) throws IOException {
      deltaWriter.write(row);
    }

    public void delete(Record row) throws IOException {
      deltaWriter.delete(row);
    }

    // The caller of this function is responsible for passing in a record with only the key fields
    public void deleteKey(Record key) throws IOException {
      deltaWriter.deleteKey(key);
    }

    @Override
    public void close() throws IOException {
      deltaWriter.close();
    }

    private class GenericEqualityDeltaWriter extends BaseEqualityDeltaWriter {
      private GenericEqualityDeltaWriter(
          PartitionKey partition,
          Schema schema,
          Schema eqDeleteSchema,
          DeleteGranularity deleteGranularity) {
        super(partition, schema, eqDeleteSchema, deleteGranularity);
      }

      @Override
      protected StructLike asStructLike(Record row) {
        return row;
      }

      @Override
      protected StructLike asStructLikeKey(Record data) {
        return data;
      }
    }
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
