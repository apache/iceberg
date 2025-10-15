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
import org.apache.iceberg.Table;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDeleteIndex;
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

  @Parameters(name = "formatVersion = {0}, FileFormat = {1}")
  protected static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (FileFormat format :
        new FileFormat[] {FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET}) {
      for (int version : TestHelpers.V2_AND_ABOVE) {
        parameters.add(new Object[] {version, format});
      }
    }
    return parameters;
  }

  @Override
  @BeforeEach
  public void setupTable() throws IOException {
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
    assertThat(readRecordsAsList(table.schema(), dataFile.location()))
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

    if (formatVersion == FORMAT_V2) {
      // Check records in the pos-delete file.
      Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
      assertThat(readRecordsAsList(posDeleteSchema, posDeleteFile.location()))
          .isEqualTo(
              ImmutableList.of(
                  posRecord.copy("file_path", dataFile.location(), "pos", 0L),
                  posRecord.copy("file_path", dataFile.location(), "pos", 1L),
                  posRecord.copy("file_path", dataFile.location(), "pos", 2L),
                  posRecord.copy("file_path", dataFile.location(), "pos", 3L)));
    } else {
      assertThat(posDeleteFile.format()).isEqualTo(FileFormat.PUFFIN);
      PositionDeleteIndex positionDeleteIndex = readDVFile(table, posDeleteFile);
      assertThat(positionDeleteIndex.cardinality()).isEqualTo(4);
      assertThat(positionDeleteIndex.isDeleted(0L)).isTrue();
      assertThat(positionDeleteIndex.isDeleted(1L)).isTrue();
      assertThat(positionDeleteIndex.isDeleted(2L)).isTrue();
      assertThat(positionDeleteIndex.isDeleted(3L)).isTrue();
    }
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
    assertThat(readRecordsAsList(table.schema(), dataFile.location()))
        .isEqualTo(ImmutableList.of(record, record));

    DeleteFile posDeleteFile = result.deleteFiles()[0];
    if (formatVersion == FORMAT_V2) {
      // Check records in the pos-delete file.
      assertThat(readRecordsAsList(DeleteSchemaUtil.pathPosSchema(), posDeleteFile.location()))
          .isEqualTo(ImmutableList.of(posRecord.copy("file_path", dataFile.location(), "pos", 0L)));
    } else {
      assertThat(posDeleteFile.format()).isEqualTo(FileFormat.PUFFIN);
      PositionDeleteIndex positionDeleteIndex = readDVFile(table, posDeleteFile);
      assertThat(positionDeleteIndex.cardinality()).isEqualTo(1);
      assertThat(positionDeleteIndex.isDeleted(0L)).isTrue();
    }

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
    assertThat(readRecordsAsList(table.schema(), dataFile.location()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(5, "aaa"), createRecord(6, "aaa"), createRecord(7, "ccc")));

    // Check records in the eq-delete file.
    DeleteFile eqDeleteFile = result.deleteFiles()[0];
    assertThat(eqDeleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(readRecordsAsList(eqDeleteRowSchema, eqDeleteFile.location()))
        .isEqualTo(
            ImmutableList.of(keyFunc.apply("aaa"), keyFunc.apply("ccc"), keyFunc.apply("bbb")));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[1];
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);

    if (formatVersion == FORMAT_V2) {
      assertThat(readRecordsAsList(posDeleteSchema, posDeleteFile.location()))
          .isEqualTo(ImmutableList.of(posRecord.copy("file_path", dataFile.location(), "pos", 0L)));
    } else {
      assertThat(posDeleteFile.format()).isEqualTo(FileFormat.PUFFIN);
      PositionDeleteIndex positionDeleteIndex = readDVFile(table, posDeleteFile);
      assertThat(positionDeleteIndex.cardinality()).isEqualTo(1);
      assertThat(positionDeleteIndex.isDeleted(0L)).isTrue();
    }
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
    assertThat(readRecordsAsList(table.schema(), dataFile.location()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(5, "aaa"), createRecord(6, "aaa"), createRecord(7, "ccc")));

    // Check records in the eq-delete file.
    DeleteFile eqDeleteFile = result.deleteFiles()[0];
    assertThat(eqDeleteFile.content()).isEqualTo(FileContent.EQUALITY_DELETES);
    assertThat(readRecordsAsList(eqDeleteRowSchema, eqDeleteFile.location()))
        .isEqualTo(
            ImmutableList.of(
                createRecord(3, "aaa"), createRecord(4, "ccc"), createRecord(2, "bbb")));

    // Check records in the pos-delete file.
    DeleteFile posDeleteFile = result.deleteFiles()[1];
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    assertThat(posDeleteFile.content()).isEqualTo(FileContent.POSITION_DELETES);
    if (formatVersion == FORMAT_V2) {
      assertThat(readRecordsAsList(posDeleteSchema, posDeleteFile.location()))
          .isEqualTo(ImmutableList.of(posRecord.copy("file_path", dataFile.location(), "pos", 0L)));
    } else {
      assertThat(posDeleteFile.format()).isEqualTo(FileFormat.PUFFIN);
      PositionDeleteIndex positionDeleteIndex = readDVFile(table, posDeleteFile);
      assertThat(positionDeleteIndex.cardinality()).isEqualTo(1);
      assertThat(positionDeleteIndex.isDeleted(0L)).isTrue();
    }
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
    int expectedDeleteCount = 0;
    // Create enough records, so we have multiple files
    for (int i = 0; i < 2000; ++i) {
      Record record = createRecord(i, "aaa" + i);
      deltaWriter.write(record);
      if (i % 5 == 0) {
        deltaWriter.delete(record);
        ++expectedDeleteCount;
      } else {
        expected.put(i, record);
      }
    }

    // Add some deletes in the end
    for (int i = 0; i < 199; ++i) {
      int id = i * 10 + 1;
      Record record = createRecord(id, "aaa" + id);
      deltaWriter.delete(record);
      ++expectedDeleteCount;
      expected.remove(id);
    }

    WriteResult result = deltaWriter.complete();

    // Should have 2 files, as BaseRollingWriter checks the size on every 1000 rows (ROWS_DIVISOR)
    assertThat(result.dataFiles()).as("Should have 2 data files.").hasSize(2);
    if (formatVersion == FORMAT_V2) {
      assertThat(result.deleteFiles())
          .as("Should have correct number of pos-delete files")
          .hasSize(granularity.equals(DeleteGranularity.FILE) ? 2 : 1);
    }

    assertThat(Arrays.stream(result.deleteFiles()).mapToLong(delete -> delete.recordCount()).sum())
        .isEqualTo(expectedDeleteCount);

    commitTransaction(result);
    assertThat(actualRowSet("*"))
        .as("Should have expected record")
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
    FileWriterFactory<Record> fileWriterFactory =
        new GenericFileWriterFactory.Builder(table)
            .dataFileFormat(format)
            .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
            .equalityDeleteRowSchema(eqDeleteRowSchema)
            .build();

    List<String> columns = Lists.newArrayList();
    for (Integer fieldId : equalityFieldIds) {
      columns.add(table.schema().findField(fieldId).name());
    }
    Schema deleteSchema = table.schema().select(columns);

    boolean useDv = formatVersion > 2;
    return new GenericTaskDeltaWriter(
        table.schema(),
        deleteSchema,
        table.spec(),
        format,
        fileWriterFactory,
        fileFactory,
        table.io(),
        TARGET_FILE_SIZE,
        deleteGranularity,
        useDv);
  }

  private static class GenericTaskDeltaWriter extends BaseTaskWriter<Record> {
    private final GenericEqualityDeltaWriter deltaWriter;

    private GenericTaskDeltaWriter(
        Schema schema,
        Schema deleteSchema,
        PartitionSpec spec,
        FileFormat format,
        FileWriterFactory<Record> fileWriterFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        DeleteGranularity deleteGranularity,
        boolean useDv) {
      super(spec, format, fileWriterFactory, fileFactory, io, targetFileSize);
      PartitioningDVWriter<Object> dvWriter =
          useDv ? new PartitioningDVWriter<>(fileFactory, p -> null) : null;
      this.deltaWriter =
          new GenericEqualityDeltaWriter(null, schema, deleteSchema, deleteGranularity, dvWriter);
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
          DeleteGranularity deleteGranularity,
          PartitioningDVWriter dvWriter) {
        super(partition, schema, eqDeleteSchema, deleteGranularity, dvWriter);
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
            Avro.read(inputFile)
                .project(schema)
                .createResolvingReader(PlannedDataReader::create)
                .build();
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

  private PositionDeleteIndex readDVFile(Table table, DeleteFile dvFile) {
    BaseDeleteLoader deleteLoader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.location()));
    PositionDeleteIndex index =
        deleteLoader.loadPositionDeletes(List.of(dvFile), dvFile.referencedDataFile());
    return index;
  }
}
