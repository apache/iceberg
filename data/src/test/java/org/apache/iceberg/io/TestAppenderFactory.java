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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestAppenderFactory<T> extends TestBase {
  private static final int FORMAT_V2 = 2;

  private PartitionKey partition = null;
  private OutputFileFactory fileFactory = null;

  @Parameter(index = 1)
  protected FileFormat format;

  @Parameter(index = 2)
  private boolean partitioned;

  @Parameters(name = "formatVersion = {0}, FileFormat={1}, partitioned={2}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {FORMAT_V2, FileFormat.AVRO, false},
        new Object[] {FORMAT_V2, FileFormat.AVRO, true},
        new Object[] {FORMAT_V2, FileFormat.ORC, false},
        new Object[] {FORMAT_V2, FileFormat.ORC, true},
        new Object[] {FORMAT_V2, FileFormat.PARQUET, false},
        new Object[] {FORMAT_V2, FileFormat.PARQUET, true});
  }

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");

    if (partitioned) {
      this.table = create(SCHEMA, SPEC);
    } else {
      this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    }
    this.partition = createPartitionKey();
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();

    table.updateProperties().defaultFormat(format).commit();
  }

  protected abstract FileAppenderFactory<T> createAppenderFactory(
      List<Integer> equalityFieldIds, Schema eqDeleteSchema, Schema posDeleteRowSchema);

  protected abstract T createRow(Integer id, String data);

  protected abstract StructLikeSet expectedRowSet(Iterable<T> records) throws IOException;

  private StructLikeSet actualRowSet(String... columns) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }

  private PartitionKey createPartitionKey() {
    if (table.spec().isUnpartitioned()) {
      return null;
    }

    Record record = GenericRecord.create(table.schema()).copy(ImmutableMap.of("data", "aaa"));

    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    partitionKey.partition(record);

    return partitionKey;
  }

  private EncryptedOutputFile createEncryptedOutputFile() {
    if (partition == null) {
      return fileFactory.newOutputFile();
    } else {
      return fileFactory.newOutputFile(partition);
    }
  }

  private List<T> testRowSet() {
    return Lists.newArrayList(
        createRow(1, "aaa"),
        createRow(2, "bbb"),
        createRow(3, "ccc"),
        createRow(4, "ddd"),
        createRow(5, "eee"));
  }

  private DataFile prepareDataFile(List<T> rowSet, FileAppenderFactory<T> appenderFactory)
      throws IOException {
    DataWriter<T> writer =
        appenderFactory.newDataWriter(createEncryptedOutputFile(), format, partition);
    try (DataWriter<T> closeableWriter = writer) {
      for (T row : rowSet) {
        closeableWriter.write(row);
      }
    }

    return writer.toDataFile();
  }

  @TestTemplate
  public void testDataWriter() throws IOException {
    FileAppenderFactory<T> appenderFactory = createAppenderFactory(null, null, null);

    List<T> rowSet = testRowSet();
    DataFile dataFile = prepareDataFile(rowSet, appenderFactory);

    table.newRowDelta().addRows(dataFile).commit();

    assertThat(actualRowSet("*"))
        .as("Should have the expected records.")
        .isEqualTo(expectedRowSet(rowSet));
  }

  @TestTemplate
  public void testEqDeleteWriter() throws IOException {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField("id").fieldId());
    Schema eqDeleteRowSchema = table.schema().select("id");
    FileAppenderFactory<T> appenderFactory =
        createAppenderFactory(equalityFieldIds, eqDeleteRowSchema, null);

    List<T> rowSet = testRowSet();
    DataFile dataFile = prepareDataFile(rowSet, appenderFactory);

    table.newRowDelta().addRows(dataFile).commit();

    // The equality field is 'id'. No matter what the value  of 'data' field is, we should delete
    // the 1th, 3th, 5th
    // rows.
    List<T> deletes =
        Lists.newArrayList(createRow(1, "aaa"), createRow(3, "bbb"), createRow(5, "ccc"));
    EncryptedOutputFile out = createEncryptedOutputFile();
    EqualityDeleteWriter<T> eqDeleteWriter =
        appenderFactory.newEqDeleteWriter(out, format, partition);
    try (EqualityDeleteWriter<T> closeableWriter = eqDeleteWriter) {
      closeableWriter.write(deletes);
    }

    // Check that the delete equality file has the expected equality deletes.
    GenericRecord gRecord = GenericRecord.create(eqDeleteRowSchema);
    Set<Record> expectedDeletes =
        Sets.newHashSet(gRecord.copy("id", 1), gRecord.copy("id", 3), gRecord.copy("id", 5));
    assertThat(
            Sets.newHashSet(
                createReader(eqDeleteRowSchema, out.encryptingOutputFile().toInputFile())))
        .isEqualTo(expectedDeletes);

    table.newRowDelta().addDeletes(eqDeleteWriter.toDeleteFile()).commit();

    List<T> expected = Lists.newArrayList(createRow(2, "bbb"), createRow(4, "ddd"));
    assertThat(actualRowSet("*"))
        .as("Should have the expected records")
        .isEqualTo(expectedRowSet(expected));
  }

  @TestTemplate
  public void testPosDeleteWriter() throws IOException {
    // Initialize FileAppenderFactory without pos-delete row schema.
    FileAppenderFactory<T> appenderFactory = createAppenderFactory(null, null, null);

    List<T> rowSet = testRowSet();
    DataFile dataFile = prepareDataFile(rowSet, appenderFactory);

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 0L),
            Pair.of(dataFile.path(), 2L),
            Pair.of(dataFile.path(), 4L));

    EncryptedOutputFile out = createEncryptedOutputFile();
    PositionDeleteWriter<T> eqDeleteWriter =
        appenderFactory.newPosDeleteWriter(out, format, partition);
    PositionDelete<T> posDelete = PositionDelete.create();
    try (PositionDeleteWriter<T> closeableWriter = eqDeleteWriter) {
      for (Pair<CharSequence, Long> delete : deletes) {
        closeableWriter.write(posDelete.set(delete.first(), delete.second(), null));
      }
    }

    // Check that the pos delete file has the expected pos deletes.
    Schema pathPosSchema = DeleteSchemaUtil.pathPosSchema();
    GenericRecord gRecord = GenericRecord.create(pathPosSchema);
    Set<Record> expectedDeletes =
        Sets.newHashSet(
            gRecord.copy("file_path", dataFile.path(), "pos", 0L),
            gRecord.copy("file_path", dataFile.path(), "pos", 2L),
            gRecord.copy("file_path", dataFile.path(), "pos", 4L));
    assertThat(
            Sets.newHashSet(createReader(pathPosSchema, out.encryptingOutputFile().toInputFile())))
        .isEqualTo(expectedDeletes);

    table
        .newRowDelta()
        .addRows(dataFile)
        .addDeletes(eqDeleteWriter.toDeleteFile())
        .validateDataFilesExist(eqDeleteWriter.referencedDataFiles())
        .validateDeletedFiles()
        .commit();

    List<T> expected = Lists.newArrayList(createRow(2, "bbb"), createRow(4, "ddd"));
    assertThat(actualRowSet("*"))
        .as("Should have the expected records")
        .isEqualTo(expectedRowSet(expected));
  }

  @TestTemplate
  public void testPosDeleteWriterWithRowSchema() throws IOException {
    FileAppenderFactory<T> appenderFactory = createAppenderFactory(null, null, table.schema());

    List<T> rowSet = testRowSet();
    DataFile dataFile = prepareDataFile(rowSet, appenderFactory);

    List<PositionDelete<T>> deletes =
        Lists.newArrayList(
            positionDelete(dataFile.path(), 0, rowSet.get(0)),
            positionDelete(dataFile.path(), 2, rowSet.get(2)),
            positionDelete(dataFile.path(), 4, rowSet.get(4)));

    EncryptedOutputFile out = createEncryptedOutputFile();
    PositionDeleteWriter<T> eqDeleteWriter =
        appenderFactory.newPosDeleteWriter(out, format, partition);
    PositionDelete<T> posDelete = PositionDelete.create();
    try (PositionDeleteWriter<T> closeableWriter = eqDeleteWriter) {
      for (PositionDelete<T> delete : deletes) {
        closeableWriter.write(posDelete.set(delete.path(), delete.pos(), delete.row()));
      }
    }

    // Check that the pos delete file has the expected pos deletes.
    Schema pathPosRowSchema = DeleteSchemaUtil.posDeleteSchema(table.schema());
    GenericRecord gRecord = GenericRecord.create(pathPosRowSchema);
    GenericRecord rowRecord = GenericRecord.create(table.schema());
    Set<Record> expectedDeletes =
        Sets.newHashSet(
            gRecord.copy(
                "file_path",
                dataFile.path(),
                "pos",
                0L,
                "row",
                rowRecord.copy("id", 1, "data", "aaa")),
            gRecord.copy(
                "file_path",
                dataFile.path(),
                "pos",
                2L,
                "row",
                rowRecord.copy("id", 3, "data", "ccc")),
            gRecord.copy(
                "file_path",
                dataFile.path(),
                "pos",
                4L,
                "row",
                rowRecord.copy("id", 5, "data", "eee")));
    assertThat(
            Sets.newHashSet(
                createReader(pathPosRowSchema, out.encryptingOutputFile().toInputFile())))
        .isEqualTo(expectedDeletes);

    table
        .newRowDelta()
        .addRows(dataFile)
        .addDeletes(eqDeleteWriter.toDeleteFile())
        .validateDataFilesExist(eqDeleteWriter.referencedDataFiles())
        .validateDeletedFiles()
        .commit();

    List<T> expected = Lists.newArrayList(createRow(2, "bbb"), createRow(4, "ddd"));
    assertThat(actualRowSet("*"))
        .as("Should have the expected records")
        .isEqualTo(expectedRowSet(expected));
  }

  private CloseableIterable<Record> createReader(Schema schema, InputFile inputFile) {
    switch (format) {
      case PARQUET:
        return Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build();

      case AVRO:
        return Avro.read(inputFile).project(schema).createReaderFunc(DataReader::create).build();

      case ORC:
        return ORC.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
            .build();

      default:
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }
  }
}
