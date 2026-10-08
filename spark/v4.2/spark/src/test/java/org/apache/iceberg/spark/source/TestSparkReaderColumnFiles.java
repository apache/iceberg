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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.ColumnFile;
import org.apache.iceberg.ColumnFileTestHelpers;
import org.apache.iceberg.ColumnFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
class TestSparkReaderColumnFiles {
  private static final String TABLE_NAME = "column_files";
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "category", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("category").build();

  @Parameters(name = "format = {0}")
  private static List<FileFormat> parameters() {
    return List.of(FileFormat.PARQUET);
  }

  @Parameter private FileFormat format;

  @TempDir private Path temp;

  private Table table;
  private StructLike partition;
  private ColumnFile columnFile;
  private DataFile dataFile;

  @BeforeEach
  void createTable() throws IOException {
    this.table = TestTables.create(temp.toFile(), TABLE_NAME, SCHEMA, SPEC);
    this.partition = GenericRecord.create(SPEC.partitionType()).copy("category", "x");

    Schema columnFileSchema = SCHEMA.select("data");
    GenericRecord columnRecord = GenericRecord.create(columnFileSchema);
    DataFile columnFileData =
        write(
            "column",
            columnFileSchema,
            PartitionSpec.unpartitioned(),
            null,
            1,
            List.of(
                columnRecord.copy("data", "A"),
                columnRecord.copy("data", "B"),
                columnRecord.copy("data", "C"),
                columnRecord.copy("data", "D")));

    this.columnFile =
        ColumnFiles.builder()
            .withFormatVersion(4)
            .withFieldIds(List.of(SCHEMA.findField("data").fieldId()))
            .withLocation(columnFileData.location())
            .withFileFormat(columnFileData.format())
            .withFileSizeInBytes(columnFileData.fileSizeInBytes())
            .build();

    this.dataFile = dataFile("base", 1);
  }

  @AfterEach
  void dropTable() {
    TestTables.drop(TABLE_NAME);
  }

  @TestTemplate
  void readFieldsFromColumnFile() throws IOException {
    assertThat(read(task(Expressions.alwaysTrue()), SCHEMA))
        .extracting(row -> row.getInt(0), row -> row.getUTF8String(1).toString())
        .containsExactly(tuple(1, "A"), tuple(2, "B"), tuple(3, "C"), tuple(4, "D"));
  }

  @TestTemplate
  void readOnlyFieldsFromColumnFile() throws IOException {
    assertThat(read(task(Expressions.alwaysTrue()), SCHEMA.select("data")))
        .extracting(row -> row.getUTF8String(0).toString())
        .containsExactly("A", "B", "C", "D");
  }

  @TestTemplate
  void readSplitOfDataFile() throws IOException {
    List<FileScanTask> splits = Lists.newArrayList(task(Expressions.alwaysTrue()).split(1));
    assertThat(splits).hasSize(4);

    assertThat(read(splits.get(2), SCHEMA))
        .extracting(row -> row.getInt(0), row -> row.getUTF8String(1).toString())
        .containsExactly(tuple(3, "C"));
  }

  @TestTemplate
  void skipRowsFilteredFromDataFile() throws IOException {
    assertThat(read(task(Expressions.equal("id", 2)), SCHEMA))
        .extracting(row -> row.getInt(0), row -> row.getUTF8String(1).toString())
        .containsExactly(tuple(2, "B"));
  }

  @TestTemplate
  void skipRowsFilteredFromColumnFile() throws IOException {
    assertThat(read(task(Expressions.equal("data", "C")), SCHEMA))
        .extracting(row -> row.getInt(0), row -> row.getUTF8String(1).toString())
        .containsExactly(tuple(3, "C"));
  }

  @TestTemplate
  void readMetadataColumns() throws IOException {
    Schema projection =
        new Schema(
            SCHEMA.findField("data"),
            MetadataColumns.ROW_POSITION,
            MetadataColumns.FILE_PATH,
            MetadataColumns.SPEC_ID,
            MetadataColumns.metadataColumn(table, MetadataColumns.PARTITION_COLUMN_NAME));

    assertThat(read(task(Expressions.alwaysTrue()), projection))
        .extracting(
            row -> row.getUTF8String(0).toString(),
            row -> row.getLong(1),
            row -> row.getUTF8String(2).toString(),
            row -> row.getInt(3),
            row -> row.getStruct(4, 1).getUTF8String(0).toString())
        .containsExactly(
            tuple("A", 0L, dataFile.location(), SPEC.specId(), "x"),
            tuple("B", 1L, dataFile.location(), SPEC.specId(), "x"),
            tuple("C", 2L, dataFile.location(), SPEC.specId(), "x"),
            tuple("D", 3L, dataFile.location(), SPEC.specId(), "x"));
  }

  @TestTemplate
  void readPartitionValues() throws IOException {
    assertThat(read(task(Expressions.alwaysTrue()), SCHEMA.select("data", "category")))
        .extracting(row -> row.getUTF8String(0).toString(), row -> row.getUTF8String(1).toString())
        .containsExactly(tuple("A", "x"), tuple("B", "x"), tuple("C", "x"), tuple("D", "x"));
  }

  @TestTemplate
  void readWithDV() throws IOException {
    assertThat(read(task(Expressions.alwaysTrue(), deletionVector(1L, 3L)), SCHEMA))
        .extracting(row -> row.getInt(0), row -> row.getUTF8String(1).toString())
        .containsExactly(tuple(1, "A"), tuple(3, "C"));
  }

  @TestTemplate
  void readOnlyFromColumnFileAndApplyDV() throws IOException {
    assertThat(read(task(Expressions.alwaysTrue(), deletionVector(1L, 3L)), SCHEMA.select("data")))
        .extracting(row -> row.getUTF8String(0).toString())
        .containsExactly("A", "C");
  }

  @TestTemplate
  void readSplitWithDV() throws IOException {
    DataFile file = dataFile("base-row-group-pairs", 2);
    DeleteFile dv = deletionVector(file, 1L, 3L);
    List<FileScanTask> splits =
        Lists.newArrayList(task(file, Expressions.alwaysTrue(), dv).split(1));
    assertThat(splits).hasSize(2);

    assertThat(read(splits.get(0), SCHEMA))
        .extracting(row -> row.getInt(0), row -> row.getUTF8String(1).toString())
        .containsExactly(tuple(1, "A"));
    assertThat(read(splits.get(1), SCHEMA))
        .extracting(row -> row.getInt(0), row -> row.getUTF8String(1).toString())
        .containsExactly(tuple(3, "C"));
  }

  @TestTemplate
  void readWithIsDeletedMetadataColumn() throws IOException {
    Schema projection = new Schema(SCHEMA.findField("data"), MetadataColumns.IS_DELETED);

    assertThat(read(task(Expressions.alwaysTrue(), deletionVector(1L, 3L)), projection))
        .extracting(row -> row.getUTF8String(0).toString(), row -> row.getBoolean(1))
        .containsExactly(tuple("A", false), tuple("B", true), tuple("C", false), tuple("D", true));
  }

  private DeleteFile deletionVector(Long... positions) throws IOException {
    return deletionVector(dataFile, positions);
  }

  private DeleteFile deletionVector(DataFile file, Long... positions) throws IOException {
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    for (Long position : positions) {
      deletes.add(Pair.of(file.location(), position));
    }

    OutputFile out = Files.localOutput(temp.resolve("dv.puffin").toFile());
    return FileHelpers.writeDeleteFile(table, out, partition, deletes, 3).first();
  }

  private FileScanTask task(Expression residual, DeleteFile... deletes) {
    return task(dataFile, residual, deletes);
  }

  private FileScanTask task(DataFile file, Expression residual, DeleteFile... deletes) {
    return new BaseFileScanTask(
        file,
        deletes,
        SchemaParser.toJson(table.schema()),
        PartitionSpecParser.toJson(table.spec()),
        ResidualEvaluator.unpartitioned(residual));
  }

  private List<InternalRow> read(FileScanTask task, Schema projection) throws IOException {
    List<InternalRow> rows = Lists.newArrayList();
    try (RowDataReader reader =
        new RowDataReader(
            table,
            table.io(),
            new BaseScanTaskGroup<>(ImmutableList.of(task)),
            projection,
            false,
            true)) {
      while (reader.next()) {
        rows.add(reader.get().copy());
      }
    }

    return rows;
  }

  private DataFile dataFile(String name, int rowsPerRowGroup) throws IOException {
    GenericRecord record = GenericRecord.create(SCHEMA);
    DataFile baseFile =
        write(
            name,
            SCHEMA,
            SPEC,
            partition,
            rowsPerRowGroup,
            List.of(
                record.copy("id", 1, "data", "a", "category", "x"),
                record.copy("id", 2, "data", "b", "category", "x"),
                record.copy("id", 3, "data", "c", "category", "x"),
                record.copy("id", 4, "data", "d", "category", "x")));

    return ColumnFileTestHelpers.withColumnFiles(baseFile, SPEC, List.of(columnFile));
  }

  // small row groups, so filters and splits can skip single rows
  private DataFile write(
      String name,
      Schema schema,
      PartitionSpec spec,
      StructLike partitionData,
      int rowsPerRowGroup,
      List<Record> records)
      throws IOException {
    OutputFile out = Files.localOutput(temp.resolve(format.addExtension(name)).toFile());
    DataWriter<Record> writer =
        FormatModelRegistry.<Record, Object>dataWriteBuilder(
                format, Record.class, EncryptedFiles.plainAsEncryptedOutput(out))
            .schema(schema)
            .spec(spec)
            .partition(partitionData)
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
            .set(
                TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
                String.valueOf(rowsPerRowGroup))
            .set(
                TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
                String.valueOf(rowsPerRowGroup))
            .build();

    try (writer) {
      records.forEach(writer::write);
    }

    return writer.toDataFile();
  }
}
