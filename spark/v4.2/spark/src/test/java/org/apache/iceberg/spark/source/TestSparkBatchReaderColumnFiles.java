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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.ImmutableParquetBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
class TestSparkBatchReaderColumnFiles {
  private static final String TABLE_NAME = "column_files_batch";
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "amount", Types.LongType.get()),
          optional(4, "category", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("category").build();
  private static final int ROWS = 10;
  private static final int BASE_ROWS_PER_ROW_GROUP = 4;
  private static final int DATA_ROWS_PER_ROW_GROUP = 7;
  private static final int AMOUNT_ROWS_PER_ROW_GROUP = 5;
  private static final Set<Long> DELETED = ImmutableSet.of(1L, 4L, 5L, 9L);

  @Parameters(name = "batchSize = {0}")
  private static List<Integer> parameters() {
    return List.of(3, 6);
  }

  @Parameter private int batchSize;

  @TempDir private Path temp;

  private Table table;
  private StructLike partition;
  private DataFile dataFile;

  @BeforeEach
  void createTable() throws IOException {
    this.table = TestTables.create(temp.toFile(), TABLE_NAME, SCHEMA, SPEC);
    this.partition = GenericRecord.create(SPEC.partitionType()).copy("category", "x");

    // the row groups of the three files and the batches do not line up
    DataFile baseFile =
        write(
            "base",
            SCHEMA,
            SPEC,
            partition,
            BASE_ROWS_PER_ROW_GROUP,
            pos -> new Object[] {(int) pos, "base-" + pos, pos, "x"});
    DataFile dataUpdates =
        write(
            "data",
            SCHEMA.select("data"),
            PartitionSpec.unpartitioned(),
            null,
            DATA_ROWS_PER_ROW_GROUP,
            pos -> new Object[] {data(pos)});
    DataFile amountUpdates =
        write(
            "amount",
            SCHEMA.select("amount"),
            PartitionSpec.unpartitioned(),
            null,
            AMOUNT_ROWS_PER_ROW_GROUP,
            pos -> new Object[] {amount(pos)});

    this.dataFile =
        ColumnFileTestHelpers.withColumnFiles(
            baseFile,
            SPEC,
            List.of(columnFile(dataUpdates, "data"), columnFile(amountUpdates, "amount")));
  }

  @AfterEach
  void dropTable() {
    TestTables.drop(TABLE_NAME);
  }

  @TestTemplate
  void readOneColumnFile() throws IOException {
    assertSameRows(task(Expressions.alwaysTrue()), SCHEMA.select("id", "data"), 0);
  }

  @TestTemplate
  void readTwoColumnFiles() throws IOException {
    assertSameRows(task(Expressions.alwaysTrue()), SCHEMA, 0);
  }

  @TestTemplate
  void readOnlyFieldsFromColumnFiles() throws IOException {
    assertSameRows(task(Expressions.alwaysTrue()), SCHEMA.select("data", "amount"), 0);
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
    FileScanTask task = task(Expressions.alwaysTrue());

    assertThat(readBatches(task, projection)).hasSize(ROWS).isEqualTo(readRows(task, projection));
  }

  @TestTemplate
  void skipRowGroupsFilteredFromColumnFile() throws IOException {
    // the first row group of the amount file is skipped, and Spark filters the remaining rows
    assertSameRows(
        task(Expressions.greaterThanOrEqual("amount", 600L)), SCHEMA, AMOUNT_ROWS_PER_ROW_GROUP);
  }

  @TestTemplate
  void readSplitsOfDataFile() throws IOException {
    List<FileScanTask> splits = Lists.newArrayList(task(Expressions.alwaysTrue()).split(1));
    assertThat(splits).hasSize((ROWS + BASE_ROWS_PER_ROW_GROUP - 1) / BASE_ROWS_PER_ROW_GROUP);

    List<InternalRow> batchRows = Lists.newArrayList();
    List<InternalRow> rows = Lists.newArrayList();
    for (FileScanTask split : splits) {
      batchRows.addAll(readBatches(split, SCHEMA));
      rows.addAll(readRows(split, SCHEMA));
    }

    assertThat(batchRows).isEqualTo(rows).isEqualTo(expected(SCHEMA, 0));
  }

  @TestTemplate
  void applyDeletionVector() throws IOException {
    FileScanTask task = task(Expressions.alwaysTrue(), deletionVector());
    List<InternalRow> live =
        expected(SCHEMA, 0).stream()
            .filter(row -> !DELETED.contains((long) row.getInt(0)))
            .collect(Collectors.toList());

    assertThat(readBatches(task, SCHEMA)).isEqualTo(readRows(task, SCHEMA)).isEqualTo(live);
  }

  @TestTemplate
  void applyEqualityDeleteOnUpdatedField() throws IOException {
    FileScanTask task = task(Expressions.alwaysTrue(), equalityDelete(data(2), data(7)));
    Schema projection = SCHEMA.select("id");

    assertThat(readBatches(task, projection)).isNotEmpty().isEqualTo(readRows(task, projection));
  }

  @TestTemplate
  void readIsDeletedMetadataColumn() throws IOException {
    Schema projection = new Schema(SCHEMA.findField("data"), MetadataColumns.IS_DELETED);
    assertSameRows(task(Expressions.alwaysTrue(), deletionVector()), projection, 0);
  }

  private void assertSameRows(FileScanTask task, Schema projection, long from) throws IOException {
    assertThat(readBatches(task, projection))
        .isEqualTo(readRows(task, projection))
        .isEqualTo(expected(projection, from));
  }

  private static String data(long pos) {
    return pos % 3 == 0 ? null : "updated-" + pos;
  }

  private static Long amount(long pos) {
    return pos % 4 == 0 ? null : pos * 100;
  }

  private static List<InternalRow> expected(Schema projection, long from) {
    return LongStream.range(from, ROWS)
        .mapToObj(
            pos ->
                new GenericInternalRow(
                    projection.columns().stream().map(field -> value(field, pos)).toArray()))
        .collect(Collectors.toList());
  }

  private static Object value(Types.NestedField field, long pos) {
    return switch (field.name()) {
      case "id" -> (int) pos;
      case "data" -> data(pos) == null ? null : UTF8String.fromString(data(pos));
      case "amount" -> amount(pos);
      case "category" -> UTF8String.fromString("x");
      case "_deleted" -> DELETED.contains(pos);
      default -> throw new IllegalArgumentException("Unknown field: " + field);
    };
  }

  private List<InternalRow> readBatches(FileScanTask task, Schema projection) throws IOException {
    ParquetBatchReadConf conf =
        ImmutableParquetBatchReadConf.builder().batchSize(batchSize).build();
    StructType type = SparkSchemaUtil.convert(projection);
    List<InternalRow> rows = Lists.newArrayList();
    try (BatchDataReader reader =
        new BatchDataReader(
            table,
            table.io(),
            new BaseScanTaskGroup<>(ImmutableList.of(task)),
            projection,
            false,
            conf,
            null,
            true)) {
      while (reader.next()) {
        reader.get().rowIterator().forEachRemaining(row -> rows.add(projected(row, type)));
      }
    }

    return rows;
  }

  private List<InternalRow> readRows(FileScanTask task, Schema projection) throws IOException {
    StructType type = SparkSchemaUtil.convert(projection);
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
        rows.add(projected(reader.get(), type));
      }
    }

    return rows;
  }

  // readers may add columns needed to apply deletes after the projected ones
  private static InternalRow projected(InternalRow row, StructType type) {
    InternalRow copy = row.copy();
    Object[] values = new Object[type.size()];
    for (int pos = 0; pos < values.length; pos += 1) {
      values[pos] = copy.isNullAt(pos) ? null : copy.get(pos, type.fields()[pos].dataType());
    }

    return new GenericInternalRow(values);
  }

  private DeleteFile deletionVector() throws IOException {
    List<Pair<CharSequence, Long>> deletes =
        DELETED.stream()
            .map(pos -> Pair.<CharSequence, Long>of(dataFile.location(), pos))
            .collect(Collectors.toList());
    OutputFile out = Files.localOutput(temp.resolve("dv.puffin").toFile());
    return FileHelpers.writeDeleteFile(table, out, partition, deletes, 3).first();
  }

  private DeleteFile equalityDelete(String... values) throws IOException {
    Schema deleteSchema = SCHEMA.select("data");
    List<Record> deletes = Lists.newArrayList();
    for (String value : values) {
      deletes.add(GenericRecord.create(deleteSchema).copy("data", value));
    }

    OutputFile out = Files.localOutput(temp.resolve("eq-deletes.parquet").toFile());
    return FileHelpers.writeDeleteFile(table, out, partition, deletes, deleteSchema);
  }

  private FileScanTask task(Expression residual, DeleteFile... deletes) {
    return new BaseFileScanTask(
        dataFile,
        deletes,
        SchemaParser.toJson(table.schema()),
        PartitionSpecParser.toJson(table.spec()),
        ResidualEvaluator.unpartitioned(residual));
  }

  private static ColumnFile columnFile(DataFile file, String field) {
    return ColumnFiles.builder()
        .withFormatVersion(4)
        .withFieldIds(List.of(SCHEMA.findField(field).fieldId()))
        .withLocation(file.location())
        .withFileFormat(file.format())
        .withFileSizeInBytes(file.fileSizeInBytes())
        .build();
  }

  private DataFile write(
      String name,
      Schema schema,
      PartitionSpec spec,
      StructLike partitionData,
      int rowsPerRowGroup,
      LongFunction<Object[]> valuesAt)
      throws IOException {
    OutputFile out =
        Files.localOutput(temp.resolve(FileFormat.PARQUET.addExtension(name)).toFile());
    DataWriter<Record> writer =
        FormatModelRegistry.<Record, Object>dataWriteBuilder(
                FileFormat.PARQUET, Record.class, EncryptedFiles.plainAsEncryptedOutput(out))
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
      for (long pos = 0; pos < ROWS; pos += 1) {
        GenericRecord record = GenericRecord.create(schema);
        Object[] values = valuesAt.apply(pos);
        for (int field = 0; field < values.length; field += 1) {
          record.set(field, values[field]);
        }

        writer.write(record);
      }
    }

    return writer.toDataFile();
  }
}
