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
import java.util.Locale;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
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
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
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
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
class TestSparkParquetReaderColumnFiles {
  private static final String TABLE_NAME = "unaligned_column_files";

  @Parameters(name = "rows = {0}, base file row group = {1}, column file row groups = {2}")
  private static Object[][] parameters() {
    return new Object[][] {
      {10, 1, List.of(1, 1)},
      {10, 5, List.of(1, 1)},
      {10, 1, List.of(4, 3)},
      {10, 3, List.of(4, 7)},
      {10, 10, List.of(2, 5)},
      {10, 4, List.of(10)}
    };
  }

  @Parameter(index = 0)
  private int rowCount;

  @Parameter(index = 1)
  private int baseRowsPerRowGroup;

  @Parameter(index = 2)
  private List<Integer> columnFileRowsPerRowGroup;

  @TempDir private Path temp;

  private Schema schema;
  private Table table;
  private DataFile dataFile;

  @BeforeEach
  void createTable() throws IOException {
    List<Types.NestedField> fields = Lists.newArrayList(required(1, "id", Types.IntegerType.get()));
    for (int column = 1; column <= columnFileRowsPerRowGroup.size(); column += 1) {
      fields.add(optional(column + 1, "c" + column, Types.StringType.get()));
    }

    this.schema = new Schema(fields);
    this.table =
        TestTables.create(temp.toFile(), TABLE_NAME, schema, PartitionSpec.unpartitioned());

    DataFile baseFile = write("base", schema.select("id"), baseRowsPerRowGroup);

    List<ColumnFile> columnFiles = Lists.newArrayList();
    for (int column = 1; column <= columnFileRowsPerRowGroup.size(); column += 1) {
      DataFile file =
          write(
              "column-" + column,
              schema.select("c" + column),
              columnFileRowsPerRowGroup.get(column - 1));

      columnFiles.add(
          ColumnFiles.builder()
              .withFormatVersion(4)
              .withFieldIds(List.of(column + 1))
              .withLocation(file.location())
              .withFileFormat(file.format())
              .withFileSizeInBytes(file.fileSizeInBytes())
              .build());
    }

    this.dataFile =
        ColumnFileTestHelpers.withColumnFiles(baseFile, PartitionSpec.unpartitioned(), columnFiles);
  }

  @AfterEach
  void dropTable() {
    TestTables.drop(TABLE_NAME);
  }

  @TestTemplate
  void readAllRows() throws IOException {
    assertThat(read(task(Expressions.alwaysTrue()))).containsExactlyElementsOf(rows(pos -> true));
  }

  @TestTemplate
  void readEachSplitOfDataFile() throws IOException {
    List<FileScanTask> splits = Lists.newArrayList(task(Expressions.alwaysTrue()).split(1));
    assertThat(splits).hasSize(rowGroupCount(baseRowsPerRowGroup));

    for (int split = 0; split < splits.size(); split += 1) {
      int rowGroup = split;
      assertThat(read(splits.get(split)))
          .containsExactlyElementsOf(rows(pos -> pos / baseRowsPerRowGroup == rowGroup));
    }
  }

  @TestTemplate
  void skipRowGroupsFilteredFromDataFile() throws IOException {
    int target = rowCount / 2;
    int rowGroup = target / baseRowsPerRowGroup;

    assertThat(read(task(Expressions.equal("id", target))))
        .containsExactlyElementsOf(rows(pos -> pos / baseRowsPerRowGroup == rowGroup));
  }

  @TestTemplate
  void skipRowGroupsFilteredFromEachColumnFile() throws IOException {
    int target = rowCount / 2;
    for (int column = 1; column <= columnFileRowsPerRowGroup.size(); column += 1) {
      int rowsPerRowGroup = columnFileRowsPerRowGroup.get(column - 1);
      int rowGroup = target / rowsPerRowGroup;

      assertThat(read(task(Expressions.equal("c" + column, value(column, target)))))
          .containsExactlyElementsOf(rows(pos -> pos / rowsPerRowGroup == rowGroup));
    }
  }

  private int rowGroupCount(int rowsPerRowGroup) {
    return (rowCount + rowsPerRowGroup - 1) / rowsPerRowGroup;
  }

  // zero-padded so that string order matches row order for row group min/max filtering
  private static String value(int column, int pos) {
    return String.format(Locale.ROOT, "c%d-%05d", column, pos);
  }

  private List<List<Object>> rows(IntPredicate selected) {
    return IntStream.range(0, rowCount)
        .filter(selected)
        .mapToObj(
            pos -> {
              List<Object> row = Lists.newArrayList(pos);
              for (int column = 1; column <= columnFileRowsPerRowGroup.size(); column += 1) {
                row.add(value(column, pos));
              }

              return row;
            })
        .toList();
  }

  private FileScanTask task(Expression residual, DeleteFile... deletes) {
    return new BaseFileScanTask(
        dataFile,
        deletes,
        SchemaParser.toJson(table.schema()),
        PartitionSpecParser.toJson(table.spec()),
        ResidualEvaluator.unpartitioned(residual));
  }

  private List<List<Object>> read(FileScanTask task) throws IOException {
    List<List<Object>> rows = Lists.newArrayList();
    try (RowDataReader reader =
        new RowDataReader(
            table,
            table.io(),
            new BaseScanTaskGroup<>(ImmutableList.of(task)),
            schema,
            false,
            true)) {
      while (reader.next()) {
        InternalRow row = reader.get();
        List<Object> values = Lists.newArrayList(row.getInt(0));
        for (int column = 1; column <= columnFileRowsPerRowGroup.size(); column += 1) {
          values.add(row.getUTF8String(column).toString());
        }

        rows.add(values);
      }
    }

    return rows;
  }

  private DataFile write(String name, Schema fileSchema, int rowsPerRowGroup) throws IOException {
    OutputFile out =
        Files.localOutput(temp.resolve(FileFormat.PARQUET.addExtension(name)).toFile());
    DataWriter<Record> writer =
        FormatModelRegistry.<Record, Object>dataWriteBuilder(
                FileFormat.PARQUET, Record.class, EncryptedFiles.plainAsEncryptedOutput(out))
            .schema(fileSchema)
            .spec(PartitionSpec.unpartitioned())
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "1")
            .set(
                TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
                String.valueOf(rowsPerRowGroup))
            .set(
                TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT,
                String.valueOf(rowsPerRowGroup))
            .build();

    GenericRecord record = GenericRecord.create(fileSchema);
    try (writer) {
      for (int pos = 0; pos < rowCount; pos += 1) {
        for (Types.NestedField field : fileSchema.columns()) {
          int column = field.fieldId() - 1;
          record.setField(field.name(), column == 0 ? pos : value(column, pos));
        }

        writer.write(record.copy());
      }
    }

    DataFile file = writer.toDataFile();
    assertThat(file.splitOffsets()).hasSize(rowGroupCount(rowsPerRowGroup));
    return file;
  }
}
