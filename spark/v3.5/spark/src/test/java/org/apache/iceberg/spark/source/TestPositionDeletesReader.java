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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.PositionDeletesTable;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPositionDeletesReader extends TestBase {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private Table table;
  private DataFile dataFile1;
  private DataFile dataFile2;

  @TempDir private Path temp;

  @Parameter(index = 0)
  private int formatVersion;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> parameters() {
    return TestHelpers.V2_AND_ABOVE;
  }

  @BeforeEach
  public void before() throws IOException {
    table =
        catalog.createTable(
            TableIdentifier.of("default", "test"),
            SCHEMA,
            SPEC,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));

    GenericRecord record = GenericRecord.create(table.schema());
    List<Record> records1 = Lists.newArrayList();
    records1.add(record.copy("id", 29, "data", "a"));
    records1.add(record.copy("id", 43, "data", "b"));
    records1.add(record.copy("id", 61, "data", "c"));
    records1.add(record.copy("id", 89, "data", "d"));

    List<Record> records2 = Lists.newArrayList();
    records2.add(record.copy("id", 100, "data", "e"));
    records2.add(record.copy("id", 121, "data", "f"));
    records2.add(record.copy("id", 122, "data", "g"));

    dataFile1 = writeDataFile(records1);
    dataFile2 = writeDataFile(records2);
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();
  }

  @AfterEach
  public void after() {
    catalog.dropTable(TableIdentifier.of("default", "test"));
  }

  @TestTemplate
  public void readPositionDeletesTableWithNoDeleteFiles() {
    Table positionDeletesTable =
        catalog.loadTable(TableIdentifier.of("default", "test", "position_deletes"));

    assertThat(positionDeletesTable.newBatchScan().planFiles()).isEmpty();
  }

  @TestTemplate
  public void readPositionDeletesTableWithMultipleDeleteFiles() throws IOException {
    Pair<DeleteFile, CharSequenceSet> posDeletes1 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            Lists.newArrayList(
                Pair.of(dataFile1.location(), 0L), Pair.of(dataFile1.location(), 1L)),
            formatVersion);

    Pair<DeleteFile, CharSequenceSet> posDeletes2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            Lists.newArrayList(
                Pair.of(dataFile2.location(), 2L), Pair.of(dataFile2.location(), 3L)),
            formatVersion);

    DeleteFile deleteFile1 = posDeletes1.first();
    DeleteFile deleteFile2 = posDeletes2.first();
    table
        .newRowDelta()
        .addDeletes(deleteFile1)
        .addDeletes(deleteFile2)
        .validateDataFilesExist(posDeletes1.second())
        .validateDataFilesExist(posDeletes2.second())
        .commit();

    Table positionDeletesTable =
        catalog.loadTable(TableIdentifier.of("default", "test", "position_deletes"));

    List<String> columns =
        Lists.newArrayList(
            MetadataColumns.DELETE_FILE_PATH.name(),
            MetadataColumns.DELETE_FILE_POS.name(),
            PositionDeletesTable.DELETE_FILE_PATH);
    if (formatVersion >= 3) {
      columns.add(PositionDeletesTable.CONTENT_OFFSET);
      columns.add(PositionDeletesTable.CONTENT_SIZE_IN_BYTES);
    }

    Schema projectedSchema = positionDeletesTable.schema().select(columns);

    List<ScanTask> scanTasks =
        Lists.newArrayList(
            positionDeletesTable.newBatchScan().project(projectedSchema).planFiles());
    assertThat(scanTasks).hasSize(2);

    assertThat(scanTasks.get(0)).isInstanceOf(PositionDeletesScanTask.class);
    PositionDeletesScanTask scanTask1 = (PositionDeletesScanTask) scanTasks.get(0);

    try (PositionDeletesRowReader reader =
        new PositionDeletesRowReader(
            table,
            new BaseScanTaskGroup<>(null, ImmutableList.of(scanTask1)),
            positionDeletesTable.schema(),
            projectedSchema,
            false,
            true)) {
      List<InternalRow> actualRows = Lists.newArrayList();
      while (reader.next()) {
        actualRows.add(reader.get().copy());
      }

      String dataFileLocation =
          formatVersion >= 3 ? deleteFile1.referencedDataFile() : dataFile1.location();
      List<Object> first =
          Lists.newArrayList(
              UTF8String.fromString(dataFileLocation),
              0L,
              UTF8String.fromString(deleteFile1.location()));
      List<Object> second =
          Lists.newArrayList(
              UTF8String.fromString(dataFileLocation),
              1L,
              UTF8String.fromString(deleteFile1.location()));

      if (formatVersion >= 3) {
        first.add(deleteFile1.contentOffset());
        first.add(deleteFile1.contentSizeInBytes());
        second.add(deleteFile1.contentOffset());
        second.add(deleteFile1.contentSizeInBytes());
      }

      assertThat(internalRowsToJava(actualRows, projectedSchema))
          .hasSize(2)
          .containsExactly(first.toArray(), second.toArray());
    }

    assertThat(scanTasks.get(1)).isInstanceOf(PositionDeletesScanTask.class);
    PositionDeletesScanTask scanTask2 = (PositionDeletesScanTask) scanTasks.get(1);
    try (PositionDeletesRowReader reader =
        new PositionDeletesRowReader(
            table,
            new BaseScanTaskGroup<>(null, ImmutableList.of(scanTask2)),
            positionDeletesTable.schema(),
            projectedSchema,
            false,
            true)) {
      List<InternalRow> actualRows = Lists.newArrayList();
      while (reader.next()) {
        actualRows.add(reader.get().copy());
      }

      String dataFileLocation =
          formatVersion >= 3 ? deleteFile2.referencedDataFile() : dataFile2.location();
      List<Object> first =
          Lists.newArrayList(
              UTF8String.fromString(dataFileLocation),
              2L,
              UTF8String.fromString(deleteFile2.location()));
      List<Object> second =
          Lists.newArrayList(
              UTF8String.fromString(dataFileLocation),
              3L,
              UTF8String.fromString(deleteFile2.location()));

      if (formatVersion >= 3) {
        first.add(deleteFile2.contentOffset());
        first.add(deleteFile2.contentSizeInBytes());
        second.add(deleteFile2.contentOffset());
        second.add(deleteFile2.contentSizeInBytes());
      }

      assertThat(internalRowsToJava(actualRows, projectedSchema))
          .hasSize(2)
          .containsExactly(first.toArray(), second.toArray());
    }
  }

  @TestTemplate
  public void readPositionDeletesTableWithDifferentColumnOrdering() throws IOException {
    Pair<DeleteFile, CharSequenceSet> posDeletes1 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            Lists.newArrayList(
                Pair.of(dataFile1.location(), 0L), Pair.of(dataFile1.location(), 1L)),
            formatVersion);

    DeleteFile deleteFile1 = posDeletes1.first();
    table
        .newRowDelta()
        .addDeletes(deleteFile1)
        .validateDataFilesExist(posDeletes1.second())
        .commit();

    Table positionDeletesTable =
        catalog.loadTable(TableIdentifier.of("default", "test", "position_deletes"));

    // select a few fields in backwards order
    Schema projectedSchema =
        new Schema(MetadataColumns.DELETE_FILE_POS, MetadataColumns.DELETE_FILE_PATH);

    List<ScanTask> scanTasks =
        Lists.newArrayList(
            positionDeletesTable.newBatchScan().project(projectedSchema).planFiles());
    assertThat(scanTasks).hasSize(1);

    assertThat(scanTasks.get(0)).isInstanceOf(PositionDeletesScanTask.class);
    PositionDeletesScanTask scanTask1 = (PositionDeletesScanTask) scanTasks.get(0);

    try (PositionDeletesRowReader reader =
        new PositionDeletesRowReader(
            table,
            new BaseScanTaskGroup<>(null, ImmutableList.of(scanTask1)),
            positionDeletesTable.schema(),
            projectedSchema,
            false,
            true)) {
      List<InternalRow> actualRows = Lists.newArrayList();
      while (reader.next()) {
        actualRows.add(reader.get().copy());
      }

      assertThat(internalRowsToJava(actualRows, projectedSchema))
          .hasSize(2)
          .containsExactly(
              new Object[] {0L, UTF8String.fromString(dataFile1.location())},
              new Object[] {1L, UTF8String.fromString(dataFile1.location())});
    }
  }

  private DataFile writeDataFile(List<Record> records) throws IOException {
    return FileHelpers.writeDataFile(
        table,
        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
        TestHelpers.Row.of(0),
        records);
  }

  private List<Object[]> internalRowsToJava(List<InternalRow> rows, Schema projection) {
    return rows.stream().map(row -> toJava(row, projection)).collect(Collectors.toList());
  }

  private Object[] toJava(InternalRow row, Schema projection) {
    Object[] values = new Object[row.numFields()];
    for (int i = 0; i < projection.columns().size(); i++) {
      values[i] = row.get(i, SparkSchemaUtil.convert(projection.columns().get(i).type()));
    }
    return values;
  }
}
