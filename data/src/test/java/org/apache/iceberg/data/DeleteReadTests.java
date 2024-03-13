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
package org.apache.iceberg.data;


import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;


public abstract class DeleteReadTests {
  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  public static final Schema DATE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "dt", Types.DateType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "id", Types.IntegerType.get()));

  // Partition spec used to create tables
  public static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  public static final PartitionSpec DATE_SPEC =
      PartitionSpec.builderFor(DATE_SCHEMA).day("dt").build();

  @TempDir
  public static File temp;
  
  protected String tableName = null;
  protected String dateTableName = null;
  protected Table table = null;
  protected Table dateTable = null;
  protected List<Record> records = null;
  private List<Record> dateRecords = null;
  protected DataFile dataFile = null;

  @Parameter protected FileFormat format;

  @Parameters(name = "fileFormat = {0}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.PARQUET},
      new Object[] {FileFormat.AVRO},
      new Object[] {FileFormat.ORC}
    };
  }

  @BeforeEach
  public void writeTestDataFile() throws IOException {
    this.tableName = "test";
    this.table = createTable(tableName, SCHEMA, SPEC);
    this.records = Lists.newArrayList();

    // records all use IDs that are in bucket id_bucket=0
    GenericRecord record = GenericRecord.create(table.schema());
    records.add(record.copy("id", 29, "data", "a"));
    records.add(record.copy("id", 43, "data", "b"));
    records.add(record.copy("id", 61, "data", "c"));
    records.add(record.copy("id", 89, "data", "d"));
    records.add(record.copy("id", 100, "data", "e"));
    records.add(record.copy("id", 121, "data", "f"));
    records.add(record.copy("id", 122, "data", "g"));

    this.dataFile =
        FileHelpers.writeDataFile(table, Files.localOutput(temp), Row.of(0), records);


    table.newAppend().appendFile(dataFile).commit();
  }

  @AfterEach
  public void cleanup() throws IOException {
    dropTable("test");
    dropTable("test2");
  }

  private void initDateTable() throws IOException {
    dropTable("test2");
    this.dateTableName = "test2";
    this.dateTable = createTable(dateTableName, DATE_SCHEMA, DATE_SPEC);

    GenericRecord record = GenericRecord.create(dateTable.schema());

    this.dateRecords =
        Lists.newArrayList(
            record.copy("dt", LocalDate.parse("2021-09-01"), "data", "a", "id", 1),
            record.copy("dt", LocalDate.parse("2021-09-02"), "data", "b", "id", 2),
            record.copy("dt", LocalDate.parse("2021-09-03"), "data", "c", "id", 3),
            record.copy("dt", LocalDate.parse("2021-09-04"), "data", "d", "id", 4),
            record.copy("dt", LocalDate.parse("2021-09-05"), "data", "e", "id", 5));

    DataFile dataFile1 =
        FileHelpers.writeDataFile(
            dateTable,

            Files.localOutput(temp),

            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-01"))),
            dateRecords.subList(0, 1));
    DataFile dataFile2 =
        FileHelpers.writeDataFile(
            dateTable,

            Files.localOutput(temp),

            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-02"))),
            dateRecords.subList(1, 2));
    DataFile dataFile3 =
        FileHelpers.writeDataFile(
            dateTable,
            Files.localOutput(temp),

            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-03"))),
            dateRecords.subList(2, 3));
    DataFile dataFile4 =
        FileHelpers.writeDataFile(
            dateTable,
            Files.localOutput(temp),


            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-04"))),
            dateRecords.subList(3, 4));
    DataFile dataFile5 =
        FileHelpers.writeDataFile(
            dateTable,
            Files.localOutput(temp),

            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-05"))),
            dateRecords.subList(4, 5));

    dateTable
        .newAppend()
        .appendFile(dataFile1)
        .appendFile(dataFile2)
        .appendFile(dataFile3)
        .appendFile(dataFile4)
        .appendFile(dataFile5)
        .commit();
  }

  protected abstract Table createTable(String name, Schema schema, PartitionSpec spec)
      throws IOException;

  protected abstract void dropTable(String name) throws IOException;

  protected abstract StructLikeSet rowSet(String name, Table testTable, String... columns)
      throws IOException;

  protected boolean expectPruned() {
    return true;
  }

  protected boolean countDeletes() {
    return false;
  }

  /**
   * This will only be called after calling rowSet(String, Table, String...), and only if
   * countDeletes() is true.
   */
  protected long deleteCount() {
    return 0L;
  }

  protected void checkDeleteCount(long expectedDeletes) {
    if (countDeletes()) {
      long actualDeletes = deleteCount();
      assertThat(actualDeletes)
          .as("Table should contain expected number of deletes")
          .isEqualTo(expectedDeletes);
    }
  }

  @TestTemplate
  public void testEqualityDeletes() throws IOException {
    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp), Row.of(0), dataDeletes, deleteRowSchema);


    table.newRowDelta().addDeletes(eqDeletes).commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 89, 122);
    StructLikeSet actual = rowSet(tableName, table, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(3L);
  }

  @TestTemplate
  public void testEqualityDateDeletes() throws IOException {
    initDateTable();

    Schema deleteRowSchema = dateTable.schema().select("*");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("dt", LocalDate.parse("2021-09-01"), "data", "a", "id", 1),
            dataDelete.copy("dt", LocalDate.parse("2021-09-02"), "data", "b", "id", 2),
            dataDelete.copy("dt", LocalDate.parse("2021-09-03"), "data", "c", "id", 3));

    DeleteFile eqDeletes1 =
        FileHelpers.writeDeleteFile(
            dateTable,
            Files.localOutput(temp),

            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-01"))),
            dataDeletes.subList(0, 1),
            deleteRowSchema);
    DeleteFile eqDeletes2 =
        FileHelpers.writeDeleteFile(
            dateTable,
            Files.localOutput(temp),

            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-02"))),
            dataDeletes.subList(1, 2),
            deleteRowSchema);
    DeleteFile eqDeletes3 =
        FileHelpers.writeDeleteFile(
            dateTable,

            Files.localOutput(temp),
            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-03"))),
            dataDeletes.subList(2, 3),
            deleteRowSchema);

    dateTable
        .newRowDelta()
        .addDeletes(eqDeletes1)
        .addDeletes(eqDeletes2)
        .addDeletes(eqDeletes3)
        .commit();

    StructLikeSet expected = rowSetWithoutIds(dateTable, dateRecords, 1, 2, 3);

    StructLikeSet actual = rowSet(dateTableName, dateTable, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(3L);
  }

  @TestTemplate
  public void testEqualityDeletesWithRequiredEqColumn() throws IOException {
    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp), Row.of(0), dataDeletes, deleteRowSchema);


    table.newRowDelta().addDeletes(eqDeletes).commit();

    StructLikeSet expected = selectColumns(rowSetWithoutIds(table, records, 29, 89, 122), "id");
    StructLikeSet actual = rowSet(tableName, table, "id");

    if (expectPruned()) {
      assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    } else {
      // data is added by the reader to apply the eq deletes, use StructProjection to remove it from
      // comparison
      assertThat(selectColumns(actual, "id"))
          .as("Table should contain expected rows")
          .isEqualTo(expected);
    }

    checkDeleteCount(3L);
  }

  @TestTemplate
  public void testEqualityDeletesSpanningMultipleDataFiles() throws IOException {
    // Add another DataFile with common values
    GenericRecord record = GenericRecord.create(table.schema());
    records.add(record.copy("id", 144, "data", "a"));

    this.dataFile =
        FileHelpers.writeDataFile(table, Files.localOutput(temp), Row.of(0), records);


    // At this point, the table has two data files, with 7 and 8 rows respectively, of which all but
    // one are in duplicate.
    table.newAppend().appendFile(dataFile).commit();

    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29, 144
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp), Row.of(0), dataDeletes, deleteRowSchema);

    // At this point, 3 rows in the first data file and 4 rows in the second data file are deleted.
    table.newRowDelta().addDeletes(eqDeletes).commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 89, 122, 144);
    StructLikeSet actual = rowSet(tableName, table, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(7L);
  }

  @TestTemplate
  public void testPositionDeletes() throws IOException {
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 0L), // id = 29
            Pair.of(dataFile.path(), 3L), // id = 89
            Pair.of(dataFile.path(), 6L) // id = 122
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(temp), Row.of(0), deletes);


    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 89, 122);
    StructLikeSet actual = rowSet(tableName, table, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(3L);
  }

  @TestTemplate
  public void testMultiplePosDeleteFiles() throws IOException {
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 0L), // id = 29
            Pair.of(dataFile.path(), 3L) // id = 89
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(temp), Row.of(0), deletes);


    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 6L) // id = 122
            );

    posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(temp), Row.of(0), deletes);


    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 89, 122);
    StructLikeSet actual = rowSet(tableName, table, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(3L);
  }

  @TestTemplate
  public void testMixedPositionAndEqualityDeletes() throws IOException {
    Schema dataSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(dataSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp), Row.of(0), dataDeletes, dataSchema);

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.path(), 3L), // id = 89
            Pair.of(dataFile.path(), 5L) // id = 121
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(table, Files.localOutput(temp), Row.of(0), deletes);


    table
        .newRowDelta()
        .addDeletes(eqDeletes)
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 89, 121, 122);
    StructLikeSet actual = rowSet(tableName, table, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(4L);
  }

  @TestTemplate
  public void testMultipleEqualityDeleteSchemas() throws IOException {
    Schema dataSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(dataSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile dataEqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp), Row.of(0), dataDeletes, dataSchema);


    Schema idSchema = table.schema().select("id");
    Record idDelete = GenericRecord.create(idSchema);
    List<Record> idDeletes =
        Lists.newArrayList(
            idDelete.copy("id", 121), // id = 121
            idDelete.copy("id", 29) // id = 29
            );

    DeleteFile idEqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp), Row.of(0), idDeletes, idSchema);


    table.newRowDelta().addDeletes(dataEqDeletes).addDeletes(idEqDeletes).commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 29, 89, 121, 122);
    StructLikeSet actual = rowSet(tableName, table, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(4L);
  }

  @TestTemplate
  public void testEqualityDeleteByNull() throws IOException {
    // data is required in the test table; make it optional for this test
    table.updateSchema().makeColumnOptional("data").commit();

    // add a new data file with a record where data is null
    Record record = GenericRecord.create(table.schema());
    DataFile dataFileWithNull =
        FileHelpers.writeDataFile(
            table,

            Files.localOutput(temp),

            Row.of(0),
            Lists.newArrayList(record.copy("id", 131, "data", null)));

    table.newAppend().appendFile(dataFileWithNull).commit();

    // delete where data is null
    Schema dataSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(dataSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", null) // id = 131
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp), Row.of(0), dataDeletes, dataSchema);

    table.newRowDelta().addDeletes(eqDeletes).commit();

    StructLikeSet expected = rowSetWithoutIds(table, records, 131);
    StructLikeSet actual = rowSet(tableName, table, "*");

    assertThat(actual).as("Table should contain expected rows").isEqualTo(expected);
    checkDeleteCount(1L);
  }

  private StructLikeSet selectColumns(StructLikeSet rows, String... columns) {
    Schema projection = table.schema().select(columns);
    StructLikeSet set = StructLikeSet.create(projection.asStruct());
    rows.stream()
        .map(row -> StructProjection.create(table.schema(), projection).wrap(row))
        .forEach(set::add);
    return set;
  }

  protected static StructLikeSet rowSetWithoutIds(
      Table table, List<Record> recordList, int... idsToRemove) {
    Set<Integer> deletedIds = Sets.newHashSet(ArrayUtil.toIntList(idsToRemove));
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    recordList.stream()
        .filter(row -> !deletedIds.contains(row.getField("id")))
        .map(record -> new InternalRecordWrapper(table.schema().asStruct()).wrap(record))
        .forEach(set::add);
    return set;
  }

  protected StructLikeSet rowSetWithIds(int... idsToRetain) {
    Set<Integer> deletedIds = Sets.newHashSet(ArrayUtil.toIntList(idsToRetain));
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.stream().filter(row -> deletedIds.contains(row.getField("id"))).forEach(set::add);
    return set;
  }
}
