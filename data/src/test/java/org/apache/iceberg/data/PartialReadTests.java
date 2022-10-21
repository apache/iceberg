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

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public abstract class PartialReadTests {
  // Schema passed to create tables
  public static final Schema DATE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "dt", Types.DateType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "data2", Types.StringType.get()),
          Types.NestedField.required(4, "id", Types.IntegerType.get()));

  // Partition spec used to create tables
  public static final PartitionSpec DATE_SPEC =
      PartitionSpec.builderFor(DATE_SCHEMA).day("dt").build();

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  protected String tableName = null;
  protected String dateTableName = null;
  protected Table table = null;
  protected Table dateTable = null;
  protected List<Record> records = null;
  private List<Record> dateRecords = null;
  protected DataFile dataFile = null;

  @Before
  public void writeTestDataFile() throws IOException {
    this.dateTableName = "test2";
    this.dateTable = createTable(dateTableName, DATE_SCHEMA, DATE_SPEC);

    GenericRecord record = GenericRecord.create(dateTable.schema());
    this.dateRecords = Lists.newArrayList();

    Map<String, Object> overwriteValues = Maps.newHashMapWithExpectedSize(4);
    overwriteValues.put("dt", LocalDate.parse("2021-09-01"));
    overwriteValues.put("data", "a");
    overwriteValues.put("data2", "a2");
    overwriteValues.put("id", 1);
    dateRecords.add(record.copy(overwriteValues));

    overwriteValues.put("dt", LocalDate.parse("2021-09-02"));
    overwriteValues.put("data", "b");
    overwriteValues.put("data2", "b2");
    overwriteValues.put("id", 2);
    dateRecords.add(record.copy(overwriteValues));

    DataFile dataFile1 =
        FileHelpers.writeDataFile(
            dateTable,
            Files.localOutput(temp.newFile()),
            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-01"))),
            dateRecords);

    dateTable.newAppend().appendFile(dataFile1).commit();
  }

  @After
  public void cleanup() throws IOException {
    dropTable("test");
    dropTable("test2");
  }

  protected abstract Table createTable(String name, Schema schema, PartitionSpec spec)
      throws IOException;

  protected abstract void dropTable(String name) throws IOException;

  protected abstract StructLikeSet rowSet(String name, Table testTable, String... columns)
      throws IOException;

  protected boolean countPartial() {
    return false;
  }

  /**
   * This will only be called after calling rowSet(String, Table, String...), and only if
   * countDeletes() is true.
   */
  protected long deleteCount() {
    return 0L;
  }

  protected void checkPartialCount(long expectedPartials) {
    if (countPartial()) {
      long actualPartials = deleteCount();
      Assert.assertEquals(
          "Table should contain expected number of deletes", expectedPartials, actualPartials);
    }
  }

  // todo need more tests for partial update

  @Test
  public void testPartialData() throws IOException {
    Schema partialFullSchema = dateTable.schema().select("dt", "data", "id");
    Schema eqDeleteSchema = dateTable.schema().select("dt", "id");
    Schema partialDataSchema = dateTable.schema().select("data");

    Record dataPartial = GenericRecord.create(partialFullSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataPartial.copy("dt", LocalDate.parse("2021-09-01"), "data", "a3", "id", 1));

    DeleteFile partialFile =
        FileHelpers.writePartialFile(
            dateTable,
            Files.localOutput(temp.newFile()),
            Row.of(DateTimeUtil.daysFromDate(LocalDate.parse("2021-09-01"))),
            dataDeletes,
            eqDeleteSchema,
            partialDataSchema,
            partialFullSchema);

    dateTable.newRowDelta().addDeletes(partialFile).commit();

    List<Record> expectedDateRecords = Lists.newArrayList();
    GenericRecord record = GenericRecord.create(dateTable.schema());

    Map<String, Object> overwriteValues = Maps.newHashMapWithExpectedSize(4);
    overwriteValues.put("dt", LocalDate.parse("2021-09-01"));
    overwriteValues.put("data", "a3");
    overwriteValues.put("data2", "a2");
    overwriteValues.put("id", 1);
    expectedDateRecords.add(record.copy(overwriteValues));

    overwriteValues.put("dt", LocalDate.parse("2021-09-02"));
    overwriteValues.put("data", "b");
    overwriteValues.put("data2", "b2");
    overwriteValues.put("id", 2);
    expectedDateRecords.add(record.copy(overwriteValues));

    StructLikeSet expected = rowSetWithoutIds(dateTable, expectedDateRecords);

    StructLikeSet actual = rowSet(dateTableName, dateTable, "*");

    Assert.assertEquals("Table should contain expected rows", expected, actual);
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
}
