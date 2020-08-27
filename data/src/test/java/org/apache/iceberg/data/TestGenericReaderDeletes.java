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
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestGenericReaderDeletes extends TableTestBase {
  public TestGenericReaderDeletes() {
    super(2 /* format v2 with delete files */);
  }

  private List<Record> records = null;
  private DataFile dataFile = null;

  @Before
  public void writeTestDataFile() throws IOException {
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

    this.dataFile = FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()), Row.of(0), records);

    table.newAppend()
        .appendFile(dataFile)
        .commit();
  }

  @Test
  public void testEqualityDeletes() throws IOException {
    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes = Lists.newArrayList(
        dataDelete.copy("data", "a"), // id = 29
        dataDelete.copy("data", "d"), // id = 89
        dataDelete.copy("data", "g") // id = 122
    );

    DeleteFile eqDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), Row.of(0), dataDeletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(eqDeletes)
        .commit();

    StructLikeSet expected = rowSetWithoutIds(29, 89, 122);
    StructLikeSet actual = rowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  @Test
  public void testEqualityDeletesWithRequiredEqColumn() throws IOException {
    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes = Lists.newArrayList(
        dataDelete.copy("data", "a"), // id = 29
        dataDelete.copy("data", "d"), // id = 89
        dataDelete.copy("data", "g") // id = 122
    );

    DeleteFile eqDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), Row.of(0), dataDeletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(eqDeletes)
        .commit();

    StructLikeSet expected = rowSetWithoutIds(29, 89, 122);
    StructLikeSet actual = rowSet(table, "id"); // data is added by the reader to apply the eq deletes

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  @Test
  public void testPositionDeletes() throws IOException {
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList(
        Pair.of(dataFile.path(), 0L), // id = 29
        Pair.of(dataFile.path(), 3L), // id = 89
        Pair.of(dataFile.path(), 6L) // id = 122
    );

    DeleteFile posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), Row.of(0), deletes);

    table.newRowDelta()
        .addDeletes(posDeletes)
        .commit();

    StructLikeSet expected = rowSetWithoutIds(29, 89, 122);
    StructLikeSet actual = rowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  @Test
  public void testMixedPositionAndEqualityDeletes() throws IOException {
    Schema dataSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(dataSchema);
    List<Record> dataDeletes = Lists.newArrayList(
        dataDelete.copy("data", "a"), // id = 29
        dataDelete.copy("data", "d"), // id = 89
        dataDelete.copy("data", "g") // id = 122
    );

    DeleteFile eqDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), Row.of(0), dataDeletes, dataSchema);

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList(
        Pair.of(dataFile.path(), 3L), // id = 89
        Pair.of(dataFile.path(), 5L) // id = 121
    );

    DeleteFile posDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), Row.of(0), deletes);

    table.newRowDelta()
        .addDeletes(eqDeletes)
        .addDeletes(posDeletes)
        .commit();

    StructLikeSet expected = rowSetWithoutIds(29, 89, 121, 122);
    StructLikeSet actual = rowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  @Test
  public void testMultipleEqualityDeleteSchemas() throws IOException {
    Schema dataSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(dataSchema);
    List<Record> dataDeletes = Lists.newArrayList(
        dataDelete.copy("data", "a"), // id = 29
        dataDelete.copy("data", "d"), // id = 89
        dataDelete.copy("data", "g") // id = 122
    );

    DeleteFile dataEqDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), Row.of(0), dataDeletes, dataSchema);

    Schema idSchema = table.schema().select("id");
    Record idDelete = GenericRecord.create(idSchema);
    List<Record> idDeletes = Lists.newArrayList(
        idDelete.copy("id", 121), // id = 121
        idDelete.copy("id", 29) // id = 29
    );

    DeleteFile idEqDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), Row.of(0), idDeletes, idSchema);

    table.newRowDelta()
        .addDeletes(dataEqDeletes)
        .addDeletes(idEqDeletes)
        .commit();

    StructLikeSet expected = rowSetWithoutIds(29, 89, 121, 122);
    StructLikeSet actual = rowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  @Test
  public void testEqualityDeleteByNull() throws IOException {
    // data is required in the test table; make it optional for this test
    table.updateSchema()
        .makeColumnOptional("data")
        .commit();

    // add a new data file with a record where data is null
    Record record = GenericRecord.create(table.schema());
    DataFile dataFileWithNull = FileHelpers.writeDataFile(
        table, Files.localOutput(temp.newFile()), Row.of(0),
        Lists.newArrayList(record.copy("id", 131, "data", null)));

    table.newAppend()
        .appendFile(dataFileWithNull)
        .commit();

    // delete where data is null
    Schema dataSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(dataSchema);
    List<Record> dataDeletes = Lists.newArrayList(
        dataDelete.copy("data", null) // id = 131
    );

    DeleteFile eqDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), Row.of(0), dataDeletes, dataSchema);

    table.newRowDelta()
        .addDeletes(eqDeletes)
        .commit();

    StructLikeSet expected = rowSetWithoutIds(131);
    StructLikeSet actual = rowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  private static StructLikeSet rowSet(Table table) throws IOException {
    return rowSet(table, "*");
  }

  private static StructLikeSet rowSet(Table table, String... columns) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }

  private StructLikeSet rowSetWithoutIds(int... idsToRemove) {
    Set<Integer> deletedIds = Sets.newHashSet(ArrayUtil.toIntList(idsToRemove));
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.stream()
        .filter(row -> !deletedIds.contains(row.getField("id")))
        .forEach(set::add);
    return set;
  }
}
