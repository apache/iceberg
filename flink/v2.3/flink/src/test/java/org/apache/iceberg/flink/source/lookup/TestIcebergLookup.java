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
package org.apache.iceberg.flink.source.lookup;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class TestIcebergLookup {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "category", Types.StringType.get()));

  private static final String[] PROJECTED_COLUMNS = {"id", "data", "category"};
  private static final RowType ROW_TYPE = FlinkSchemaUtil.convert(SCHEMA);
  private static final int[] ID_KEY_INDICES = {0};
  private static final int[] ID_AND_CATEGORY_KEY_INDICES = {0, 2};

  @TempDir private Path temporaryFolder;

  @RegisterExtension
  protected static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  private IcebergFullCachingLookupFunction lookupFunction;

  @AfterEach
  void after() throws Exception {
    if (lookupFunction != null) {
      lookupFunction.close();
    }
  }

  @Test
  void lookupReaderReadsWithBaseFilters() throws Exception {
    Table table = createTableWithRecords();
    IcebergLookupReader reader =
        lookupReader(table, ImmutableList.of(Expressions.equal("category", "B")), false);

    List<List<Object>> rows = Lists.newArrayList();
    reader.read(
        IcebergLookupReader.CURRENT_SNAPSHOT,
        row ->
            rows.add(
                ImmutableList.of(
                    row.getLong(0), row.getString(1).toString(), row.getString(2).toString())));

    assertThat(rows).containsExactly(ImmutableList.of(2L, "bob", "B"));
  }

  @Test
  void lookupReaderRespectsCaseSensitivity() throws Exception {
    Table table = createTableWithRecords();
    List<Expression> filters = ImmutableList.of(Expressions.equal("CATEGORY", "B"));

    IcebergLookupReader caseSensitiveReader = lookupReader(table, filters, true);
    assertThatThrownBy(
            () -> caseSensitiveReader.read(IcebergLookupReader.CURRENT_SNAPSHOT, row -> {}))
        .as("Case sensitive lookup should reject a filter that doesn't match the column case")
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("CATEGORY");

    assertThat(readIds(lookupReader(table, filters, false), IcebergLookupReader.CURRENT_SNAPSHOT))
        .containsExactly(2L);
  }

  @Test
  void lookupReaderReadsPinnedSnapshot() throws Exception {
    Table table = createTableWithRecords();
    long pinnedSnapshot = table.currentSnapshot().snapshotId();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    IcebergLookupReader reader = lookupReader(table, ImmutableList.of(), false);

    assertThat(readIds(reader, pinnedSnapshot))
        .as("Pinned snapshot should not contain the appended row")
        .containsExactlyInAnyOrder(1L, 2L, 3L);
    assertThat(readIds(reader, IcebergLookupReader.CURRENT_SNAPSHOT))
        .as("Current snapshot should contain the appended row")
        .containsExactlyInAnyOrder(1L, 2L, 3L, 6L);
  }

  @Test
  void lookupFunctionReturnsRowsFromCache() throws Exception {
    Table table = createTableWithRecords();

    lookupFunction = newLookupFunction(ID_KEY_INDICES, false);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    assertThat(lookupFunction.lookup(keyRow(6L))).isEmpty();
    assertThat(lookupFunction.lookup(keyRow(404L))).isEmpty();
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  void lookupFunctionWithMultiColumnKey() throws Exception {
    Table table = createTableWithRecords();

    lookupFunction = newLookupFunction(ID_AND_CATEGORY_KEY_INDICES, false);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L, "A")))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));

    assertThat(lookupFunction.lookup(keyRow(1L, "B"))).isEmpty();

    appendRecords(table, ImmutableList.of(record(3L, "carol-2", "A")));

    assertThat(lookupFunction.lookup(keyRow(3L, "A")))
        .singleElement()
        .satisfies(row -> assertRow(row, 3L, "carol", "A"));
  }

  @Test
  void lookupFunctionLoadsCacheLazily() throws Exception {
    Table table = createTableWithRecords();

    lookupFunction = newLookupFunction(ID_KEY_INDICES, false);
    lookupFunction.open(new FunctionContext(null));

    // Nothing is loaded yet, so the first lookup sees the row appended after opening.
    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    assertThat(lookupFunction.lookup(keyRow(6L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 6L, "frank", "D"));
  }

  @Test
  void lookupFunctionLoadsCacheEagerly() throws Exception {
    Table table = createTableWithRecords();

    lookupFunction = newLookupFunction(ID_KEY_INDICES, true);
    lookupFunction.open(new FunctionContext(null));

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    assertThat(lookupFunction.lookup(keyRow(6L))).isEmpty();
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  void lookupFunctionHandlesMultipleRowsPerKey() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    appendRecords(
        table,
        ImmutableList.of(
            record(1L, "alice", "A"), record(1L, "alice-2", "A"), record(2L, "bob", "B")));

    lookupFunction = newLookupFunction(ID_KEY_INDICES, false);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L)))
        .hasSize(2)
        .allSatisfy(row -> assertThat(row.getLong(0)).isEqualTo(1L));
    assertThat(lookupFunction.lookup(keyRow(2L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 2L, "bob", "B"));
  }

  @Test
  void lookupFunctionAppliesPushedFilters() throws Exception {
    createTableWithRecords();

    lookupFunction =
        new IcebergFullCachingLookupFunction(
            CATALOG_EXTENSION.tableLoader().clone(),
            ROW_TYPE,
            ID_KEY_INDICES,
            ImmutableList.of(Expressions.equal("category", "B")),
            true,
            false);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L))).isEmpty();
    assertThat(lookupFunction.lookup(keyRow(2L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 2L, "bob", "B"));
  }

  private IcebergFullCachingLookupFunction newLookupFunction(int[] keyIndices, boolean eagerLoad) {
    return new IcebergFullCachingLookupFunction(
        CATALOG_EXTENSION.tableLoader().clone(),
        ROW_TYPE,
        keyIndices,
        ImmutableList.of(),
        false,
        eagerLoad);
  }

  private static IcebergLookupReader lookupReader(
      Table table, List<Expression> filters, boolean caseSensitive) {
    return new IcebergLookupReader(
        table, SCHEMA.select(PROJECTED_COLUMNS), filters, caseSensitive, null);
  }

  private static List<Long> readIds(IcebergLookupReader reader, long snapshotId)
      throws IOException {
    List<Long> ids = Lists.newArrayList();
    reader.read(snapshotId, row -> ids.add(row.isNullAt(0) ? null : row.getLong(0)));
    return ids;
  }

  private Table createTableWithRecords() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    appendRecords(
        table,
        ImmutableList.of(
            record(1L, "alice", "A"), record(2L, "bob", "B"), record(3L, "carol", "A")));
    return table;
  }

  private void appendRecords(Table table, List<Record> records) throws Exception {
    new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder).appendToTable(records);
  }

  private static GenericRecord record(Long id, String data, String category) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    record.setField("category", category);
    return record;
  }

  private static RowData keyRow(Long id) {
    GenericRowData row = new GenericRowData(1);
    row.setField(0, id);
    return row;
  }

  private static RowData keyRow(Long id, String category) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, id);
    row.setField(1, StringData.fromString(category));
    return row;
  }

  private static void assertRow(RowData row, Long id, String data, String category) {
    assertThat(row.isNullAt(0)).isEqualTo(id == null);
    if (id != null) {
      assertThat(row.getLong(0)).isEqualTo(id);
    }

    assertThat(row.getString(1)).hasToString(data);
    assertThat(row.getString(2)).hasToString(category);
  }
}
