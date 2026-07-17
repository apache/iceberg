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

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class TestIcebergLookup {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "category", Types.StringType.get()));

  private static final String[] PROJECTED_COLUMNS = {"id", "data", "category"};
  private static final RowType ROW_TYPE = FlinkSchemaUtil.convert(SCHEMA);
  private static final int[] ID_KEY_INDICES = {0};

  @TempDir private Path temporaryFolder;

  @RegisterExtension
  protected static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  @Test
  public void testLookUpReaderReadsWithBaseAndExtraFilters() throws Exception {
    Table table = createTableWithRecords();
    IcebergLookUpReader reader =
        new IcebergLookUpReader(
            table,
            SCHEMA.select(PROJECTED_COLUMNS),
            ImmutableList.of(Expressions.equal("category", "A")),
            false,
            null);

    List<List<Object>> rows = Lists.newArrayList();
    reader.read(
        Expressions.greaterThan("id", 1L),
        row ->
            rows.add(
                ImmutableList.of(
                    row.getLong(0), row.getString(1).toString(), row.getString(2).toString())));

    assertThat(rows).containsExactly(ImmutableList.of(3L, "carol", "A"));
  }

  @Test
  public void testLookupFunctionReturnsRowsByKey() throws Exception {
    createTableWithRecords();

    IcebergLookupFunction lookupFunction =
        new IcebergLookupFunction(
            CATALOG_EXTENSION.tableLoader().clone(),
            PROJECTED_COLUMNS,
            ROW_TYPE,
            ID_KEY_INDICES,
            ImmutableList.of());
    try {
      lookupFunction.open(null);

      Collection<RowData> rows = lookupFunction.lookup(keyRow(2L));

      assertThat(rows).singleElement().satisfies(row -> assertRow(row, 2L, "bob", "B"));
    } finally {
      lookupFunction.close();
    }
  }

  @Test
  public void testLookupFunctionReturnsRowsByNullKey() throws Exception {
    createTableWithRecords();

    IcebergLookupFunction lookupFunction =
        new IcebergLookupFunction(
            CATALOG_EXTENSION.tableLoader().clone(),
            PROJECTED_COLUMNS,
            ROW_TYPE,
            ID_KEY_INDICES,
            ImmutableList.of());
    try {
      lookupFunction.open(null);

      Collection<RowData> rows = lookupFunction.lookup(keyRow(null));

      assertThat(rows).singleElement().satisfies(row -> assertRow(row, null, "nobody", "A"));
    } finally {
      lookupFunction.close();
    }
  }

  @Test
  public void testFullCachingLookupFunctionReturnsRowsFromCache() throws Exception {
    Table table = createTableWithRecords();

    IcebergFullCachingLookupFunction lookupFunction =
        new IcebergFullCachingLookupFunction(
            CATALOG_EXTENSION.tableLoader().clone(),
            PROJECTED_COLUMNS,
            ROW_TYPE,
            ID_KEY_INDICES,
            ImmutableList.of(),
            null);
    try {
      lookupFunction.open(null);

      Collection<RowData> rows = lookupFunction.lookup(keyRow(1L));

      assertThat(rows).singleElement().satisfies(row -> assertRow(row, 1L, "alice", "A"));

      assertThat(lookupFunction.lookup(keyRow(1L))).isSameAs(rows);

      appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

      assertThat(lookupFunction.lookup(keyRow(6L))).isEmpty();
      assertThat(lookupFunction.lookup(keyRow(404L))).isEmpty();
    } finally {
      lookupFunction.close();
    }
  }

  @Test
  public void testFullCachingLookupFunctionRefreshesCachePeriodically() throws Exception {
    Table table = createTableWithRecords();

    IcebergFullCachingLookupFunction lookupFunction =
        new IcebergFullCachingLookupFunction(
            CATALOG_EXTENSION.tableLoader().clone(),
            PROJECTED_COLUMNS,
            ROW_TYPE,
            ID_KEY_INDICES,
            ImmutableList.of(),
            Duration.ofMillis(100));
    try {
      lookupFunction.open(null);

      assertThat(lookupFunction.lookup(keyRow(5L))).isEmpty();

      appendRecords(table, ImmutableList.of(record(5L, "eve", "C")));

      Awaitility.await("full lookup cache should be refreshed")
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () ->
                  assertThat(lookupFunction.lookup(keyRow(5L)))
                      .singleElement()
                      .satisfies(row -> assertRow(row, 5L, "eve", "C")));
    } finally {
      lookupFunction.close();
    }
  }

  private Table createTableWithRecords() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    appendRecords(
        table,
        ImmutableList.of(
            record(1L, "alice", "A"),
            record(2L, "bob", "B"),
            record(3L, "carol", "A"),
            record(null, "nobody", "A")));
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

  private static void assertRow(RowData row, Long id, String data, String category) {
    assertThat(row.isNullAt(0)).isEqualTo(id == null);
    if (id != null) {
      assertThat(row.getLong(0)).isEqualTo(id);
    }

    assertThat(row.getString(1)).hasToString(data);
    assertThat(row.getString(2)).hasToString(category);
  }
}
