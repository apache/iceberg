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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
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

  private IcebergFullCachingLookupFunction lookupFunction;

  @AfterEach
  public void after() throws Exception {
    if (lookupFunction != null) {
      lookupFunction.close();
    }
  }

  @Test
  public void lookupReaderReadsWithBaseFilters() throws Exception {
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
  public void lookupReaderRespectsCaseSensitivity() throws Exception {
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
  public void lookupReaderReadsPinnedSnapshot() throws Exception {
    Table table = createTableWithRecords();
    long pinnedSnapshot = table.currentSnapshot().snapshotId();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    IcebergLookupReader reader = lookupReader(table, ImmutableList.of(), false);

    assertThat(readIds(reader, pinnedSnapshot))
        .as("Pinned snapshot should not contain the appended row")
        .containsExactlyInAnyOrder(1L, 2L, 3L, null);
    assertThat(readIds(reader, IcebergLookupReader.CURRENT_SNAPSHOT))
        .as("Current snapshot should contain the appended row")
        .containsExactlyInAnyOrder(1L, 2L, 3L, null, 6L);
  }

  @Test
  public void lookupFunctionReturnsRowsFromCache() throws Exception {
    Table table = createTableWithRecords();

    lookupFunction = newLookupFunction(false, null);
    lookupFunction.open(new FunctionContext(null));

    Collection<RowData> rows = lookupFunction.lookup(keyRow(1L));
    assertThat(rows).singleElement().satisfies(row -> assertRow(row, 1L, "alice", "A"));

    // Served from the cache, not re-read from the table.
    assertThat(lookupFunction.lookup(keyRow(1L))).isSameAs(rows);

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    assertThat(lookupFunction.lookup(keyRow(6L))).isEmpty();
    assertThat(lookupFunction.lookup(keyRow(404L))).isEmpty();
  }

  @Test
  public void lookupFunctionLoadsCacheLazily() throws Exception {
    Table table = createTableWithRecords();

    lookupFunction = newLookupFunction(false, null);
    lookupFunction.open(new FunctionContext(null));

    // Nothing is loaded yet, so the first lookup sees the row appended after opening.
    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    assertThat(lookupFunction.lookup(keyRow(6L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 6L, "frank", "D"));
  }

  @Test
  public void lookupFunctionLoadsCacheEagerly() throws Exception {
    Table table = createTableWithRecords();

    lookupFunction = newLookupFunction(true, null);
    lookupFunction.open(new FunctionContext(null));

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    assertThat(lookupFunction.lookup(keyRow(6L))).isEmpty();
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  public void lookupFunctionReloadsCacheFromTrigger() throws Exception {
    Table table = createTableWithRecords();
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(true, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));

    assertThat(lookupFunction.lookup(keyRow(6L))).isEmpty();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    reloadTrigger.triggerReload().join();

    assertThat(lookupFunction.lookup(keyRow(6L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 6L, "frank", "D"));
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .as("Rows that were already cached must still be served after a reload")
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  public void lookupFunctionWithTriggerLoadsCacheLazily() throws Exception {
    Table table = createTableWithRecords();
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(false, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    reloadTrigger.triggerReload().join();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    assertThat(lookupFunction.lookup(keyRow(6L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 6L, "frank", "D"));
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));

    appendRecords(table, ImmutableList.of(record(7L, "grace", "E")));
    reloadTrigger.triggerReload().join();
    assertThat(lookupFunction.lookup(keyRow(7L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 7L, "grace", "E"));
  }

  @Test
  public void lookupFunctionSkipsReloadWhenSnapshotIsUnchanged() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    DataFile dataFile = appendRecords(table, ImmutableList.of(record(1L, "alice", "A")));
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(true, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    table.io().deleteFile(dataFile.location());

    assertThatCode(() -> reloadTrigger.triggerReload().join())
        .as("A reload without a new snapshot must not read the table")
        .doesNotThrowAnyException();

    assertThat(lookupFunction.lookup(keyRow(1L)))
        .as("A reload without a new snapshot must keep serving the loaded cache")
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  public void lookupFunctionFailsLookupsWhenReloadFails() throws Exception {
    Table table = createTableWithRecords();
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(true, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));

    DataFile dataFile = appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    table.io().deleteFile(dataFile.location());

    assertThatThrownBy(() -> reloadTrigger.triggerReload().join())
        .isInstanceOf(CompletionException.class)
        .hasMessageContaining(dataFile.location());

    assertThatThrownBy(() -> lookupFunction.lookup(keyRow(1L)))
        .as("A failed reload must fail the lookup instead of serving the previous cache")
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Lookup cache reload failed");
  }

  @Test
  public void lookupFunctionHandlesMultipleRowsPerKey() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    appendRecords(
        table,
        ImmutableList.of(
            record(1L, "alice", "A"), record(1L, "alice-2", "A"), record(2L, "bob", "B")));

    lookupFunction = newLookupFunction(false, null);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L)))
        .hasSize(2)
        .allSatisfy(row -> assertThat(row.getLong(0)).isEqualTo(1L));
    assertThat(lookupFunction.lookup(keyRow(2L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 2L, "bob", "B"));
  }

  @Test
  public void lookupFunctionAppliesPushedFilters() throws Exception {
    createTableWithRecords();

    lookupFunction =
        new IcebergFullCachingLookupFunction(
            CATALOG_EXTENSION.tableLoader().clone(),
            ROW_TYPE,
            ID_KEY_INDICES,
            ImmutableList.of(Expressions.equal("category", "B")),
            true,
            false,
            null);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L))).isEmpty();
    assertThat(lookupFunction.lookup(keyRow(2L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 2L, "bob", "B"));
  }

  private IcebergFullCachingLookupFunction newLookupFunction(
      boolean eagerLoad, @Nullable CacheReloadTrigger reloadTrigger) {
    return new IcebergFullCachingLookupFunction(
        CATALOG_EXTENSION.tableLoader().clone(),
        ROW_TYPE,
        ID_KEY_INDICES,
        ImmutableList.of(),
        false,
        eagerLoad,
        reloadTrigger);
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
            record(1L, "alice", "A"),
            record(2L, "bob", "B"),
            record(3L, "carol", "A"),
            record(null, "nobody", "A")));
    return table;
  }

  private DataFile appendRecords(Table table, List<Record> records) throws Exception {
    GenericAppenderHelper helper =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);
    DataFile dataFile = helper.writeFile(records);
    helper.appendToTable(dataFile);
    return dataFile;
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

  private static class ManualCacheReloadTrigger implements CacheReloadTrigger {
    private Context context;

    @Override
    public void open(Context reloadContext) {
      this.context = reloadContext;
    }

    @Override
    public void close() {}

    CompletableFuture<Void> triggerReload() {
      return context.triggerReload();
    }
  }
}
