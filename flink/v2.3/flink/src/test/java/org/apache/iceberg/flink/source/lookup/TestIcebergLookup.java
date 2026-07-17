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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.io.CloseableIterable;
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
  private static final int[] ID_KEY_INDEXES = {0};
  private static final int[] ID_AND_CATEGORY_KEY_INDEXES = {0, 2};

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
    try (CloseableIterable<RowData> rowData = reader.read(IcebergLookupReader.NO_SNAPSHOT_ID)) {
      for (RowData row : rowData) {
        rows.add(
            ImmutableList.of(
                row.getLong(0), row.getString(1).toString(), row.getString(2).toString()));
      }
    }

    assertThat(rows).containsExactly(ImmutableList.of(2L, "bob", "B"));
  }

  @Test
  void lookupReaderRespectsCaseSensitivity() throws Exception {
    Table table = createTableWithRecords();
    List<Expression> filters = ImmutableList.of(Expressions.equal("CATEGORY", "B"));

    IcebergLookupReader caseSensitiveReader = lookupReader(table, filters, true);
    assertThatThrownBy(() -> readIds(caseSensitiveReader, IcebergLookupReader.NO_SNAPSHOT_ID))
        .as("Case sensitive lookup should reject a filter that doesn't match the column case")
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("CATEGORY");

    assertThat(readIds(lookupReader(table, filters, false), IcebergLookupReader.NO_SNAPSHOT_ID))
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
    assertThat(readIds(reader, IcebergLookupReader.NO_SNAPSHOT_ID))
        .as("Current snapshot should contain the appended row")
        .containsExactlyInAnyOrder(1L, 2L, 3L, 6L);
  }

  @Test
  void lookupReaderReadsDelta() throws Exception {
    Table table = createTableWithRecords();
    long firstSnapshot = table.currentSnapshot().snapshotId();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    long secondSnapshot = table.currentSnapshot().snapshotId();

    IcebergLookupReader reader = lookupReader(table, ImmutableList.of(), false);

    assertThat(readDeltaIds(reader, firstSnapshot, secondSnapshot))
        .as("A delta read must only return the rows added in the range")
        .containsExactly(6L);
  }

  @Test
  void lookupReaderDetectsAppendOnlyRange() throws Exception {
    Table table = createTableWithRecords();
    long firstSnapshot = table.currentSnapshot().snapshotId();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    long secondSnapshot = table.currentSnapshot().snapshotId();

    IcebergLookupReader reader = lookupReader(table, ImmutableList.of(), false);

    assertThat(reader.isAppendOnlyRange(firstSnapshot, secondSnapshot))
        .as("A range that only appends rows can be applied to a loaded cache")
        .isTrue();
  }

  @Test
  void lookupReaderRejectsRangeWithDeletes() throws Exception {
    Table table = createTableWithRecords();
    long firstSnapshot = table.currentSnapshot().snapshotId();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 6L)).commit();

    IcebergLookupReader reader = lookupReader(table, ImmutableList.of(), false);

    assertThat(reader.isAppendOnlyRange(firstSnapshot, table.currentSnapshot().snapshotId()))
        .as("A range that deletes rows cannot be applied to a loaded cache")
        .isFalse();
  }

  @Test
  void lookupReaderAcceptsRangeAfterExistingDeletes() throws Exception {
    Table table = createTableWithRecords();
    addEqualityDelete(table, 2L);
    long deleteSnapshot = table.currentSnapshot().snapshotId();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    long appendSnapshot = table.currentSnapshot().snapshotId();

    IcebergLookupReader reader = lookupReader(table, ImmutableList.of(), false);

    assertThat(reader.isAppendOnlyRange(deleteSnapshot, appendSnapshot))
        .as("Delete files added before the range do not prevent applying an append")
        .isTrue();
  }

  @Test
  void lookupReaderRejectsRangeThatIsNotAnAncestor() throws Exception {
    Table table = createTableWithRecords();
    long firstSnapshot = table.currentSnapshot().snapshotId();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    long secondSnapshot = table.currentSnapshot().snapshotId();

    IcebergLookupReader reader = lookupReader(table, ImmutableList.of(), false);

    assertThat(reader.isAppendOnlyRange(secondSnapshot, firstSnapshot))
        .as("A range that goes back in history cannot be applied to a loaded cache")
        .isFalse();
  }

  @Test
  void lookupReaderAcceptsCompactionRange() throws Exception {
    Table table = createTableWithRecords();
    long firstSnapshot = table.currentSnapshot().snapshotId();

    compactDataFiles(table);
    long secondSnapshot = table.currentSnapshot().snapshotId();

    IcebergLookupReader reader = lookupReader(table, ImmutableList.of(), false);

    assertThat(reader.isAppendOnlyRange(firstSnapshot, secondSnapshot))
        .as("A compaction snapshot changes the file layout but not the live rows")
        .isTrue();
  }

  @Test
  void lookupReaderReadsNoRowsFromCompaction() throws Exception {
    Table table = createTableWithRecords();
    long firstSnapshot = table.currentSnapshot().snapshotId();

    compactDataFiles(table);
    long secondSnapshot = table.currentSnapshot().snapshotId();

    IcebergLookupReader reader = lookupReader(table, ImmutableList.of(), false);

    assertThat(readDeltaIds(reader, firstSnapshot, secondSnapshot))
        .as("A compaction snapshot contributes no rows to a delta read")
        .isEmpty();
  }

  @Test
  void lookupFunctionReturnsRowsFromCache() throws Exception {
    Table table = createTableWithRecords();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, false, null);
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

    lookupFunction = newLookupFunction(ID_AND_CATEGORY_KEY_INDEXES, false, null);
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

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, false, null);
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

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, null);
    lookupFunction.open(new FunctionContext(null));

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    assertThat(lookupFunction.lookup(keyRow(6L))).isEmpty();
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  void lookupFunctionHandlesEmptyTable() throws Exception {
    CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, null);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L))).isEmpty();
  }

  @Test
  void lookupFunctionHandlesMultipleRowsPerKey() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    appendRecords(
        table,
        ImmutableList.of(
            record(1L, "alice", "A"), record(1L, "alice-2", "A"), record(2L, "bob", "B")));

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, false, null);
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
            ID_KEY_INDEXES,
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

  @Test
  void lookupFunctionReloadsCacheFromTrigger() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    DataFile initialFile = appendRecords(table, ImmutableList.of(record(1L, "alice", "A")));
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    assertThat(lookupFunction.lookup(keyRow(1L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));

    assertThat(lookupFunction.lookup(keyRow(6L))).isEmpty();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    replaceFile(table, initialFile, ImmutableList.of(record(1L, "alicia", "C")));

    reloadTrigger.triggerReload().join();

    assertThat(lookupFunction.lookup(keyRow(6L)))
        .singleElement()
        .satisfies(row -> assertRow(row, 6L, "frank", "D"));
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .as("A reload must replace the rows cached for an overwritten key")
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alicia", "C"));
  }

  @Test
  void lookupFunctionAppliesDeltaOnReload() throws Exception {
    Table table = createTableWithRecords();
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    reloadTrigger.triggerReload().join();

    assertThat(lookupFunction.lookup(keyRow(6L)))
        .as("The reload must pick up the appended row")
        .singleElement()
        .satisfies(row -> assertRow(row, 6L, "frank", "D"));
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .as("An incremental reload must keep the rows loaded before it")
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  void lookupFunctionReloadsFullyWhenRowsAreDeleted() throws Exception {
    Table table = createTableWithRecords();
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    reloadTrigger.triggerReload().join();
    assertThat(lookupFunction.lookup(keyRow(6L))).hasSize(1);

    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 6L)).commit();
    reloadTrigger.triggerReload().join();

    assertThat(lookupFunction.lookup(keyRow(6L)))
        .as("A delete snapshot must be applied by a full reload")
        .isEmpty();
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .as("A full reload must keep the rows that were not deleted")
        .hasSize(1);
  }

  @Test
  void lookupFunctionAppliesDeltaAfterCompaction() throws Exception {
    Table table = createTableWithRecords();
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    compactDataFiles(table);
    reloadTrigger.triggerReload().join();

    appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));
    reloadTrigger.triggerReload().join();

    assertThat(lookupFunction.lookup(keyRow(6L)))
        .as("An append after a compaction must still be applied incrementally")
        .singleElement()
        .satisfies(row -> assertRow(row, 6L, "frank", "D"));
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .as("The cache must keep the rows loaded before the compaction")
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  void lookupFunctionFallsBackToFullLoadWhenDeltaFails() throws Exception {
    Table table = createTableWithRecords();
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    DataFile appendedFile = appendRecords(table, ImmutableList.of(record(6L, "frank", "D")));

    DataFile replacementFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder)
            .writeFile(ImmutableList.of(record(6L, "frank", "D")));
    table.newRewrite().deleteFile(appendedFile).addFile(replacementFile).commit();

    table.io().deleteFile(appendedFile.location());

    assertThatCode(() -> reloadTrigger.triggerReload().join())
        .as("A failed incremental reload must not fail the reload itself")
        .doesNotThrowAnyException();

    assertThat(lookupFunction.lookup(keyRow(6L)))
        .as("The failed incremental reload must be replaced by a full load")
        .singleElement()
        .satisfies(row -> assertRow(row, 6L, "frank", "D"));
    assertThat(lookupFunction.lookup(keyRow(1L)))
        .as("A full load must keep the rows that were cached before it")
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  void lookupFunctionLoadsCacheFromEmptyTableAfterAppend() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, reloadTrigger);
    lookupFunction.open(new FunctionContext(null));

    appendRecords(table, ImmutableList.of(record(1L, "alice", "A")));
    reloadTrigger.triggerReload().join();

    assertThat(lookupFunction.lookup(keyRow(1L)))
        .as("A cache loaded from an empty table must be rebuilt after the first append")
        .singleElement()
        .satisfies(row -> assertRow(row, 1L, "alice", "A"));
  }

  @Test
  void lookupFunctionWithTriggerLoadsCacheLazily() throws Exception {
    Table table = createTableWithRecords();
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, false, reloadTrigger);
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
  void lookupFunctionSkipsReloadWhenSnapshotIsUnchanged() throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    DataFile dataFile = appendRecords(table, ImmutableList.of(record(1L, "alice", "A")));
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, reloadTrigger);
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
  void lookupFunctionFailsLookupsWhenReloadFails() throws Exception {
    Table table = createTableWithRecords();
    ManualCacheReloadTrigger reloadTrigger = new ManualCacheReloadTrigger();

    lookupFunction = newLookupFunction(ID_KEY_INDEXES, true, reloadTrigger);
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

  private IcebergFullCachingLookupFunction newLookupFunction(
      int[] lookupKeyIndexes, boolean eagerLoad, @Nullable CacheReloadTrigger reloadTrigger) {
    return new IcebergFullCachingLookupFunction(
        CATALOG_EXTENSION.tableLoader().clone(),
        ROW_TYPE,
        lookupKeyIndexes,
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
    try (CloseableIterable<RowData> rows = reader.read(snapshotId)) {
      return collectIds(rows);
    }
  }

  private static List<Long> readDeltaIds(
      IcebergLookupReader reader, long fromExclusive, long toInclusive) throws IOException {
    try (CloseableIterable<RowData> rows = reader.readDelta(fromExclusive, toInclusive)) {
      return collectIds(rows);
    }
  }

  private static List<Long> collectIds(CloseableIterable<RowData> rows) {
    List<Long> ids = Lists.newArrayList();
    for (RowData row : rows) {
      ids.add(row.isNullAt(0) ? null : row.getLong(0));
    }

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

  private DataFile appendRecords(Table table, List<Record> records) throws Exception {
    GenericAppenderHelper helper =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);
    DataFile dataFile = helper.writeFile(records);
    helper.appendToTable(dataFile);
    return dataFile;
  }

  private void replaceFile(Table table, DataFile oldFile, List<Record> records) throws Exception {
    GenericAppenderHelper helper =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);
    table.newOverwrite().deleteFile(oldFile).addFile(helper.writeFile(records)).commit();
  }

  private void compactDataFiles(Table table) throws Exception {
    List<DataFile> oldFiles = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        oldFiles.add(task.file());
      }
    }

    List<Record> records = Lists.newArrayList();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      rows.forEach(records::add);
    }

    GenericAppenderHelper helper =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);
    RewriteFiles rewrite = table.newRewrite();
    oldFiles.forEach(rewrite::deleteFile);
    rewrite.addFile(helper.writeFile(records)).commit();
  }

  private void addEqualityDelete(Table table, long id) throws Exception {
    Schema deleteSchema = SCHEMA.select("id");
    GenericRecord deleteRecord = GenericRecord.create(deleteSchema);
    deleteRecord.setField("id", id);

    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
            table,
            table.io().newOutputFile(temporaryFolder.resolve("equality-delete.parquet").toString()),
            ImmutableList.of(deleteRecord),
            deleteSchema);
    table.newRowDelta().addDeletes(deleteFile).commit();
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

  private static void assertRow(RowData row, long id, String data, String category) {
    assertThat(row.getLong(0)).isEqualTo(id);
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
