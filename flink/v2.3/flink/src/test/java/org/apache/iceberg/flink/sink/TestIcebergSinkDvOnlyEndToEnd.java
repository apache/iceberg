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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntPredicate;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Runs the DV-only write path on a cluster in the situations the operator tests cannot reproduce:
 * several writers and index shards at once, and a compaction landing between the moment the deletes
 * of a checkpoint are resolved and the moment they are committed.
 *
 * <p>Every test checks the invariants of the table besides its rows: no equality delete, at most
 * one deletion vector per data file, and no deletion vector for a data file that is gone.
 */
@Timeout(value = 300)
class TestIcebergSinkDvOnlyEndToEnd {

  private static final int PARALLELISM = 4;
  private static final int WRITE_PARALLELISM = 4;
  private static final int RESOLVE_PARALLELISM = 3;
  private static final int MAX_PARALLELISM = 128;

  private static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(
          SimpleDataUtil.FLINK_SCHEMA.getColumnDataTypes().stream()
              .map(ExternalTypeInfo::of)
              .toArray(TypeInformation[]::new));

  @RegisterExtension
  private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  private Table table;
  private TableLoader tableLoader;

  @BeforeEach
  void before() {
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.DEFAULT_FILE_FORMAT,
                    FileFormat.PARQUET.name(),
                    TableProperties.FORMAT_VERSION,
                    "3"));
    tableLoader = CATALOG_EXTENSION.tableLoader();
  }

  @AfterEach
  void after() {
    InterceptingTableLoader.BEFORE_FIRST_ROW_DELTA.set(null);
  }

  @Test
  void resolvesDeletesAcrossParallelWritersAndIndexShards() throws Exception {
    Model model = new Model();
    List<List<Row>> first = Lists.newArrayList();
    first.add(model.upsertAll(0, 200, "v1"));
    first.add(model.change(key -> key % 2 == 0, "v2", key -> key % 5 == 0));
    first.add(model.change(key -> key % 5 == 0 || key % 3 == 0, "v3", key -> false));
    // A key deleted and inserted again within the same checkpoint.
    first.add(model.deleteAndReinsert(key -> key % 7 == 0, "v4"));
    run("parallel-first", first, tableLoader);

    assertTable(model);
    assertThat(deletionVectorCount()).isPositive();

    // A new job starts without state, so every shard of its index is built from the table.
    List<List<Row>> second = Lists.newArrayList();
    second.add(model.change(key -> key % 4 == 1, "v5", key -> key % 6 == 0));
    second.add(model.deleteAndReinsert(key -> key % 9 == 0, "v6"));
    run("parallel-second", second, tableLoader);

    assertTable(model);
  }

  @Test
  void resolvesDeletesAgainAfterACompactionInTheCommitWindow() throws Exception {
    // The first commit carrying deletion vectors compacts the table right before it commits, so
    // the data file those vectors were resolved against is gone by the time they are committed.
    InterceptingTableLoader.BEFORE_FIRST_ROW_DELTA.set(this::compactEverything);

    Model model = new Model();
    List<List<Row>> checkpoints = Lists.newArrayList();
    checkpoints.add(model.upsertAll(0, 40, "v1"));
    checkpoints.add(model.change(key -> key % 2 == 0, "v2", key -> key % 3 == 0));
    // Resolves against the compacted file, which the index learns about from the table.
    checkpoints.add(model.change(key -> key % 5 == 1, "v3", key -> key % 7 == 1));
    run("commit-window", checkpoints, new InterceptingTableLoader(tableLoader));

    assertThat(InterceptingTableLoader.BEFORE_FIRST_ROW_DELTA.get())
        .as("The compaction ran in the commit window")
        .isNull();
    assertThat(table.snapshots())
        .as("Snapshots of the table")
        .anySatisfy(snapshot -> assertThat(snapshot.operation()).isEqualTo(DataOperations.REPLACE));
    assertTable(model);
  }

  private void run(String uidSuffix, List<List<Row>> checkpoints, TableLoader loader)
      throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100L)
            .setParallelism(PARALLELISM)
            .setMaxParallelism(MAX_PARALLELISM);
    DataStream<Row> input = env.addSource(new BoundedTestSource<>(checkpoints), ROW_TYPE_INFO);
    IcebergSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
        .tableLoader(loader)
        .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(WRITE_PARALLELISM)
        .uidSuffix(uidSuffix)
        .equalityFieldColumns(ImmutableList.of("id"))
        .upsert(true)
        .dvOnly(
            ImmutableMap.of(
                FlinkWriteOptions.DV_ONLY_RESOLVE_PARALLELISM.key(),
                Integer.toString(RESOLVE_PARALLELISM)))
        .append();

    env.execute("dv-only " + uidSuffix);
    table.refresh();
  }

  /**
   * Rewrites every live row of the table into a single data file, removing the data files and the
   * deletion vectors it replaces, the way a compaction applying deletes does.
   */
  private void compactEverything() {
    Table current = CATALOG_EXTENSION.catalog().loadTable(TestFixtures.TABLE_IDENTIFIER);
    Snapshot snapshot = current.currentSnapshot();
    Set<DataFile> dataFiles = Sets.newHashSet();
    Set<DeleteFile> deleteFiles = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> tasks = current.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        dataFiles.add(task.file());
        deleteFiles.addAll(task.deletes());
      }

      List<RowData> rows = Lists.newArrayList();
      try (CloseableIterable<Record> records = IcebergGenerics.read(current).build()) {
        for (Record record : records) {
          rows.add(
              SimpleDataUtil.createInsert(
                  (Integer) record.getField("id"), (String) record.getField("data")));
        }
      }

      DataFile compacted =
          SimpleDataUtil.writeFile(
              current,
              current.schema(),
              current.spec(),
              new Configuration(),
              current.location(),
              FileFormat.PARQUET.addExtension("compacted-" + UUID.randomUUID()),
              rows);
      RewriteFiles rewrite = current.newRewrite().validateFromSnapshot(snapshot.snapshotId());
      dataFiles.forEach(rewrite::deleteFile);
      deleteFiles.forEach(rewrite::deleteFile);
      rewrite.addFile(compacted).commit();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void assertTable(Model model) throws IOException {
    table.refresh();
    List<String> actual = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        actual.add(record.getField("id") + ":" + record.getField("data"));
      }
    }

    assertThat(actual).containsExactlyInAnyOrderElementsOf(model.rows());

    Snapshot snapshot = table.snapshot(SnapshotRef.MAIN_BRANCH);
    Set<String> dataFiles = Sets.newHashSet();
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        reader.forEach(file -> dataFiles.add(file.location()));
      }
    }

    Map<String, Integer> vectorsPerDataFile = Maps.newHashMap();
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile deleteFile : reader) {
          assertThat(deleteFile.content())
              .as("Delete file %s", deleteFile.location())
              .isNotEqualTo(FileContent.EQUALITY_DELETES);
          assertThat(ContentFileUtil.isDV(deleteFile)).isTrue();
          assertThat(dataFiles)
              .as("Data file of deletion vector %s", deleteFile.location())
              .contains(deleteFile.referencedDataFile());
          vectorsPerDataFile.merge(deleteFile.referencedDataFile(), 1, Integer::sum);
        }
      }
    }

    assertThat(vectorsPerDataFile.values())
        .as("Deletion vectors per data file")
        .allSatisfy(count -> assertThat(count).isEqualTo(1));
  }

  private long deletionVectorCount() throws IOException {
    long count = 0;
    Snapshot snapshot = table.snapshot(SnapshotRef.MAIN_BRANCH);
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile ignored : reader) {
          count++;
        }
      }
    }

    return count;
  }

  /** The rows the upsert stream expresses, next to the stream itself. */
  private static class Model {
    private final NavigableMap<Integer, String> rows = Maps.newTreeMap();

    private List<Row> upsertAll(int from, int to, String version) {
      List<Row> changes = Lists.newArrayList();
      for (int key = from; key < to; key++) {
        changes.add(upsert(key, version));
      }

      return changes;
    }

    /**
     * Upserts the keys matching {@code upserted}, then deletes the ones matching {@code deleted}.
     */
    private List<Row> change(IntPredicate upserted, String version, IntPredicate deleted) {
      List<Row> changes = Lists.newArrayList();
      for (int key : Lists.newArrayList(rows.keySet())) {
        if (upserted.test(key)) {
          changes.add(upsert(key, version));
        }
      }

      for (int key : Lists.newArrayList(rows.keySet())) {
        if (deleted.test(key)) {
          changes.add(Row.ofKind(RowKind.DELETE, key, rows.remove(key)));
        }
      }

      return changes;
    }

    /** Deletes the matching keys, deleted ones included, and inserts them again right after. */
    private List<Row> deleteAndReinsert(IntPredicate matches, String version) {
      List<Row> changes = Lists.newArrayList();
      int maxKey = rows.isEmpty() ? 0 : rows.lastKey();
      for (int key = 0; key <= maxKey; key++) {
        if (matches.test(key)) {
          String current = rows.remove(key);
          if (current != null) {
            changes.add(Row.ofKind(RowKind.DELETE, key, current));
          }

          changes.add(upsert(key, version));
        }
      }

      return changes;
    }

    private Row upsert(int key, String version) {
      String data = version + "-" + key;
      rows.put(key, data);
      return Row.ofKind(RowKind.INSERT, key, data);
    }

    private List<String> rows() {
      List<String> expected = Lists.newArrayList();
      rows.forEach((key, data) -> expected.add(key + ":" + data));
      return expected;
    }
  }

  /**
   * Hands out tables that run a hook right before the first {@link RowDelta} is created. Only the
   * committer creates row deltas, and only for commits that carry deletion vectors, so the hook
   * runs after the deletes of that commit were resolved and before they are committed.
   *
   * <p>The hook is static because the loader is serialized into every operator; the tests run in
   * the JVM of the mini cluster.
   */
  private static class InterceptingTableLoader implements TableLoader {
    private static final AtomicReference<Runnable> BEFORE_FIRST_ROW_DELTA = new AtomicReference<>();

    private final TableLoader delegate;

    private InterceptingTableLoader(TableLoader delegate) {
      this.delegate = delegate;
    }

    @Override
    public void open() {
      delegate.open();
    }

    @Override
    public boolean isOpen() {
      return delegate.isOpen();
    }

    @Override
    public Table loadTable() {
      Table loaded = delegate.loadTable();
      return new InterceptingTable(((HasTableOperations) loaded).operations(), loaded.name());
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public TableLoader clone() {
      return new InterceptingTableLoader(delegate.clone());
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private static class InterceptingTable extends BaseTable {
    private InterceptingTable(TableOperations ops, String name) {
      super(ops, name);
    }

    @Override
    public RowDelta newRowDelta() {
      Runnable hook = InterceptingTableLoader.BEFORE_FIRST_ROW_DELTA.getAndSet(null);
      if (hook != null) {
        hook.run();
      }

      return super.newRowDelta();
    }
  }
}
