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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Covers the write path that resolves equality deletes inside the sink. The table must never end up
 * with an equality delete, and its rows must match what the upsert stream expresses.
 */
@Timeout(value = 180)
class TestIcebergSinkDvOnly {

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

  @Test
  void upsertWithinOneCheckpoint() throws Exception {
    // The second row supersedes the first while both are still in flight, so the writer can point
    // the delete straight at the row it just wrote.
    runDvOnlySink(
        "single",
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "aaa"), row("+I", 1, "bbb"), row("+I", 2, "ccc"))));

    assertNoEqualityDeletes();
    assertRows(record(1, "bbb"), record(2, "ccc"));
  }

  @Test
  void upsertAcrossCheckpoints() throws Exception {
    // The row replaced in the second checkpoint was committed by the first one, so its location is
    // only known through the primary key index.
    runDvOnlySink(
        "across",
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "aaa"), row("+I", 2, "bbb")),
            ImmutableList.of(row("+I", 1, "zzz"))));

    assertNoEqualityDeletes();
    assertRows(record(1, "zzz"), record(2, "bbb"));
    assertThat(deletionVectorCount()).isPositive();
  }

  @Test
  void deleteAcrossCheckpoints() throws Exception {
    runDvOnlySink(
        "delete",
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "aaa"), row("+I", 2, "bbb")),
            ImmutableList.of(row("-D", 1, "aaa"))));

    assertNoEqualityDeletes();
    assertRows(record(2, "bbb"));
  }

  @Test
  void reInsertOfADeletedKeySurvivesItsOwnCheckpoint() throws Exception {
    runDvOnlySink(
        "reinsert",
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "aaa")),
            ImmutableList.of(row("-D", 1, "aaa"), row("+I", 1, "ccc"))));

    assertNoEqualityDeletes();
    assertRows(record(1, "ccc"));
  }

  @Test
  void repeatedUpsertsKeepOnlyTheLastRow() throws Exception {
    runDvOnlySink(
        "repeated",
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "v1")),
            ImmutableList.of(row("+I", 1, "v2")),
            ImmutableList.of(row("+I", 1, "v3")),
            ImmutableList.of(row("+I", 1, "v4"))));

    assertNoEqualityDeletes();
    assertRows(record(1, "v4"));
  }

  @Test
  void resolvesRowsCommittedBeforeTheJobStarted() throws Exception {
    // A separate job leaves no state behind, so the second one can only learn where these rows
    // live by reading the table while it starts up.
    runDvOnlySink(
        "seed-first",
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "old-1"), row("+I", 2, "old-2"), row("+I", 3, "old-3"))));
    assertRows(record(1, "old-1"), record(2, "old-2"), record(3, "old-3"));

    runDvOnlySink(
        "seed-second",
        ImmutableList.of(ImmutableList.of(row("+I", 1, "new-1"), row("-D", 2, "old-2"))));

    assertNoEqualityDeletes();
    assertRows(record(1, "new-1"), record(3, "old-3"));
  }

  @Test
  void deletesRowsOfDataFilesWrittenWithAnEarlierSpec() throws Exception {
    runDvOnlySink(
        "spec-0",
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "old-1"), row("+I", 2, "old-2"), row("+I", 3, "old-3"))));
    table.updateSpec().addField(Expressions.bucket("id", 2)).commit();

    // The rows replaced here live in a data file of the unpartitioned spec, while the rows written
    // now use the bucketed one.
    runDvOnlySink(
        "spec-1", ImmutableList.of(ImmutableList.of(row("+I", 1, "new-1"), row("-D", 2, "old-2"))));

    assertNoEqualityDeletes();
    assertRows(record(1, "new-1"), record(3, "old-3"));
    assertDeletionVectorsTrackedWithTheirDataFileSpec();
  }

  @Test
  void resolvesTheLastDeletesWhenTheInputEndsWithoutACheckpoint() throws Exception {
    runDvOnlySink(
        "no-checkpoint-seed",
        ImmutableList.of(ImmutableList.of(row("+I", 1, "aaa"), row("+I", 2, "bbb"))));

    // Without checkpointing everything is resolved and committed when the input ends.
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .setParallelism(1)
            .setMaxParallelism(1);
    DataStream<Row> input =
        env.addSource(
            new BoundedTestSource<>(
                ImmutableList.of(ImmutableList.of(row("-D", 1, "aaa"), row("+I", 3, "ccc"))),
                false),
            ROW_TYPE_INFO);
    sinkBuilder(input, "no-checkpoint", true)
        .equalityFieldColumns(ImmutableList.of("id"))
        .upsert(true)
        .append();
    env.execute("dv-only without checkpoints");
    table.refresh();

    assertNoEqualityDeletes();
    assertRows(record(2, "bbb"), record(3, "ccc"));
  }

  @Test
  void rejectsBatchExecution() {
    StreamExecutionEnvironment env = newEnv();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);

    assertThatThrownBy(
            () ->
                builder(env, "batch", true, ImmutableList.of())
                    .equalityFieldColumns(ImmutableList.of("id"))
                    .upsert(true)
                    .append())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("requires streaming execution");
  }

  @Test
  void switchedOffKeepsWritingEqualityDeletes() throws Exception {
    runSink("off", false, ImmutableList.of(ImmutableList.of(row("+I", 1, "aaa"))));
    runSink("off-2", false, ImmutableList.of(ImmutableList.of(row("+I", 1, "bbb"))));

    assertThat(equalityDeleteCount())
        .as("Default write path still uses equality deletes")
        .isPositive();
    assertRows(record(1, "bbb"));
  }

  @Test
  void requiresEqualityFieldColumns() {
    assertThatThrownBy(() -> builder(newEnv(), "no-keys", true, ImmutableList.of()).append())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Equality field columns must be set");
  }

  @Test
  void cannotBeCombinedWithTheMaintenanceTask() {
    assertThatThrownBy(
            () ->
                builder(newEnv(), "conflict", true, ImmutableList.of())
                    .equalityFieldColumns(ImmutableList.of("id"))
                    .upsert(true)
                    .convertEqualityDeletes()
                    .append())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("cannot be enabled together");
  }

  @Test
  void rejectsATableThatStillHasEqualityDeletes() throws Exception {
    runSink("legacy", false, ImmutableList.of(ImmutableList.of(row("+I", 1, "aaa"))));
    runSink("legacy-2", false, ImmutableList.of(ImmutableList.of(row("+I", 1, "bbb"))));
    assertThat(equalityDeleteCount()).isPositive();

    assertThatThrownBy(
            () ->
                builder(newEnv(), "reject", true, ImmutableList.of())
                    .equalityFieldColumns(ImmutableList.of("id"))
                    .upsert(true)
                    .append())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("equality delete record(s)");
  }

  @Test
  void rejectsATableUpgradedWithPositionDeleteFiles() {
    CATALOG_EXTENSION.catalog().dropTable(TestFixtures.TABLE_IDENTIFIER, true);
    table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));
    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(2)
            .build();
    DeleteFile positionDeletes =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath("/path/to/position-deletes.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newRowDelta().addRows(dataFile).addDeletes(positionDeletes).commit();
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

    assertThatThrownBy(
            () ->
                builder(newEnv(), "upgraded", true, ImmutableList.of())
                    .equalityFieldColumns(ImmutableList.of("id"))
                    .upsert(true)
                    .append())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("position delete file")
        .hasMessageContaining(positionDeletes.location());
  }

  private void runDvOnlySink(String uidSuffix, List<List<Row>> elementsPerCheckpoint)
      throws Exception {
    runSink(uidSuffix, true, elementsPerCheckpoint);
  }

  private void runSink(String uidSuffix, boolean dvOnly, List<List<Row>> elementsPerCheckpoint)
      throws Exception {
    StreamExecutionEnvironment env = newEnv();
    builder(env, uidSuffix, dvOnly, elementsPerCheckpoint)
        .equalityFieldColumns(ImmutableList.of("id"))
        .upsert(true)
        .append();

    env.execute("dv-only=" + dvOnly + " " + uidSuffix);
    table.refresh();
  }

  private IcebergSink.Builder builder(
      StreamExecutionEnvironment env,
      String uidSuffix,
      boolean dvOnly,
      List<List<Row>> elementsPerCheckpoint) {
    DataStream<Row> input =
        env.addSource(new BoundedTestSource<>(elementsPerCheckpoint), ROW_TYPE_INFO);
    return sinkBuilder(input, uidSuffix, dvOnly);
  }

  private IcebergSink.Builder sinkBuilder(DataStream<Row> input, String uidSuffix, boolean dvOnly) {
    IcebergSink.Builder builder =
        IcebergSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
            .tableLoader(tableLoader)
            .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
            .writeParallelism(1)
            .uidSuffix(uidSuffix);
    return dvOnly ? builder.dvOnly() : builder;
  }

  /**
   * Every deletion vector has to be tracked by a manifest of the spec of the data file it
   * references, otherwise it is read with the wrong partition.
   */
  private void assertDeletionVectorsTrackedWithTheirDataFileSpec() throws IOException {
    Snapshot snapshot = table.snapshot(SnapshotRef.MAIN_BRANCH);
    Map<String, Integer> dataFileSpecs = Maps.newHashMap();
    for (ManifestFile manifest : snapshot.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        reader.forEach(file -> dataFileSpecs.put(file.location(), file.specId()));
      }
    }

    long checked = 0;
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile deleteFile : reader) {
          assertThat(dataFileSpecs.get(deleteFile.referencedDataFile()))
              .as("Spec of the data file %s", deleteFile.referencedDataFile())
              .isEqualTo(manifest.partitionSpecId());
          checked++;
        }
      }
    }

    assertThat(checked).as("Deletion vectors checked").isPositive();
  }

  private static StreamExecutionEnvironment newEnv() {
    return StreamExecutionEnvironment.getExecutionEnvironment(
            MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
        .enableCheckpointing(100L)
        .setParallelism(1)
        .setMaxParallelism(1);
  }

  private void assertNoEqualityDeletes() {
    assertThat(equalityDeleteCount()).as("Equality delete files on the table").isZero();
  }

  private void assertRows(Record... expected) throws IOException {
    assertThat(SimpleDataUtil.actualRowSet(table, "*"))
        .isEqualTo(SimpleDataUtil.expectedRowSet(table, expected));
  }

  private long equalityDeleteCount() {
    return countDeleteFiles(file -> file.content() == FileContent.EQUALITY_DELETES);
  }

  private long deletionVectorCount() {
    return countDeleteFiles(ContentFileUtil::isDV);
  }

  private long countDeleteFiles(java.util.function.Predicate<DeleteFile> matches) {
    Snapshot snapshot = table.snapshot(SnapshotRef.MAIN_BRANCH);
    if (snapshot == null) {
      return 0;
    }

    long count = 0;
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile deleteFile : reader) {
          if (matches.test(deleteFile)) {
            count++;
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read " + manifest.path(), e);
      }
    }

    return count;
  }

  private static Record record(int id, String data) {
    return SimpleDataUtil.createRecord(id, data);
  }

  private static Row row(String rowKind, int id, String data) {
    Map<String, RowKind> kinds =
        ImmutableMap.of(
            "+I", RowKind.INSERT,
            "-D", RowKind.DELETE,
            "-U", RowKind.UPDATE_BEFORE,
            "+U", RowKind.UPDATE_AFTER);
    return Row.ofKind(kinds.get(rowKind), id, data);
  }
}
