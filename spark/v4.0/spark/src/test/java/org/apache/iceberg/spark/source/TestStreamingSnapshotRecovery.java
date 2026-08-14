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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RecoveryMetadataTestHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalogServer;
import org.apache.iceberg.rest.RESTServerExtension;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class TestStreamingSnapshotRecovery extends TestBase {
  private static final String TABLE_NAME = "default.streaming_snapshot_recovery";
  private static final String RECOVERY_TABLE_NAME = "default.streaming_snapshot_recovery_repair";
  private static final String REST_CATALOG_NAME = "recovery_rest";
  private static final String REST_TABLE_NAME = REST_CATALOG_NAME + "." + TABLE_NAME;
  private static final String REST_RECOVERY_TABLE_NAME =
      REST_CATALOG_NAME + "." + RECOVERY_TABLE_NAME;
  private static final long ONE_BATCH_TRIGGER_MS = 3_600_000L;
  private static final long BATCH_TIMEOUT_MS = 30_000L;
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  @RegisterExtension
  private static final RESTServerExtension REST_SERVER =
      new RESTServerExtension(
          Map.of(
              RESTCatalogServer.REST_PORT,
              RESTServerExtension.FREE_PORT,
              CatalogProperties.CATALOG_IMPL,
              SwappableTestCatalog.class.getName()));

  @TempDir private Path temp;

  private TestTables.TestTable table;
  private Path checkpoint;
  private Path output;
  private String streamTableName;
  private int metadataVersion;

  @BeforeAll
  static void configureCatalog() {
    spark.conf().set("spark.sql.catalog.spark_catalog", TestSparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
    spark.conf().set("spark.sql.catalog.spark_catalog.cache-enabled", "false");
    spark.conf().set("spark.sql.catalog." + REST_CATALOG_NAME, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + REST_CATALOG_NAME + ".type", "rest");
    spark
        .conf()
        .set(
            "spark.sql.catalog." + REST_CATALOG_NAME + ".uri",
            "http://localhost:" + REST_SERVER.config().get(RESTCatalogServer.REST_PORT));
    spark.conf().set("spark.sql.catalog." + REST_CATALOG_NAME + ".cache-enabled", "true");
  }

  @BeforeEach
  void createTable() {
    table =
        TestTables.create(
            temp.resolve("table").toFile(),
            TABLE_NAME,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            Map.of(TableProperties.FORMAT_VERSION, "2"));
    checkpoint = temp.resolve("checkpoint");
    output = temp.resolve("output");
    metadataVersion = 0;
  }

  @AfterEach
  void cleanup() throws Exception {
    for (StreamingQuery query : spark.streams().active()) {
      query.stop();
    }

    TestSparkCatalog.clearTables();
    TestTables.clearTables();
  }

  @Test
  void resumesThroughRecoveryMetadataAndHandsOffToLiveTable() throws Exception {
    verifyRecovery(TABLE_NAME, RECOVERY_TABLE_NAME);
  }

  @Test
  void resumesThroughRecoveryMetadataOverRest() throws Exception {
    verifyRecovery(REST_TABLE_NAME, REST_RECOVERY_TABLE_NAME);
  }

  private void verifyRecovery(String liveStreamTableName, String recoveryStreamTableName)
      throws Exception {
    this.streamTableName = liveStreamTableName;
    appendSnapshot(1);
    long snapshot1Id = table.currentSnapshot().snapshotId();

    runOneBatch();
    assertThat(readOutputIds()).containsExactly(1);

    appendSnapshot(2, 3);
    long checkpointSnapshotId = table.currentSnapshot().snapshotId();
    runOneBatch();

    StreamingOffset partialOffset = latestCheckpointOffset();
    assertThat(partialOffset.snapshotId()).isEqualTo(checkpointSnapshotId);
    assertThat(partialOffset.position()).isEqualTo(1L);
    assertThat(readOutputIds()).hasSize(2).contains(1);

    appendSnapshot(4);
    appendSnapshot(5);
    Snapshot oldTerminalSnapshot = table.currentSnapshot();
    TableMetadata oldMetadata = TestTables.readMetadata(TABLE_NAME);
    List<DataFile> oldFiles = activeDataFiles();
    Map<Long, byte[]> oldManifestLists = readManifestLists(oldMetadata);

    assertThat(snapshotIds(oldMetadata))
        .containsExactly(
            snapshot1Id,
            checkpointSnapshotId,
            oldTerminalSnapshot.parentId(),
            oldTerminalSnapshot.snapshotId());

    String liveTableUUID = oldMetadata.uuid();
    replaceWithEmptyV3Metadata(oldMetadata, liveTableUUID);
    appendFiles(oldFiles.subList(0, 2));
    Snapshot firstReplacementSnapshot = table.currentSnapshot();
    appendFiles(oldFiles.subList(2, oldFiles.size()));
    Snapshot finalReplacementSnapshot = table.currentSnapshot();

    assertThat(firstReplacementSnapshot.parentId()).isNull();
    assertThat(finalReplacementSnapshot.parentId())
        .isEqualTo(firstReplacementSnapshot.snapshotId());
    assertThat(finalReplacementSnapshot.snapshotId())
        .isNotEqualTo(oldTerminalSnapshot.snapshotId());
    assertThat(activeFileLocations()).containsExactlyInAnyOrderElementsOf(fileLocations(oldFiles));
    assertThat(activeDataFiles()).allSatisfy(file -> assertThat(file.firstRowId()).isNotNull());
    assertThat(firstReplacementSnapshot.firstRowId()).isZero();
    assertThat(firstReplacementSnapshot.addedRows()).isEqualTo(2L);
    assertThat(finalReplacementSnapshot.firstRowId()).isEqualTo(2L);
    assertThat(finalReplacementSnapshot.addedRows()).isEqualTo(3L);
    assertThat(TestTables.readMetadata(TABLE_NAME).nextRowId()).isEqualTo(5L);

    appendSnapshot(6);
    Snapshot recoveryJoinSnapshot = table.currentSnapshot();
    appendSnapshot(7);
    Snapshot writeBeforeRecovery = table.currentSnapshot();

    StreamingQuery failedRestart = startStream(null);
    try {
      assertThatThrownBy(failedRestart::processAllAvailable)
          .hasCauseInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              String.format(
                  "Cannot load current offset at snapshot %d, the snapshot was expired or removed",
                  checkpointSnapshotId));
    } finally {
      stop(failedRestart);
    }

    String recoveryTableUUID = UUID.randomUUID().toString();
    TableMetadata recoveryMetadata =
        recoveryMetadata(
            oldMetadata,
            oldTerminalSnapshot,
            finalReplacementSnapshot,
            recoveryJoinSnapshot,
            oldFiles.stream().mapToLong(DataFile::recordCount).sum(),
            recoveryTableUUID);
    TableMetadata persistedRecoveryMetadata = roundTrip(recoveryMetadata);
    assertThat(oldMetadata.uuid()).isEqualTo(liveTableUUID);
    assertThat(recoveryTableUUID).isNotEqualTo(liveTableUUID);
    assertOldManifestListsUnchanged(oldMetadata, persistedRecoveryMetadata, oldManifestLists);
    TestTables.replaceMetadata(RECOVERY_TABLE_NAME, persistedRecoveryMetadata);
    TestTables.TestTable recoveryTable = TestTables.load(RECOVERY_TABLE_NAME);

    assertThat(recoveryTable.snapshot(checkpointSnapshotId)).isNotNull();
    assertThat(recoveryTable.currentSnapshot().snapshotId())
        .isEqualTo(recoveryJoinSnapshot.snapshotId());
    assertThat(recoveryTable.snapshot(finalReplacementSnapshot.snapshotId()).operation())
        .isEqualTo(DataOperations.REPLACE);
    assertThat(recoveryTable.snapshot(finalReplacementSnapshot.snapshotId()).parentId())
        .isEqualTo(oldTerminalSnapshot.snapshotId());
    assertThat(recoveryTable.snapshot(finalReplacementSnapshot.snapshotId()).firstRowId()).isZero();
    assertThat(recoveryTable.snapshot(finalReplacementSnapshot.snapshotId()).addedRows())
        .isEqualTo(5L);
    assertThat(recoveryTable.snapshot(finalReplacementSnapshot.snapshotId()).summary()).isEmpty();
    assertThat(recoveryTable.snapshot(finalReplacementSnapshot.snapshotId()).manifestListLocation())
        .isNotEqualTo(finalReplacementSnapshot.manifestListLocation());
    assertThat(
            manifestListHeader(
                recoveryTable
                    .snapshot(finalReplacementSnapshot.snapshotId())
                    .manifestListLocation(),
                "parent-snapshot-id"))
        .isEqualTo(String.valueOf(oldTerminalSnapshot.snapshotId()));
    assertThat(recoveryTable.operations().current().nextRowId()).isEqualTo(6L);
    assertThat(readRowIDs(recoveryStreamTableName)).containsExactly(0L, 1L, 2L, 3L, 4L, 5L);

    streamTableName = recoveryStreamTableName;
    StreamingQuery recoveryQuery = startStream(null);
    appendSnapshot(8);
    Snapshot writeDuringRecovery = table.currentSnapshot();
    recoveryQuery.processAllAvailable();
    stop(recoveryQuery);

    assertThat(readOutputIds()).containsExactly(1, 2, 3, 4, 5, 6);
    assertThat(latestCheckpointOffset().snapshotId()).isEqualTo(recoveryJoinSnapshot.snapshotId());
    assertLatestOffsetCommitted();

    table.refresh();
    assertThat(table.operations().current().uuid()).isEqualTo(liveTableUUID);
    assertThat(table.snapshot(recoveryJoinSnapshot.snapshotId())).isNotNull();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(writeDuringRecovery.snapshotId());
    assertThat(writeDuringRecovery.parentId()).isEqualTo(writeBeforeRecovery.snapshotId());

    streamTableName = liveStreamTableName;
    StreamingQuery liveQuery = startStream(null);
    appendSnapshot(9);
    liveQuery.processAllAvailable();
    stop(liveQuery);

    assertThat(readOutputIds()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);

    StreamingQuery finalRestart = startStream(null);
    finalRestart.processAllAvailable();
    stop(finalRestart);
    assertThat(readOutputIds()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertThat(readRowIDs(liveStreamTableName)).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
  }

  private void replaceWithEmptyV3Metadata(TableMetadata oldMetadata, String liveTableUUID) {
    TableMetadata emptyV3Metadata =
        TableMetadata.buildFrom(oldMetadata)
            .removeSnapshots(snapshotIds(oldMetadata))
            .upgradeFormatVersion(3)
            .assignUUID(liveTableUUID)
            .build();
    TestTables.replaceMetadata(TABLE_NAME, emptyV3Metadata);
    table.refresh();
    assertThat(table.currentSnapshot()).isNull();
  }

  private TableMetadata recoveryMetadata(
      TableMetadata oldMetadata,
      Snapshot oldTerminalSnapshot,
      Snapshot finalReplacementSnapshot,
      Snapshot recoveryJoinSnapshot,
      long existingRows,
      String recoveryTableUUID)
      throws IOException {
    Snapshot bridge =
        RecoveryMetadataTestHelpers.newReplaceBridge(
            finalReplacementSnapshot,
            oldTerminalSnapshot.snapshotId(),
            0L,
            existingRows,
            table.io(),
            table.encryption(),
            table
                .io()
                .newOutputFile(temp.resolve("recovery-bridge-manifest-list.avro").toString()));

    return TableMetadata.buildFrom(oldMetadata)
        .upgradeFormatVersion(3)
        .assignUUID(recoveryTableUUID)
        .addSnapshot(bridge)
        .addSnapshot(recoveryJoinSnapshot)
        .setBranchSnapshot(recoveryJoinSnapshot.snapshotId(), SnapshotRef.MAIN_BRANCH)
        .build();
  }

  private TableMetadata roundTrip(TableMetadata metadata) {
    String metadataLocation =
        temp.resolve(String.format("v%05d.metadata.json", metadataVersion++)).toString();
    TableMetadataParser.write(metadata, table.io().newOutputFile(metadataLocation));
    return TableMetadataParser.read(table.io(), metadataLocation);
  }

  private String manifestListHeader(String location, String key) throws IOException {
    try (DataFileReader<Object> reader =
        new DataFileReader<>(
            new SeekableFileInput(new File(location)), new GenericDatumReader<>())) {
      return reader.getMetaString(key);
    }
  }

  private Map<Long, byte[]> readManifestLists(TableMetadata metadata) throws IOException {
    Map<Long, byte[]> manifestLists = new HashMap<>();
    for (Snapshot snapshot : metadata.snapshots()) {
      manifestLists.put(
          snapshot.snapshotId(), Files.readAllBytes(Path.of(snapshot.manifestListLocation())));
    }

    return manifestLists;
  }

  private void assertOldManifestListsUnchanged(
      TableMetadata oldMetadata,
      TableMetadata recoveryMetadata,
      Map<Long, byte[]> expectedManifestLists)
      throws IOException {
    for (Snapshot oldSnapshot : oldMetadata.snapshots()) {
      Snapshot recoverySnapshot = recoveryMetadata.snapshot(oldSnapshot.snapshotId());
      assertThat(recoverySnapshot.manifestListLocation())
          .isEqualTo(oldSnapshot.manifestListLocation());
      assertThat(Files.readAllBytes(Path.of(recoverySnapshot.manifestListLocation())))
          .containsExactly(expectedManifestLists.get(oldSnapshot.snapshotId()));
    }
  }

  private void appendSnapshot(int... ids) throws IOException {
    List<DataFile> files = new ArrayList<>();
    for (int id : ids) {
      files.add(writeDataFile(id));
    }

    appendFiles(files);
  }

  private DataFile writeDataFile(int id) throws IOException {
    Record record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("data", "value-" + id);
    return FileHelpers.writeDataFile(
        table,
        table.io().newOutputFile(temp.resolve("data-" + id + ".parquet").toString()),
        List.of(record));
  }

  private void appendFiles(List<DataFile> files) {
    AppendFiles append = table.newFastAppend();
    files.forEach(append::appendFile);
    append.commit();
    TestTables.replaceMetadata(TABLE_NAME, roundTrip(TestTables.readMetadata(TABLE_NAME)));
    table.refresh();
  }

  private List<DataFile> activeDataFiles() throws IOException {
    List<DataFile> files = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> files.add(task.file().copy()));
    }

    files.sort(Comparator.comparing(file -> file.location().toString()));
    return files;
  }

  private List<String> activeFileLocations() throws IOException {
    return fileLocations(activeDataFiles());
  }

  private List<String> fileLocations(List<DataFile> files) {
    return files.stream()
        .map(file -> file.location().toString())
        .sorted()
        .collect(Collectors.toList());
  }

  private List<Long> snapshotIds(TableMetadata metadata) {
    return metadata.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toList());
  }

  private void runOneBatch() throws Exception {
    StreamingQuery query = startStream(Trigger.ProcessingTime(ONE_BATCH_TRIGGER_MS));
    try {
      long deadline = System.currentTimeMillis() + BATCH_TIMEOUT_MS;
      while (query.lastProgress() == null && System.currentTimeMillis() < deadline) {
        query.awaitTermination(100L);
      }

      assertThat(query.lastProgress()).as("One streaming batch should finish").isNotNull();
    } finally {
      stop(query);
    }
  }

  private StreamingQuery startStream(Trigger trigger) throws TimeoutException {
    DataStreamWriter<Row> writer =
        spark
            .readStream()
            .format("iceberg")
            .option(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1")
            .load(streamTableName)
            .writeStream()
            .format("parquet")
            .option("checkpointLocation", checkpoint.toString())
            .option("path", output.toString());
    if (trigger != null) {
      writer = writer.trigger(trigger);
    }

    return writer.start();
  }

  private void stop(StreamingQuery query) throws Exception {
    if (query != null && query.isActive()) {
      query.stop();
      query.awaitTermination(BATCH_TIMEOUT_MS);
    }
  }

  private StreamingOffset latestCheckpointOffset() throws IOException {
    Path latest = checkpoint.resolve("offsets").resolve(String.valueOf(latestCheckpointBatchId()));

    List<String> lines = Files.readAllLines(latest);
    return StreamingOffset.fromJson(lines.get(lines.size() - 1));
  }

  private long latestCheckpointBatchId() throws IOException {
    try (Stream<Path> files = Files.list(checkpoint.resolve("offsets"))) {
      return files
          .filter(path -> path.getFileName().toString().matches("[0-9]+"))
          .mapToLong(path -> Long.parseLong(path.getFileName().toString()))
          .max()
          .orElseThrow();
    }
  }

  private void assertLatestOffsetCommitted() throws IOException {
    long latestBatchId = latestCheckpointBatchId();
    assertThat(checkpoint.resolve("commits").resolve(String.valueOf(latestBatchId))).exists();
  }

  private List<Integer> readOutputIds() {
    if (!Files.exists(output)) {
      return List.of();
    }

    DataFrameReader reader = spark.read();
    return reader.parquet(output.toString()).orderBy("id").collectAsList().stream()
        .map(row -> row.getInt(0))
        .collect(Collectors.toList());
  }

  private List<Long> readRowIDs(String tableName) {
    return spark
        .read()
        .format("iceberg")
        .load(tableName)
        .select(MetadataColumns.ROW_ID.name())
        .orderBy(MetadataColumns.ROW_ID.name())
        .collectAsList()
        .stream()
        .map(row -> row.getLong(0))
        .collect(Collectors.toList());
  }
}
