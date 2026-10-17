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
import java.util.List;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Covers how the primary key index of the DV-only write path is built and how it keeps up with the
 * data files that other operations rewrite.
 */
class TestDvOnlyIndexMaintenance {

  private static final Schema KEY_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private static final String FILE_A = "s3://bucket/table/data/file-a.parquet";
  private static final String FILE_B = "s3://bucket/table/data/file-b.parquet";

  private static final int WORKER_POOL_SIZE = 4;

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
  void theFirstCheckpointIndexesTheWholeBranch() throws Exception {
    DataFile file = appendFile("initial", row(1, "a"), row(2, "b"));

    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);

      assertThat(harness.extractOutputValues())
          .singleElement()
          .satisfies(
              record -> {
                assertThat(record.type()).isEqualTo(DvOnlyRecord.Type.READ_FILE);
                assertThat(record.filePath()).isEqualTo(file.location());
                assertThat(PkIndexReadTask.decode(record.readTask()).file().location())
                    .isEqualTo(file.location());
              });
    }
  }

  @Test
  void anEmptyBranchNeedsNoIndexing() throws Exception {
    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void aRewriteDropsTheOldFileAndIndexesItsReplacement() throws Exception {
    DataFile initial = appendFile("initial", row(1, "a"), row(2, "b"));

    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);
      harness.getOutput().clear();

      DataFile compacted = writeFile("compacted", row(1, "a"), row(2, "b"));
      table
          .newRewrite()
          .rewriteFiles(ImmutableSet.of(initial), ImmutableSet.of(compacted))
          .commit();

      harness.prepareSnapshotPreBarrier(2L);

      List<DvOnlyRecord> records = harness.extractOutputValues();
      assertThat(records).hasSize(2);
      assertThat(records.get(0).type()).isEqualTo(DvOnlyRecord.Type.DROP_FILE);
      assertThat(records.get(0).filePath()).isEqualTo(initial.location());
      assertThat(records.get(1).type()).isEqualTo(DvOnlyRecord.Type.READ_FILE);
      assertThat(records.get(1).filePath()).isEqualTo(compacted.location());
    }
  }

  @Test
  void aCheckpointWithoutTableChangesRequestsNothing() throws Exception {
    appendFile("initial", row(1, "a"));

    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);
      harness.getOutput().clear();

      harness.prepareSnapshotPreBarrier(2L);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void aCommitOfThisSinkLeavesTheIndexAlone() throws Exception {
    appendFile("initial", row(1, "a"));

    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);
      harness.getOutput().clear();

      // The rows a sink writes are reported by the writer itself.
      DataFile appended = writeFile("appended", row(3, "c"));
      table
          .newAppend()
          .appendFile(appended)
          .set(SinkUtil.FLINK_JOB_ID, harness.getEnvironment().getJobID().toString())
          .set(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID, "1")
          .commit();
      harness.prepareSnapshotPreBarrier(2L);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void anAppendOfAnotherWriterIsIndexed() throws Exception {
    appendFile("initial", row(1, "a"));

    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);
      harness.getOutput().clear();

      DataFile appended = appendFile("appended", row(3, "c"));
      harness.prepareSnapshotPreBarrier(2L);

      assertThat(harness.extractOutputValues())
          .singleElement()
          .satisfies(
              record -> {
                assertThat(record.type()).isEqualTo(DvOnlyRecord.Type.READ_FILE);
                assertThat(record.filePath()).isEqualTo(appended.location());
              });
    }
  }

  @Test
  void anExpiredBaselineIndexesTheWholeBranchAgain() throws Exception {
    DataFile initial = appendFile("initial", row(1, "a"));
    long indexedSnapshotId = table.currentSnapshot().snapshotId();

    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);
      harness.getOutput().clear();

      DataFile appended = appendFile("appended", row(3, "c"));
      table.expireSnapshots().expireSnapshotId(indexedSnapshotId).cleanExpiredFiles(false).commit();
      harness.prepareSnapshotPreBarrier(2L);

      assertThat(harness.extractOutputValues())
          .allSatisfy(record -> assertThat(record.type()).isEqualTo(DvOnlyRecord.Type.READ_FILE))
          .extracting(DvOnlyRecord::filePath)
          .containsExactlyInAnyOrder(initial.location(), appended.location());
    }
  }

  @Test
  void aRebuildBroadcastsACleanupForItsGeneration() throws Exception {
    appendFile("initial", row(1, "a"));
    long indexedSnapshotId = table.currentSnapshot().snapshotId();

    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);
      harness.getOutput().clear();

      table
          .newAppend()
          .appendFile(writeFile("appended", row(3, "c")))
          .set(SinkUtil.FLINK_JOB_ID, harness.getEnvironment().getJobID().toString())
          .set(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID, "3")
          .commit();
      table.expireSnapshots().expireSnapshotId(indexedSnapshotId).cleanExpiredFiles(false).commit();
      harness.prepareSnapshotPreBarrier(2L);

      assertThat(harness.extractOutputValues())
          .isNotEmpty()
          .allSatisfy(record -> assertThat(record.generation()).isEqualTo(1L));
      assertThat(harness.getSideOutput(DvOnlyCoordinator.CLEANUP_STREAM))
          .singleElement()
          .satisfies(
              record -> {
                assertThat(record.getValue().type()).isEqualTo(DvOnlyRecord.Type.CLEANUP);
                assertThat(record.getValue().generation()).isEqualTo(1L);
                assertThat(record.getValue().checkpointId()).isEqualTo(3L);
              });
    }
  }

  @Test
  void anIncrementalUpdateBroadcastsNoCleanup() throws Exception {
    appendFile("initial", row(1, "a"));

    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.prepareSnapshotPreBarrier(1L);
      appendFile("appended", row(3, "c"));
      harness.prepareSnapshotPreBarrier(2L);

      assertThat(harness.getSideOutput(DvOnlyCoordinator.CLEANUP_STREAM)).isNullOrEmpty();
    }
  }

  @Test
  void aCleanupDropsACommittedFileTheRebuildDidNotRead() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, DvOnlyRecord, DvOnlyRecord, DvOnlyRecord>
        harness = fileIndexHarness()) {
      harness.open();
      harness.processElement1(DvOnlyRecord.addRow(entry(key(1), FILE_A, 0L), 1L), 1L);
      harness.processElement2(DvOnlyRecord.cleanup(1L, 1L), 1L);

      assertThat(harness.extractOutputValues())
          .filteredOn(record -> record.type() == DvOnlyRecord.Type.DROP_POSITIONS)
          .singleElement()
          .satisfies(
              record -> {
                assertThat(record.filePath()).isEqualTo(FILE_A);
                assertThat(record.key()).isEqualTo(key(1));
              });
    }
  }

  @Test
  void aCleanupKeepsAFileOfAnUncommittedCheckpoint() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, DvOnlyRecord, DvOnlyRecord, DvOnlyRecord>
        harness = fileIndexHarness()) {
      harness.open();
      harness.processElement1(DvOnlyRecord.addRow(entry(key(1), FILE_A, 0L), 2L), 1L);
      harness.processElement2(DvOnlyRecord.cleanup(1L, 1L), 1L);

      assertThat(harness.extractOutputValues())
          .noneMatch(record -> record.type() == DvOnlyRecord.Type.DROP_POSITIONS);
    }
  }

  @Test
  void aCleanupKeepsAFileReadForItsGeneration() throws Exception {
    DataFile file = appendFile("initial", row(1, "a"));

    try (KeyedTwoInputStreamOperatorTestHarness<String, DvOnlyRecord, DvOnlyRecord, DvOnlyRecord>
        harness = fileIndexHarness()) {
      harness.open();
      harness.processElement1(readFile(file, 1L), 1L);
      harness.processElement2(DvOnlyRecord.cleanup(1L, 1L), 1L);

      assertThat(harness.extractOutputValues())
          .noneMatch(record -> record.type() == DvOnlyRecord.Type.DROP_POSITIONS);
    }
  }

  @Test
  void aCleanupDropsAFileReadForAnEarlierGeneration() throws Exception {
    DataFile file = appendFile("initial", row(1, "a"));

    try (KeyedTwoInputStreamOperatorTestHarness<String, DvOnlyRecord, DvOnlyRecord, DvOnlyRecord>
        harness = fileIndexHarness()) {
      harness.open();
      harness.processElement1(readFile(file, 0L), 1L);
      harness.processElement2(DvOnlyRecord.cleanup(1L, 1L), 1L);

      assertThat(harness.extractOutputValues())
          .filteredOn(record -> record.type() == DvOnlyRecord.Type.DROP_POSITIONS)
          .singleElement()
          .satisfies(record -> assertThat(record.filePath()).isEqualTo(file.location()));
    }
  }

  @Test
  void theEndOfTheInputUpdatesTheIndexAndReportsTheBaseline() throws Exception {
    DataFile file = appendFile("initial", row(1, "a"));
    long head = table.currentSnapshot().snapshotId();

    try (OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
        harness = coordinatorHarness()) {
      harness.open();
      harness.getOneInputOperator().finish();

      assertThat(harness.extractOutputValues())
          .singleElement()
          .extracting(DvOnlyRecord::filePath)
          .isEqualTo(file.location());
      assertThat(harness.getSideOutput(DvOnlyCoordinator.BASELINE_STREAM))
          .singleElement()
          .satisfies(
              record -> {
                SinkWriteResult baseline =
                    ((CommittableWithLineage<SinkWriteResult>) record.getValue()).getCommittable();
                assertThat(baseline.isBaseline()).isTrue();
                assertThat(baseline.baselineSnapshotId()).isEqualTo(head);
              });

      // Everything was reported already, so the final checkpoint requests nothing again.
      harness.getOutput().clear();
      harness.prepareSnapshotPreBarrier(1L);
      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void aFileReadTwiceIsForgottenOnce() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, DvOnlyRecord, DvOnlyRecord, DvOnlyRecord>
        harness = fileIndexHarness()) {
      harness.open();
      harness.processElement1(DvOnlyRecord.addRow(entry(key(1), FILE_A, 0L), 1L), 1L);
      harness.processElement1(DvOnlyRecord.addRow(entry(key(1), FILE_A, 0L), 1L), 1L);
      harness.processElement1(DvOnlyRecord.dropFile(FILE_A), 1L);

      assertThat(harness.extractOutputValues())
          .filteredOn(record -> record.type() == DvOnlyRecord.Type.DROP_POSITIONS)
          .hasSize(1);
    }
  }

  @Test
  void aRowReportedTwiceIsIndexedOnce() throws Exception {
    try (KeyedOneInputStreamOperatorTestHarness<SerializedEqualityValues, DvOnlyRecord, DVPosition>
        harness = resolveHarness()) {
      harness.open();
      harness.processElement(DvOnlyRecord.bootstrapRow(entry(key(1), FILE_A, 7L)), 1L);
      harness.processElement(DvOnlyRecord.bootstrapRow(entry(key(1), FILE_A, 7L)), 1L);
      harness.processElement(DvOnlyRecord.delete(key(1), 1L), 1L);
      harness.prepareSnapshotPreBarrier(1L);

      assertThat(harness.extractOutputValues()).singleElement();
    }
  }

  @Test
  void theEndOfTheInputResolvesPendingDeletes() throws Exception {
    try (KeyedOneInputStreamOperatorTestHarness<SerializedEqualityValues, DvOnlyRecord, DVPosition>
        harness = resolveHarness()) {
      harness.open();
      harness.processElement(DvOnlyRecord.bootstrapRow(entry(key(1), FILE_A, 7L)), 1L);
      harness.processElement(DvOnlyRecord.delete(key(1), 1L), 1L);
      harness.getOneInputOperator().finish();

      assertThat(harness.extractOutputValues())
          .singleElement()
          .satisfies(position -> assertThat(position.position()).isEqualTo(7L));
    }
  }

  @Test
  void droppingAFileAsksItsKeysToForgetIt() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, DvOnlyRecord, DvOnlyRecord, DvOnlyRecord>
        harness = fileIndexHarness()) {
      harness.open();
      harness.processElement1(DvOnlyRecord.addRow(entry(key(1), FILE_A, 0L), 1L), 1L);
      harness.processElement1(DvOnlyRecord.addRow(entry(key(2), FILE_A, 1L), 1L), 1L);
      harness.processElement1(DvOnlyRecord.dropFile(FILE_A), 1L);

      List<DvOnlyRecord> records = harness.extractOutputValues();
      assertThat(records).hasSize(4);
      assertThat(records.subList(0, 2))
          .allSatisfy(record -> assertThat(record.type()).isEqualTo(DvOnlyRecord.Type.ADD_ROW));
      assertThat(records.subList(2, 4))
          .allSatisfy(
              record -> {
                assertThat(record.type()).isEqualTo(DvOnlyRecord.Type.DROP_POSITIONS);
                assertThat(record.filePath()).isEqualTo(FILE_A);
              })
          .extracting(DvOnlyRecord::key)
          .containsExactlyInAnyOrder(key(1), key(2));
    }
  }

  @Test
  void aFileIsForgottenOnlyOnce() throws Exception {
    try (KeyedTwoInputStreamOperatorTestHarness<String, DvOnlyRecord, DvOnlyRecord, DvOnlyRecord>
        harness = fileIndexHarness()) {
      harness.open();
      harness.processElement1(DvOnlyRecord.addRow(entry(key(1), FILE_A, 0L), 1L), 1L);
      harness.processElement1(DvOnlyRecord.dropFile(FILE_A), 1L);
      harness.processElement1(DvOnlyRecord.dropFile(FILE_A), 1L);

      assertThat(harness.extractOutputValues())
          .filteredOn(record -> record.type() == DvOnlyRecord.Type.DROP_POSITIONS)
          .hasSize(1);
    }
  }

  @Test
  void aDroppedPositionIsNotResolvedAnyMore() throws Exception {
    try (KeyedOneInputStreamOperatorTestHarness<SerializedEqualityValues, DvOnlyRecord, DVPosition>
        harness = resolveHarness()) {
      harness.open();
      // What a rewrite of FILE_A into FILE_B looks like to the index.
      harness.processElement(DvOnlyRecord.bootstrapRow(entry(key(1), FILE_A, 7L)), 1L);
      harness.processElement(DvOnlyRecord.dropPositions(key(1), FILE_A), 1L);
      harness.processElement(DvOnlyRecord.bootstrapRow(entry(key(1), FILE_B, 3L)), 1L);
      harness.processElement(DvOnlyRecord.delete(key(1), 1L), 1L);
      harness.prepareSnapshotPreBarrier(1L);

      assertThat(harness.extractOutputValues())
          .as("The delete resolves against the file that replaced the rewritten one")
          .singleElement()
          .satisfies(
              position -> {
                assertThat(position.dataFilePath()).isEqualTo(FILE_B);
                assertThat(position.position()).isEqualTo(3L);
              });
    }
  }

  @Test
  void droppingOneFileKeepsThePositionsOfTheOthers() throws Exception {
    try (KeyedOneInputStreamOperatorTestHarness<SerializedEqualityValues, DvOnlyRecord, DVPosition>
        harness = resolveHarness()) {
      harness.open();
      harness.processElement(DvOnlyRecord.bootstrapRow(entry(key(1), FILE_A, 7L)), 1L);
      harness.processElement(DvOnlyRecord.bootstrapRow(entry(key(1), FILE_B, 3L)), 1L);
      harness.processElement(DvOnlyRecord.dropPositions(key(1), FILE_A), 1L);
      harness.processElement(DvOnlyRecord.delete(key(1), 1L), 1L);
      harness.prepareSnapshotPreBarrier(1L);

      assertThat(harness.extractOutputValues())
          .singleElement()
          .satisfies(position -> assertThat(position.dataFilePath()).isEqualTo(FILE_B));
    }
  }

  @Test
  void aDeleteOfAnUnknownKeyResolvesToNothing() throws Exception {
    try (KeyedOneInputStreamOperatorTestHarness<SerializedEqualityValues, DvOnlyRecord, DVPosition>
        harness = resolveHarness()) {
      harness.open();
      harness.processElement(DvOnlyRecord.delete(key(1), 1L), 1L);
      harness.prepareSnapshotPreBarrier(1L);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  private OneInputStreamOperatorTestHarness<CommittableMessage<SinkWriteResult>, DvOnlyRecord>
      coordinatorHarness() throws Exception {
    return new OneInputStreamOperatorTestHarness<>(
        new DvOnlyCoordinator(
            tableLoader,
            SnapshotRef.MAIN_BRANCH,
            StructLikeSerializer.keyFingerprint(KEY_SCHEMA.asStruct()),
            WORKER_POOL_SIZE));
  }

  private KeyedTwoInputStreamOperatorTestHarness<String, DvOnlyRecord, DvOnlyRecord, DvOnlyRecord>
      fileIndexHarness() throws Exception {
    return new KeyedTwoInputStreamOperatorTestHarness<>(
        new DvOnlyFileIndexOperator(tableLoader, ImmutableSet.of(1)),
        DvOnlyRecord::filePath,
        null,
        BasicTypeInfo.STRING_TYPE_INFO);
  }

  private KeyedOneInputStreamOperatorTestHarness<SerializedEqualityValues, DvOnlyRecord, DVPosition>
      resolveHarness() throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        new DvOnlyResolveOperator(),
        DvOnlyRecord::key,
        TypeInformation.of(SerializedEqualityValues.class));
  }

  private DataFile appendFile(String name, RowData... rows) throws IOException {
    DataFile file = writeFile(name, rows);
    table.newAppend().appendFile(file).commit();
    return file;
  }

  private DataFile writeFile(String name, RowData... rows) throws IOException {
    return SimpleDataUtil.writeFile(
        table,
        table.schema(),
        table.spec(),
        new Configuration(),
        table.location(),
        FileFormat.PARQUET.addExtension(name),
        Lists.newArrayList(rows));
  }

  private static DvOnlyRecord readFile(DataFile file, long generation) {
    return DvOnlyRecord.readFile(
        file.location(), new PkIndexReadTask(file, ImmutableList.of()).encode(), generation);
  }

  private static RowData row(int id, String data) {
    return SimpleDataUtil.createInsert(id, data);
  }

  private static PkIndexEntry entry(SerializedEqualityValues key, String filePath, long position) {
    return new PkIndexEntry(
        key,
        new DVPosition(
            filePath,
            position,
            0,
            StructLikeSerializer.EMPTY_PARTITION,
            PkIndexEntry.UNKNOWN_SEQUENCE));
  }

  private static SerializedEqualityValues key(int id) {
    GenericRecord record = GenericRecord.create(KEY_SCHEMA);
    record.set(0, id);
    return new StructLikeSerializer().serializeKey(record, KEY_SCHEMA.asStruct());
  }
}
