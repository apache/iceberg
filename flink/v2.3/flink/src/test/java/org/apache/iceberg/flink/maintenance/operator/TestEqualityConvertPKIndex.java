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
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestEqualityConvertPKIndex {

  private static final SerializedEqualityValues KEY_1 =
      new SerializedEqualityValues(new byte[] {1, 2, 3});
  private static final SerializedEqualityValues KEY_2 =
      new SerializedEqualityValues(new byte[] {4, 5, 6});
  private static final Long MAIN_SNAPSHOT = 100L;
  private static final Long MAIN_SEQ = 10L;
  private static final Long NEW_MAIN_SNAPSHOT = 200L;
  private static final Long NEW_MAIN_SEQ = 20L;
  private static final byte[] EMPTY_PARTITION = new byte[0];
  // Data-row sequence below the delete sequence, so existing tests still tombstone on resolve.
  private static final long DATA_SEQ = 1L;
  private static final long DELETE_SEQ = 2L;
  // Default delete scope for the single-spec tests: global, so it resolves rows of any spec.
  private static final int GLOBAL = IndexCommand.GLOBAL_DELETE_SPEC_ID;

  @Test
  void addDataRowProducesNoOutput() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void resolveDeleteEmitsDVPosition() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  5,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 1));

      harness.watermark(1);

      List<DVPosition> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).dataFilePath()).isEqualTo("file1.parquet");
      assertThat(output.get(0).position()).isEqualTo(5);
    }
  }

  @Test
  void resolveDeleteEmitsMultipleDVPositionsForDuplicateKeys() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file2.parquet",
                  3,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 1));

      harness.watermark(1);

      List<DVPosition> output = harness.extractOutputValues();
      assertThat(output).hasSize(2);
      assertThat(output)
          .anySatisfy(
              pos -> {
                assertThat(pos.dataFilePath()).isEqualTo("file1.parquet");
                assertThat(pos.position()).isEqualTo(0);
              })
          .anySatisfy(
              pos -> {
                assertThat(pos.dataFilePath()).isEqualTo("file2.parquet");
                assertThat(pos.position()).isEqualTo(3);
              });
    }
  }

  @ParameterizedTest(name = "deleteSpecs={0} resolvedSpecs={1}")
  @MethodSource("deleteSpecScopes")
  void resolvesDeletesScopedBySpec(List<Integer> deleteSpecIds, List<Integer> resolvedSpecIds)
      throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      // Same key, one row under each of two specs. Partition evolution co-locates them on one
      // shard.
      harness.processElement(new StreamRecord<>(addRow("spec0.parquet", 0, 0), 0));
      harness.processElement(new StreamRecord<>(addRow("spec1.parquet", 1, 1), 0));
      // All deletes fall in one delete phase (ts 1), so their scopes accumulate.
      for (int deleteSpecId : deleteSpecIds) {
        harness.processElement(
            new StreamRecord<>(
                IndexCommand.resolveDelete(
                    MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, deleteSpecId),
                1));
      }

      harness.watermark(1);

      assertThat(harness.extractOutputValues())
          .extracting(DVPosition::specId)
          .containsExactlyInAnyOrderElementsOf(resolvedSpecIds);
    }
  }

  private static Stream<Arguments> deleteSpecScopes() {
    return Stream.of(
        // A partitioned delete resolves only same-spec rows.
        Arguments.of(List.of(1), List.of(1)),
        // An unpartitioned (global) delete resolves rows of every spec.
        Arguments.of(List.of(GLOBAL), List.of(0, 1)),
        // Two partitioned deletes from different specs in one phase both apply.
        Arguments.of(List.of(0, 1), List.of(0, 1)),
        // A global delete scope affects a later partitioned delete in the same phase.
        Arguments.of(List.of(GLOBAL, 0), List.of(0, 1)));
  }

  private static IndexCommand addRow(String filePath, long position, int specId) {
    return IndexCommand.addDataRow(
        MAIN_SNAPSHOT,
        MAIN_SEQ,
        KEY_1,
        filePath,
        position,
        specId,
        EMPTY_PARTITION,
        DATA_SEQ,
        false);
  }

  @Test
  void resolveDeleteClearsEntries() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 1));
      harness.watermark(1);
      assertThat(harness.extractOutputValues()).hasSize(1);

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 2));
      harness.watermark(2);
      assertThat(harness.extractOutputValues()).hasSize(1);
    }
  }

  @Test
  void resolveDeleteWithNoMatchEmitsNothing() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 1));
      harness.watermark(1);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void mainSnapshotChangeClearsState() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(
                  NEW_MAIN_SNAPSHOT, NEW_MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL),
              1));
      harness.watermark(1);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void clearBeforeReindexEvictsOrphanKeyWithoutKeyedInput() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      // KEY_1 indexed against the old main; KEY_2 left untouched.
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));

      // External commit removed KEY_1's data file; planner advances main and broadcasts CLEAR.
      // No keyed input arrives for KEY_1 in the new cycle.
      harness.processBroadcastElement(
          new StreamRecord<>(IndexCommand.clearBeforeReindex(NEW_MAIN_SNAPSHOT, NEW_MAIN_SEQ), 1));

      // Resolving KEY_1 against the (now-empty) index emits nothing.
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(
                  NEW_MAIN_SNAPSHOT, NEW_MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL),
              2));
      harness.watermark(2);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void clearBeforeReindexAfterAddPreservesEntry() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      // ADD with the new generation arrives before the broadcast.
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  NEW_MAIN_SNAPSHOT,
                  NEW_MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  7,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));
      harness.processBroadcastElement(
          new StreamRecord<>(IndexCommand.clearBeforeReindex(NEW_MAIN_SNAPSHOT, NEW_MAIN_SEQ), 1));

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(
                  NEW_MAIN_SNAPSHOT, NEW_MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL),
              2));
      harness.watermark(2);

      List<DVPosition> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).dataFilePath()).isEqualTo("file1.parquet");
      assertThat(output.get(0).position()).isEqualTo(7);
    }
  }

  @Test
  void clearBeforeReindexThenAddIndexesAtNewVersion() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "old.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));

      // Broadcast arrives before the new ADD (race direction 2): old entry is wiped, then the
      // reindex re-emits KEY_1 at its new file/position.
      harness.processBroadcastElement(
          new StreamRecord<>(IndexCommand.clearBeforeReindex(NEW_MAIN_SNAPSHOT, NEW_MAIN_SEQ), 1));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  NEW_MAIN_SNAPSHOT,
                  NEW_MAIN_SEQ,
                  KEY_1,
                  "new.parquet",
                  4,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              2));

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(
                  NEW_MAIN_SNAPSHOT, NEW_MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL),
              3));
      harness.watermark(3);

      List<DVPosition> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).dataFilePath()).isEqualTo("new.parquet");
      assertThat(output.get(0).position()).isEqualTo(4);
    }
  }

  @Test
  void olderBroadcastDoesNotEvictNewerEntry() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      // KEY_1 indexed at the newer generation: higher sequence number (20) but a lower snapshot id
      // (50) than the stale broadcast below.
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  50L, 20L, KEY_1, "file1.parquet", 7, 0, EMPTY_PARTITION, DATA_SEQ, false),
              0));

      // A stale CLEAR_INDEX from an older generation arrives late: higher snapshot id (999) but
      // lower sequence number (10). Ordering by snapshot id would wrongly evict (50 < 999);
      // ordering
      // by sequence number keeps the entry (20 is not < 10).
      harness.processBroadcastElement(
          new StreamRecord<>(IndexCommand.clearBeforeReindex(999L, 10L), 1));

      harness.processElement(
          new StreamRecord<>(IndexCommand.resolveDelete(50L, 20L, KEY_1, DELETE_SEQ, GLOBAL), 2));
      harness.watermark(2);

      List<DVPosition> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).dataFilePath()).isEqualTo("file1.parquet");
      assertThat(output.get(0).position()).isEqualTo(7);
    }
  }

  @Test
  void differentKeysAreIndependent() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_2,
                  "file2.parquet",
                  1,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 1));
      harness.watermark(1);

      List<DVPosition> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).dataFilePath()).isEqualTo("file1.parquet");
    }
  }

  @Test
  void phaseAwareBufferingProcessesInCorrectOrder() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      // A main row (ts 0) is indexed and the cycle's delete (ts 1) removes it. The next cycle's new
      // row (ts 2) arrives before that delete fires; event-time ordering keeps it out of the index
      // until after the delete, so it survives.
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "main.parquet",
                  10,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 1));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "new.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  true),
              2));

      assertThat(harness.extractOutputValues()).isEmpty();

      harness.watermark(1);
      assertThat(harness.extractOutputValues()).hasSize(1);
      assertThat(harness.extractOutputValues().get(0).dataFilePath()).isEqualTo("main.parquet");

      // A later cycle's delete (ts 3) removes the now-indexed new row.
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 3));
      harness.watermark(3);
      assertThat(harness.extractOutputValues()).hasSize(2);
      assertThat(harness.extractOutputValues().get(1).dataFilePath()).isEqualTo("new.parquet");
    }
  }

  @Test
  void laterPhaseAddArrivingBeforeResolveSurvives() throws Exception {
    // The next cycle's new row (ts 2) is delivered before the current cycle's
    // delete (ts 1) is processed; the new row must not be affected by the delete.
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness(false)) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "new.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  true),
              2));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 1));
      harness.watermark(2);

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void resolveDeleteEmitsDVPositionsForSameFileDifferentPositions() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  5,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  10,
                  0,
                  EMPTY_PARTITION,
                  DATA_SEQ,
                  false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, DELETE_SEQ, GLOBAL), 1));

      harness.watermark(1);

      List<DVPosition> output = harness.extractOutputValues();
      assertThat(output).hasSize(2);
      assertThat(output)
          .anySatisfy(
              pos -> {
                assertThat(pos.dataFilePath()).isEqualTo("file1.parquet");
                assertThat(pos.position()).isEqualTo(5);
              })
          .anySatisfy(
              pos -> {
                assertThat(pos.dataFilePath()).isEqualTo("file1.parquet");
                assertThat(pos.position()).isEqualTo(10);
              });
    }
  }

  @Test
  void resolveDeleteSparesReinsertWithHigherSequence() throws Exception {
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness()) {
      harness.open();

      // Same key indexed twice: an older row (seq 1) and a re-insert (seq 3).
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, "old.parquet", 0, 0, EMPTY_PARTITION, 1L, false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, "new.parquet", 0, 0, EMPTY_PARTITION, 3L, false),
              0));

      // Delete at seq 2 tombstones only the older row; the re-insert (seq 3) survives.
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, 2L, GLOBAL), 1));
      harness.watermark(1);

      List<DVPosition> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).dataFilePath()).isEqualTo("old.parquet");

      // The survivor stays indexed: a later delete with a higher sequence removes it.
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, 4L, GLOBAL), 2));
      harness.watermark(2);

      List<DVPosition> afterSecond = harness.extractOutputValues();
      assertThat(afterSecond).hasSize(2);
      assertThat(afterSecond.get(1).dataFilePath()).isEqualTo("new.parquet");
    }
  }

  @Test
  void deletesHigherSequenceWhenStagingNotOnTargetBranch() throws Exception {
    // On a separate target branch the committer reassigns data sequence numbers, so the worker must
    // not spare rows by sequence: every key match is deleted (phase ordering prevents
    // over-deletion across cycles).
    try (KeyedBroadcastOperatorTestHarness<
            SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
        harness = createHarness(false)) {
      harness.open();

      // Row sequence (5) above the delete sequence (2) would survive on a shared branch.
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.addDataRow(
                  MAIN_SNAPSHOT,
                  MAIN_SEQ,
                  KEY_1,
                  "file1.parquet",
                  0,
                  0,
                  EMPTY_PARTITION,
                  5L,
                  false),
              0));
      harness.processElement(
          new StreamRecord<>(
              IndexCommand.resolveDelete(MAIN_SNAPSHOT, MAIN_SEQ, KEY_1, 2L, GLOBAL), 1));
      harness.watermark(1);

      List<DVPosition> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).dataFilePath()).isEqualTo("file1.parquet");
    }
  }

  private static KeyedBroadcastOperatorTestHarness<
          SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
      createHarness() throws Exception {
    return createHarness(true);
  }

  private static KeyedBroadcastOperatorTestHarness<
          SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition>
      createHarness(boolean stagingOnTargetBranch) throws Exception {
    return new KeyedBroadcastOperatorTestHarness<>(
        new CoBroadcastWithKeyedOperator<>(
            new EqualityConvertPKIndex(stagingOnTargetBranch),
            Collections.singletonList(EqualityConvertPKIndex.CLEAR_BROADCAST_DESCRIPTOR)),
        IndexCommand::key,
        TypeInformation.of(SerializedEqualityValues.class),
        1,
        1,
        0);
  }
}
