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

import static org.apache.iceberg.flink.SimpleDataUtil.createRecord;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestEqualityConvertDVWriter extends OperatorTestBase {

  private static final byte[] EMPTY_PARTITION = new byte[0];

  @Test
  void writesDVFileForSinglePosition() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    String dataFilePath = getFirstDataFilePath(table);

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, 0, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time));

      harness.processBothWatermarks(new Watermark(time));

      List<DVWriteResult> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).hasError()).isFalse();
      assertThat(output.get(0).dvFiles()).hasSize(1);
    }
  }

  @Test
  void writesSingleDVFileForMultiplePositionsOnSameDataFile() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");

    String dataFilePath = getFirstDataFilePath(table);

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, 0, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, 1, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, 2, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time));

      harness.processBothWatermarks(new Watermark(time));

      List<DVWriteResult> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).hasError()).isFalse();
      assertThat(output.get(0).dvFiles()).hasSize(1);
      assertThat(output.get(0).dvFiles().get(0).recordCount()).isEqualTo(3);
      assertThat(output.get(0).rewrittenDvFiles()).isEmpty();
    }
  }

  @Test
  void emptyPositionsProducesNoOutput() throws Exception {
    createTableWithDelete(3);

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time));

      harness.processBothWatermarks(new Watermark(time));

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void noOutputWithoutPlanResult() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    String dataFilePath = getFirstDataFilePath(table);

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, 0, 0, EMPTY_PARTITION, 0L), time));

      harness.processBothWatermarks(new Watermark(time));

      assertThat(harness.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void writesDVFilesForMultipleDataFiles() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");
    insert(table, 2, "b");

    List<String> dataFilePaths = getDataFilePaths(table);
    assertThat(dataFilePaths).hasSize(2);

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement1(
          new StreamRecord<>(
              new DVPosition(dataFilePaths.get(0), 0, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement1(
          new StreamRecord<>(
              new DVPosition(dataFilePaths.get(1), 0, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time));

      harness.processBothWatermarks(new Watermark(time));

      List<DVWriteResult> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).dvFiles()).hasSize(2);
    }
  }

  @Test
  void routesErrorToErrorStream() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    String dataFilePath = getFirstDataFilePath(table);

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, 0, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time));

      dropTable();

      harness.processBothWatermarks(new Watermark(time));

      assertThat(harness.extractOutputValues()).hasSize(1);
      assertThat(harness.extractOutputValues().get(0).hasError()).isTrue();
      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
    }
  }

  @Test
  void mergesStagingDVIntoWrittenDV() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    String dataFilePath = getFirstDataFilePath(table);

    // Staging DV for the data file at position 0, produced but not committed to the target branch.
    DeleteFile stagingDV = writeStagingDV(dataFilePath, 0);

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(),
              Lists.newArrayList(stagingDV),
              Lists.newArrayList(),
              1L,
              null,
              0L,
              0L);

      // New position 1 in the same file. Must merge it with the staged position 0.
      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, 1, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement2(new StreamRecord<>(planResult, time));

      harness.processBothWatermarks(new Watermark(time));

      List<DVWriteResult> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).hasError()).isFalse();
      assertThat(output.get(0).dvFiles()).hasSize(1);
      assertThat(output.get(0).dvFiles().get(0).recordCount()).isEqualTo(2);
      assertThat(output.get(0).rewrittenDvFiles()).hasSize(1);
      assertThat(output.get(0).rewrittenDvFiles().get(0).location())
          .isEqualTo(stagingDV.location());
    }
  }

  @Test
  void abortsWhenMainSnapshotChangedSincePlanning() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    String dataFilePath = getFirstDataFilePath(table);
    long plannedSnapshotId = table.currentSnapshot().snapshotId();

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      EqualityConvertPlan planResult =
          new EqualityConvertPlan(
              Lists.newArrayList(),
              Lists.newArrayList(),
              Lists.newArrayList(),
              1L,
              plannedSnapshotId,
              0L,
              0L);

      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, 0, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement2(new StreamRecord<>(planResult, time));

      // Advance the target branch after planning. The writer must refuse to write stale DVs.
      insert(table, 2, "b");

      harness.processBothWatermarks(new Watermark(time));

      List<DVWriteResult> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).hasError()).isTrue();
      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
    }
  }

  @Test
  void stateResetBetweenBatches() throws Exception {
    Table table = createTableWithDelete(3);
    insert(table, 1, "a");

    String dataFilePath = getFirstDataFilePath(table);

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time1 = System.currentTimeMillis();
      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, 0, 0, EMPTY_PARTITION, 0L), time1));
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time1));
      harness.processBothWatermarks(new Watermark(time1));

      assertThat(harness.extractOutputValues()).hasSize(1);

      // Second batch with no positions, plan only.
      long time2 = time1 + 1;
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time2));
      harness.processBothWatermarks(new Watermark(time2));

      // Cumulative output: still hasSize(1) since the empty batch produced nothing.
      assertThat(harness.extractOutputValues()).hasSize(1);
    }
  }

  @Test
  void abortsOnUpstreamAbortPosition() throws Exception {
    createTableWithDelete(3);

    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement1(new StreamRecord<>(DVPosition.ABORT, time));
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time));
      harness.processBothWatermarks(new Watermark(time));

      List<DVWriteResult> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).hasError()).isTrue();
      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void doesNotRetainStateAcrossCycles() throws Exception {
    Table table = createTableWithDelete(3);
    int cycles = 5;
    for (int i = 0; i < cycles; i++) {
      insert(table, i, "v" + i);
    }

    List<String> dataFilePaths = getDataFilePaths(table);
    assertThat(dataFilePaths).hasSize(cycles);

    EqualityConvertDVWriter resolver = newResolver();
    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        new TwoInputStreamOperatorTestHarness<>(resolver)) {
      harness.open();

      long time = System.currentTimeMillis();
      for (int i = 0; i < cycles; i++) {
        // Each cycle resolves a position in a distinct data file, growing the set of files seen.
        harness.processElement1(
            new StreamRecord<>(
                new DVPosition(dataFilePaths.get(i), 0, 0, EMPTY_PARTITION, 0L), time + i));
        harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time + i));
        harness.processBothWatermarks(new Watermark(time + i));

        // No cross-cycle state: the buffer clears every cycle regardless of how many distinct data
        // files have been processed.
        assertThat(resolver.retainedStateSize()).isZero();
        assertThat(harness.extractOutputValues()).hasSize(i + 1);
      }
    }
  }

  @Test
  void prunesDeleteManifestsByPartition() throws Exception {
    Table table = createPartitionedTableWithDelete(3);
    insertPartitioned(table, Lists.newArrayList(createRecord(1, "a")), "a");
    DataFile fileA = dataFile(table, "a");
    insertPartitioned(table, Lists.newArrayList(createRecord(2, "b")), "b");
    DataFile fileB = dataFile(table, "b");

    // Commit one DV per partition: two separate delete manifests, summaries {a} and {b}.
    commitDV(table, fileA);
    commitDV(table, fileB);
    assertThat(table.currentSnapshot().deleteManifests(table.io())).hasSize(2);

    // A new cycle resolves another position in partition a's file. Only partition a's delete
    // manifest overlaps; partition b's manifest must be pruned.
    EqualityConvertDVWriter resolver = newResolver();
    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        new TwoInputStreamOperatorTestHarness<>(resolver)) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement1(new StreamRecord<>(dvPosition(table, fileA, 0), time));
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time));
      harness.processBothWatermarks(new Watermark(time));

      List<DVWriteResult> output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      // Folded partition a's existing DV: the matching manifest was read.
      assertThat(output.get(0).rewrittenDvFiles()).hasSize(1);
      assertThat(output.get(0).dvFiles()).hasSize(1);
      // Partition b's manifest was skipped: only one manifest read this cycle.
      assertThat(resolver.manifestsReadLastCycle()).isEqualTo(1);
    }
  }

  private void commitDV(Table table, DataFile dataFile) throws Exception {
    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement1(new StreamRecord<>(dvPosition(table, dataFile, 0), time));
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time));
      harness.processBothWatermarks(new Watermark(time));

      RowDelta rowDelta = table.newRowDelta();
      harness.extractOutputValues().get(0).dvFiles().forEach(rowDelta::addDeletes);
      rowDelta.commit();
      table.refresh();
    }
  }

  private static DVPosition dvPosition(Table table, DataFile dataFile, long position) {
    PartitionSpec spec = table.specs().get(dataFile.specId());
    byte[] partition =
        new StructLikeSerializer().encodePartition(dataFile.partition(), spec.partitionType());
    return new DVPosition(dataFile.location(), position, dataFile.specId(), partition, 0L);
  }

  private static DataFile dataFile(Table table, String partitionValue) {
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        for (DataFile file : reader) {
          if (partitionValue.equals(String.valueOf(file.partition().get(0, Object.class)))) {
            return file;
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    throw new IllegalStateException("No data file for partition: " + partitionValue);
  }

  private DeleteFile writeStagingDV(String dataFilePath, long position) throws Exception {
    try (TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult> harness =
        createHarness()) {
      harness.open();

      long time = System.currentTimeMillis();
      harness.processElement1(
          new StreamRecord<>(new DVPosition(dataFilePath, position, 0, EMPTY_PARTITION, 0L), time));
      harness.processElement2(new StreamRecord<>(emptyEqualityConvertPlan(), time));
      harness.processBothWatermarks(new Watermark(time));

      return harness.extractOutputValues().get(0).dvFiles().get(0);
    }
  }

  private EqualityConvertDVWriter newResolver() {
    return new EqualityConvertDVWriter(
        DUMMY_TABLE_NAME, DUMMY_TASK_NAME, tableLoader(), SnapshotRef.MAIN_BRANCH);
  }

  private TwoInputStreamOperatorTestHarness<DVPosition, EqualityConvertPlan, DVWriteResult>
      createHarness() throws Exception {
    return new TwoInputStreamOperatorTestHarness<>(newResolver());
  }

  private static EqualityConvertPlan emptyEqualityConvertPlan() {
    return new EqualityConvertPlan(
        Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList(), 1L, null, 0L, 0L);
  }

  private static String getFirstDataFilePath(Table table) {
    return getDataFilePaths(table).get(0);
  }

  private static List<String> getDataFilePaths(Table table) {
    List<String> paths = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        for (DataFile file : reader) {
          paths.add(file.location());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return paths;
  }
}
