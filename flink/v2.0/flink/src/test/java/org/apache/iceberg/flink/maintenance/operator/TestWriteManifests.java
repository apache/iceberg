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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestWriteManifests extends OperatorTestBase {

  @Test
  void testUnPartitioned() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    List<MetadataTablePlanner.SplitInfo> splits = ManifestUtil.planEntriesSplits(tableLoader());
    List<RowData> entries = ManifestUtil.readSplits(tableLoader(), splits);

    List<ManifestFile> actual = ManifestUtil.writeManifests(tableLoader(), entries);
    assertThat(actual).hasSize(1);
    assertManifests(tableLoader(), actual);
  }

  @Test
  void testPartitioned() throws Exception {

    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "a");
    insertPartitioned(table, 2, "b");
    insertPartitioned(table, 3, "b");

    List<MetadataTablePlanner.SplitInfo> splits = ManifestUtil.planEntriesSplits(tableLoader());
    List<RowData> entries = ManifestUtil.readSplits(tableLoader(), splits);

    List<ManifestFile> actual = ManifestUtil.writeManifests(tableLoader(), entries);
    assertThat(actual).hasSize(1);
    assertManifests(tableLoader(), actual);
  }

  @Test
  void testStateRestore() throws Exception {
    int rowsPerManifest = 2;

    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    insert(table, 4, "d");
    insert(table, 5, "e");

    List<MetadataTablePlanner.SplitInfo> splits = ManifestUtil.planEntriesSplits(tableLoader());
    List<RowData> entries = ManifestUtil.readSplits(tableLoader(), splits);

    // Run the actual test
    WriteManifests writeManifests =
        new WriteManifests(DUMMY_TASK_NAME, 0, tableLoader(), 1L, rowsPerManifest);

    // Write some data, create a checkpoint, check the data which is already written
    OperatorSubtaskState state;
    List<ManifestFile> manifestFiles = Lists.newArrayList();
    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.open();

      for (int i = 0; i < 3; ++i) {
        testHarness.processElement(entries.get(i), EVENT_TIME);
      }

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      manifestFiles.addAll(testHarness.extractOutputValues());
      assertThat(manifestFiles).hasSize(1);

      state = testHarness.snapshot(1, EVENT_TIME);
    }

    // Restore the state, write some more data, create a checkpoint, check the data which is written
    writeManifests = new WriteManifests(DUMMY_TASK_NAME, 0, tableLoader(), 1L, rowsPerManifest);

    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement(entries.get(3), EVENT_TIME);
      testHarness.processElement(entries.get(4), EVENT_TIME);
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();

      manifestFiles.addAll(testHarness.extractOutputValues());
      assertThat(manifestFiles).hasSize(2);
      state = testHarness.snapshot(2, EVENT_TIME);
    }

    // Restore the checkpoint, do not write data, but emit a watermark which flushes the data, check
    // the result
    writeManifests = new WriteManifests(DUMMY_TASK_NAME, 0, tableLoader(), 1L, rowsPerManifest);
    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.initializeState(state);
      testHarness.open();
      testHarness.processWatermark(EVENT_TIME);

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      manifestFiles.addAll(testHarness.extractOutputValues());
      assertThat(manifestFiles).hasSize(3);
    }

    assertManifests(tableLoader(), manifestFiles);
  }

  private static void assertManifests(TableLoader tableLoader, List<ManifestFile> actual)
      throws IOException {
    Table table = tableLoader.loadTable();
    Snapshot current = table.currentSnapshot();
    List<ContentFile> expectedDataFiles = readDataFiles(current.dataManifests(table.io()), table);
    List<ContentFile> expectedDeleteFiles =
        readDeleteFiles(current.deleteManifests(table.io()), table);

    List<ManifestFile> oldManifestFiles = table.currentSnapshot().allManifests(table.io());

    RewriteManifests rewriteManifests = table.rewriteManifests();
    actual.forEach(rewriteManifests::addManifest);
    oldManifestFiles.forEach(rewriteManifests::deleteManifest);
    rewriteManifests.commit();

    table.refresh();
    List<ContentFile> actualDataFiles = readDataFiles(current.dataManifests(table.io()), table);
    List<ContentFile> actualDeleteFiles =
        readDeleteFiles(current.deleteManifests(table.io()), table);

    assertEntries(actualDataFiles, expectedDataFiles);
    assertEntries(actualDeleteFiles, expectedDeleteFiles);
  }

  private static List<ContentFile> readDataFiles(List<ManifestFile> manifestFiles, Table table)
      throws IOException {
    List<ContentFile> result = Lists.newArrayList();
    FileIO fileIO = EncryptingFileIO.combine(table.io(), table.encryption());
    for (ManifestFile manifestFile : manifestFiles) {
      try (CloseableIterator<DataFile> iterator =
          ManifestFiles.read(manifestFile, fileIO, table.specs()).iterator()) {
        result.addAll(Lists.newArrayList(iterator));
      }
    }

    return result;
  }

  private static List<ContentFile> readDeleteFiles(List<ManifestFile> manifestFiles, Table table)
      throws IOException {
    List<ContentFile> result = Lists.newArrayList();
    FileIO fileIO = EncryptingFileIO.combine(table.io(), table.encryption());
    for (ManifestFile manifestFile : manifestFiles) {
      try (CloseableIterator<DeleteFile> iterator =
          ManifestFiles.readDeleteManifest(manifestFile, fileIO, table.specs()).iterator()) {
        result.addAll(Lists.newArrayList(iterator));
      }
    }

    return result;
  }

  private static <F extends ContentFile<F>> void assertEntries(List<F> actual, List<F> expected) {
    assertThat(actual).hasSize(expected.size());
    actual.forEach(
        actualFile -> {
          Optional<F> found =
              expected.stream()
                  .filter(expectedFile -> expectedFile.path().equals(actualFile.path()))
                  .findFirst();
          assertThat(found).isPresent();
          assertEntry(actualFile, found.get());
        });
  }

  private static <F extends ContentFile<F>> void assertEntry(F actual, F expected) {
    assertThat(actual.specId()).isEqualTo(expected.specId());
    assertThat(actual.content()).isEqualTo(expected.content());
    assertThat(actual.path()).isEqualTo(expected.path());
    assertThat(actual.format()).isEqualTo(expected.format());
    assertThat(actual.partition()).isEqualTo(expected.partition());
    assertThat(actual.recordCount()).isEqualTo(expected.recordCount());
    assertThat(actual.fileSizeInBytes()).isEqualTo(expected.fileSizeInBytes());
    assertThat(actual.columnSizes()).isEqualTo(expected.columnSizes());
    assertThat(actual.valueCounts()).isEqualTo(expected.valueCounts());
    assertThat(actual.nullValueCounts()).isEqualTo(expected.nullValueCounts());
    assertThat(actual.nanValueCounts()).isEqualTo(expected.nanValueCounts());
    assertThat(actual.lowerBounds()).isEqualTo(expected.lowerBounds());
    assertThat(actual.keyMetadata()).isEqualTo(expected.keyMetadata());
    assertThat(actual.splitOffsets()).isEqualTo(expected.splitOffsets());
    assertThat(actual.equalityFieldIds()).isEqualTo(expected.equalityFieldIds());
    assertThat(actual.sortOrderId()).isEqualTo(expected.sortOrderId());
    assertThat(actual.dataSequenceNumber()).isEqualTo(expected.dataSequenceNumber());
    assertThat(actual.fileSequenceNumber()).isEqualTo(expected.fileSequenceNumber());
  }
}
