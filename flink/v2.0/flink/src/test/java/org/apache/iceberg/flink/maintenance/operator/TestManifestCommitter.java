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

import java.util.List;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.junit.jupiter.api.Test;

class TestManifestCommitter extends OperatorTestBase {
  @Test
  void testUpdate() throws Exception {

    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    List<MetadataTablePlanner.SplitInfo> splits = ManifestUtil.planEntriesSplits(tableLoader());
    List<RowData> entries = ManifestUtil.readSplits(tableLoader(), splits);

    List<ManifestFile> oldManifests = table.currentSnapshot().allManifests(table.io());
    List<ManifestFile> actual = ManifestUtil.writeManifests(tableLoader(), entries);

    assertThat(oldManifests).hasSize(2);

    try (TwoInputStreamOperatorTestHarness<
            Tuple2<ManifestFile, ManifestCommitter.ManifestSource>, Exception, Trigger>
        testHarness =
            new TwoInputStreamOperatorTestHarness<>(
                new ManifestCommitter(OperatorTestBase.DUMMY_TASK_NAME, 0, tableLoader()))) {
      testHarness.open();

      for (ManifestFile manifestFile : actual) {
        testHarness.processElement1(
            Tuple2.of(manifestFile, ManifestCommitter.ManifestSource.NEW), EVENT_TIME);
      }

      for (ManifestFile manifestFile : oldManifests) {
        testHarness.processElement1(
            Tuple2.of(manifestFile, ManifestCommitter.ManifestSource.OLD), EVENT_TIME);
      }

      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(new Watermark(EVENT_TIME));

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }

    table.refresh();
    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(newManifests).hasSize(actual.size());
    assertThat(newManifests).containsAll(actual);
  }

  @Test
  void testRestore() throws Exception {

    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    List<MetadataTablePlanner.SplitInfo> splits = ManifestUtil.planEntriesSplits(tableLoader());
    List<RowData> entries = ManifestUtil.readSplits(tableLoader(), splits);

    List<ManifestFile> oldManifests = table.currentSnapshot().allManifests(table.io());
    List<ManifestFile> actual = ManifestUtil.writeManifests(tableLoader(), entries);

    assertThat(oldManifests).hasSize(2);

    OperatorSubtaskState state;
    try (TwoInputStreamOperatorTestHarness<
            Tuple2<ManifestFile, ManifestCommitter.ManifestSource>, Exception, Trigger>
        testHarness =
            new TwoInputStreamOperatorTestHarness<>(
                new ManifestCommitter(OperatorTestBase.DUMMY_TASK_NAME, 0, tableLoader()))) {
      testHarness.open();

      for (ManifestFile manifestFile : actual) {
        testHarness.processElement1(
            Tuple2.of(manifestFile, ManifestCommitter.ManifestSource.NEW), EVENT_TIME);
      }

      assertThat(testHarness.extractOutputValues()).isEmpty();
      state = testHarness.snapshot(1, EVENT_TIME);
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }

    try (TwoInputStreamOperatorTestHarness<
            Tuple2<ManifestFile, ManifestCommitter.ManifestSource>, Exception, Trigger>
        testHarness =
            new TwoInputStreamOperatorTestHarness<>(
                new ManifestCommitter(OperatorTestBase.DUMMY_TASK_NAME, 0, tableLoader()))) {
      testHarness.initializeState(state);
      testHarness.open();

      for (ManifestFile manifestFile : oldManifests) {
        testHarness.processElement1(
            Tuple2.of(manifestFile, ManifestCommitter.ManifestSource.OLD), EVENT_TIME);
      }

      testHarness.processBothWatermarks(new Watermark(EVENT_TIME));

      assertThat(testHarness.extractOutputValues()).isEmpty();
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }

    table.refresh();
    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(newManifests).hasSize(actual.size());
    assertThat(newManifests).containsAll(actual);
  }
}
