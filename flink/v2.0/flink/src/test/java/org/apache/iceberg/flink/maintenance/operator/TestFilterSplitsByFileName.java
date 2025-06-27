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
import java.util.stream.Collectors;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestFilterSplitsByFileName extends OperatorTestBase {

  private final IcebergSourceSplitSerializer splitSerializer =
      new IcebergSourceSplitSerializer(false);

  @Test
  void testFilter() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    List<MetadataTablePlanner.SplitInfo> icebergSourceSplits;
    try (OneInputStreamOperatorTestHarness<Trigger, MetadataTablePlanner.SplitInfo> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new MetadataTablePlanner(
                OperatorTestBase.DUMMY_TASK_NAME,
                0,
                tableLoader(),
                MetadataTableType.ALL_FILES,
                1))) {
      testHarness.open();
      OperatorTestBase.trigger(testHarness);
      icebergSourceSplits = testHarness.extractOutputValues();
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }

    try (TwoInputStreamOperatorTestHarness<
            ManifestFile, MetadataTablePlanner.SplitInfo, MetadataTablePlanner.SplitInfo>
        testHarness = new TwoInputStreamOperatorTestHarness<>(new FilterSplitsByFileName())) {
      testHarness.open();

      for (ManifestFile manifestFile : table.currentSnapshot().allManifests(table.io())) {
        testHarness.processElement1(manifestFile, EVENT_TIME);
      }

      for (MetadataTablePlanner.SplitInfo icebergSourceSplit : icebergSourceSplits) {
        testHarness.processElement2(icebergSourceSplit, EVENT_TIME);
      }

      testHarness.processBothWatermarks(WATERMARK);
      List<MetadataTablePlanner.SplitInfo> splitInfos = testHarness.extractOutputValues();
      assertThat(splitInfos).hasSize(1);
      assertThat(splitInfos.get(0).split()).isEqualTo(icebergSourceSplits.get(0).split());
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testNoRecord() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    List<MetadataTablePlanner.SplitInfo> icebergSourceSplits;

    try (TwoInputStreamOperatorTestHarness<
            ManifestFile, MetadataTablePlanner.SplitInfo, MetadataTablePlanner.SplitInfo>
        testHarness = new TwoInputStreamOperatorTestHarness<>(new FilterSplitsByFileName())) {
      testHarness.open();

      for (ManifestFile manifestFile : table.currentSnapshot().allManifests(table.io())) {
        testHarness.processElement1(manifestFile, EVENT_TIME);
      }

      testHarness.processBothWatermarks(WATERMARK);
      List<MetadataTablePlanner.SplitInfo> splitInfos = testHarness.extractOutputValues();
      assertThat(splitInfos).hasSize(0);
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testCombineTask() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    List<MetadataTablePlanner.SplitInfo> icebergSourceSplits;
    try (OneInputStreamOperatorTestHarness<Trigger, MetadataTablePlanner.SplitInfo> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new MetadataTablePlanner(
                OperatorTestBase.DUMMY_TASK_NAME,
                0,
                tableLoader(),
                MetadataTableType.ALL_FILES,
                1))) {
      testHarness.open();
      OperatorTestBase.trigger(testHarness);
      icebergSourceSplits = testHarness.extractOutputValues();
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }

    try (TwoInputStreamOperatorTestHarness<
            ManifestFile, MetadataTablePlanner.SplitInfo, MetadataTablePlanner.SplitInfo>
        testHarness = new TwoInputStreamOperatorTestHarness<>(new FilterSplitsByFileName())) {
      testHarness.open();

      List<String> manifestLocations = Lists.newArrayList();
      int index = 0;
      for (ManifestFile manifestFile : table.currentSnapshot().allManifests(table.io())) {
        if (index == 0) {
          index++;
        } else {
          manifestLocations.add(manifestFile.path());
          testHarness.processElement1(manifestFile, EVENT_TIME);
        }
      }

      for (MetadataTablePlanner.SplitInfo icebergSourceSplit : icebergSourceSplits) {
        testHarness.processElement2(icebergSourceSplit, EVENT_TIME);
      }

      testHarness.processBothWatermarks(WATERMARK);
      List<MetadataTablePlanner.SplitInfo> splitInfos = testHarness.extractOutputValues();
      assertThat(splitInfos).hasSize(1);

      MetadataTablePlanner.SplitInfo splitInfo = splitInfos.get(0);
      IcebergSourceSplit icebergSourceSplit =
          splitSerializer.deserialize(splitInfo.version(), splitInfo.split());

      List<String> filePaths =
          icebergSourceSplit.task().tasks().stream()
              .map(FileScanTask::file)
              .map(ContentFile::location)
              .collect(Collectors.toList());
      assertThat(filePaths).containsAll(manifestLocations);

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }
  }
}
