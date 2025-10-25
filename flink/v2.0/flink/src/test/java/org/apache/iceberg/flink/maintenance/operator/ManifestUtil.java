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

import static org.apache.iceberg.flink.maintenance.operator.OperatorTestBase.DUMMY_TASK_NAME;
import static org.apache.iceberg.flink.maintenance.operator.OperatorTestBase.EVENT_TIME;

import java.util.List;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.flink.source.ScanContext;

public class ManifestUtil {

  private ManifestUtil() {}

  static List<MetadataTablePlanner.SplitInfo> planEntriesSplits(TableLoader tableLoader)
      throws Exception {

    try (OneInputStreamOperatorTestHarness<Trigger, MetadataTablePlanner.SplitInfo> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new MetadataTablePlanner(
                DUMMY_TASK_NAME,
                0,
                tableLoader,
                ScanContext.builder().streaming(true).build(),
                MetadataTableType.ENTRIES,
                1))) {
      testHarness.open();

      testHarness.processElement(
          Trigger.create(System.currentTimeMillis(), 0), System.currentTimeMillis());

      return testHarness.extractOutputValues();
    }
  }

  static List<RowData> readSplits(
      TableLoader tableLoader, List<MetadataTablePlanner.SplitInfo> splits) throws Exception {
    try (OneInputStreamOperatorTestHarness<MetadataTablePlanner.SplitInfo, RowData> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ManifestEntityReader(DUMMY_TASK_NAME, 0, tableLoader, MetadataTableType.ENTRIES))) {
      testHarness.open();

      for (MetadataTablePlanner.SplitInfo split : splits) {
        testHarness.processElement(split, System.currentTimeMillis());
      }

      return testHarness.extractOutputValues();
    }
  }

  static List<ManifestFile> writeManifests(TableLoader tableLoader, List<RowData> entries)
      throws Exception {

    WriteManifests writeManifests = new WriteManifests(DUMMY_TASK_NAME, 0, tableLoader, null, null);

    try (OneInputStreamOperatorTestHarness<RowData, ManifestFile> testHarness =
        new OneInputStreamOperatorTestHarness<>(writeManifests)) {
      testHarness.open();

      for (RowData entry : entries) {
        testHarness.processElement(entry, EVENT_TIME);
      }

      testHarness.processWatermark(new Watermark(EVENT_TIME));
      return testHarness.extractOutputValues();
    }
  }
}
