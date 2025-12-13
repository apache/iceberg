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

import static org.apache.iceberg.actions.SizeBasedFileRewritePlanner.MIN_INPUT_FILES;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

class RewriteUtil {
  private RewriteUtil() {}

  static List<DataFileRewritePlanner.PlannedGroup> planDataFileRewrite(TableLoader tableLoader)
      throws Exception {
    try (OneInputStreamOperatorTestHarness<Trigger, DataFileRewritePlanner.PlannedGroup>
        testHarness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new DataFileRewritePlanner(
                    OperatorTestBase.DUMMY_TABLE_NAME,
                    OperatorTestBase.DUMMY_TABLE_NAME,
                    0,
                    tableLoader,
                    11,
                    10_000_000L,
                    ImmutableMap.of(MIN_INPUT_FILES, "2"),
                    Expressions.alwaysTrue()))) {
      testHarness.open();

      OperatorTestBase.trigger(testHarness);

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      return testHarness.extractOutputValues();
    }
  }

  static List<DataFileRewriteRunner.ExecutedGroup> executeRewrite(
      List<DataFileRewritePlanner.PlannedGroup> elements) throws Exception {
    try (OneInputStreamOperatorTestHarness<
            DataFileRewritePlanner.PlannedGroup, DataFileRewriteRunner.ExecutedGroup>
        testHarness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new DataFileRewriteRunner(
                    OperatorTestBase.DUMMY_TABLE_NAME, OperatorTestBase.DUMMY_TABLE_NAME, 0))) {
      testHarness.open();

      for (DataFileRewritePlanner.PlannedGroup element : elements) {
        testHarness.processElement(element, System.currentTimeMillis());
      }

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      return testHarness.extractOutputValues();
    }
  }

  static Set<DataFile> newDataFiles(Table table) {
    table.refresh();
    return Sets.newHashSet(table.currentSnapshot().addedDataFiles(table.io()));
  }
}
