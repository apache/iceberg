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
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.junit.jupiter.api.Test;

class TestListMetadataFiles extends OperatorTestBase {

  @Test
  void testMetadataFilesWithTable() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");
    insert(table, 3, "c");
    try (OneInputStreamOperatorTestHarness<Trigger, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ListMetadataFiles(OperatorTestBase.DUMMY_TABLE_NAME, 0, tableLoader()))) {
      testHarness.open();

      OperatorTestBase.trigger(testHarness);

      List<String> tableMetadataFiles = testHarness.extractOutputValues();
      tableMetadataFiles.forEach(System.out::println);
      assertThat(tableMetadataFiles).hasSize(24);

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testMetadataFilesWithPartitionTable() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    insertPartitioned(table, 3, "p2");
    insertPartitioned(table, 4, "p2");
    try (OneInputStreamOperatorTestHarness<Trigger, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ListMetadataFiles(OperatorTestBase.DUMMY_TABLE_NAME, 0, tableLoader()))) {
      testHarness.open();

      OperatorTestBase.trigger(testHarness);

      List<String> tableMetadataFiles = testHarness.extractOutputValues();
      assertThat(tableMetadataFiles).hasSize(38);

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }
  }

  @Test
  void testMetadataFilesWithEmptyTable() throws Exception {
    createTable();
    try (OneInputStreamOperatorTestHarness<Trigger, String> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new ListMetadataFiles(OperatorTestBase.DUMMY_TABLE_NAME, 0, tableLoader()))) {
      testHarness.open();

      OperatorTestBase.trigger(testHarness);

      List<String> tableMetadataFiles = testHarness.extractOutputValues();
      assertThat(tableMetadataFiles).hasSize(0);

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }
  }
}
