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

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestTablePlaner extends OperatorTestBase {
  private static final Schema FILE_PATH_SCHEMA = new Schema(DataFile.FILE_PATH);
  private static final ScanContext FILE_PATH_SCAN_CONTEXT =
      ScanContext.builder().streaming(true).project(FILE_PATH_SCHEMA).build();

  @Override
  @BeforeEach
  void before() {
    super.before();
    System.setProperty("org.apache.flink.serialization.checkLambdaSerialization", "false");
  }

  @Test
  void testUnpartitioned() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    try (OneInputStreamOperatorTestHarness<Trigger, IcebergSourceSplit> testHarness =
        ProcessFunctionTestHarnesses.forProcessFunction(
            new TablePlanner(tableLoader(), FILE_PATH_SCAN_CONTEXT, 1))) {
      testHarness.open();

      OperatorTestBase.trigger(testHarness);

      //      List<IcebergSourceSplit> icebergSourceSplits = testHarness.extractOutputValues();

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
    }
  }
}
