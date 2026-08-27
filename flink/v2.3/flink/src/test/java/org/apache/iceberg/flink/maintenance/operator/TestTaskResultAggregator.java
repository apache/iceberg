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
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.junit.jupiter.api.Test;

class TestTaskResultAggregator extends OperatorTestBase {

  @Test
  void testPassWatermark() throws Exception {
    TaskResultAggregator taskResultAggregator =
        new TaskResultAggregator("table-name", "task-name", 0);
    try (TwoInputStreamOperatorTestHarness<Trigger, Exception, TaskResult> testHarness =
        new TwoInputStreamOperatorTestHarness<>(taskResultAggregator)) {
      testHarness.open();
      testHarness.processBothWatermarks(WATERMARK);
      ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
      assertThat(output).containsOnlyOnce(WATERMARK);
    }
  }

  @Test
  void testProcessWatermarkWithoutElement() throws Exception {
    TaskResultAggregator taskResultAggregator =
        new TaskResultAggregator("table-name", "task-name", 0);
    try (TwoInputStreamOperatorTestHarness<Trigger, Exception, TaskResult> testHarness =
        new TwoInputStreamOperatorTestHarness<>(taskResultAggregator)) {
      testHarness.open();
      testHarness.processBothWatermarks(WATERMARK);
      List<TaskResult> taskResults = testHarness.extractOutputValues();
      assertThat(taskResults).hasSize(0);
    }
  }

  @Test
  void testProcessWatermark() throws Exception {
    TaskResultAggregator taskResultAggregator =
        new TaskResultAggregator("table-name", "task-name", 0);
    try (TwoInputStreamOperatorTestHarness<Trigger, Exception, TaskResult> testHarness =
        new TwoInputStreamOperatorTestHarness<>(taskResultAggregator)) {
      testHarness.open();

      testHarness.processElement1(new StreamRecord<>(Trigger.create(EVENT_TIME, 0)));
      testHarness.processBothWatermarks(WATERMARK);
      List<TaskResult> taskResults = testHarness.extractOutputValues();
      assertThat(taskResults).hasSize(1);
      TaskResult taskResult = taskResults.get(0);
      assertThat(taskResult.taskIndex()).isEqualTo(0);
      assertThat(taskResult.startEpoch()).isEqualTo(EVENT_TIME);
      assertThat(taskResult.success()).isEqualTo(true);
      assertThat(taskResult.exceptions()).hasSize(0);
    }
  }
}
