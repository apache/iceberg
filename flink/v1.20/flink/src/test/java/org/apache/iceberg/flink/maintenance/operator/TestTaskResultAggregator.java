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

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.streaming.api.watermark.Watermark;
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
      testHarness.processWatermark1(new Watermark(EVENT_TIME));
      testHarness.processWatermark2(new Watermark(EVENT_TIME));
      ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
      assertThat(output).containsOnlyOnce(new Watermark(EVENT_TIME));
    }
  }
}
