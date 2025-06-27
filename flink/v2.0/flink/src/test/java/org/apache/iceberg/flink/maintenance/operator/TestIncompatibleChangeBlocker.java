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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIncompatibleChangeBlocker extends OperatorTestBase {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBlockSchemaChange(boolean failOnChange) throws Exception {
    Table table = createTable();

    try (OneInputStreamOperatorTestHarness<Trigger, Trigger> testHarness =
        new OneInputStreamOperatorTestHarness<>(
            new IncompatibleChangeBlocker(
                OperatorTestBase.DUMMY_TASK_NAME, 0, tableLoader(), failOnChange))) {

      testHarness.open();
      // Should not fail on the same table
      testHarness.processElement(
          Trigger.create(System.currentTimeMillis(), 0), System.currentTimeMillis());
      assertThat(testHarness.extractOutputValues()).hasSize(0);

      // Should not fail on spec change
      table.updateSpec().addField("data").commit();

      if (failOnChange) {
        assertThatThrownBy(
                () ->
                    testHarness.processElement(
                        Trigger.create(System.currentTimeMillis(), 0), System.currentTimeMillis()))
            .hasMessage(
                "Unrecoverable failure. This suppresses job restarts. Please check the stack trace for the root cause.")
            .isInstanceOf(SuppressRestartsException.class);
      } else {
        testHarness.processElement(
            Trigger.create(System.currentTimeMillis(), 0), System.currentTimeMillis());
        assertThat(testHarness.extractOutputValues()).hasSize(0);
        assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBlockSchemaChangeRestoreState(boolean failOnChange) throws Exception {
    Table table = createTable();

    OperatorSubtaskState state;
    try (OneInputStreamOperatorTestHarness<Trigger, Trigger> testHarness =
        new OneInputStreamOperatorTestHarness<>(
            new IncompatibleChangeBlocker(
                OperatorTestBase.DUMMY_TASK_NAME, 0, tableLoader(), failOnChange))) {

      testHarness.open();
      // Should not fail on the same table
      testHarness.processElement(
          Trigger.create(System.currentTimeMillis(), 0), System.currentTimeMillis());
      assertThat(testHarness.extractOutputValues()).hasSize(0);

      state = testHarness.snapshot(1, System.currentTimeMillis());
    }

    try (OneInputStreamOperatorTestHarness<Trigger, Trigger> testHarness =
        new OneInputStreamOperatorTestHarness<>(
            new IncompatibleChangeBlocker(
                OperatorTestBase.DUMMY_TASK_NAME, 0, tableLoader(), failOnChange))) {
      testHarness.initializeState(state);
      testHarness.open();

      // Should not fail on spec change
      table.updateSpec().addField("data").commit();

      if (failOnChange) {
        assertThatThrownBy(
                () ->
                    testHarness.processElement(
                        Trigger.create(System.currentTimeMillis(), 0), System.currentTimeMillis()))
            .isInstanceOf(SuppressRestartsException.class)
            .hasMessage(
                "Unrecoverable failure. This suppresses job restarts. Please check the stack trace for the root cause.");
      } else {
        testHarness.processElement(
            Trigger.create(System.currentTimeMillis(), 0), System.currentTimeMillis());
        assertThat(testHarness.extractOutputValues()).hasSize(0);
        assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      }
    }
  }
}
