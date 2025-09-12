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

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestSkipOnError extends OperatorTestBase {

  private static final Exception EXCEPTION = new Exception("Test error");

  @Test
  void testNoFailure() throws Exception {
    try (TwoInputStreamOperatorTestHarness<String, Exception, String> testHarness =
        new TwoInputStreamOperatorTestHarness<>(new SkipOnError())) {
      testHarness.open();

      testHarness.processElement1(FILE_NAME_1, EVENT_TIME);
      testHarness.processElement1(FILE_NAME_2, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues())
          .isEqualTo(ImmutableList.of(FILE_NAME_1, FILE_NAME_2));
    }
  }

  @Test
  void testFailure() throws Exception {
    try (TwoInputStreamOperatorTestHarness<String, Exception, String> testHarness =
        new TwoInputStreamOperatorTestHarness<>(new SkipOnError())) {
      testHarness.open();

      testHarness.processElement1(FILE_NAME_1, EVENT_TIME);
      testHarness.processElement2(EXCEPTION, EVENT_TIME);
      testHarness.processElement1(FILE_NAME_2, EVENT_TIME);
      assertThat(testHarness.extractOutputValues()).isEmpty();

      testHarness.processBothWatermarks(WATERMARK);
      assertThat(testHarness.extractOutputValues()).isEmpty();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testStateRestore(boolean withError) throws Exception {
    OperatorSubtaskState state;
    try (TwoInputStreamOperatorTestHarness<String, Exception, String> testHarness =
        new TwoInputStreamOperatorTestHarness<>(new SkipOnError())) {
      testHarness.open();

      testHarness.processElement1(FILE_NAME_1, EVENT_TIME);
      if (withError) {
        testHarness.processElement2(EXCEPTION, EVENT_TIME);
      }

      assertThat(testHarness.extractOutputValues()).isEmpty();
      state = testHarness.snapshot(1L, EVENT_TIME);
    }

    try (TwoInputStreamOperatorTestHarness<String, Exception, String> testHarness =
        new TwoInputStreamOperatorTestHarness<>(new SkipOnError())) {
      testHarness.initializeState(state);
      testHarness.open();

      testHarness.processElement1(FILE_NAME_2, EVENT_TIME);

      assertThat(testHarness.extractOutputValues()).isEmpty();
      testHarness.processBothWatermarks(WATERMARK);
      if (withError) {
        assertThat(testHarness.extractOutputValues()).isEmpty();
      } else {
        assertThat(testHarness.extractOutputValues())
            .isEqualTo(ImmutableList.of(FILE_NAME_1, FILE_NAME_2));
      }
    }
  }
}
