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
package org.apache.iceberg.flink.sink.shuffle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.flink.sink.shuffle.statistics.DataStatisticsFactory;
import org.apache.iceberg.flink.sink.shuffle.statistics.MapDataStatistics;
import org.apache.iceberg.flink.sink.shuffle.statistics.MapDataStatisticsFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsOperator {
  private DataStatisticsOperator<String, String> operator;

  private Environment getTestingEnvironment() {
    return new StreamMockEnvironment(
        new Configuration(),
        new Configuration(),
        new ExecutionConfig(),
        1L,
        new MockInputSplitProvider(),
        1,
        new TestTaskStateManager());
  }

  @Before
  public void before() throws Exception {
    MockOperatorEventGateway mockGateway = new MockOperatorEventGateway();
    KeySelector<String, String> keySelector =
        new KeySelector<String, String>() {
          private static final long serialVersionUID = 7662520075515707428L;

          @Override
          public String getKey(String value) {
            return value;
          }
        };
    DataStatisticsFactory<String> dataStatisticsFactory = new MapDataStatisticsFactory<>();

    this.operator = new DataStatisticsOperator<>(keySelector, mockGateway, dataStatisticsFactory);
    Environment env = getTestingEnvironment();
    this.operator.setup(
        new OneInputStreamTask<String, String>(env),
        new MockStreamConfig(new Configuration(), 1),
        new MockOutput<>(Lists.newArrayList()));
  }

  @After
  public void clean() throws Exception {
    operator.close();
  }

  @Test
  public void testProcessElement() throws Exception {
    StateInitializationContext stateContext = getStateContext();
    operator.initializeState(stateContext);
    operator.processElement(new StreamRecord<>("a"));
    operator.processElement(new StreamRecord<>("a"));
    operator.processElement(new StreamRecord<>("b"));
    assertTrue(operator.localDataStatistics() instanceof MapDataStatistics);
    MapDataStatistics<String> mapDataStatistics =
        (MapDataStatistics<String>) operator.localDataStatistics();
    assertTrue(mapDataStatistics.dataStatistics().containsKey("a"));
    assertTrue(mapDataStatistics.dataStatistics().containsKey("b"));
    assertEquals(2L, (long) mapDataStatistics.dataStatistics().get("a"));
    assertEquals(1L, (long) mapDataStatistics.dataStatistics().get("b"));
  }

  @Test
  public void testOperatorOutput() throws Exception {
    try (OneInputStreamOperatorTestHarness<String, DataStatisticsOrRecord<String, String>>
        testHarness = createHarness(this.operator)) {
      testHarness.processElement(new StreamRecord<>("a"));
      testHarness.processElement(new StreamRecord<>("b"));
      testHarness.processElement(new StreamRecord<>("b"));

      List<String> recordsOutput =
          testHarness.extractOutputValues().stream()
              .filter(DataStatisticsOrRecord::hasRecord)
              .map(DataStatisticsOrRecord::record)
              .collect(Collectors.toList());
      assertThat(recordsOutput)
          .containsExactlyInAnyOrderElementsOf(ImmutableList.of("a", "b", "b"));
    }
  }

  private StateInitializationContext getStateContext() throws Exception {
    MockEnvironment env = new MockEnvironmentBuilder().build();
    AbstractStateBackend abstractStateBackend = new HashMapStateBackend();
    CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
    OperatorStateStore operatorStateStore = abstractStateBackend.createOperatorStateBackend(
            env, "test-operator", Collections.emptyList(), cancelStreamRegistry);
    return new StateInitializationContextImpl(null, operatorStateStore, null, null, null);
  }

  private OneInputStreamOperatorTestHarness<String, DataStatisticsOrRecord<String, String>>
      createHarness(final DataStatisticsOperator<String, String> dataStatisticsOperator) throws Exception {
    OneInputStreamOperatorTestHarness<String, DataStatisticsOrRecord<String, String>> harness =
        new OneInputStreamOperatorTestHarness<>(dataStatisticsOperator, 1, 1, 0);
    harness.setup();
    harness.open();
    return harness;
  }
}
