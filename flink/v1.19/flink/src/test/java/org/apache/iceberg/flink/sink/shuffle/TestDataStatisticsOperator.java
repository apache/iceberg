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

import static org.apache.iceberg.flink.sink.shuffle.Fixtures.CHAR_KEYS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackendParametersImpl;
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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestDataStatisticsOperator {

  private Environment env;

  @BeforeEach
  public void before() throws Exception {
    this.env =
        new StreamMockEnvironment(
            new Configuration(),
            new Configuration(),
            new ExecutionConfig(),
            1L,
            new MockInputSplitProvider(),
            1,
            new TestTaskStateManager());
  }

  private DataStatisticsOperator createOperator(StatisticsType type) throws Exception {
    MockOperatorEventGateway mockGateway = new MockOperatorEventGateway();
    DataStatisticsOperator operator =
        new DataStatisticsOperator(
            "testOperator",
            Fixtures.SCHEMA,
            Fixtures.SORT_ORDER,
            mockGateway,
            Fixtures.NUM_SUBTASKS,
            type);
    operator.setup(
        new OneInputStreamTask<String, String>(env),
        new MockStreamConfig(new Configuration(), 1),
        new MockOutput<>(Lists.newArrayList()));
    return operator;
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @EnumSource(StatisticsType.class)
  public void testProcessElement(StatisticsType type) throws Exception {
    DataStatisticsOperator operator = createOperator(type);
    try (OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> testHarness =
        createHarness(operator)) {
      StateInitializationContext stateContext = getStateContext();
      operator.initializeState(stateContext);
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("a"), 5)));
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("a"), 3)));
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("b"), 1)));

      DataStatistics localStatistics = operator.localStatistics();
      assertThat(localStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        Map<SortKey, Long> keyFrequency = (Map<SortKey, Long>) localStatistics.result();
        assertThat(keyFrequency)
            .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 2L, CHAR_KEYS.get("b"), 1L));
      } else {
        ReservoirItemsSketch<SortKey> sketch =
            (ReservoirItemsSketch<SortKey>) localStatistics.result();
        assertThat(sketch.getSamples())
            .containsExactly(CHAR_KEYS.get("a"), CHAR_KEYS.get("a"), CHAR_KEYS.get("b"));
      }

      testHarness.endInput();
    }
  }

  @ParameterizedTest
  @EnumSource(StatisticsType.class)
  public void testOperatorOutput(StatisticsType type) throws Exception {
    DataStatisticsOperator operator = createOperator(type);
    try (OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> testHarness =
        createHarness(operator)) {
      testHarness.processElement(
          new StreamRecord<>(GenericRowData.of(StringData.fromString("a"), 2)));
      testHarness.processElement(
          new StreamRecord<>(GenericRowData.of(StringData.fromString("b"), 3)));
      testHarness.processElement(
          new StreamRecord<>(GenericRowData.of(StringData.fromString("b"), 1)));

      List<RowData> recordsOutput =
          testHarness.extractOutputValues().stream()
              .filter(StatisticsOrRecord::hasRecord)
              .map(StatisticsOrRecord::record)
              .collect(Collectors.toList());
      assertThat(recordsOutput)
          .containsExactlyInAnyOrderElementsOf(
              ImmutableList.of(
                  GenericRowData.of(StringData.fromString("a"), 2),
                  GenericRowData.of(StringData.fromString("b"), 3),
                  GenericRowData.of(StringData.fromString("b"), 1)));
    }
  }

  @ParameterizedTest
  @EnumSource(StatisticsType.class)
  public void testRestoreState(StatisticsType type) throws Exception {
    Map<SortKey, Long> keyFrequency =
        ImmutableMap.of(CHAR_KEYS.get("a"), 2L, CHAR_KEYS.get("b"), 1L, CHAR_KEYS.get("c"), 1L);
    SortKey[] rangeBounds = new SortKey[] {CHAR_KEYS.get("a")};
    DataStatisticsOperator operator = createOperator(type);
    OperatorSubtaskState snapshot;
    try (OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> testHarness1 =
        createHarness(operator)) {
      AggregatedStatistics statistics;
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        statistics = AggregatedStatistics.fromKeyFrequency(1L, keyFrequency);
      } else {
        statistics = AggregatedStatistics.fromRangeBounds(1L, rangeBounds);
      }

      StatisticsEvent event =
          StatisticsEvent.createAggregatedStatisticsEvent(
              1L, statistics, Fixtures.AGGREGATED_STATISTICS_SERIALIZER);
      operator.handleOperatorEvent(event);

      AggregatedStatistics globalStatistics = operator.globalStatistics();
      assertThat(globalStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(globalStatistics.keyFrequency()).isEqualTo(keyFrequency);
        assertThat(globalStatistics.rangeBounds()).isNull();
      } else {
        assertThat(globalStatistics.keyFrequency()).isNull();
        assertThat(globalStatistics.rangeBounds()).isEqualTo(rangeBounds);
      }

      snapshot = testHarness1.snapshot(1L, 0);
    }

    // Use the snapshot to initialize state for another new operator and then verify that the global
    // statistics for the new operator is same as before
    DataStatisticsOperator restoredOperator = createOperator(type);
    try (OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> testHarness2 =
        new OneInputStreamOperatorTestHarness<>(restoredOperator, 2, 2, 1)) {
      testHarness2.setup();
      testHarness2.initializeState(snapshot);

      AggregatedStatistics globalStatistics = restoredOperator.globalStatistics();
      assertThat(globalStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(globalStatistics.keyFrequency()).isEqualTo(keyFrequency);
        assertThat(globalStatistics.rangeBounds()).isNull();
      } else {
        assertThat(globalStatistics.keyFrequency()).isNull();
        assertThat(globalStatistics.rangeBounds()).isEqualTo(rangeBounds);
      }
    }
  }

  private StateInitializationContext getStateContext() throws Exception {
    AbstractStateBackend abstractStateBackend = new HashMapStateBackend();
    CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
    OperatorStateStore operatorStateStore =
        abstractStateBackend.createOperatorStateBackend(
            new OperatorStateBackendParametersImpl(
                env, "test-operator", Collections.emptyList(), cancelStreamRegistry));
    return new StateInitializationContextImpl(null, operatorStateStore, null, null, null);
  }

  private OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> createHarness(
      DataStatisticsOperator dataStatisticsOperator) throws Exception {
    OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> harness =
        new OneInputStreamOperatorTestHarness<>(dataStatisticsOperator, 1, 1, 0);
    harness.setup(
        new StatisticsOrRecordSerializer(
            Fixtures.AGGREGATED_STATISTICS_SERIALIZER, Fixtures.ROW_SERIALIZER));
    harness.open();
    return harness;
  }
}
