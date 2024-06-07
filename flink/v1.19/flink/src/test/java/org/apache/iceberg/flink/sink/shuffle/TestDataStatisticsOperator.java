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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

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

  private DataStatisticsOperator createOperator(StatisticsType type, int downstreamParallelism)
      throws Exception {
    MockOperatorEventGateway mockGateway = new MockOperatorEventGateway();
    return createOperator(type, downstreamParallelism, mockGateway);
  }

  private DataStatisticsOperator createOperator(
      StatisticsType type, int downstreamParallelism, MockOperatorEventGateway mockGateway)
      throws Exception {
    DataStatisticsOperator operator =
        new DataStatisticsOperator(
            "testOperator",
            Fixtures.SCHEMA,
            Fixtures.SORT_ORDER,
            mockGateway,
            downstreamParallelism,
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
    DataStatisticsOperator operator = createOperator(type, Fixtures.NUM_SUBTASKS);
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
    DataStatisticsOperator operator = createOperator(type, Fixtures.NUM_SUBTASKS);
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

  private static Stream<Arguments> provideRestoreStateParameters() {
    return Stream.of(
        Arguments.of(StatisticsType.Map, -1),
        Arguments.of(StatisticsType.Map, 0),
        Arguments.of(StatisticsType.Map, 1),
        Arguments.of(StatisticsType.Sketch, -1),
        Arguments.of(StatisticsType.Sketch, 0),
        Arguments.of(StatisticsType.Sketch, 1));
  }

  @ParameterizedTest
  @MethodSource("provideRestoreStateParameters")
  public void testRestoreState(StatisticsType type, int parallelismAdjustment) throws Exception {
    Map<SortKey, Long> keyFrequency =
        ImmutableMap.of(CHAR_KEYS.get("a"), 2L, CHAR_KEYS.get("b"), 1L, CHAR_KEYS.get("c"), 1L);
    SortKey[] rangeBounds = new SortKey[] {CHAR_KEYS.get("a")};
    DataStatisticsOperator operator = createOperator(type, Fixtures.NUM_SUBTASKS);
    OperatorSubtaskState snapshot;
    try (OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> testHarness1 =
        createHarness(operator)) {
      AggregatedStatistics statistics;
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        statistics = AggregatedStatistics.fromKeyFrequency(1L, keyFrequency);
      } else {
        statistics = AggregatedStatistics.fromKeySamples(1L, rangeBounds);
      }

      StatisticsEvent event =
          StatisticsEvent.createAggregatedStatisticsEvent(
              statistics, Fixtures.AGGREGATED_STATISTICS_SERIALIZER, false);
      operator.handleOperatorEvent(event);

      AggregatedStatistics globalStatistics = operator.globalStatistics();
      assertThat(globalStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(globalStatistics.keyFrequency()).isEqualTo(keyFrequency);
        assertThat(globalStatistics.keySamples()).isNull();
      } else {
        assertThat(globalStatistics.keyFrequency()).isNull();
        assertThat(globalStatistics.keySamples()).isEqualTo(rangeBounds);
      }

      snapshot = testHarness1.snapshot(1L, 0);
    }

    // Use the snapshot to initialize state for another new operator and then verify that the global
    // statistics for the new operator is same as before
    MockOperatorEventGateway spyGateway = Mockito.spy(new MockOperatorEventGateway());
    DataStatisticsOperator restoredOperator =
        createOperator(type, Fixtures.NUM_SUBTASKS + parallelismAdjustment, spyGateway);
    try (OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> testHarness2 =
        new OneInputStreamOperatorTestHarness<>(restoredOperator, 2, 2, 1)) {
      testHarness2.setup();
      testHarness2.initializeState(snapshot);

      AggregatedStatistics globalStatistics = restoredOperator.globalStatistics();
      // global statistics is always restored and used initially even if
      // downstream parallelism changed.
      assertThat(globalStatistics).isNotNull();
      // request is always sent to coordinator during initialization.
      // coordinator would respond with a new global statistics that
      // has range bound recomputed with new parallelism.
      verify(spyGateway).sendEventToCoordinator(any(RequestGlobalStatisticsEvent.class));
      assertThat(globalStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(globalStatistics.keyFrequency()).isEqualTo(keyFrequency);
        assertThat(globalStatistics.keySamples()).isNull();
      } else {
        assertThat(globalStatistics.keyFrequency()).isNull();
        assertThat(globalStatistics.keySamples()).isEqualTo(rangeBounds);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMigrationWithLocalStatsOverThreshold() throws Exception {
    DataStatisticsOperator operator = createOperator(StatisticsType.Auto, Fixtures.NUM_SUBTASKS);
    try (OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> testHarness =
        createHarness(operator)) {
      StateInitializationContext stateContext = getStateContext();
      operator.initializeState(stateContext);

      // add rows with unique keys
      for (int i = 0; i < SketchUtil.OPERATOR_SKETCH_SWITCH_THRESHOLD; ++i) {
        operator.processElement(
            new StreamRecord<>(GenericRowData.of(StringData.fromString(String.valueOf(i)), i)));
        assertThat(operator.localStatistics().type()).isEqualTo(StatisticsType.Map);
        assertThat((Map<SortKey, Long>) operator.localStatistics().result()).hasSize(i + 1);
      }

      // one more item should trigger the migration to sketch stats
      operator.processElement(
          new StreamRecord<>(GenericRowData.of(StringData.fromString("key-trigger-migration"), 1)));

      int reservoirSize =
          SketchUtil.determineOperatorReservoirSize(Fixtures.NUM_SUBTASKS, Fixtures.NUM_SUBTASKS);

      assertThat(operator.localStatistics().type()).isEqualTo(StatisticsType.Sketch);
      ReservoirItemsSketch<SortKey> sketch =
          (ReservoirItemsSketch<SortKey>) operator.localStatistics().result();
      assertThat(sketch.getK()).isEqualTo(reservoirSize);
      assertThat(sketch.getN()).isEqualTo(SketchUtil.OPERATOR_SKETCH_SWITCH_THRESHOLD + 1);
      // reservoir not full yet
      assertThat(sketch.getN()).isLessThan(reservoirSize);
      assertThat(sketch.getSamples()).hasSize((int) sketch.getN());

      // add more items to saturate the reservoir
      for (int i = 0; i < reservoirSize; ++i) {
        operator.processElement(
            new StreamRecord<>(GenericRowData.of(StringData.fromString(String.valueOf(i)), i)));
      }

      assertThat(operator.localStatistics().type()).isEqualTo(StatisticsType.Sketch);
      sketch = (ReservoirItemsSketch<SortKey>) operator.localStatistics().result();
      assertThat(sketch.getK()).isEqualTo(reservoirSize);
      assertThat(sketch.getN())
          .isEqualTo(SketchUtil.OPERATOR_SKETCH_SWITCH_THRESHOLD + 1 + reservoirSize);
      // reservoir is full now
      assertThat(sketch.getN()).isGreaterThan(reservoirSize);
      assertThat(sketch.getSamples()).hasSize(reservoirSize);

      testHarness.endInput();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMigrationWithGlobalSketchStatistics() throws Exception {
    DataStatisticsOperator operator = createOperator(StatisticsType.Auto, Fixtures.NUM_SUBTASKS);
    try (OneInputStreamOperatorTestHarness<RowData, StatisticsOrRecord> testHarness =
        createHarness(operator)) {
      StateInitializationContext stateContext = getStateContext();
      operator.initializeState(stateContext);

      // started with Map stype
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("a"), 1)));
      assertThat(operator.localStatistics().type()).isEqualTo(StatisticsType.Map);
      assertThat((Map<SortKey, Long>) operator.localStatistics().result())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L));

      // received global statistics with sketch type
      AggregatedStatistics globalStatistics =
          AggregatedStatistics.fromKeySamples(
              1L, new SortKey[] {CHAR_KEYS.get("c"), CHAR_KEYS.get("f")});
      operator.handleOperatorEvent(
          StatisticsEvent.createAggregatedStatisticsEvent(
              globalStatistics, Fixtures.AGGREGATED_STATISTICS_SERIALIZER, false));

      int reservoirSize =
          SketchUtil.determineOperatorReservoirSize(Fixtures.NUM_SUBTASKS, Fixtures.NUM_SUBTASKS);

      assertThat(operator.localStatistics().type()).isEqualTo(StatisticsType.Sketch);
      ReservoirItemsSketch<SortKey> sketch =
          (ReservoirItemsSketch<SortKey>) operator.localStatistics().result();
      assertThat(sketch.getK()).isEqualTo(reservoirSize);
      assertThat(sketch.getN()).isEqualTo(1);
      assertThat(sketch.getSamples()).isEqualTo(new SortKey[] {CHAR_KEYS.get("a")});

      testHarness.endInput();
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
        new OneInputStreamOperatorTestHarness<>(
            dataStatisticsOperator, Fixtures.NUM_SUBTASKS, Fixtures.NUM_SUBTASKS, 0);
    harness.setup(
        new StatisticsOrRecordSerializer(
            Fixtures.AGGREGATED_STATISTICS_SERIALIZER, Fixtures.ROW_SERIALIZER));
    harness.open();
    return harness;
  }
}
