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
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsOperator {
  private final RowType rowType = RowType.of(new VarCharType());
  private final TypeSerializer<RowData> rowSerializer = new RowDataSerializer(rowType);

  private DataStatisticsOperator<MapDataStatistics, Map<RowData, Long>> operator;

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
    this.operator = createOperator();
    Environment env = getTestingEnvironment();
    this.operator.setup(
        new OneInputStreamTask<String, String>(env),
        new MockStreamConfig(new Configuration(), 1),
        new MockOutput<>(Lists.newArrayList()));
  }

  private DataStatisticsOperator<MapDataStatistics, Map<RowData, Long>> createOperator() {
    MockOperatorEventGateway mockGateway = new MockOperatorEventGateway();
    KeySelector<RowData, RowData> keySelector =
        new KeySelector<RowData, RowData>() {
          private static final long serialVersionUID = 7662520075515707428L;

          @Override
          public RowData getKey(RowData value) {
            return value;
          }
        };

    TypeSerializer<DataStatistics<MapDataStatistics, Map<RowData, Long>>> statisticsSerializer =
        MapDataStatisticsSerializer.fromKeySerializer(rowSerializer);
    return new DataStatisticsOperator<>(keySelector, mockGateway, statisticsSerializer);
  }

  @After
  public void clean() throws Exception {
    operator.close();
  }

  @Test
  public void testProcessElement() throws Exception {
    try (OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<RowData, Long>>>
        testHarness = createHarness(this.operator)) {
      StateInitializationContext stateContext = getStateContext();
      operator.initializeState(stateContext);
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("a"))));
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("a"))));
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("b"))));
      assertTrue(operator.localDataStatistics() instanceof MapDataStatistics);
      MapDataStatistics mapDataStatistics = (MapDataStatistics) operator.localDataStatistics();
      Map<RowData, Long> statsMap = mapDataStatistics.statistics();
      assertThat(statsMap).hasSize(2);
      assertThat(statsMap)
          .containsExactlyInAnyOrderEntriesOf(
              ImmutableMap.of(
                  GenericRowData.of(StringData.fromString("a")),
                  2L,
                  GenericRowData.of(StringData.fromString("b")),
                  1L));
      testHarness.endInput();
    }
  }

  @Test
  public void testOperatorOutput() throws Exception {
    try (OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<RowData, Long>>>
        testHarness = createHarness(this.operator)) {
      testHarness.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("a"))));
      testHarness.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("b"))));
      testHarness.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("b"))));

      List<RowData> recordsOutput =
          testHarness.extractOutputValues().stream()
              .filter(DataStatisticsOrRecord::hasRecord)
              .map(DataStatisticsOrRecord::record)
              .collect(Collectors.toList());
      assertThat(recordsOutput)
          .containsExactlyInAnyOrderElementsOf(
              ImmutableList.of(
                  GenericRowData.of(StringData.fromString("a")),
                  GenericRowData.of(StringData.fromString("b")),
                  GenericRowData.of(StringData.fromString("b"))));
    }
  }

  @Test
  public void testRestoreState() throws Exception {
    OperatorSubtaskState snapshot;
    try (OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<RowData, Long>>>
        testHarness1 = createHarness(this.operator)) {
      DataStatistics<MapDataStatistics, Map<RowData, Long>> mapDataStatistics =
          new MapDataStatistics();
      mapDataStatistics.add(GenericRowData.of(StringData.fromString("a")));
      mapDataStatistics.add(GenericRowData.of(StringData.fromString("a")));
      mapDataStatistics.add(GenericRowData.of(StringData.fromString("b")));
      mapDataStatistics.add(GenericRowData.of(StringData.fromString("c")));
      operator.handleOperatorEvent(new DataStatisticsEvent(0, mapDataStatistics));
      assertThat(operator.globalDataStatistics()).isInstanceOf(MapDataStatistics.class);
      assertThat(((MapDataStatistics) operator.globalDataStatistics()).statistics())
          .containsExactlyInAnyOrderEntriesOf(
              ImmutableMap.of(
                  GenericRowData.of(StringData.fromString("a")),
                  2L,
                  GenericRowData.of(StringData.fromString("b")),
                  1L,
                  GenericRowData.of(StringData.fromString("c")),
                  1L));
      snapshot = testHarness1.snapshot(1L, 0);
    }

    // Use the snapshot to initialize state for another new operator and then verify that the global
    // statistics for the new operator is same as before
    DataStatisticsOperator<MapDataStatistics, Map<RowData, Long>> restoredOperator =
        createOperator();
    try (OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<RowData, Long>>>
        testHarness2 = new OneInputStreamOperatorTestHarness<>(restoredOperator, 2, 2, 1)) {
      testHarness2.setup();
      testHarness2.initializeState(snapshot);
      assertThat(restoredOperator.globalDataStatistics()).isInstanceOf(MapDataStatistics.class);
      // restored RowData is BinaryRowData. convert to GenericRowData for comparison
      Map<RowData, Long> restoredStatistics = Maps.newHashMap();
      ((MapDataStatistics) restoredOperator.globalDataStatistics())
          .statistics()
          .forEach(
              (rowData, count) ->
                  restoredStatistics.put(GenericRowData.of(rowData.getString(0)), count));
      assertThat(restoredStatistics)
          .containsExactlyInAnyOrderEntriesOf(
              ImmutableMap.of(
                  GenericRowData.of(StringData.fromString("a")),
                  2L,
                  GenericRowData.of(StringData.fromString("b")),
                  1L,
                  GenericRowData.of(StringData.fromString("c")),
                  1L));
    }
  }

  private StateInitializationContext getStateContext() throws Exception {
    MockEnvironment env = new MockEnvironmentBuilder().build();
    AbstractStateBackend abstractStateBackend = new HashMapStateBackend();
    CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
    OperatorStateStore operatorStateStore =
        abstractStateBackend.createOperatorStateBackend(
            env, "test-operator", Collections.emptyList(), cancelStreamRegistry);
    return new StateInitializationContextImpl(null, operatorStateStore, null, null, null);
  }

  private OneInputStreamOperatorTestHarness<
          RowData, DataStatisticsOrRecord<MapDataStatistics, Map<RowData, Long>>>
      createHarness(
          final DataStatisticsOperator<MapDataStatistics, Map<RowData, Long>>
              dataStatisticsOperator)
          throws Exception {

    OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<RowData, Long>>>
        harness = new OneInputStreamOperatorTestHarness<>(dataStatisticsOperator, 1, 1, 0);
    harness.setup(
        new DataStatisticsOrRecordSerializer<>(
            MapDataStatisticsSerializer.fromKeySerializer(rowSerializer), rowSerializer));
    harness.open();
    return harness;
  }
}
