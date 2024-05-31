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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsOperator {
  private final Schema schema =
      new Schema(
          Types.NestedField.optional(1, "id", Types.StringType.get()),
          Types.NestedField.optional(2, "number", Types.IntegerType.get()));
  private final SortOrder sortOrder = SortOrder.builderFor(schema).asc("id").build();
  private final SortKey sortKey = new SortKey(schema, sortOrder);
  private final RowType rowType = RowType.of(new VarCharType(), new IntType());
  private final TypeSerializer<RowData> rowSerializer = new RowDataSerializer(rowType);
  private final TypeSerializer<DataStatistics<MapDataStatistics, Map<SortKey, Long>>>
      statisticsSerializer =
          MapDataStatisticsSerializer.fromSortKeySerializer(
              new SortKeySerializer(schema, sortOrder));

  private DataStatisticsOperator<MapDataStatistics, Map<SortKey, Long>> operator;

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

  private DataStatisticsOperator<MapDataStatistics, Map<SortKey, Long>> createOperator() {
    MockOperatorEventGateway mockGateway = new MockOperatorEventGateway();
    return new DataStatisticsOperator<>(
        "testOperator", schema, sortOrder, mockGateway, statisticsSerializer);
  }

  @After
  public void clean() throws Exception {
    operator.close();
  }

  @Test
  public void testProcessElement() throws Exception {
    try (OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<SortKey, Long>>>
        testHarness = createHarness(this.operator)) {
      StateInitializationContext stateContext = getStateContext();
      operator.initializeState(stateContext);
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("a"), 5)));
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("a"), 3)));
      operator.processElement(new StreamRecord<>(GenericRowData.of(StringData.fromString("b"), 1)));
      assertThat(operator.localDataStatistics()).isInstanceOf(MapDataStatistics.class);

      SortKey keyA = sortKey;
      keyA.set(0, "a");
      SortKey keyB = sortKey;
      keyB.set(0, "b");
      Map<SortKey, Long> expectedMap = ImmutableMap.of(keyA, 2L, keyB, 1L);

      MapDataStatistics mapDataStatistics = (MapDataStatistics) operator.localDataStatistics();
      Map<SortKey, Long> statsMap = mapDataStatistics.statistics();
      assertThat(statsMap).hasSize(2);
      assertThat(statsMap).containsExactlyInAnyOrderEntriesOf(expectedMap);

      testHarness.endInput();
    }
  }

  @Test
  public void testOperatorOutput() throws Exception {
    try (OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<SortKey, Long>>>
        testHarness = createHarness(this.operator)) {
      testHarness.processElement(
          new StreamRecord<>(GenericRowData.of(StringData.fromString("a"), 2)));
      testHarness.processElement(
          new StreamRecord<>(GenericRowData.of(StringData.fromString("b"), 3)));
      testHarness.processElement(
          new StreamRecord<>(GenericRowData.of(StringData.fromString("b"), 1)));

      List<RowData> recordsOutput =
          testHarness.extractOutputValues().stream()
              .filter(DataStatisticsOrRecord::hasRecord)
              .map(DataStatisticsOrRecord::record)
              .collect(Collectors.toList());
      assertThat(recordsOutput)
          .containsExactlyInAnyOrderElementsOf(
              ImmutableList.of(
                  GenericRowData.of(StringData.fromString("a"), 2),
                  GenericRowData.of(StringData.fromString("b"), 3),
                  GenericRowData.of(StringData.fromString("b"), 1)));
    }
  }

  @Test
  public void testRestoreState() throws Exception {
    OperatorSubtaskState snapshot;
    try (OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<SortKey, Long>>>
        testHarness1 = createHarness(this.operator)) {
      MapDataStatistics mapDataStatistics = new MapDataStatistics();

      SortKey key = sortKey;
      key.set(0, "a");
      mapDataStatistics.add(key);
      key.set(0, "a");
      mapDataStatistics.add(key);
      key.set(0, "b");
      mapDataStatistics.add(key);
      key.set(0, "c");
      mapDataStatistics.add(key);

      SortKey keyA = sortKey;
      keyA.set(0, "a");
      SortKey keyB = sortKey;
      keyB.set(0, "b");
      SortKey keyC = sortKey;
      keyC.set(0, "c");
      Map<SortKey, Long> expectedMap = ImmutableMap.of(keyA, 2L, keyB, 1L, keyC, 1L);

      DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>> event =
          DataStatisticsEvent.create(0, mapDataStatistics, statisticsSerializer);
      operator.handleOperatorEvent(event);
      assertThat(operator.globalDataStatistics()).isInstanceOf(MapDataStatistics.class);
      assertThat(operator.globalDataStatistics().statistics())
          .containsExactlyInAnyOrderEntriesOf(expectedMap);
      snapshot = testHarness1.snapshot(1L, 0);
    }

    // Use the snapshot to initialize state for another new operator and then verify that the global
    // statistics for the new operator is same as before
    DataStatisticsOperator<MapDataStatistics, Map<SortKey, Long>> restoredOperator =
        createOperator();
    try (OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<SortKey, Long>>>
        testHarness2 = new OneInputStreamOperatorTestHarness<>(restoredOperator, 2, 2, 1)) {
      testHarness2.setup();
      testHarness2.initializeState(snapshot);
      assertThat(restoredOperator.globalDataStatistics()).isInstanceOf(MapDataStatistics.class);

      // restored RowData is BinaryRowData. convert to GenericRowData for comparison
      Map<SortKey, Long> restoredStatistics = Maps.newHashMap();
      restoredStatistics.putAll(restoredOperator.globalDataStatistics().statistics());

      SortKey keyA = sortKey;
      keyA.set(0, "a");
      SortKey keyB = sortKey;
      keyB.set(0, "b");
      SortKey keyC = sortKey;
      keyC.set(0, "c");
      Map<SortKey, Long> expectedMap = ImmutableMap.of(keyA, 2L, keyB, 1L, keyC, 1L);

      assertThat(restoredStatistics).containsExactlyInAnyOrderEntriesOf(expectedMap);
    }
  }

  private StateInitializationContext getStateContext() throws Exception {
    MockEnvironment env = new MockEnvironmentBuilder().build();
    AbstractStateBackend abstractStateBackend = new HashMapStateBackend();
    CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
    OperatorStateStore operatorStateStore =
        abstractStateBackend.createOperatorStateBackend(
            new OperatorStateBackendParametersImpl(
                env, "test-operator", Collections.emptyList(), cancelStreamRegistry));
    return new StateInitializationContextImpl(null, operatorStateStore, null, null, null);
  }

  private OneInputStreamOperatorTestHarness<
          RowData, DataStatisticsOrRecord<MapDataStatistics, Map<SortKey, Long>>>
      createHarness(
          final DataStatisticsOperator<MapDataStatistics, Map<SortKey, Long>>
              dataStatisticsOperator)
          throws Exception {

    OneInputStreamOperatorTestHarness<
            RowData, DataStatisticsOrRecord<MapDataStatistics, Map<SortKey, Long>>>
        harness = new OneInputStreamOperatorTestHarness<>(dataStatisticsOperator, 1, 1, 0);
    harness.setup(new DataStatisticsOrRecordSerializer<>(statisticsSerializer, rowSerializer));
    harness.open();
    return harness;
  }
}
