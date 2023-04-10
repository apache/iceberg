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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.flink.sink.shuffle.statistics.DataStatistics;
import org.apache.iceberg.flink.sink.shuffle.statistics.DataStatisticsFactory;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * DataStatisticsOperator collects traffic distribution statistics. A custom partitioner shall be
 * attached to the DataStatisticsOperator output. The custom partitioner leverages the statistics to
 * shuffle record to improve data clustering while maintaining relative balanced traffic
 * distribution to downstream subtasks.
 */
class DataStatisticsOperator<T, K> extends AbstractStreamOperator<DataStatisticsOrRecord<T, K>>
    implements OneInputStreamOperator<T, DataStatisticsOrRecord<T, K>>, OperatorEventHandler {
  private static final long serialVersionUID = 1L;

  // keySelector will be used to generate key from data for collecting data statistics
  private final KeySelector<T, K> keySelector;
  private final OperatorEventGateway operatorEventGateway;
  private final DataStatisticsFactory<K> statisticsFactory;
  private transient volatile DataStatistics<K> localStatistics;
  private transient volatile DataStatistics<K> globalStatistics;
  private transient ListState<DataStatistics<K>> globalStatisticsState;

  DataStatisticsOperator(
      KeySelector<T, K> keySelector,
      OperatorEventGateway operatorEventGateway,
      DataStatisticsFactory<K> statisticsFactory) {
    this.keySelector = keySelector;
    this.operatorEventGateway = operatorEventGateway;
    this.statisticsFactory = statisticsFactory;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    localStatistics = statisticsFactory.createDataStatistics();
    globalStatisticsState =
        context
            .getOperatorStateStore()
            .getUnionListState(
                new ListStateDescriptor<>(
                    "globalStatisticsState",
                    TypeInformation.of(new TypeHint<DataStatistics<K>>() {})));

    if (context.isRestored()) {
      int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
      if (globalStatisticsState.get() == null
          || !globalStatisticsState.get().iterator().hasNext()) {
        LOG.warn("Subtask {} doesn't have global statistics state to restore", subtaskIndex);
        globalStatistics = statisticsFactory.createDataStatistics();
      } else {
        LOG.info("Restoring global statistics state for subtask {}", subtaskIndex);
        globalStatistics = globalStatisticsState.get().iterator().next();
      }
    } else {
      globalStatistics = statisticsFactory.createDataStatistics();
    }
  }

  @Override
  public void open() throws Exception {
    if (!globalStatistics.isEmpty()) {
      output.collect(
          new StreamRecord<>(DataStatisticsOrRecord.fromDataStatistics(globalStatistics)));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handleOperatorEvent(OperatorEvent event) {
    Preconditions.checkArgument(
        event instanceof DataStatisticsEvent,
        "Received unexpected operator event " + event.getClass());
    globalStatistics = ((DataStatisticsEvent<K>) event).dataStatistics();
    output.collect(new StreamRecord<>(DataStatisticsOrRecord.fromDataStatistics(globalStatistics)));
  }

  @Override
  public void processElement(StreamRecord<T> streamRecord) throws Exception {
    T record = streamRecord.getValue();
    K key = keySelector.getKey(record);
    localStatistics.add(key);
    output.collect(new StreamRecord<>(DataStatisticsOrRecord.fromRecord(record)));
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    long checkpointId = context.getCheckpointId();
    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    LOG.info(
        "Taking data statistics operator snapshot for checkpoint {} in subtask {}",
        checkpointId,
        subTaskId);

    // Only subtask 0 saves the state so that globalStatisticsState(UnionListState) stores
    // an exact copy of globalStatistics
    if (!globalStatistics.isEmpty() && getRuntimeContext().getIndexOfThisSubtask() == 0) {
      globalStatisticsState.clear();
      LOG.info("Saving global statistics {} to state in subtask {}", globalStatistics, subTaskId);
      globalStatisticsState.add(globalStatistics);
    }

    // For now, we make it simple to send globalStatisticsState at checkpoint
    operatorEventGateway.sendEventToCoordinator(
        new DataStatisticsEvent<>(checkpointId, localStatistics));

    // Recreate the local statistics
    localStatistics = statisticsFactory.createDataStatistics();
  }

  @VisibleForTesting
  DataStatistics<K> localDataStatistics() {
    return localStatistics;
  }

  @VisibleForTesting
  DataStatistics<K> globalDataStatistics() {
    return globalStatistics;
  }
}
