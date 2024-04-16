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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * DataStatisticsOperator collects traffic distribution statistics. A custom partitioner shall be
 * attached to the DataStatisticsOperator output. The custom partitioner leverages the statistics to
 * shuffle record to improve data clustering while maintaining relative balanced traffic
 * distribution to downstream subtasks.
 */
@Internal
class DataStatisticsOperator<D extends DataStatistics<D, S>, S>
    extends AbstractStreamOperator<DataStatisticsOrRecord<D, S>>
    implements OneInputStreamOperator<RowData, DataStatisticsOrRecord<D, S>>, OperatorEventHandler {

  private static final long serialVersionUID = 1L;

  private final String operatorName;
  private final RowDataWrapper rowDataWrapper;
  private final SortKey sortKey;
  private final OperatorEventGateway operatorEventGateway;
  private final TypeSerializer<DataStatistics<D, S>> statisticsSerializer;
  private transient volatile DataStatistics<D, S> localStatistics;
  private transient volatile DataStatistics<D, S> globalStatistics;
  private transient ListState<DataStatistics<D, S>> globalStatisticsState;

  DataStatisticsOperator(
      String operatorName,
      Schema schema,
      SortOrder sortOrder,
      OperatorEventGateway operatorEventGateway,
      TypeSerializer<DataStatistics<D, S>> statisticsSerializer) {
    this.operatorName = operatorName;
    this.rowDataWrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());
    this.sortKey = new SortKey(schema, sortOrder);
    this.operatorEventGateway = operatorEventGateway;
    this.statisticsSerializer = statisticsSerializer;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    localStatistics = statisticsSerializer.createInstance();
    globalStatisticsState =
        context
            .getOperatorStateStore()
            .getUnionListState(
                new ListStateDescriptor<>("globalStatisticsState", statisticsSerializer));

    if (context.isRestored()) {
      int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
      if (globalStatisticsState.get() == null
          || !globalStatisticsState.get().iterator().hasNext()) {
        LOG.warn(
            "Operator {} subtask {} doesn't have global statistics state to restore",
            operatorName,
            subtaskIndex);
        globalStatistics = statisticsSerializer.createInstance();
      } else {
        LOG.info(
            "Restoring operator {} global statistics state for subtask {}",
            operatorName,
            subtaskIndex);
        globalStatistics = globalStatisticsState.get().iterator().next();
      }
    } else {
      globalStatistics = statisticsSerializer.createInstance();
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
    int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    Preconditions.checkArgument(
        event instanceof DataStatisticsEvent,
        String.format(
            "Operator %s subtask %s received unexpected operator event %s",
            operatorName, subtaskIndex, event.getClass()));
    DataStatisticsEvent<D, S> statisticsEvent = (DataStatisticsEvent<D, S>) event;
    LOG.info(
        "Operator {} received global data event from coordinator checkpoint {}",
        operatorName,
        statisticsEvent.checkpointId());
    globalStatistics =
        DataStatisticsUtil.deserializeDataStatistics(
            statisticsEvent.statisticsBytes(), statisticsSerializer);
    output.collect(new StreamRecord<>(DataStatisticsOrRecord.fromDataStatistics(globalStatistics)));
  }

  @Override
  public void processElement(StreamRecord<RowData> streamRecord) {
    RowData record = streamRecord.getValue();
    StructLike struct = rowDataWrapper.wrap(record);
    sortKey.wrap(struct);
    localStatistics.add(sortKey);
    output.collect(new StreamRecord<>(DataStatisticsOrRecord.fromRecord(record)));
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    long checkpointId = context.getCheckpointId();
    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    LOG.info(
        "Snapshotting data statistics operator {} for checkpoint {} in subtask {}",
        operatorName,
        checkpointId,
        subTaskId);

    // Pass global statistics to partitioners so that all the operators refresh statistics
    // at same checkpoint barrier
    if (!globalStatistics.isEmpty()) {
      output.collect(
          new StreamRecord<>(DataStatisticsOrRecord.fromDataStatistics(globalStatistics)));
    }

    // Only subtask 0 saves the state so that globalStatisticsState(UnionListState) stores
    // an exact copy of globalStatistics
    if (!globalStatistics.isEmpty() && getRuntimeContext().getIndexOfThisSubtask() == 0) {
      globalStatisticsState.clear();
      LOG.info(
          "Saving operator {} global statistics {} to state in subtask {}",
          operatorName,
          globalStatistics,
          subTaskId);
      globalStatisticsState.add(globalStatistics);
    }

    // For now, local statistics are sent to coordinator at checkpoint
    operatorEventGateway.sendEventToCoordinator(
        DataStatisticsEvent.create(checkpointId, localStatistics, statisticsSerializer));
    LOG.debug(
        "Subtask {} of operator {} sent local statistics to coordinator at checkpoint{}: {}",
        subTaskId,
        operatorName,
        checkpointId,
        localStatistics);

    // Recreate the local statistics
    localStatistics = statisticsSerializer.createInstance();
  }

  @VisibleForTesting
  DataStatistics<D, S> localDataStatistics() {
    return localStatistics;
  }

  @VisibleForTesting
  DataStatistics<D, S> globalDataStatistics() {
    return globalStatistics;
  }
}
