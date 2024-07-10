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

import java.util.Map;
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
public class DataStatisticsOperator extends AbstractStreamOperator<StatisticsOrRecord>
    implements OneInputStreamOperator<RowData, StatisticsOrRecord>, OperatorEventHandler {

  private static final long serialVersionUID = 1L;

  private final String operatorName;
  private final RowDataWrapper rowDataWrapper;
  private final SortKey sortKey;
  private final OperatorEventGateway operatorEventGateway;
  private final int downstreamParallelism;
  private final StatisticsType statisticsType;
  private final TypeSerializer<DataStatistics> taskStatisticsSerializer;
  private final TypeSerializer<AggregatedStatistics> aggregatedStatisticsSerializer;

  private transient int parallelism;
  private transient int subtaskIndex;
  private transient ListState<AggregatedStatistics> globalStatisticsState;
  // current statistics type may be different from the config due to possible
  // migration from Map statistics to Sketch statistics when high cardinality detected
  private transient volatile StatisticsType taskStatisticsType;
  private transient volatile DataStatistics localStatistics;
  private transient volatile AggregatedStatistics globalStatistics;

  DataStatisticsOperator(
      String operatorName,
      Schema schema,
      SortOrder sortOrder,
      OperatorEventGateway operatorEventGateway,
      int downstreamParallelism,
      StatisticsType statisticsType) {
    this.operatorName = operatorName;
    this.rowDataWrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());
    this.sortKey = new SortKey(schema, sortOrder);
    this.operatorEventGateway = operatorEventGateway;
    this.downstreamParallelism = downstreamParallelism;
    this.statisticsType = statisticsType;

    SortKeySerializer sortKeySerializer = new SortKeySerializer(schema, sortOrder);
    this.taskStatisticsSerializer = new DataStatisticsSerializer(sortKeySerializer);
    this.aggregatedStatisticsSerializer = new AggregatedStatisticsSerializer(sortKeySerializer);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    this.parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
    this.subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    this.globalStatisticsState =
        context
            .getOperatorStateStore()
            .getUnionListState(
                new ListStateDescriptor<>("globalStatisticsState", aggregatedStatisticsSerializer));

    if (context.isRestored()) {
      if (globalStatisticsState.get() == null
          || !globalStatisticsState.get().iterator().hasNext()) {
        LOG.warn(
            "Operator {} subtask {} doesn't have global statistics state to restore",
            operatorName,
            subtaskIndex);
      } else {
        LOG.info(
            "Operator {} subtask {} restoring global statistics state", operatorName, subtaskIndex);
        this.globalStatistics = globalStatisticsState.get().iterator().next();
      }
    }

    this.taskStatisticsType = StatisticsUtil.collectType(statisticsType, globalStatistics);
    this.localStatistics =
        StatisticsUtil.createTaskStatistics(taskStatisticsType, parallelism, downstreamParallelism);
  }

  @Override
  public void open() throws Exception {
    if (globalStatistics != null) {
      output.collect(new StreamRecord<>(StatisticsOrRecord.fromStatistics(globalStatistics)));
    }
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    Preconditions.checkArgument(
        event instanceof StatisticsEvent,
        String.format(
            "Operator %s subtask %s received unexpected operator event %s",
            operatorName, subtaskIndex, event.getClass()));
    StatisticsEvent statisticsEvent = (StatisticsEvent) event;
    LOG.info(
        "Operator {} subtask {} received global data event from coordinator checkpoint {}",
        operatorName,
        subtaskIndex,
        statisticsEvent.checkpointId());
    globalStatistics =
        StatisticsUtil.deserializeAggregatedStatistics(
            statisticsEvent.statisticsBytes(), aggregatedStatisticsSerializer);
    checkStatisticsTypeMigration();
    output.collect(new StreamRecord<>(StatisticsOrRecord.fromStatistics(globalStatistics)));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void processElement(StreamRecord<RowData> streamRecord) {
    // collect data statistics
    RowData record = streamRecord.getValue();
    StructLike struct = rowDataWrapper.wrap(record);
    sortKey.wrap(struct);
    localStatistics.add(sortKey);

    checkStatisticsTypeMigration();
    output.collect(new StreamRecord<>(StatisticsOrRecord.fromRecord(record)));
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    long checkpointId = context.getCheckpointId();
    LOG.info(
        "Operator {} subtask {} snapshotting data statistics for checkpoint {}",
        operatorName,
        subtaskIndex,
        checkpointId);

    // Pass global statistics to partitioner so that all the operators refresh statistics
    // at same checkpoint barrier
    if (globalStatistics != null) {
      output.collect(new StreamRecord<>(StatisticsOrRecord.fromStatistics(globalStatistics)));
    }

    // Only subtask 0 saves the state so that globalStatisticsState(UnionListState) stores
    // an exact copy of globalStatistics
    if (globalStatistics != null
        && getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == 0) {
      globalStatisticsState.clear();
      LOG.info(
          "Operator {} subtask {} saving global statistics to state", operatorName, subtaskIndex);
      globalStatisticsState.add(globalStatistics);
      LOG.debug(
          "Operator {} subtask {} saved global statistics to state: {}",
          operatorName,
          subtaskIndex,
          globalStatistics);
    }

    // For now, local statistics are sent to coordinator at checkpoint
    LOG.info(
        "Operator {} Subtask {} sending local statistics to coordinator for checkpoint {}",
        operatorName,
        subtaskIndex,
        checkpointId);
    operatorEventGateway.sendEventToCoordinator(
        StatisticsEvent.createTaskStatisticsEvent(
            checkpointId, localStatistics, taskStatisticsSerializer));

    // Recreate the local statistics
    localStatistics =
        StatisticsUtil.createTaskStatistics(taskStatisticsType, parallelism, downstreamParallelism);
  }

  private void checkStatisticsTypeMigration() {
    // only check if the statisticsType config is Auto and localStatistics is currently Map type
    if (statisticsType == StatisticsType.Auto && localStatistics.type() == StatisticsType.Map) {
      Map<SortKey, Long> mapStatistics = (Map<SortKey, Long>) localStatistics.result();
      // convert if local statistics has cardinality over the threshold or
      // if received global statistics is already sketch type
      if (mapStatistics.size() > SketchUtil.OPERATOR_SKETCH_SWITCH_THRESHOLD
          || (globalStatistics != null && globalStatistics.type() == StatisticsType.Sketch)) {
        LOG.info(
            "Operator {} subtask {} switched local statistics from Map to Sketch.",
            operatorName,
            subtaskIndex);
        this.taskStatisticsType = StatisticsType.Sketch;
        this.localStatistics =
            StatisticsUtil.createTaskStatistics(
                taskStatisticsType, parallelism, downstreamParallelism);
        SketchUtil.convertMapToSketch(mapStatistics, localStatistics::add);
      }
    }
  }

  @VisibleForTesting
  DataStatistics localStatistics() {
    return localStatistics;
  }

  @VisibleForTesting
  AggregatedStatistics globalStatistics() {
    return globalStatistics;
  }
}
