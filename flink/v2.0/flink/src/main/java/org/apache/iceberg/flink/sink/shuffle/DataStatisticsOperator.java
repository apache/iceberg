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
  private final TypeSerializer<GlobalStatistics> globalStatisticsSerializer;

  private transient int parallelism;
  private transient int subtaskIndex;
  private transient ListState<GlobalStatistics> globalStatisticsState;
  // current statistics type may be different from the config due to possible
  // migration from Map statistics to Sketch statistics when high cardinality detected
  private transient volatile StatisticsType taskStatisticsType;
  private transient volatile DataStatistics localStatistics;
  private transient volatile GlobalStatistics globalStatistics;

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
    this.globalStatisticsSerializer = new GlobalStatisticsSerializer(sortKeySerializer);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    this.parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
    this.subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

    // Use union state so that new subtasks can also restore global statistics during scale-up.
    this.globalStatisticsState =
        context
            .getOperatorStateStore()
            .getUnionListState(
                new ListStateDescriptor<>("globalStatisticsState", globalStatisticsSerializer));

    if (context.isRestored()) {
      if (globalStatisticsState.get() == null
          || !globalStatisticsState.get().iterator().hasNext()) {
        LOG.info(
            "Operator {} subtask {} doesn't have global statistics state to restore",
            operatorName,
            subtaskIndex);
        // If Flink deprecates union state in the future, RequestGlobalStatisticsEvent can be
        // leveraged to request global statistics from coordinator if new subtasks (scale-up case)
        // has nothing to restore from.
      } else {
        GlobalStatistics restoredStatistics = globalStatisticsState.get().iterator().next();
        LOG.info(
            "Operator {} subtask {} restored global statistics state", operatorName, subtaskIndex);
        this.globalStatistics = restoredStatistics;
      }

      // Always request for new statistics from coordinator upon task initialization.
      // There are a few scenarios this is needed
      // 1. downstream writer parallelism changed due to rescale.
      // 2. coordinator failed to send the aggregated statistics to subtask
      //    (e.g. due to subtask failure at the time).
      // Records may flow before coordinator can respond. Range partitioner should be
      // able to continue to operate with potentially suboptimal behavior (in sketch case).
      LOG.info(
          "Operator {} subtask {} requests new global statistics from coordinator ",
          operatorName,
          subtaskIndex);
      // coordinator can use the hashCode (if available) in the request event to determine
      // if operator already has the latest global statistics and respond can be skipped.
      // This makes the handling cheap in most situations.
      RequestGlobalStatisticsEvent event =
          globalStatistics != null
              ? new RequestGlobalStatisticsEvent(globalStatistics.hashCode())
              : new RequestGlobalStatisticsEvent();
      operatorEventGateway.sendEventToCoordinator(event);
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
    this.globalStatistics =
        StatisticsUtil.deserializeGlobalStatistics(
            statisticsEvent.statisticsBytes(), globalStatisticsSerializer);
    checkStatisticsTypeMigration();
    // if applyImmediately not set, wait until the checkpoint time to switch
    if (statisticsEvent.applyImmediately()) {
      output.collect(new StreamRecord<>(StatisticsOrRecord.fromStatistics(globalStatistics)));
    }
  }

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

  @SuppressWarnings("unchecked")
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
  GlobalStatistics globalStatistics() {
    return globalStatistics;
  }
}
