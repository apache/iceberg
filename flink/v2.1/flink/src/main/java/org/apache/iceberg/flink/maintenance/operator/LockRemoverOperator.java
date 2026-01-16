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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
@Internal
public class LockRemoverOperator extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<TaskResult, Void>, OperatorEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LockRemoverOperator.class);

  private static final long serialVersionUID = 1L;

  private final String tableName;
  private final OperatorEventGateway operatorEventGateway;
  private long lastProcessedTaskStartEpoch;
  private final List<String> maintenanceTaskNames;
  private transient List<Counter> succeededTaskResultCounters;
  private transient List<Counter> failedTaskResultCounters;
  private transient List<AtomicLong> taskLastRunDurationMs;

  LockRemoverOperator(
      StreamOperatorParameters<Void> parameters,
      OperatorEventGateway operatorEventGateway,
      String tableName,
      List<String> maintenanceTaskNames) {
    super(parameters);
    this.tableName = tableName;
    this.operatorEventGateway = operatorEventGateway;
    this.maintenanceTaskNames = maintenanceTaskNames;
  }

  @Override
  public void open() throws Exception {
    this.succeededTaskResultCounters =
        Lists.newArrayListWithExpectedSize(maintenanceTaskNames.size());
    this.failedTaskResultCounters = Lists.newArrayListWithExpectedSize(maintenanceTaskNames.size());
    this.taskLastRunDurationMs = Lists.newArrayListWithExpectedSize(maintenanceTaskNames.size());
    for (int taskIndex = 0; taskIndex < maintenanceTaskNames.size(); ++taskIndex) {
      MetricGroup taskMetricGroup =
          TableMaintenanceMetrics.groupFor(
              getRuntimeContext(), tableName, maintenanceTaskNames.get(taskIndex), taskIndex);
      succeededTaskResultCounters.add(
          taskMetricGroup.counter(TableMaintenanceMetrics.SUCCEEDED_TASK_COUNTER));
      failedTaskResultCounters.add(
          taskMetricGroup.counter(TableMaintenanceMetrics.FAILED_TASK_COUNTER));
      AtomicLong duration = new AtomicLong(0);
      taskLastRunDurationMs.add(duration);
      taskMetricGroup.gauge(TableMaintenanceMetrics.LAST_RUN_DURATION_MS, duration::get);
    }
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    // dong nothing
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public void processElement(StreamRecord<TaskResult> streamRecord) {
    TaskResult taskResult = streamRecord.getValue();
    LOG.info(
        "Processing result {} for task {}",
        taskResult,
        maintenanceTaskNames.get(taskResult.taskIndex()));
    long duration = System.currentTimeMillis() - taskResult.startEpoch();
    operatorEventGateway.sendEventToCoordinator(
        new LockReleasedEvent(tableName, streamRecord.getTimestamp()));
    this.lastProcessedTaskStartEpoch = taskResult.startEpoch();

    // Update the metrics
    taskLastRunDurationMs.get(taskResult.taskIndex()).set(duration);
    if (taskResult.success()) {
      succeededTaskResultCounters.get(taskResult.taskIndex()).inc();
    } else {
      failedTaskResultCounters.get(taskResult.taskIndex()).inc();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    if (mark.getTimestamp() > lastProcessedTaskStartEpoch) {
      operatorEventGateway.sendEventToCoordinator(
          new LockReleasedEvent(tableName, mark.getTimestamp()));
    }

    super.processWatermark(mark);
  }
}
