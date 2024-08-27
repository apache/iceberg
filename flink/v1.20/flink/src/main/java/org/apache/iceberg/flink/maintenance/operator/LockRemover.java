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
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages locks and collect {@link org.apache.flink.metrics.Metric} for the Maintenance Tasks.
 *
 * <p>The assumptions about the locks are the following:
 *
 * <ul>
 *   <li>Every {@link TaskResult} is followed by a {@link Watermark} for normal {@link Trigger}s
 *   <li>For the {@link Trigger#recovery(long)} {@link Watermark} there is no element to process
 * </ul>
 *
 * When processing the inputs there are 3 possibilities:
 *
 * <ul>
 *   <li>Normal execution - we receive a {@link TaskResult} and then a {@link Watermark} - unlocking
 *       the lock is handled by the {@link #processElement(StreamRecord)}
 *   <li>Recovery without ongoing execution (unlocking the recoveryLock) - we receive the {@link
 *       Trigger#recovery(long)} {@link Watermark} without any {@link TaskResult} - unlocking the
 *       {@link TriggerLockFactory#createRecoveryLock()} and a possible {@link
 *       TriggerLockFactory#createLock()} is handled by the {@link #processWatermark(Watermark)}
 *       (the {@link #lastProcessedTaskStartEpoch} is 0 in this case)
 *   <li>Recovery with an ongoing execution - we receive a {@link TaskResult} and then a {@link
 *       Watermark} - unlocking the {@link TriggerLockFactory#createLock()} is handled by the {@link
 *       #processElement(StreamRecord)}, unlocking the {@link
 *       TriggerLockFactory#createRecoveryLock()} is handled by the {@link
 *       #processWatermark(Watermark)} (the {@link #lastProcessedTaskStartEpoch} is the start time
 *       of the old task)
 * </ul>
 */
@Internal
public class LockRemover extends AbstractStreamOperator<MaintenanceResult>
    implements OneInputStreamOperator<TaskResult, MaintenanceResult> {
  private static final Logger LOG = LoggerFactory.getLogger(LockRemover.class);

  private final TriggerLockFactory lockFactory;
  private final List<String> maintenanceTaskNames;

  private transient List<Counter> succeededTaskResultCounters;
  private transient List<Counter> failedTaskResultCounters;
  private transient List<AtomicLong> taskLastRunDurationMs;
  private transient TriggerLockFactory.Lock lock;
  private transient TriggerLockFactory.Lock recoveryLock;
  private transient long lastProcessedTaskStartEpoch = 0L;

  public LockRemover(TriggerLockFactory lockFactory, List<String> maintenanceTaskNames) {
    Preconditions.checkNotNull(lockFactory, "Lock factory should no be null");
    Preconditions.checkArgument(
        maintenanceTaskNames != null && !maintenanceTaskNames.isEmpty(),
        "Invalid maintenance task names: null or empty");

    this.lockFactory = lockFactory;
    this.maintenanceTaskNames = maintenanceTaskNames;
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.succeededTaskResultCounters =
        Lists.newArrayListWithExpectedSize(maintenanceTaskNames.size());
    this.failedTaskResultCounters = Lists.newArrayListWithExpectedSize(maintenanceTaskNames.size());
    this.taskLastRunDurationMs = Lists.newArrayListWithExpectedSize(maintenanceTaskNames.size());
    for (String name : maintenanceTaskNames) {
      succeededTaskResultCounters.add(
          getRuntimeContext()
              .getMetricGroup()
              .addGroup(TableMaintenanceMetrics.GROUP_KEY, name)
              .counter(TableMaintenanceMetrics.SUCCEEDED_TASK_COUNTER));
      failedTaskResultCounters.add(
          getRuntimeContext()
              .getMetricGroup()
              .addGroup(TableMaintenanceMetrics.GROUP_KEY, name)
              .counter(TableMaintenanceMetrics.FAILED_TASK_COUNTER));
      AtomicLong length = new AtomicLong(0);
      taskLastRunDurationMs.add(length);
      getRuntimeContext()
          .getMetricGroup()
          .addGroup(TableMaintenanceMetrics.GROUP_KEY, name)
          .gauge(TableMaintenanceMetrics.LAST_RUN_DURATION_MS, length::get);
    }

    this.lock = lockFactory.createLock();
    this.recoveryLock = lockFactory.createRecoveryLock();
  }

  @Override
  public void processElement(StreamRecord<TaskResult> streamRecord) {
    TaskResult taskResult = streamRecord.getValue();
    LOG.info(
        "Processing result {} for task {}",
        taskResult,
        maintenanceTaskNames.get(taskResult.taskIndex()));
    long duration = System.currentTimeMillis() - taskResult.startEpoch();
    output.collect(
        new StreamRecord<>(
            new MaintenanceResult(
                taskResult.startEpoch(),
                taskResult.taskIndex(),
                duration,
                taskResult.success(),
                taskResult.exceptions())));
    lock.unlock();
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
  public void processWatermark(Watermark mark) {
    if (mark.getTimestamp() > lastProcessedTaskStartEpoch) {
      lock.unlock();
      recoveryLock.unlock();
    }
  }
}
