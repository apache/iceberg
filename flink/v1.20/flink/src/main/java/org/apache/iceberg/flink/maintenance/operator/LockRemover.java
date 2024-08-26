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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages locks and collect {@link org.apache.flink.metrics.Metric} for the Maintenance Tasks. */
@Internal
public class LockRemover extends AbstractStreamOperator<MaintenanceResult>
    implements OneInputStreamOperator<TaskResult, MaintenanceResult> {
  private static final Logger LOG = LoggerFactory.getLogger(LockRemover.class);

  private final TriggerLockFactory lockFactory;
  private final List<String> taskNames;

  private transient Map<Integer, Counter> successfulStreamResultCounterMap;
  private transient Map<Integer, Counter> failedStreamResultCounterMap;
  private transient Map<Integer, AtomicLong> lastRunLength;
  private transient TriggerLockFactory.Lock lock;
  private transient TriggerLockFactory.Lock recoveryLock;
  private transient long lastProcessed = 0L;

  public LockRemover(TriggerLockFactory lockFactory, List<String> taskNames) {
    Preconditions.checkNotNull(lockFactory, "Lock factory should no be null");
    Preconditions.checkArgument(
        taskNames != null && !taskNames.isEmpty(), "Invalid task names: null or empty");

    this.lockFactory = lockFactory;
    this.taskNames = taskNames;
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.successfulStreamResultCounterMap = Maps.newHashMapWithExpectedSize(taskNames.size());
    this.failedStreamResultCounterMap = Maps.newHashMapWithExpectedSize(taskNames.size());
    this.lastRunLength = Maps.newHashMapWithExpectedSize(taskNames.size());
    for (int i = 0; i < taskNames.size(); ++i) {
      String name = taskNames.get(i);
      successfulStreamResultCounterMap.put(
          i,
          getRuntimeContext()
              .getMetricGroup()
              .addGroup(TableMaintenanceMetrics.GROUP_KEY, name)
              .counter(TableMaintenanceMetrics.SUCCESSFUL_STREAM_COUNTER));
      failedStreamResultCounterMap.put(
          i,
          getRuntimeContext()
              .getMetricGroup()
              .addGroup(TableMaintenanceMetrics.GROUP_KEY, name)
              .counter(TableMaintenanceMetrics.FAILED_STREAM_COUNTER));
      AtomicLong length = new AtomicLong(0);
      lastRunLength.put(i, length);
      getRuntimeContext()
          .getMetricGroup()
          .addGroup(TableMaintenanceMetrics.GROUP_KEY, name)
          .gauge(TableMaintenanceMetrics.LAST_RUN_LENGTH, length::get);
    }

    this.lock = lockFactory.createLock();
    this.recoveryLock = lockFactory.createRecoveryLock();
  }

  @Override
  public void processElement(StreamRecord<TaskResult> element) {
    TaskResult value = element.getValue();
    LOG.debug("TaskResult {} arrived", value);
    long length = System.currentTimeMillis() - value.startEpoch();
    output.collect(
        new StreamRecord<>(
            new MaintenanceResult(
                value.startEpoch(), value.taskId(), length, value.success(), value.exceptions())));
    lock.unlock();
    this.lastProcessed = value.startEpoch();

    // Update the metrics
    lastRunLength.get(value.taskId()).set(length);
    if (value.success()) {
      successfulStreamResultCounterMap.get(value.taskId()).inc();
    } else {
      failedStreamResultCounterMap.get(value.taskId()).inc();
    }
  }

  @Override
  public void processWatermark(Watermark mark) {
    if (mark.getTimestamp() > lastProcessed) {
      lock.unlock();
      recoveryLock.unlock();
    }
  }
}
