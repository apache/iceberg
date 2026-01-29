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
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TriggerManagerOperator sends events to the coordinator to acquire a lock, then waits for the
 * response. If the response indicates that the lock has been acquired, it fires a trigger;
 * otherwise, it schedules the next attempt. When the job recovers from a failure, tasks from
 * different execution paths of the previous run may still be running. Therefore, it first needs to
 * send a lock with the maximum timestamp, and then send a recovery trigger. Only after the
 * downstream removes this lock can we be sure that all tasks have fully stopped.
 */
@Experimental
@Internal
public class TriggerManagerOperator extends AbstractStreamOperator<Trigger>
    implements OneInputStreamOperator<TableChange, Trigger>,
        OperatorEventHandler,
        ProcessingTimeCallback {
  private static final Logger LOG = LoggerFactory.getLogger(TriggerManagerOperator.class);

  private final List<String> maintenanceTaskNames;
  private final List<TriggerEvaluator> evaluators;
  private transient Long nextEvaluationTime;
  private final long minFireDelayMs;
  private final OperatorEventGateway operatorEventGateway;
  private transient List<TableChange> accumulatedChanges;
  private transient ListState<Long> nextEvaluationTimeState;
  private transient ListState<TableChange> accumulatedChangesState;
  private transient ListState<Long> lastTriggerTimesState;
  private transient Counter rateLimiterTriggeredCounter;
  private transient Counter concurrentRunThrottledCounter;
  private transient Counter nothingToTriggerCounter;
  private transient List<Counter> triggerCounters;
  private final long lockCheckDelayMs;
  private transient List<Long> lastTriggerTimes;
  // To keep the task scheduling fair we keep the last triggered task position in memory.
  // If we find a task to trigger, then we run it, but after it is finished, we start from the given
  // position to prevent "starvation" of the tasks.
  // When there is nothing to trigger, we start from the beginning, as the order of the tasks might
  // be important (RewriteDataFiles first, and then RewriteManifestFiles later)
  private transient int startsFrom = 0;
  private transient boolean triggered = false;
  private final String tableName;
  private transient Long lockTime;
  private transient boolean shouldRestoreTasks = false;

  public TriggerManagerOperator(
      StreamOperatorParameters<Trigger> parameters,
      OperatorEventGateway operatorEventGateway,
      List<String> maintenanceTaskNames,
      List<TriggerEvaluator> evaluators,
      long minFireDelayMs,
      long lockCheckDelayMs,
      String tableName) {
    super(parameters);
    Preconditions.checkArgument(
        maintenanceTaskNames != null && !maintenanceTaskNames.isEmpty(),
        "Invalid maintenance task names: null or empty");
    Preconditions.checkArgument(
        evaluators != null && !evaluators.isEmpty(), "Invalid evaluators: null or empty");
    Preconditions.checkArgument(
        maintenanceTaskNames.size() == evaluators.size(),
        "Provide a name and evaluator for all of the maintenance tasks");
    Preconditions.checkArgument(minFireDelayMs > 0, "Minimum fire delay should be at least 1.");
    Preconditions.checkArgument(
        lockCheckDelayMs > 0, "Minimum lock delay rate should be at least 1 ms.");

    this.maintenanceTaskNames = maintenanceTaskNames;
    this.evaluators = evaluators;
    this.minFireDelayMs = minFireDelayMs;
    this.lockCheckDelayMs = lockCheckDelayMs;
    this.tableName = tableName;
    this.operatorEventGateway = operatorEventGateway;
  }

  @Override
  public void open() throws Exception {
    super.open();
    MetricGroup mainGroup = TableMaintenanceMetrics.groupFor(getRuntimeContext(), tableName);
    this.rateLimiterTriggeredCounter =
        mainGroup.counter(TableMaintenanceMetrics.RATE_LIMITER_TRIGGERED);
    this.concurrentRunThrottledCounter =
        mainGroup.counter(TableMaintenanceMetrics.CONCURRENT_RUN_THROTTLED);
    this.nothingToTriggerCounter = mainGroup.counter(TableMaintenanceMetrics.NOTHING_TO_TRIGGER);
    this.triggerCounters = Lists.newArrayListWithExpectedSize(maintenanceTaskNames.size());
    for (int taskIndex = 0; taskIndex < maintenanceTaskNames.size(); ++taskIndex) {
      triggerCounters.add(
          TableMaintenanceMetrics.groupFor(
                  mainGroup, maintenanceTaskNames.get(taskIndex), taskIndex)
              .counter(TableMaintenanceMetrics.TRIGGERED));
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.nextEvaluationTimeState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("triggerManagerNextTriggerTime", Types.LONG));

    this.accumulatedChangesState =
        context
            .getOperatorStateStore()
            .getListState(
                new ListStateDescriptor<>(
                    "triggerManagerAccumulatedChange", TypeInformation.of(TableChange.class)));

    this.lastTriggerTimesState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("triggerManagerLastTriggerTime", Types.LONG));

    long current = getProcessingTimeService().getCurrentProcessingTime();

    // Initialize from state
    if (!Iterables.isEmpty(nextEvaluationTimeState.get())) {
      nextEvaluationTime = Iterables.getOnlyElement(nextEvaluationTimeState.get());
    }

    this.accumulatedChanges = Lists.newArrayList(accumulatedChangesState.get());
    this.lastTriggerTimes = Lists.newArrayList(lastTriggerTimesState.get());

    // Initialize if the state was empty
    if (accumulatedChanges.isEmpty()) {
      for (int i = 0; i < evaluators.size(); ++i) {
        accumulatedChanges.add(TableChange.empty());
        lastTriggerTimes.add(current);
      }
    }

    // register the lock register event
    operatorEventGateway.sendEventToCoordinator(new LockRegisterEvent(tableName, current));

    if (context.isRestored()) {
      // When the job state is restored, there could be ongoing tasks.
      // To prevent collision with the new triggers the following is done:
      //  - add a recovery lock
      //  - fire a recovery trigger
      // This ensures that the tasks of the previous trigger are executed, and the lock is removed
      // in the end. The result of the 'tryLock' is ignored as an already existing lock prevents
      // collisions as well.
      // register the recover lock
      this.lockTime = current;
      this.shouldRestoreTasks = true;
      output.collect(new StreamRecord<>(Trigger.recovery(current), current));
      if (nextEvaluationTime == null) {
        schedule(getProcessingTimeService(), current + minFireDelayMs);
      } else {
        schedule(getProcessingTimeService(), nextEvaluationTime);
      }
    } else {
      this.lockTime = null;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    nextEvaluationTimeState.clear();
    if (nextEvaluationTime != null) {
      nextEvaluationTimeState.add(nextEvaluationTime);
    }

    accumulatedChangesState.update(accumulatedChanges);
    lastTriggerTimesState.update(lastTriggerTimes);
    LOG.info(
        "Storing state: nextEvaluationTime {}, accumulatedChanges {}, lastTriggerTimes {}",
        nextEvaluationTime,
        accumulatedChanges,
        lastTriggerTimes);
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    if (event instanceof LockReleasedEvent) {
      LOG.info("Received lock released event: {}", event);
      handleLockReleaseResult((LockReleasedEvent) event);
    } else {
      throw new IllegalArgumentException(
          "Invalid operator event type: " + event.getClass().getCanonicalName());
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public void processElement(StreamRecord<TableChange> streamRecord) throws Exception {
    TableChange change = streamRecord.getValue();
    accumulatedChanges.forEach(tableChange -> tableChange.merge(change));
    if (nextEvaluationTime == null) {
      checkAndFire(getProcessingTimeService());
    } else {
      LOG.info(
          "Trigger manager rate limiter triggered current: {}, next: {}, accumulated changes: {},{}",
          getProcessingTimeService().getCurrentProcessingTime(),
          nextEvaluationTime,
          accumulatedChanges,
          maintenanceTaskNames);
      rateLimiterTriggeredCounter.inc();
    }
  }

  @Override
  public void onProcessingTime(long l) {
    this.nextEvaluationTime = null;
    checkAndFire(getProcessingTimeService());
  }

  @Override
  public void close() throws Exception {
    super.close();
    this.lockTime = null;
  }

  @VisibleForTesting
  void handleLockReleaseResult(LockReleasedEvent event) {
    if (lockTime == null) {
      return;
    }

    if (event.timestamp() >= lockTime) {
      this.lockTime = null;
      this.shouldRestoreTasks = false;
    }
  }

  private void checkAndFire(ProcessingTimeService timerService) {
    long current = timerService.getCurrentProcessingTime();
    if (shouldRestoreTasks) {
      if (lockTime != null) {
        // Recovered tasks in progress. Skip trigger check
        LOG.info("The recovery lock is still held at {}", current);
        schedule(timerService, current + lockCheckDelayMs);
        return;
      }
    }

    Integer taskToStart =
        TriggerUtil.nextTrigger(
            evaluators, accumulatedChanges, lastTriggerTimes, current, startsFrom);
    if (taskToStart == null) {
      // Nothing to execute
      if (!triggered) {
        nothingToTriggerCounter.inc();
        LOG.debug("Nothing to execute at {} for collected: {}", current, accumulatedChanges);
      } else {
        LOG.debug("Execution check finished");
      }

      // Next time start from the beginning
      startsFrom = 0;
      triggered = false;
      return;
    }

    if (lockTime == null) {
      this.lockTime = current;
      TableChange change = accumulatedChanges.get(taskToStart);
      output.collect(new StreamRecord<>(Trigger.create(current, taskToStart), current));
      LOG.debug("Fired event with time: {}, collected: {} for {}", current, change, tableName);
      triggerCounters.get(taskToStart).inc();
      accumulatedChanges.set(taskToStart, TableChange.empty());
      lastTriggerTimes.set(taskToStart, current);
      schedule(timerService, current + minFireDelayMs);
      startsFrom = (taskToStart + 1) % evaluators.size();
      triggered = true;
    } else {
      // A task is already running, waiting for it to finish
      LOG.info("Failed to acquire lock. Delaying task to {}", current + lockCheckDelayMs);

      startsFrom = taskToStart;
      concurrentRunThrottledCounter.inc();
      schedule(timerService, current + lockCheckDelayMs);
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void schedule(ProcessingTimeService timerService, long time) {
    this.nextEvaluationTime = time;
    timerService.registerTimer(time, this);
  }
}
