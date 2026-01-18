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

import java.io.IOException;
import java.util.List;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
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
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
@Internal
public class TriggerManagerOperator extends AbstractStreamOperator<Trigger>
    implements OneInputStreamOperator<TableChange, Trigger>,
        OperatorEventHandler,
        ProcessingTimeCallback {
  private static final Logger LOG = LoggerFactory.getLogger(TriggerManagerOperator.class);

  private final OperatorEventGateway operatorEventGateway;
  private final List<String> maintenanceTaskNames;
  private final List<TriggerEvaluator> evaluators;
  private transient Long nextEvaluationTime;
  private final long minFireDelayMs;
  private transient List<TableChange> accumulatedChanges;
  private transient ListState<Long> nextEvaluationTimeState;
  private transient ListState<TableChange> accumulatedChangesState;
  private transient ListState<Long> lastTriggerTimesState;
  private transient Counter rateLimiterTriggeredCounter;
  private transient Counter concurrentRunThrottledCounter;
  private transient Counter nothingToTriggerCounter;
  private transient List<Counter> triggerCounters;
  private final long lockCheckDelayMs;
  private transient boolean shouldRestoreTasks = false;
  private transient boolean recoveryLockHeld = false;
  private transient List<Long> lastTriggerTimes;
  // To keep the task scheduling fair we keep the last triggered task position in memory.
  // If we find a task to trigger, then we run it, but after it is finished, we start from the given
  // position to prevent "starvation" of the tasks.
  // When there is nothing to trigger, we start from the beginning, as the order of the tasks might
  // be important (RewriteDataFiles first, and then RewriteManifestFiles later)
  private transient int startsFrom = 0;
  private transient boolean triggered = false;
  private transient boolean inited = false;
  private transient Integer taskToStart;
  private final String lockId;
  private final String tableName;

  public TriggerManagerOperator(
      StreamOperatorParameters<Trigger> parameters,
      OperatorEventGateway operatorEventGateway,
      List<String> maintenanceTaskNames,
      List<TriggerEvaluator> evaluators,
      long minFireDelayMs,
      long lockCheckDelayMs,
      String lockId) {
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

    this.operatorEventGateway = operatorEventGateway;

    this.maintenanceTaskNames = maintenanceTaskNames;
    this.evaluators = evaluators;
    this.minFireDelayMs = minFireDelayMs;
    this.lockCheckDelayMs = lockCheckDelayMs;
    this.lockId = lockId;
    this.tableName = lockId;
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

    if (!Iterables.isEmpty(nextEvaluationTimeState.get())) {
      nextEvaluationTime = Iterables.getOnlyElement(nextEvaluationTimeState.get());
    }

    if (context.isRestored()) {
      shouldRestoreTasks = true;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    if (inited) {
      // Only store state if initialized
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
    } else {
      LOG.info("Not initialized, state is not stored");
    }
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    if (event instanceof LockAcquireResultEvent) {
      LOG.info("Received lock acquire result event: {}", event);
      handleLockAcquireResult((LockAcquireResultEvent) event, output, getProcessingTimeService());
    } else {
      throw new IllegalArgumentException(
          "Invalid operator event type: " + event.getClass().getCanonicalName());
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  @Override
  public void processElement(StreamRecord<TableChange> streamRecord) throws Exception {
    init(output, getProcessingTimeService());
    TableChange change = streamRecord.getValue();
    accumulatedChanges.forEach(tableChange -> tableChange.merge(change));
    long current = getProcessingTimeService().getCurrentProcessingTime();

    if (nextEvaluationTime == null) {
      checkAndFire(current, getProcessingTimeService());
    } else {
      LOG.info(
          "Trigger manager rate limiter triggered current: {}, next: {}, accumulated changes: {},{}",
          current,
          nextEvaluationTime,
          accumulatedChanges,
          maintenanceTaskNames);
      rateLimiterTriggeredCounter.inc();
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
  }

  @Override
  public void onProcessingTime(long l) throws IOException, InterruptedException, Exception {
    init(output, getProcessingTimeService());
    this.nextEvaluationTime = null;
    checkAndFire(getProcessingTimeService().getCurrentProcessingTime(), getProcessingTimeService());
  }

  private void handleLockAcquireResult(
      LockAcquireResultEvent event,
      Output<StreamRecord<Trigger>> output,
      ProcessingTimeService timerService) {
    long current = timerService.getCurrentProcessingTime();
    if (event.isRecoverLock()) {
      recoveryLockHeld = event.isLockHeld();
    } else {
      if (event.isLockHeld()) {
        TableChange change = accumulatedChanges.get(taskToStart);
        output.collect(new StreamRecord<>(Trigger.create(current, taskToStart), current));
        LOG.debug("Fired event with time: {}, collected: {} for {}", current, change, lockId);
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
  }

  private void checkAndFire(long current, ProcessingTimeService timerService) {
    if (shouldRestoreTasks) {
      if (recoveryLockHeld) {
        // Recovered tasks in progress. Skip trigger check
        LOG.debug("The recovery lock is still held at {}", current);
        schedule(timerService, current + lockCheckDelayMs);
        return;
      } else {
        LOG.info("The recovery is finished at {}", current);
        shouldRestoreTasks = false;
      }
    }

    this.taskToStart =
        nextTrigger(evaluators, accumulatedChanges, lastTriggerTimes, current, startsFrom);
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

    operatorEventGateway.sendEventToCoordinator(new LockAcquiredEvent(false, lockId));
  }

  private static Integer nextTrigger(
      List<TriggerEvaluator> evaluators,
      List<TableChange> changes,
      List<Long> lastTriggerTimes,
      long currentTime,
      int startPos) {
    int current = startPos;
    do {
      if (evaluators
          .get(current)
          .check(changes.get(current), lastTriggerTimes.get(current), currentTime)) {
        return current;
      }

      current = (current + 1) % evaluators.size();
    } while (current != startPos);

    return null;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void schedule(ProcessingTimeService timerService, long time) {
    this.nextEvaluationTime = time;
    timerService.registerTimer(time, this);
  }

  private void init(Output<StreamRecord<Trigger>> out, ProcessingTimeService timerService)
      throws Exception {
    if (!inited) {
      long current = timerService.getCurrentProcessingTime();

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

      if (shouldRestoreTasks) {
        // When the job state is restored, there could be ongoing tasks.
        // To prevent collision with the new triggers the following is done:
        //  - add a recovery lock
        //  - fire a recovery trigger
        // This ensures that the tasks of the previous trigger are executed, and the lock is removed
        // in the end. The result of the 'tryLock' is ignored as an already existing lock prevents
        // collisions as well.
        operatorEventGateway.sendEventToCoordinator(new LockReleasedEvent(true, lockId));
        out.collect(new StreamRecord<>(Trigger.recovery(current), current));
        if (nextEvaluationTime == null) {
          schedule(timerService, current + minFireDelayMs);
        }
      }

      inited = true;
    }
  }
}
