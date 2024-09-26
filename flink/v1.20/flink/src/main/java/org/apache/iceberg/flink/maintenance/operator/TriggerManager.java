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
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TriggerManager starts the Maintenance Tasks by emitting {@link Trigger} messages which are
 * calculated based on the incoming {@link TableChange} messages. The TriggerManager keeps track of
 * the changes since the last run of the Maintenance Tasks and triggers a new run based on the
 * result of the {@link TriggerEvaluator}.
 *
 * <p>The TriggerManager prevents overlapping Maintenance Task runs using {@link
 * TriggerLockFactory.Lock}. The current implementation only handles conflicts within a single job.
 * Users should avoid scheduling maintenance for the same table in different Flink jobs.
 *
 * <p>The TriggerManager should run as a global operator. {@link KeyedProcessFunction} is used, so
 * the timer functions are available, but the key is not used.
 */
@Internal
public class TriggerManager extends KeyedProcessFunction<Boolean, TableChange, Trigger>
    implements CheckpointedFunction {
  private static final Logger LOG = LoggerFactory.getLogger(TriggerManager.class);

  private final String tableName;
  private final TriggerLockFactory lockFactory;
  private final List<String> maintenanceTaskNames;
  private final List<TriggerEvaluator> evaluators;
  private final long minFireDelayMs;
  private final long lockCheckDelayMs;
  private transient Counter rateLimiterTriggeredCounter;
  private transient Counter concurrentRunThrottledCounter;
  private transient Counter nothingToTriggerCounter;
  private transient List<Counter> triggerCounters;
  private transient ValueState<Long> nextEvaluationTimeState;
  private transient ListState<TableChange> accumulatedChangesState;
  private transient ListState<Long> lastTriggerTimesState;
  private transient Long nextEvaluationTime;
  private transient List<TableChange> accumulatedChanges;
  private transient List<Long> lastTriggerTimes;
  private transient TriggerLockFactory.Lock lock;
  private transient TriggerLockFactory.Lock recoveryLock;
  private transient boolean shouldRestoreTasks = false;
  private transient boolean inited = false;
  // To keep the task scheduling fair we keep the last triggered task position in memory.
  // If we find a task to trigger, then we run it, but after it is finished, we start from the given
  // position to prevent "starvation" of the tasks.
  // When there is nothing to trigger, we start from the beginning, as the order of the tasks might
  // be important (RewriteDataFiles first, and then RewriteManifestFiles later)
  private transient int startsFrom = 0;
  private transient boolean triggered = false;

  public TriggerManager(
      TableLoader tableLoader,
      TriggerLockFactory lockFactory,
      List<String> maintenanceTaskNames,
      List<TriggerEvaluator> evaluators,
      long minFireDelayMs,
      long lockCheckDelayMs) {
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");
    Preconditions.checkNotNull(lockFactory, "Lock factory should no be null");
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

    tableLoader.open();
    this.tableName = tableLoader.loadTable().name();
    this.lockFactory = lockFactory;
    this.maintenanceTaskNames = maintenanceTaskNames;
    this.evaluators = evaluators;
    this.minFireDelayMs = minFireDelayMs;
    this.lockCheckDelayMs = lockCheckDelayMs;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.rateLimiterTriggeredCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(
                TableMaintenanceMetrics.GROUP_KEY, TableMaintenanceMetrics.GROUP_VALUE_DEFAULT)
            .counter(TableMaintenanceMetrics.RATE_LIMITER_TRIGGERED);
    this.concurrentRunThrottledCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(
                TableMaintenanceMetrics.GROUP_KEY, TableMaintenanceMetrics.GROUP_VALUE_DEFAULT)
            .counter(TableMaintenanceMetrics.CONCURRENT_RUN_THROTTLED);
    this.nothingToTriggerCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(
                TableMaintenanceMetrics.GROUP_KEY, TableMaintenanceMetrics.GROUP_VALUE_DEFAULT)
            .counter(TableMaintenanceMetrics.NOTHING_TO_TRIGGER);
    this.triggerCounters =
        maintenanceTaskNames.stream()
            .map(
                name ->
                    getRuntimeContext()
                        .getMetricGroup()
                        .addGroup(TableMaintenanceMetrics.GROUP_KEY, name)
                        .counter(TableMaintenanceMetrics.TRIGGERED))
            .collect(Collectors.toList());

    this.nextEvaluationTimeState =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("triggerManagerNextTriggerTime", Types.LONG));
    this.accumulatedChangesState =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "triggerManagerAccumulatedChange", TypeInformation.of(TableChange.class)));
    this.lastTriggerTimesState =
        getRuntimeContext()
            .getListState(new ListStateDescriptor<>("triggerManagerLastTriggerTime", Types.LONG));
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    if (inited) {
      // Only store state if initialized
      nextEvaluationTimeState.update(nextEvaluationTime);
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
  public void initializeState(FunctionInitializationContext context) throws Exception {
    LOG.info("Initializing state restored: {}", context.isRestored());
    lockFactory.open();
    this.lock = lockFactory.createLock();
    this.recoveryLock = lockFactory.createRecoveryLock();
    if (context.isRestored()) {
      shouldRestoreTasks = true;
    }
  }

  @Override
  public void processElement(TableChange change, Context ctx, Collector<Trigger> out)
      throws Exception {
    init(out, ctx.timerService());

    accumulatedChanges.forEach(tableChange -> tableChange.merge(change));

    long current = ctx.timerService().currentProcessingTime();
    if (nextEvaluationTime == null) {
      checkAndFire(current, ctx.timerService(), out);
    } else {
      LOG.info(
          "Trigger manager rate limiter triggered current: {}, next: {}, accumulated changes: {}",
          current,
          nextEvaluationTime,
          accumulatedChanges);
      rateLimiterTriggeredCounter.inc();
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Trigger> out) throws Exception {
    init(out, ctx.timerService());
    this.nextEvaluationTime = null;
    checkAndFire(ctx.timerService().currentProcessingTime(), ctx.timerService(), out);
  }

  @Override
  public void close() throws IOException {
    lockFactory.close();
  }

  private void checkAndFire(long current, TimerService timerService, Collector<Trigger> out) {
    if (shouldRestoreTasks) {
      if (recoveryLock.isHeld()) {
        // Recovered tasks in progress. Skip trigger check
        LOG.debug("The recovery lock is still held at {}", current);
        schedule(timerService, current + lockCheckDelayMs);
        return;
      } else {
        LOG.info("The recovery is finished at {}", current);
        shouldRestoreTasks = false;
      }
    }

    Integer taskToStart =
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

    if (lock.tryLock()) {
      TableChange change = accumulatedChanges.get(taskToStart);
      out.collect(Trigger.create(current, taskToStart));
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

    timerService.registerProcessingTimeTimer(nextEvaluationTime);
  }

  private void schedule(TimerService timerService, long time) {
    this.nextEvaluationTime = time;
    timerService.registerProcessingTimeTimer(time);
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

  private void init(Collector<Trigger> out, TimerService timerService) throws Exception {
    if (!inited) {
      long current = timerService.currentProcessingTime();

      // Initialize from state
      this.nextEvaluationTime = nextEvaluationTimeState.value();
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
        recoveryLock.tryLock();
        out.collect(Trigger.recovery(current));
        if (nextEvaluationTime == null) {
          schedule(timerService, current + minFireDelayMs);
        }
      }

      inited = true;
    }
  }
}
