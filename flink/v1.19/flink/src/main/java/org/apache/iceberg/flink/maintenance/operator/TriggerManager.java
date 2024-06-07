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
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** . */
@Internal
class TriggerManager extends KeyedProcessFunction<Boolean, TableChange, Trigger>
    implements CheckpointedFunction {
  private static final Logger LOG = LoggerFactory.getLogger(TriggerManager.class);

  private final TableLoader tableLoader;
  private final TriggerLockFactory lockFactory;
  private final List<String> taskNames;
  private final List<TriggerEvaluator> evaluators;
  private final long minFireDelayMs;
  private final long lockCheckDelayMs;
  private final boolean clearLocks;
  private transient Counter rateLimiterTriggeredCounter;
  private transient Counter concurrentRunTriggeredCounter;
  private transient Counter nothingToTriggerCounter;
  private transient List<Counter> triggerCounters;
  private transient ValueState<Long> nextEvaluationTime;
  private transient ListState<TableChange> accumulatedChanges;
  private transient ListState<Long> lastTriggerTimes;
  private transient TriggerLockFactory.Lock lock;
  private transient TriggerLockFactory.Lock recoveryLock;
  private transient boolean isCleanUp = false;
  private transient boolean inited = false;
  private transient int startsFrom = 0;

  TriggerManager(
      TableLoader tableLoader,
      TriggerLockFactory lockFactory,
      List<String> taskNames,
      List<TriggerEvaluator> evaluators,
      long minFireDelayMs,
      long lockCheckDelayMs,
      boolean clearLocks) {
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");
    Preconditions.checkNotNull(lockFactory, "Lock factory should no be null");
    Preconditions.checkArgument(
        evaluators != null && !evaluators.isEmpty(), "Evaluators should not be empty");
    Preconditions.checkArgument(
        taskNames.size() == evaluators.size(), "Provide a name and evaluator for all of the tasks");
    Preconditions.checkArgument(minFireDelayMs > 0, "Minimum fire delay should be at least 1.");
    Preconditions.checkArgument(
        lockCheckDelayMs > 0, "Minimum lock delay rate should be at least 1.");

    this.tableLoader = tableLoader;
    this.lockFactory = lockFactory;
    this.taskNames = taskNames;
    this.evaluators = evaluators;
    this.minFireDelayMs = minFireDelayMs;
    this.lockCheckDelayMs = lockCheckDelayMs;
    this.clearLocks = clearLocks;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.rateLimiterTriggeredCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, MetricConstants.GROUP_VALUE_DEFAULT)
            .counter(MetricConstants.RATE_LIMITER_TRIGGERED);
    this.concurrentRunTriggeredCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, MetricConstants.GROUP_VALUE_DEFAULT)
            .counter(MetricConstants.CONCURRENT_RUN_TRIGGERED);
    this.nothingToTriggerCounter =
        getRuntimeContext()
            .getMetricGroup()
            .addGroup(MetricConstants.GROUP_KEY, MetricConstants.GROUP_VALUE_DEFAULT)
            .counter(MetricConstants.NOTHING_TO_TRIGGER);
    this.triggerCounters =
        taskNames.stream()
            .map(
                name ->
                    getRuntimeContext()
                        .getMetricGroup()
                        .addGroup(MetricConstants.GROUP_KEY, name)
                        .counter(MetricConstants.TRIGGERED))
            .collect(Collectors.toList());

    this.nextEvaluationTime =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("triggerManagerNextTriggerTime", Types.LONG));
    this.accumulatedChanges =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    "triggerManagerAccumulatedChange", TypeInformation.of(TableChange.class)));
    this.lastTriggerTimes =
        getRuntimeContext()
            .getListState(new ListStateDescriptor<>("triggerManagerLastTriggerTime", Types.LONG));
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // Do nothing
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    LOG.info("Initializing state restored: {}, clearLocks: {}", context.isRestored(), clearLocks);
    this.lock = lockFactory.createLock();
    this.recoveryLock = lockFactory.createRecoveryLock();
    if (context.isRestored()) {
      isCleanUp = true;
    } else if (clearLocks) {
      // Remove old lock if we are not restoring the job
      lock.unlock();
    }
  }

  @Override
  public void processElement(TableChange change, Context ctx, Collector<Trigger> out)
      throws Exception {
    long current = ctx.timerService().currentProcessingTime();
    Long nextTime = nextEvaluationTime.value();
    init(out, ctx.timerService(), current);

    // Add the new changes to the already accumulated ones
    List<TableChange> accumulated = Lists.newArrayList(accumulatedChanges.get());
    accumulated.forEach(tableChange -> tableChange.merge(change));
    accumulatedChanges.update(accumulated);

    if (nextTime == null) {
      checkAndFire(current, ctx.timerService(), out);
    } else {
      LOG.info(
          "Trigger manager rate limiter triggered current: {}, next: {}, accumulated changes: {}",
          current,
          nextTime,
          accumulated);
      rateLimiterTriggeredCounter.inc();
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Trigger> out) throws Exception {
    init(out, ctx.timerService(), timestamp);
    nextEvaluationTime.clear();
    checkAndFire(ctx.timerService().currentProcessingTime(), ctx.timerService(), out);
  }

  private void checkAndFire(long current, TimerService timerService, Collector<Trigger> out)
      throws Exception {
    if (isCleanUp) {
      if (recoveryLock.isHeld()) {
        LOG.debug("The cleanup lock is still held at {}", current);
        schedule(timerService, current + lockCheckDelayMs);
        return;
      } else {
        LOG.info("The cleanup is finished at {}", current);
        isCleanUp = false;
      }
    }

    List<TableChange> changes = Lists.newArrayList(accumulatedChanges.get());
    List<Long> times = Lists.newArrayList(lastTriggerTimes.get());
    Integer taskToStart = nextTrigger(evaluators, changes, times, current, startsFrom);
    if (taskToStart == null) {
      // Nothing to execute
      if (startsFrom == 0) {
        nothingToTriggerCounter.inc();
        LOG.debug("Nothing to execute at {} for collected: {}", current, changes);
      }

      startsFrom = 0;
      return;
    }

    if (lock.tryLock()) {
      TableChange change = changes.get(taskToStart);
      SerializableTable table =
          (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable());
      out.collect(Trigger.create(current, table, taskToStart));
      LOG.debug("Fired event with time: {}, collected: {} for {}", current, change, table.name());
      triggerCounters.get(taskToStart).inc();
      changes.set(taskToStart, TableChange.empty());
      accumulatedChanges.update(changes);
      schedule(timerService, current + minFireDelayMs);
      startsFrom = taskToStart + 1;
    } else {
      // The lock is already held by someone
      LOG.info("Delaying task on failed lock check: {}", current);

      startsFrom = taskToStart;
      concurrentRunTriggeredCounter.inc();
      schedule(timerService, current + lockCheckDelayMs);
    }

    timerService.registerProcessingTimeTimer(nextEvaluationTime.value());
  }

  private void schedule(TimerService timerService, long time) throws IOException {
    nextEvaluationTime.update(time);
    timerService.registerProcessingTimeTimer(time);
  }

  private static Integer nextTrigger(
      List<TriggerEvaluator> evaluators,
      List<TableChange> changes,
      List<Long> lastTriggerTimes,
      long currentTime,
      int startPos) {
    int normalizedStartingPos = startPos % evaluators.size();
    int current = normalizedStartingPos;
    do {
      if (evaluators
          .get(current)
          .check(changes.get(current), lastTriggerTimes.get(current), currentTime)) {
        return current;
      }

      current = (current + 1) % evaluators.size();
    } while (current != normalizedStartingPos);

    return null;
  }

  private void init(Collector<Trigger> out, TimerService timerService, long current)
      throws Exception {
    if (!inited) {
      // Initialize with empty changes and current timestamp
      if (!accumulatedChanges.get().iterator().hasNext()) {
        List<TableChange> changes = Lists.newArrayListWithCapacity(evaluators.size());
        List<Long> triggerTimes = Lists.newArrayListWithCapacity(evaluators.size());
        for (int i = 0; i < evaluators.size(); ++i) {
          changes.add(TableChange.empty());
          triggerTimes.add(current);
        }

        accumulatedChanges.update(changes);
        lastTriggerTimes.update(triggerTimes);
      }

      if (isCleanUp) {
        // When the job state is restored, there could be ongoing tasks.
        // To prevent collision with the new triggers the following is done:
        //  - add a cleanup lock
        //  - fire a clean-up trigger
        // This ensures that the tasks of the previous trigger are executed, and the lock is removed
        // in the end.
        recoveryLock.tryLock();
        out.collect(Trigger.cleanUp(current));
        schedule(timerService, current + minFireDelayMs);
      }

      inited = true;
    }
  }
}
