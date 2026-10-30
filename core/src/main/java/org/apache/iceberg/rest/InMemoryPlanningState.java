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
package org.apache.iceberg.rest;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.exceptions.NoSuchPlanIdException;
import org.apache.iceberg.exceptions.NoSuchPlanTaskException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;

/**
 * Encapsulates shared state in-memory for server-side scan planning.
 *
 * <p>This class maintains the state needed for pagination via plan tasks, mappings between plan
 * tasks and file scan tasks, and an executor service for asynchronous planning.
 */
class InMemoryPlanningState {
  private static volatile InMemoryPlanningState instance;
  private static final String INITIAL_TASK_SEQUENCE_NUMBER = "0";

  private final Map<String, List<FileScanTask>> planTaskToFileScanTasks;
  private final Map<String, String> planTaskToNext;
  private final Map<String, PlanStatus> asyncPlanningStates;

  private InMemoryPlanningState() {
    this.planTaskToFileScanTasks = Maps.newConcurrentMap();
    this.planTaskToNext = Maps.newConcurrentMap();
    this.asyncPlanningStates = Maps.newConcurrentMap();
  }

  static InMemoryPlanningState getInstance() {
    if (instance == null) {
      synchronized (InMemoryPlanningState.class) {
        if (instance == null) {
          instance = new InMemoryPlanningState();
        }
      }
    }
    return instance;
  }

  void addPlanTask(String planTaskKey, List<FileScanTask> tasks) {
    planTaskToFileScanTasks.put(planTaskKey, tasks);
  }

  void addNextPlanTask(String currentTask, String nextTask) {
    planTaskToNext.put(currentTask, nextTask);
  }

  void addAsyncPlan(String plan) {
    PlanStatus existingStatus = asyncPlanningStates.get(plan);
    Preconditions.checkArgument(
        existingStatus == null, "Plan %s already exists with status %s", plan, existingStatus);
    asyncPlanningStates.put(plan, PlanStatus.SUBMITTED);
  }

  PlanStatus asyncPlanStatus(String plan) {
    PlanStatus existingStatus = asyncPlanningStates.get(plan);
    if (existingStatus == null) {
      throw new NoSuchPlanIdException("Cannot find plan with id %s", plan);
    }

    return asyncPlanningStates.get(plan);
  }

  void markAsyncPlanAsComplete(String plan) {
    PlanStatus existingStatus = asyncPlanningStates.get(plan);
    Preconditions.checkArgument(existingStatus != null, "Cannot find plan %s", plan);
    Preconditions.checkArgument(
        existingStatus == PlanStatus.SUBMITTED,
        "Cannot mark plan %s as completed as it is %s",
        plan,
        existingStatus);
    asyncPlanningStates.put(plan, PlanStatus.COMPLETED);
  }

  void markAsyncPlanFailed(String plan) {
    PlanStatus existingStatus = asyncPlanningStates.get(plan);
    Preconditions.checkArgument(existingStatus != null, "Cannot find plan %s", plan);
    Preconditions.checkArgument(
        existingStatus == PlanStatus.SUBMITTED,
        "Cannot mark plan %s as completed as it is %s",
        plan,
        existingStatus);
    asyncPlanningStates.put(plan, PlanStatus.FAILED);
  }

  List<FileScanTask> fileScanTasksForPlanTask(String planTaskKey) {
    List<FileScanTask> tasks = planTaskToFileScanTasks.get(planTaskKey);
    if (tasks == null) {
      throw new NoSuchPlanTaskException("Could not find tasks for plan task %s", planTaskKey);
    }

    return tasks;
  }

  List<String> nextPlanTask(String planTaskKey) {
    String nextPlanTask = planTaskToNext.get(planTaskKey);
    if (nextPlanTask != null) {
      return ImmutableList.of(nextPlanTask);
    }

    return ImmutableList.of();
  }

  /**
   * Retrieves the initial set of file scan tasks for a plan. PlanIDs are assumed to be separated
   * with hyphens where the last component indicates the sequencing of plan IDs.
   *
   * @param planId the plan identifier
   * @return pair of initial file scan tasks and the plan task key
   * @throws NoSuchPlanIdException if the plan ID is not found
   */
  Pair<List<FileScanTask>, String> initialScanTasksFor(String planId) {
    Set<Map.Entry<String, List<FileScanTask>>> matchingEntries =
        planTaskToFileScanTasks.entrySet().stream()
            .filter(
                entry -> {
                  String key = entry.getKey();
                  if (!key.contains(planId)) {
                    return false;
                  }
                  List<String> keyParts = Splitter.on("-").splitToList(key);
                  if (keyParts.isEmpty()) {
                    return false;
                  }

                  return INITIAL_TASK_SEQUENCE_NUMBER.equals(keyParts.get(keyParts.size() - 1));
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            .entrySet();
    if (matchingEntries.isEmpty()) {
      throw new NoSuchPlanIdException("Could not find plan ID %s", planId);
    }

    Map.Entry<String, List<FileScanTask>> initialEntry = Iterables.getOnlyElement(matchingEntries);
    return Pair.of(initialEntry.getValue(), initialEntry.getKey());
  }

  void cancelPlan(String planId) {
    planTaskToNext.entrySet().removeIf(entry -> entry.getKey().contains(planId));
    planTaskToFileScanTasks.entrySet().removeIf(entry -> entry.getKey().contains(planId));
    // Clear the ongoing plan status in case the planID is an async one.
    if (asyncPlanningStates.containsKey(planId)) {
      PlanStatus existingStatus = asyncPlanningStates.get(planId);
      // No need to fail cancellation if the plan could not be found
      if (existingStatus == null) {
        return;
      }

      // No need to fail cancellation if the plan already terminated
      if (existingStatus == PlanStatus.SUBMITTED) {
        asyncPlanningStates.put(planId, PlanStatus.CANCELLED);
      }
    }
  }

  void clear() {
    planTaskToFileScanTasks.clear();
    planTaskToNext.clear();
    asyncPlanningStates.clear();
  }
}
