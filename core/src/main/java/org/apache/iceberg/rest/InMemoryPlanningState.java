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

  private final Map<String, List<FileScanTask>> planTaskToFileScanTasks;
  private final Map<String, String> planTaskToNext;

  private InMemoryPlanningState() {
    this.planTaskToFileScanTasks = Maps.newConcurrentMap();
    this.planTaskToNext = Maps.newConcurrentMap();
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

                  return "0".equals(keyParts.get(keyParts.size() - 1));
                })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            .entrySet();
    if (matchingEntries.isEmpty()) {
      throw new NoSuchPlanIdException("Could not find plan ID %s", planId);
    }

    Map.Entry<String, List<FileScanTask>> initialEntry = Iterables.getOnlyElement(matchingEntries);
    return Pair.of(initialEntry.getValue(), initialEntry.getKey());
  }

  void removePlan(String planId) {
    planTaskToNext.entrySet().removeIf(entry -> entry.getKey().contains(planId));
    planTaskToFileScanTasks.entrySet().removeIf(entry -> entry.getKey().contains(planId));
  }

  void clear() {
    planTaskToFileScanTasks.clear();
    planTaskToNext.clear();
  }
}
