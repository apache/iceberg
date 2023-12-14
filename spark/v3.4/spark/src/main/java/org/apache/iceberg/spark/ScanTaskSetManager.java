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
package org.apache.iceberg.spark;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;

public class ScanTaskSetManager {

  private static final ScanTaskSetManager INSTANCE = new ScanTaskSetManager();

  private final Map<Pair<String, String>, List<? extends ScanTask>> tasksMap =
      Maps.newConcurrentMap();

  private ScanTaskSetManager() {}

  public static ScanTaskSetManager get() {
    return INSTANCE;
  }

  public <T extends ScanTask> void stageTasks(Table table, String setId, List<T> tasks) {
    Preconditions.checkArgument(
        tasks != null && !tasks.isEmpty(), "Cannot stage null or empty tasks");
    Pair<String, String> id = toId(table, setId);
    tasksMap.put(id, tasks);
  }

  @SuppressWarnings("unchecked")
  public <T extends ScanTask> List<T> fetchTasks(Table table, String setId) {
    Pair<String, String> id = toId(table, setId);
    return (List<T>) tasksMap.get(id);
  }

  @SuppressWarnings("unchecked")
  public <T extends ScanTask> List<T> removeTasks(Table table, String setId) {
    Pair<String, String> id = toId(table, setId);
    return (List<T>) tasksMap.remove(id);
  }

  public Set<String> fetchSetIds(Table table) {
    return tasksMap.keySet().stream()
        .filter(e -> e.first().equals(Spark3Util.tableUUID(table)))
        .map(Pair::second)
        .collect(Collectors.toSet());
  }

  private Pair<String, String> toId(Table table, String setId) {
    return Pair.of(Spark3Util.tableUUID(table), setId);
  }
}
