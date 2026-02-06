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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;

public class TableMaintenanceMetrics {
  public static final String GROUP_KEY = "maintenance";
  public static final String TASK_NAME_KEY = "taskName";
  public static final String TASK_INDEX_KEY = "taskIndex";
  public static final String TABLE_NAME_KEY = "tableName";

  // Operator error counter
  public static final String ERROR_COUNTER = "error";

  // TriggerManager metrics
  public static final String RATE_LIMITER_TRIGGERED = "rateLimiterTriggered";
  public static final String CONCURRENT_RUN_THROTTLED = "concurrentRunThrottled";
  public static final String TRIGGERED = "triggered";
  public static final String NOTHING_TO_TRIGGER = "nothingToTrigger";

  // LockRemover metrics
  public static final String SUCCEEDED_TASK_COUNTER = "succeededTasks";
  public static final String FAILED_TASK_COUNTER = "failedTasks";
  public static final String LAST_RUN_DURATION_MS = "lastRunDurationMs";

  // DeleteFiles metrics
  public static final String DELETE_FILE_FAILED_COUNTER = "deleteFailed";
  public static final String DELETE_FILE_SUCCEEDED_COUNTER = "deleteSucceeded";

  // DataFileUpdater metrics
  public static final String ADDED_DATA_FILE_NUM_METRIC = "addedDataFileNum";
  public static final String ADDED_DATA_FILE_SIZE_METRIC = "addedDataFileSize";
  public static final String REMOVED_DATA_FILE_NUM_METRIC = "removedDataFileNum";
  public static final String REMOVED_DATA_FILE_SIZE_METRIC = "removedDataFileSize";

  static MetricGroup groupFor(
      RuntimeContext context, String tableName, String taskName, int taskIndex) {
    return groupFor(groupFor(context, tableName), taskName, taskIndex);
  }

  static MetricGroup groupFor(RuntimeContext context, String tableName) {
    return context
        .getMetricGroup()
        .addGroup(TableMaintenanceMetrics.GROUP_KEY)
        .addGroup(TableMaintenanceMetrics.TABLE_NAME_KEY, tableName);
  }

  static MetricGroup groupFor(MetricGroup mainGroup, String taskName, int taskIndex) {
    return mainGroup
        .addGroup(TableMaintenanceMetrics.TASK_NAME_KEY, taskName)
        .addGroup(TableMaintenanceMetrics.TASK_INDEX_KEY, String.valueOf(taskIndex));
  }

  private TableMaintenanceMetrics() {
    // do not instantiate
  }
}
