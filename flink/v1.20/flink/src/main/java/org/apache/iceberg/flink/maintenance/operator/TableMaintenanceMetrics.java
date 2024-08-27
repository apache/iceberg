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

public class TableMaintenanceMetrics {
  public static final String GROUP_KEY = "maintenanceTask";
  public static final String GROUP_VALUE_DEFAULT = "maintenanceTask";

  // TriggerManager metrics
  public static final String RATE_LIMITER_TRIGGERED = "rateLimiterTriggered";
  public static final String CONCURRENT_RUN_THROTTLED = "concurrentRunThrottled";
  public static final String TRIGGERED = "triggered";
  public static final String NOTHING_TO_TRIGGER = "nothingToTrigger";

  // LockRemover metrics
  public static final String SUCCEEDED_TASK_COUNTER = "succeededTasks";
  public static final String FAILED_TASK_COUNTER = "failedTasks";
  public static final String LAST_RUN_DURATION_MS = "lastRunDurationMs";

  private TableMaintenanceMetrics() {
    // do not instantiate
  }
}
