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
package org.apache.iceberg.flink.maintenance.api;

import java.util.Map;
import org.apache.iceberg.util.PropertyUtil;

public class TableMaintenanceConfig {
  private static final String CONFIG_PREFIX = "flink-maintenance.";

  public static final String RATE_LIMIT = CONFIG_PREFIX + "rate-limit-seconds";

  public static final String SLOT_SHARING_GROUP = CONFIG_PREFIX + "slot-sharing-group";

  public static final String LOCK_CHECK_DELAY = CONFIG_PREFIX + "lock-check-delay-seconds";

  public static final String MAX_READ_BACK = CONFIG_PREFIX + "max-read-back";

  public static final String PARALLELISM = CONFIG_PREFIX + "parallelism";

  private final Map<String, String> properties;

  public TableMaintenanceConfig(Map<String, String> properties) {
    this.properties = properties;
  }

  public Long getRateLimit() {
    return PropertyUtil.propertyAsNullableLong(properties, RATE_LIMIT);
  }

  public Integer getParallelism() {
    return PropertyUtil.propertyAsNullableInt(properties, PARALLELISM);
  }

  public Long getLockCheckDelay() {
    return PropertyUtil.propertyAsNullableLong(properties, LOCK_CHECK_DELAY);
  }

  public Integer getMaxReadBack() {
    return PropertyUtil.propertyAsNullableInt(properties, MAX_READ_BACK);
  }

  public String getSlotSharingGroup() {
    return PropertyUtil.propertyAsString(properties, SLOT_SHARING_GROUP, null);
  }
}
