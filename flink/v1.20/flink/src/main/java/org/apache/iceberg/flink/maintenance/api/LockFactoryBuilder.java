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

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class LockFactoryBuilder {

  private static final Map<String, BaseLockFactoryBuilder> BUILDERS =
      ImmutableMap.of(
          JdbcLockFactoryBuilder.JDBC_LOCK,
          new JdbcLockFactoryBuilder(),
          ZkLockFactoryBuilder.ZK_LOCK,
          new ZkLockFactoryBuilder());

  private LockFactoryBuilder() {}

  /**
   * Creates appropriate lock factory based on configuration.
   *
   * @param config Configuration map containing lock type and required parameters
   * @return Created lock factory instance
   * @throws IllegalArgumentException if configuration is invalid
   */
  public static TriggerLockFactory build(Map<String, String> config, Table table) {
    Map<String, String> lockProperties =
        filterAndRemovePrefix(config, BaseLockFactoryBuilder.CONFIG_PREFIX);

    Preconditions.checkArgument(
        lockProperties.containsKey(BaseLockFactoryBuilder.LOCK_TYPE_KEY),
        "Configuration must contain key: %s",
        BaseLockFactoryBuilder.CONFIG_PREFIX + BaseLockFactoryBuilder.LOCK_TYPE_KEY);

    // Set lock id to catalog.db.table if not set
    lockProperties.putIfAbsent(BaseLockFactoryBuilder.LOCK_ID, table.name());

    String lockType =
        lockProperties.get(BaseLockFactoryBuilder.LOCK_TYPE_KEY).toLowerCase(Locale.ROOT);
    BaseLockFactoryBuilder builder = BUILDERS.get(lockType);

    if (builder == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported lock type: %s. Support lock type: %s ", lockType, BUILDERS.keySet()));
    }

    return builder.build(lockProperties);
  }

  private static Map<String, String> filterAndRemovePrefix(
      Map<String, String> config, String prefix) {
    Map<String, String> newConfig = Maps.newHashMap();
    config.forEach(
        (key, value) -> {
          if (key.startsWith(prefix)) {
            newConfig.put(key.substring(prefix.length()), value);
          }
        });
    return newConfig;
  }
}
