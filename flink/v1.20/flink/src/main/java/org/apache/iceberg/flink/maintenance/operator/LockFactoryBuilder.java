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

import java.util.Map;
import java.util.Optional;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.LockConfig;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.flink.maintenance.api.ZkLockFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

@Internal
public class LockFactoryBuilder {

  private LockFactoryBuilder() {}

  public static TriggerLockFactory build(Map<String, String> config, String tableName) {
    Map<String, String> lockProperties = filterAndRemovePrefix(config, LockConfig.CONFIG_PREFIX);

    Preconditions.checkArgument(
        lockProperties.containsKey(LockConfig.LOCK_TYPE),
        "Configuration must contain key: %s",
        LockConfig.CONFIG_PREFIX + LockConfig.LOCK_TYPE);

    // Set lock id to catalog.db.table if not set
    lockProperties.putIfAbsent(LockConfig.LOCK_ID, tableName);

    Preconditions.checkNotNull(config, "Config cannot be null");
    String lockType = lockProperties.get(LockConfig.LOCK_TYPE);

    switch (lockType) {
      case LockConfig.JDBC:
        return createJdbcLockFactory(lockProperties);

      case LockConfig.ZK:
        return createZkLockFactory(lockProperties);

      default:
        throw new IllegalArgumentException(String.format("Unsupported lock type: %s ", lockType));
    }
  }

  private static TriggerLockFactory createJdbcLockFactory(Map<String, String> config) {
    Preconditions.checkArgument(
        config.containsKey(LockConfig.JDBC_URI),
        "JDBC lock requires %s parameter",
        LockConfig.CONFIG_PREFIX + LockConfig.JDBC_URI);
    Preconditions.checkArgument(
        config.containsKey(LockConfig.LOCK_ID),
        "JDBC lock requires %s parameter",
        LockConfig.CONFIG_PREFIX + LockConfig.LOCK_ID);

    Optional.ofNullable(config.get(LockConfig.JDBC_INIT_LOCK_TABLE))
        .ifPresent(value -> config.put(JdbcLockFactory.INIT_LOCK_TABLES_PROPERTY, value));

    return new JdbcLockFactory(
        config.get(LockConfig.JDBC_URI), config.get(LockConfig.LOCK_ID), config);
  }

  private static TriggerLockFactory createZkLockFactory(Map<String, String> config) {
    Preconditions.checkArgument(
        config.containsKey(LockConfig.ZK_URI),
        "Zk lock requires %s parameter",
        LockConfig.CONFIG_PREFIX + LockConfig.ZK_URI);
    Preconditions.checkArgument(
        config.containsKey(LockConfig.LOCK_ID),
        "Zk lock requires %s parameter",
        LockConfig.CONFIG_PREFIX + LockConfig.LOCK_ID);

    return new ZkLockFactory(
        config.get(LockConfig.ZK_URI),
        config.get(LockConfig.LOCK_ID),
        PropertyUtil.propertyAsInt(
            config, LockConfig.ZK_SESSION_TIMEOUT_MS, LockConfig.ZK_SESSION_TIMEOUT_MS_DEFAULT),
        PropertyUtil.propertyAsInt(
            config,
            LockConfig.ZK_CONNECTION_TIMEOUT_MS,
            LockConfig.ZK_CONNECTION_TIMEOUT_MS_DEFAULT),
        PropertyUtil.propertyAsInt(
            config, LockConfig.ZK_BASE_SLEEP_MS, LockConfig.ZK_BASE_SLEEP_MS_DEFAULT),
        PropertyUtil.propertyAsInt(
            config, LockConfig.ZK_MAX_RETRIES, LockConfig.ZK_MAX_RETRIES_DEFAULT));
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
