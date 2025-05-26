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
import java.util.stream.Collectors;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.flink.maintenance.api.FlinkMaintenanceConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class LockConfig {

  public static final String PREFIX = FlinkMaintenanceConfig.PREFIX + "lock.";
  public static final String LOCK_TYPE = PREFIX + "type";
  public static final String LOCK_ID = PREFIX + "lock-id";

  public static class JdbcLockConfig {

    public static final String JDBC = "jdbc";
    public static final String JDBC_URI = JDBC + ".uri";
    public static final String JDBC_INIT_LOCK_TABLE = JDBC + ".init-lock-table";
  }

  public static class ZkLockConfig {
    public static final String ZK = "zookeeper";
    public static final String ZK_URI = ZK + ".uri";
    public static final String ZK_SESSION_TIMEOUT_MS = ZK + ".session-timeout-ms";
    public static final String ZK_CONNECTION_TIMEOUT_MS = ZK + ".connection-timeout-ms";
    public static final String ZK_BASE_SLEEP_MS = ZK + ".base-sleep-ms";
    public static final String ZK_MAX_RETRIES = ZK + ".max-retries";

    public static final int ZK_SESSION_TIMEOUT_MS_DEFAULT = 60000;
    public static final int ZK_CONNECTION_TIMEOUT_MS_DEFAULT = 15000;
    public static final int ZK_BASE_SLEEP_MS_DEFAULT = 3000;
    public static final int ZK_MAX_RETRIES_DEFAULT = 3;
  }

  private final Map<String, String> writeProperties;
  private final Map<String, String> setProperties;

  public LockConfig(Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.writeProperties = writeOptions;
    this.setProperties = readableConfig.toMap();
  }

  public String lockType() {
    return stringValue(LOCK_TYPE, null);
  }

  public String lockId(String defaultValue) {
    return stringValue(LOCK_ID, defaultValue);
  }

  public String stringValue(String key) {
    return stringValue(key, null);
  }

  public String stringValue(String key, String defaultValue) {
    if (writeProperties.containsKey(key)) {
      return writeProperties.get(key);
    } else if (setProperties.containsKey(key)) {
      return setProperties.get(key);
    }
    return defaultValue;
  }

  public Map<String, String> properties() {
    Map<String, String> mergeConfig = Maps.newHashMap();
    mergeConfig.putAll(setProperties);
    mergeConfig.putAll(writeProperties);
    return mergeConfig.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(PREFIX))
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().substring(PREFIX.length()),
                Map.Entry::getValue,
                (existing, replacement) -> existing,
                Maps::newHashMap));
  }
}
