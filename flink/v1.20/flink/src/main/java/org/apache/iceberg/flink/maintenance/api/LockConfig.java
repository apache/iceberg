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
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfParser;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class LockConfig {

  public static final String PREFIX = FlinkMaintenanceConfig.PREFIX + "lock.";

  public static final ConfigOption<String> LOCK_TYPE_OPTION =
      ConfigOptions.key(PREFIX + "type").stringType().defaultValue(StringUtils.EMPTY);

  public static final ConfigOption<String> LOCK_ID_OPTION =
      ConfigOptions.key(PREFIX + "lock-id").stringType().defaultValue(StringUtils.EMPTY);

  public static class JdbcLockConfig {

    public static final String JDBC = "jdbc";

    public static final ConfigOption<String> JDBC_URI_OPTION =
        ConfigOptions.key(PREFIX + JDBC + ".uri").stringType().defaultValue(StringUtils.EMPTY);

    public static final ConfigOption<String> JDBC_INIT_LOCK_TABLE_OPTION =
        ConfigOptions.key(PREFIX + JDBC + ".init-lock-table")
            .stringType()
            .defaultValue(Boolean.FALSE.toString());
  }

  public static class ZkLockConfig {
    public static final String ZK = "zookeeper";

    public static final ConfigOption<String> ZK_URI_OPTION =
        ConfigOptions.key(PREFIX + ZK + ".uri").stringType().defaultValue(StringUtils.EMPTY);

    public static final ConfigOption<Integer> ZK_SESSION_TIMEOUT_MS_OPTION =
        ConfigOptions.key(PREFIX + ZK + ".session-timeout-ms").intType().defaultValue(60000);

    public static final ConfigOption<Integer> ZK_CONNECTION_TIMEOUT_MS_OPTION =
        ConfigOptions.key(PREFIX + ZK + ".connection-timeout-ms").intType().defaultValue(15000);

    public static final ConfigOption<Integer> ZK_BASE_SLEEP_MS_OPTION =
        ConfigOptions.key(PREFIX + ZK + ".base-sleep-ms").intType().defaultValue(3000);

    public static final ConfigOption<Integer> ZK_MAX_RETRIES_OPTION =
        ConfigOptions.key(PREFIX + ZK + ".max-retries").intType().defaultValue(3);
  }

  private final FlinkConfParser confParser;
  private final Map<String, String> writeProperties;
  private final Map<String, String> setProperties;

  public LockConfig(Table table, Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.writeProperties = writeOptions;
    this.setProperties = readableConfig.toMap();
    this.confParser = new FlinkConfParser(table, writeOptions, readableConfig);
  }

  public String lockType() {
    return confParser
        .stringConf()
        .option(LOCK_TYPE_OPTION.key())
        .flinkConfig(LOCK_TYPE_OPTION)
        .defaultValue(LOCK_TYPE_OPTION.defaultValue())
        .parse();
  }

  public String lockId(String defaultValue) {
    String lockId =
        confParser
            .stringConf()
            .option(LOCK_ID_OPTION.key())
            .flinkConfig(LOCK_ID_OPTION)
            .defaultValue(LOCK_ID_OPTION.defaultValue())
            .parse();
    if (StringUtils.isBlank(lockId)) {
      return defaultValue;
    }

    return lockId;
  }

  public String jdbcUri() {
    return confParser
        .stringConf()
        .option(JdbcLockConfig.JDBC_URI_OPTION.key())
        .flinkConfig(JdbcLockConfig.JDBC_URI_OPTION)
        .defaultValue(JdbcLockConfig.JDBC_URI_OPTION.defaultValue())
        .parse();
  }

  public String jdbcInitTable() {
    return confParser
        .stringConf()
        .option(JdbcLockConfig.JDBC_INIT_LOCK_TABLE_OPTION.key())
        .flinkConfig(JdbcLockConfig.JDBC_INIT_LOCK_TABLE_OPTION)
        .defaultValue(JdbcLockConfig.JDBC_INIT_LOCK_TABLE_OPTION.defaultValue())
        .parse();
  }

  public String zkUri() {
    return confParser
        .stringConf()
        .option(ZkLockConfig.ZK_URI_OPTION.key())
        .flinkConfig(ZkLockConfig.ZK_URI_OPTION)
        .defaultValue(ZkLockConfig.ZK_URI_OPTION.defaultValue())
        .parse();
  }

  public int zkSessionTimeoutMs() {
    return confParser
        .intConf()
        .option(ZkLockConfig.ZK_SESSION_TIMEOUT_MS_OPTION.key())
        .flinkConfig(ZkLockConfig.ZK_SESSION_TIMEOUT_MS_OPTION)
        .defaultValue(ZkLockConfig.ZK_SESSION_TIMEOUT_MS_OPTION.defaultValue())
        .parse();
  }

  public int zkConnectionTimeoutMs() {
    return confParser
        .intConf()
        .option(ZkLockConfig.ZK_CONNECTION_TIMEOUT_MS_OPTION.key())
        .flinkConfig(ZkLockConfig.ZK_CONNECTION_TIMEOUT_MS_OPTION)
        .defaultValue(ZkLockConfig.ZK_CONNECTION_TIMEOUT_MS_OPTION.defaultValue())
        .parse();
  }

  public int zkBaseSleepMs() {
    return confParser
        .intConf()
        .option(ZkLockConfig.ZK_BASE_SLEEP_MS_OPTION.key())
        .flinkConfig(ZkLockConfig.ZK_BASE_SLEEP_MS_OPTION)
        .defaultValue(ZkLockConfig.ZK_BASE_SLEEP_MS_OPTION.defaultValue())
        .parse();
  }

  public int zkMaxRetries() {
    return confParser
        .intConf()
        .option(ZkLockConfig.ZK_MAX_RETRIES_OPTION.key())
        .flinkConfig(ZkLockConfig.ZK_MAX_RETRIES_OPTION)
        .defaultValue(ZkLockConfig.ZK_MAX_RETRIES_OPTION.defaultValue())
        .parse();
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
