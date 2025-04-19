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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

public class ZkLockFactoryBuilder implements BaseLockFactoryBuilder {

  public static final String ZK_LOCK = "zookeeper";
  public static final String ZK_URI = ZK_LOCK + ".uri";

  public static final String ZK_SESSION_TIMEOUT_MS = ZK_LOCK + ".session-timeout-ms";
  public static final int ZK_SESSION_TIMEOUT_MS_DEFAULT = 60000;

  public static final String ZK_CONNECTION_TIMEOUT_MS = ZK_LOCK + ".connection-timeout-ms";
  public static final int ZK_CONNECTION_TIMEOUT_MS_DEFAULT = 15000;

  private static final String ZK_MAX_RETRIES = ZK_LOCK + ".max-retries";
  private static final int ZK_MAX_RETRIES_DEFAULT = 3;

  private static final String ZK_BASE_SLEEP_MS = ZK_LOCK + ".base-sleep-ms";
  private static final int ZK_BASE_SLEEP_MS_DEFAULT = 3000;

  @Override
  public TriggerLockFactory build(Map<String, String> config) {
    Preconditions.checkArgument(
        config.containsKey(ZK_URI), "Zk lock requires %s parameter", CONFIG_PREFIX + ZK_URI);
    Preconditions.checkArgument(
        config.containsKey(BaseLockFactoryBuilder.LOCK_ID),
        "Zk lock requires %s parameter",
        CONFIG_PREFIX + BaseLockFactoryBuilder.LOCK_ID);

    int sessionTimeoutMs = sessionTimeoutMs(config);
    int connectionTimeoutMs = connectionTimeoutMs(config);
    int baseSleepTimeMs = baseSleepTimeMs(config);
    int maxRetries = maxRetries(config);

    return new ZkLockFactory(
        config.get(ZK_URI),
        config.get(BaseLockFactoryBuilder.LOCK_ID),
        sessionTimeoutMs,
        connectionTimeoutMs,
        baseSleepTimeMs,
        maxRetries);
  }

  private int sessionTimeoutMs(Map<String, String> config) {
    int value =
        PropertyUtil.propertyAsInt(config, ZK_SESSION_TIMEOUT_MS, ZK_SESSION_TIMEOUT_MS_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", ZK_SESSION_TIMEOUT_MS, value);
    return value;
  }

  private int connectionTimeoutMs(Map<String, String> config) {
    int value =
        PropertyUtil.propertyAsInt(
            config, ZK_CONNECTION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", ZK_CONNECTION_TIMEOUT_MS, value);
    return value;
  }

  private int baseSleepTimeMs(Map<String, String> config) {
    int value = PropertyUtil.propertyAsInt(config, ZK_BASE_SLEEP_MS, ZK_BASE_SLEEP_MS_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", ZK_BASE_SLEEP_MS, value);
    return value;
  }

  private int maxRetries(Map<String, String> config) {
    int value = PropertyUtil.propertyAsInt(config, ZK_MAX_RETRIES, ZK_MAX_RETRIES_DEFAULT);
    Preconditions.checkArgument(
        value >= 0, "'%s' is set to %s but must be >= 0", ZK_MAX_RETRIES, value);
    return value;
  }
}
