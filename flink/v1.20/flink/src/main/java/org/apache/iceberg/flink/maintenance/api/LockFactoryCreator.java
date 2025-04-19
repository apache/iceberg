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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class LockFactoryCreator {

  private static final String LOCK_PREFIX = "flink-maintenance.lock";
  public static final String LOCK_TYPE_KEY = LOCK_PREFIX + ".type";
  public static final String LOCK_ID = LOCK_PREFIX + ".lock-id";

  public static final String JDBC_LOCK = "jdbc";
  public static final String JDBC_URI = LOCK_PREFIX + "." + JDBC_LOCK + ".uri";

  private LockFactoryCreator() {}

  /**
   * Creates appropriate lock factory based on configuration.
   *
   * @param config Configuration map containing lock type and required parameters
   * @return Created lock factory instance
   * @throws IllegalArgumentException if configuration is invalid
   */
  public static TriggerLockFactory create(Map<String, String> config) {
    Preconditions.checkArgument(
        config.containsKey(LOCK_TYPE_KEY), "Configuration must contain key: %s", LOCK_TYPE_KEY);

    String lockType = config.get(LOCK_TYPE_KEY);
    switch (lockType.toLowerCase(Locale.ROOT)) {
      case JDBC_LOCK:
        return getJdbcLockFactory(config);
      default:
        throw new IllegalArgumentException(String.format("Unsupported lock type: %s. ", lockType));
    }
  }

  private static JdbcLockFactory getJdbcLockFactory(Map<String, String> config) {
    Preconditions.checkArgument(
        config.containsKey(JDBC_URI), "JDBC lock requires %s parameter", JDBC_URI);
    Preconditions.checkArgument(
        config.containsKey(LOCK_ID), "JDBC lock requires %s parameter", LOCK_ID);
    return new JdbcLockFactory(config.get(JDBC_URI), config.get(LOCK_ID), config);
  }
}
