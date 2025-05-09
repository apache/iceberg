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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class LockFactoryCreator {

  private static final Map<String, BaseLockFactoryCreator> CREATORS =
      ImmutableMap.of(JdbcLockFactoryCreator.JDBC_LOCK, new JdbcLockFactoryCreator());

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
        config.containsKey(BaseLockFactoryCreator.LOCK_TYPE_KEY),
        "Configuration must contain key: %s",
        BaseLockFactoryCreator.LOCK_TYPE_KEY);

    String lockType = config.get(BaseLockFactoryCreator.LOCK_TYPE_KEY).toLowerCase(Locale.ROOT);
    BaseLockFactoryCreator creator = CREATORS.get(lockType);

    if (creator == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported lock type: %s. Support lock type: %s ", lockType, CREATORS.keySet()));
    }

    return creator.create(config);
  }
}
