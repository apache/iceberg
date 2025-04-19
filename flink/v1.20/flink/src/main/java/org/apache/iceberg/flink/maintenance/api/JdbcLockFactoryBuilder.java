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
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class JdbcLockFactoryBuilder implements BaseLockFactoryBuilder {

  public static final String JDBC_LOCK = "jdbc";
  public static final String JDBC_URI = JDBC_LOCK + ".uri";
  public static final String JDBC_INIT_LOCK_TABLE = JDBC_LOCK + ".init-lock-table";

  @Override
  public TriggerLockFactory build(Map<String, String> config) {
    Preconditions.checkArgument(
        config.containsKey(JDBC_URI), "JDBC lock requires %s parameter", CONFIG_PREFIX + JDBC_URI);
    Preconditions.checkArgument(
        config.containsKey(BaseLockFactoryBuilder.LOCK_ID),
        "JDBC lock requires %s parameter",
        CONFIG_PREFIX + BaseLockFactoryBuilder.LOCK_ID);

    Optional.ofNullable(config.get(JDBC_INIT_LOCK_TABLE))
        .ifPresent(value -> config.put(JdbcLockFactory.INIT_LOCK_TABLES_PROPERTY, value));

    return new JdbcLockFactory(
        config.get(JDBC_URI), config.get(BaseLockFactoryBuilder.LOCK_ID), config);
  }
}
