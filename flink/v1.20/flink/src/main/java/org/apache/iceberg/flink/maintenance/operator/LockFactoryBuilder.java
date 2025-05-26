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
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.flink.maintenance.api.ZkLockFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;

@Internal
public class LockFactoryBuilder {

  private LockFactoryBuilder() {}

  public static TriggerLockFactory build(LockConfig lockConfig, String tableName) {

    String lockType = lockConfig.lockType();

    Preconditions.checkArgument(
        StringUtils.isNotEmpty(lockType),
        "Configuration must contain key: %s",
        LockConfig.LOCK_TYPE);

    // Set lock id to catalog.db.table if not set
    switch (lockType) {
      case LockConfig.JdbcLockConfig.JDBC:
        return createJdbcLockFactory(lockConfig, tableName);

      case LockConfig.ZkLockConfig.ZK:
        return createZkLockFactory(lockConfig, tableName);

      default:
        throw new IllegalArgumentException(String.format("Unsupported lock type: %s ", lockType));
    }
  }

  private static TriggerLockFactory createJdbcLockFactory(LockConfig lockConfig, String tableName) {
    String jdbcUri = lockConfig.stringValue(LockConfig.PREFIX + LockConfig.JdbcLockConfig.JDBC_URI);
    String lockId = lockConfig.lockId(tableName);
    Map<String, String> properties = lockConfig.properties();
    Preconditions.checkArgument(
        StringUtils.isNotEmpty(jdbcUri),
        "JDBC lock requires %s parameter",
        LockConfig.PREFIX + LockConfig.JdbcLockConfig.JDBC_URI);

    Optional.ofNullable(properties.get(LockConfig.JdbcLockConfig.JDBC_INIT_LOCK_TABLE))
        .ifPresent(value -> properties.put(JdbcLockFactory.INIT_LOCK_TABLES_PROPERTY, value));

    return new JdbcLockFactory(jdbcUri, lockId, properties);
  }

  private static TriggerLockFactory createZkLockFactory(LockConfig lockConfig, String tableName) {
    String zkUri = lockConfig.stringValue(LockConfig.PREFIX + LockConfig.ZkLockConfig.ZK_URI);
    String lockId = lockConfig.lockId(tableName);
    Map<String, String> properties = lockConfig.properties();

    Preconditions.checkArgument(
        StringUtils.isNotEmpty(zkUri),
        "Zk lock requires %s parameter",
        LockConfig.PREFIX + LockConfig.ZkLockConfig.ZK_URI);

    return new ZkLockFactory(
        zkUri,
        lockId,
        PropertyUtil.propertyAsInt(
            properties,
            LockConfig.ZkLockConfig.ZK_SESSION_TIMEOUT_MS,
            LockConfig.ZkLockConfig.ZK_SESSION_TIMEOUT_MS_DEFAULT),
        PropertyUtil.propertyAsInt(
            properties,
            LockConfig.ZkLockConfig.ZK_CONNECTION_TIMEOUT_MS,
            LockConfig.ZkLockConfig.ZK_CONNECTION_TIMEOUT_MS_DEFAULT),
        PropertyUtil.propertyAsInt(
            properties,
            LockConfig.ZkLockConfig.ZK_BASE_SLEEP_MS,
            LockConfig.ZkLockConfig.ZK_BASE_SLEEP_MS_DEFAULT),
        PropertyUtil.propertyAsInt(
            properties,
            LockConfig.ZkLockConfig.ZK_MAX_RETRIES,
            LockConfig.ZkLockConfig.ZK_MAX_RETRIES_DEFAULT));
  }
}
