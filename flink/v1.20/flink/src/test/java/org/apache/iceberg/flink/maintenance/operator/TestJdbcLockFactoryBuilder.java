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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.flink.maintenance.api.LockConfig;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

class TestJdbcLockFactoryBuilder {
  private static final String TABLE_NAME = "catalog.db.table";

  @Test
  void testBuildWithMissingJdbcUri() {
    Map<String, String> config = Maps.newHashMap();
    config.put(LockConfig.CONFIG_PREFIX + LockConfig.LOCK_TYPE, LockConfig.JDBC);

    assertThatThrownBy(() -> LockFactoryBuilder.build(config, TABLE_NAME))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "JDBC lock requires %s parameter", LockConfig.CONFIG_PREFIX + LockConfig.JDBC_URI));
  }

  @Test
  void testBuildSuccessfully() {
    Map<String, String> config = Maps.newHashMap();
    config.put(LockConfig.CONFIG_PREFIX + LockConfig.LOCK_TYPE, LockConfig.JDBC);
    config.put(LockConfig.CONFIG_PREFIX + LockConfig.JDBC_URI, "jdbc:sqlite:file::memory:?ic");
    config.put(LockConfig.CONFIG_PREFIX + LockConfig.LOCK_ID, "test-lock-id");

    TriggerLockFactory factory = LockFactoryBuilder.build(config, TABLE_NAME);
    assertThat(factory).isNotNull();
  }
}
