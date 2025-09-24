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

import java.io.IOException;
import java.util.Map;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.LockConfig;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestLockFactoryBuilder extends OperatorTestBase {
  private static final String TABLE_NAME = "catalog.db.table";

  private TestingServer zkTestServer;
  private Table table;

  @BeforeEach
  void before() {
    this.table = createTable();
    try {
      zkTestServer = new TestingServer();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  public void after() throws IOException {
    if (zkTestServer != null) {
      zkTestServer.close();
    }
  }

  @Test
  void testJdbcBuildWithMissingJdbcUri() {
    Map<String, String> config = Maps.newHashMap();
    config.put(LockConfig.LOCK_TYPE_OPTION.key(), LockConfig.JdbcLockConfig.JDBC);
    LockConfig lockConfig = new LockConfig(table, config, new Configuration());

    assertThatThrownBy(() -> LockFactoryBuilder.build(lockConfig, TABLE_NAME))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "JDBC lock requires %s parameter",
                LockConfig.JdbcLockConfig.JDBC_URI_OPTION.key()));
  }

  @Test
  void testJdbcBuildSuccessfully() {
    Map<String, String> config = Maps.newHashMap();
    config.put(LockConfig.LOCK_TYPE_OPTION.key(), LockConfig.JdbcLockConfig.JDBC);
    config.put(LockConfig.JdbcLockConfig.JDBC_URI_OPTION.key(), "jdbc:sqlite:file::memory:?ic");
    config.put(LockConfig.LOCK_ID_OPTION.key(), "test-lock-id");
    LockConfig lockConfig = new LockConfig(table, config, new Configuration());

    TriggerLockFactory factory = LockFactoryBuilder.build(lockConfig, TABLE_NAME);
    assertThat(factory).isNotNull();
  }

  @Test
  void testZkBuildWithMissingUri() {
    Map<String, String> config = Maps.newHashMap();
    config.put(LockConfig.LOCK_TYPE_OPTION.key(), LockConfig.ZkLockConfig.ZK);
    LockConfig lockConfig = new LockConfig(table, config, new Configuration());

    assertThatThrownBy(() -> LockFactoryBuilder.build(lockConfig, TABLE_NAME))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "Zk lock requires %s parameter", LockConfig.ZkLockConfig.ZK_URI_OPTION.key()));
  }

  @Test
  void testZkBuildSuccessfully() {
    Map<String, String> config = Maps.newHashMap();
    config.put(LockConfig.LOCK_TYPE_OPTION.key(), LockConfig.ZkLockConfig.ZK);
    config.put(LockConfig.ZkLockConfig.ZK_URI_OPTION.key(), zkTestServer.getConnectString());
    config.put(LockConfig.LOCK_ID_OPTION.key(), "test-lock-id");
    LockConfig lockConfig = new LockConfig(table, config, new Configuration());

    TriggerLockFactory factory = LockFactoryBuilder.build(lockConfig, TABLE_NAME);
    assertThat(factory).isNotNull();
  }
}
