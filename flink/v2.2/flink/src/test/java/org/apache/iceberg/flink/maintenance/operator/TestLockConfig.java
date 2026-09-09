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

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.maintenance.api.LockConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestLockConfig extends OperatorTestBase {
  private static final String TABLE_NAME = "catalog.db.table";
  private static final String LOCK_ID = "test-lock-id";
  private Map<String, String> input = Maps.newHashMap();
  private Table table;

  @BeforeEach
  public void before() {
    input.put("flink-maintenance.lock.type", "jdbc");
    input.put("flink-maintenance.lock.lock-id", LOCK_ID);
    input.put("other.config", "should-be-ignored");
    this.table = createTable();
  }

  @AfterEach
  public void after() {
    input.clear();
  }

  @Test
  void testConfigParsing() {
    LockConfig config = new LockConfig(table, input, new Configuration());

    assertThat(config.lockType()).isEqualTo("jdbc");
    assertThat(config.lockId(LOCK_ID)).isEqualTo(LOCK_ID);
  }

  @Test
  void testEmptyConfig() {
    LockConfig config = new LockConfig(table, Maps.newHashMap(), new Configuration());

    assertThat(config.lockType()).isEmpty();
    assertThat(config.lockId(TABLE_NAME)).isEqualTo(TABLE_NAME);
  }

  @Test
  void testWriteOptionReplaceSetConfig() {
    Configuration configuration = new Configuration();
    configuration.setString("flink-maintenance.lock.type", "zk");
    configuration.setString("flink-maintenance.lock.replace-item", "test-config");
    configuration.setString("flink-maintenance.lock.jdbc.init-lock-table", "true");
    LockConfig config = new LockConfig(table, input, configuration);

    // set config should be ignored
    assertThat(config.lockType()).isEqualTo("jdbc");
    assertThat(config.jdbcInitTable()).isEqualTo("true");

    assertThat(config.properties())
        .doesNotContainKey("other.config")
        .containsEntry("type", "jdbc")
        .containsEntry("replace-item", "test-config");
  }
}
