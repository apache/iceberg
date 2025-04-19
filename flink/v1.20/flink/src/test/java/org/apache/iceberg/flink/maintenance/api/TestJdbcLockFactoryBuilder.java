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

import static org.apache.iceberg.flink.maintenance.api.JdbcLockFactoryBuilder.JDBC_URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

class TestJdbcLockFactoryBuilder {

  @Test
  void testBuildWithMissingJdbcUri() {
    JdbcLockFactoryBuilder builder = new JdbcLockFactoryBuilder();
    Map<String, String> config = Maps.newHashMap();
    config.put(BaseLockFactoryBuilder.LOCK_TYPE_KEY, JdbcLockFactoryBuilder.JDBC_LOCK);

    assertThatThrownBy(() -> builder.build(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "JDBC lock requires %s parameter",
                BaseLockFactoryBuilder.CONFIG_PREFIX + JDBC_URI));
  }

  @Test
  void testBuildSuccessfully() {
    JdbcLockFactoryBuilder builder = new JdbcLockFactoryBuilder();
    Map<String, String> config = Maps.newHashMap();
    config.put(BaseLockFactoryBuilder.LOCK_TYPE_KEY, JdbcLockFactoryBuilder.JDBC_LOCK);
    config.put(JDBC_URI, "jdbc:sqlite:file::memory:?ic");
    config.put(BaseLockFactoryBuilder.LOCK_ID, "test-lock-id");

    TriggerLockFactory factory = builder.build(config);
    assertThat(factory).isNotNull();
  }
}
