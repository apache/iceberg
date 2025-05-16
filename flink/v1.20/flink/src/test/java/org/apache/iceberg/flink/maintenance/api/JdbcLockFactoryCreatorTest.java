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

import static org.apache.iceberg.flink.maintenance.api.JdbcLockFactoryCreator.JDBC_URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

class JdbcLockFactoryCreatorTest {

  @Test
  void testCreateWithMissingJdbcUri() {
    JdbcLockFactoryCreator creator = new JdbcLockFactoryCreator();
    Map<String, String> config = Maps.newHashMap();
    config.put(BaseLockFactoryCreator.LOCK_TYPE_KEY, JdbcLockFactoryCreator.JDBC_LOCK);

    assertThatThrownBy(() -> creator.create(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(String.format("JDBC lock requires %s parameter", JDBC_URI));
  }

  @Test
  void testCreateWithMissingLockId() {
    JdbcLockFactoryCreator creator = new JdbcLockFactoryCreator();
    Map<String, String> config = Maps.newHashMap();
    config.put(BaseLockFactoryCreator.LOCK_TYPE_KEY, JdbcLockFactoryCreator.JDBC_LOCK);
    config.put(JDBC_URI, "jdbc:sqlite:file::memory:?ic");

    assertThatThrownBy(() -> creator.create(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format("JDBC lock requires %s parameter", BaseLockFactoryCreator.LOCK_ID));
  }

  @Test
  void testCreateSuccessfully() {
    JdbcLockFactoryCreator creator = new JdbcLockFactoryCreator();
    Map<String, String> config = Maps.newHashMap();
    config.put(BaseLockFactoryCreator.LOCK_TYPE_KEY, JdbcLockFactoryCreator.JDBC_LOCK);
    config.put(JDBC_URI, "jdbc:sqlite:file::memory:?ic");
    config.put(BaseLockFactoryCreator.LOCK_ID, "test-lock-id");

    TriggerLockFactory factory = creator.create(config);
    assertThat(factory).isNotNull();
  }
}
