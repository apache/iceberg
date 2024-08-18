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
import java.util.UUID;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

class TestJdbcLockFactory extends TestLockFactoryBase {
  @Override
  TriggerLockFactory lockFactory() {
    return lockFactory("tableName");
  }

  @Test
  void testMultiTableLock() {
    JdbcLockFactory other = lockFactory("tableName2");
    other.open((JdbcLockFactory) this.lockFactory);
    TriggerLockFactory.Lock lock1 = lockFactory.createLock();
    TriggerLockFactory.Lock lock2 = other.createLock();
    assertThat(lock1.tryLock()).isTrue();
    assertThat(lock2.tryLock()).isTrue();
  }

  private JdbcLockFactory lockFactory(String tableName) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    properties.put("maintenance.lock.jdbc.init-lock-tables", "true");

    return new JdbcLockFactory(
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""),
        tableName,
        properties);
  }
}
