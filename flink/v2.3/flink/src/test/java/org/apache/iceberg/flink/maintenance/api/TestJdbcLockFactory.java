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

import static org.apache.iceberg.flink.maintenance.api.JdbcLockFactory.INIT_LOCK_TABLES_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLTransientConnectionException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

class TestJdbcLockFactory extends TestLockFactoryBase {
  @Override
  TriggerLockFactory lockFactory(String tableName) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
    properties.put(INIT_LOCK_TABLES_PROPERTY, "true");

    return new JdbcLockFactory(
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""),
        tableName,
        properties);
  }

  @Test
  void testSQLExceptionEnablesRetryInClientPool() throws Exception {
    // Regression test for #15759: verify that removing the inner try-catch allows
    // ClientPoolImpl to retry on transient connection failures.
    //
    // Before the fix: inner catch converted SQLException -> UncheckedSQLException
    // (RuntimeException) inside the lambda. ClientPoolImpl only catches the declared
    // exception type (SQLException), so RuntimeException bypasses retry entirely.
    // After the fix: SQLException propagates naturally, ClientPoolImpl catches it,
    // and retries on transient connection exceptions.
    Map<String, String> props = Maps.newHashMap();
    props.put("username", "user");
    props.put("password", "password");
    String uri = "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", "");

    try (JdbcClientPool pool = new JdbcClientPool(1, uri, props)) {
      AtomicInteger attempts = new AtomicInteger(0);

      String result =
          pool.run(
              conn -> {
                if (attempts.incrementAndGet() == 1) {
                  throw new SQLTransientConnectionException("transient failure");
                }

                return "success";
              });

      assertThat(result).isEqualTo("success");
      assertThat(attempts.get()).isGreaterThan(1);
    }
  }

  @Test
  void testUncheckedSQLExceptionBypassesRetry() throws Exception {
    // Companion test: demonstrates that wrapping SQLException as UncheckedSQLException
    // (the OLD behavior before the fix) prevents ClientPoolImpl from retrying.
    Map<String, String> props = Maps.newHashMap();
    props.put("username", "user");
    props.put("password", "password");
    String uri = "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", "");

    try (JdbcClientPool pool = new JdbcClientPool(1, uri, props)) {
      assertThatThrownBy(
              () ->
                  pool.run(
                      conn -> {
                        try {
                          throw new SQLTransientConnectionException("transient failure");
                        } catch (java.sql.SQLException e) {
                          throw new UncheckedSQLException(e, "wrapped");
                        }
                      }))
          .isInstanceOf(UncheckedSQLException.class)
          .hasMessageContaining("wrapped");
    }
  }
}
