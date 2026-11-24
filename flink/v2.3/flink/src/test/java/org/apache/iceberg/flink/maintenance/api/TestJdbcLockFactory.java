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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory.Lock;
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

  @Test
  void testLockTableInitializationRecoversFromConcurrentCreation() throws Exception {
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet beforeCreation = mock(ResultSet.class);
    ResultSet afterCreation = mock(ResultSet.class);

    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getTables(null, null, "flink_maintenance_lock", null))
        .thenReturn(beforeCreation, afterCreation);
    when(beforeCreation.next()).thenReturn(false);
    when(afterCreation.next()).thenReturn(true);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.execute()).thenThrow(new SQLException("Table already exists"));

    assertThat(JdbcLockFactory.initializeLockTable(connection)).isTrue();

    verify(statement).execute();
    verify(metadata, times(2)).getTables(null, null, "flink_maintenance_lock", null);
    verify(beforeCreation).close();
    verify(afterCreation).close();
    verify(statement).close();
  }

  @Test
  void testLockTableInitializationPropagatesDatabaseFailure() throws Exception {
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet beforeCreation = mock(ResultSet.class);
    ResultSet afterCreation = mock(ResultSet.class);
    SQLException failure = new SQLException("Permission denied");

    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getTables(null, null, "flink_maintenance_lock", null))
        .thenReturn(beforeCreation, afterCreation);
    when(beforeCreation.next()).thenReturn(false);
    when(afterCreation.next()).thenReturn(false);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.execute()).thenThrow(failure);

    assertThatThrownBy(() -> JdbcLockFactory.initializeLockTable(connection)).isSameAs(failure);

    verify(statement).execute();
    verify(metadata, times(2)).getTables(null, null, "flink_maintenance_lock", null);
    verify(beforeCreation).close();
    verify(afterCreation).close();
    verify(statement).close();
  }

  @Test
  void testDeterministicConcurrentLockTableInitialization() throws Exception {
    String uri =
        "jdbc:sqlite:file:lock-race-"
            + UUID.randomUUID().toString().replace("-", "")
            + "?mode=memory&cache=shared";

    CountDownLatch bothChecked = new CountDownLatch(2);
    CountDownLatch firstCreated = new CountDownLatch(1);

    try (Connection first = DriverManager.getConnection(uri);
        Connection second = DriverManager.getConnection(uri)) {
      Connection firstConnection = mock(Connection.class);
      Connection secondConnection = mock(Connection.class);

      DatabaseMetaData firstMetadata = first.getMetaData();
      DatabaseMetaData secondMetadata = spy(second.getMetaData());

      when(firstConnection.getMetaData()).thenReturn(firstMetadata);
      when(secondConnection.getMetaData()).thenReturn(secondMetadata);

      when(firstConnection.prepareStatement(anyString()))
          .thenAnswer(
              invocation -> {
                bothChecked.countDown();
                assertThat(bothChecked.await(10, TimeUnit.SECONDS)).isTrue();
                return first.prepareStatement(invocation.getArgument(0, String.class));
              });

      when(secondConnection.prepareStatement(anyString()))
          .thenAnswer(
              invocation -> {
                bothChecked.countDown();
                assertThat(bothChecked.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(firstCreated.await(10, TimeUnit.SECONDS)).isTrue();
                return second.prepareStatement(invocation.getArgument(0, String.class));
              });

      ExecutorService executor = Executors.newFixedThreadPool(2);

      try {
        Future<Boolean> firstResult =
            executor.submit(
                () -> {
                  boolean result = JdbcLockFactory.initializeLockTable(firstConnection);
                  firstCreated.countDown();
                  return result;
                });

        Future<Boolean> secondResult =
            executor.submit(() -> JdbcLockFactory.initializeLockTable(secondConnection));

        assertThat(firstResult.get(30, TimeUnit.SECONDS)).isTrue();
        assertThat(secondResult.get(30, TimeUnit.SECONDS)).isTrue();

        verify(firstConnection).prepareStatement(anyString());
        verify(secondConnection).prepareStatement(anyString());
        verify(secondMetadata, times(2)).getTables(null, null, "flink_maintenance_lock", null);
      } finally {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
      }
    }
  }

  @Test
  void testConcurrentLockTableInitialization() throws Exception {
    Map<String, String> props = Maps.newHashMap();
    props.put(INIT_LOCK_TABLES_PROPERTY, "true");

    String uri =
        "jdbc:sqlite:file:lock-init-"
            + UUID.randomUUID().toString().replace("-", "")
            + "?mode=memory&cache=shared";

    try (JdbcLockFactory first = new JdbcLockFactory(uri, "first", props);
        JdbcLockFactory second = new JdbcLockFactory(uri, "second", props)) {
      ExecutorService executor = Executors.newFixedThreadPool(2);
      CountDownLatch ready = new CountDownLatch(2);
      CountDownLatch startSignal = new CountDownLatch(1);

      try {
        Future<?> firstResult =
            executor.submit(
                () -> {
                  ready.countDown();
                  startSignal.await();
                  first.open();
                  return null;
                });

        Future<?> secondResult =
            executor.submit(
                () -> {
                  ready.countDown();
                  startSignal.await();
                  second.open();
                  return null;
                });

        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        startSignal.countDown();

        firstResult.get(30, TimeUnit.SECONDS);
        secondResult.get(30, TimeUnit.SECONDS);

        checkConcurrentLocks(first, second);
      } finally {
        startSignal.countDown();
        executor.shutdownNow();
        assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
      }
    }
  }

  private void checkConcurrentLocks(JdbcLockFactory first, JdbcLockFactory second) {
    Lock firstLock = first.createLock();
    Lock secondLock = second.createLock();

    try {
      assertThat(firstLock.tryLock()).isTrue();
      assertThat(secondLock.tryLock()).isTrue();
    } finally {
      firstLock.unlock();
      secondLock.unlock();
    }
  }
}
