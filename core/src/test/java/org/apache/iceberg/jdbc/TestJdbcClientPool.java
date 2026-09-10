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
package org.apache.iceberg.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestJdbcClientPool {

  private final JdbcClientPool pool =
      new JdbcClientPool("jdbc:sqlite:file::memory:", ImmutableMap.of());

  @AfterEach
  void closePool() {
    pool.close();
  }

  @Test
  void connectionIsReplacedAfterRecoverableFailure() throws Exception {
    AtomicInteger attempts = new AtomicInteger();

    boolean valid =
        pool.run(
            client -> {
              if (attempts.getAndIncrement() == 0) {
                throw new CommunicationsFailure();
              }
              return client.isValid(1);
            });

    assertThat(valid).isTrue();
    assertThat(attempts).hasValue(2);
  }

  @Test
  void recoverableExceptionIsRetried() {
    assertThat(pool.isConnectionException(new SQLRecoverableException("connection closed")))
        .isTrue();
  }

  @Test
  void otherExceptionIsNotRetried() {
    assertThat(pool.isConnectionException(new SQLNonTransientException("syntax error", "42000")))
        .isFalse();
  }

  /**
   * Mirrors {@code com.mysql.cj.jdbc.exceptions.CommunicationsException}, whose SQLSTATE is not in
   * {@link JdbcClientPool#COMMON_RETRYABLE_CONNECTION_SQL_STATES}.
   */
  private static class CommunicationsFailure extends SQLRecoverableException {
    CommunicationsFailure() {
      super("Communications link failure", "08S01");
    }
  }
}
