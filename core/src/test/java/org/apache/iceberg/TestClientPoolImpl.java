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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestClientPoolImpl {

  @Test
  public void testRetrySucceedsWithinMaxAttempts() throws Exception {
    int maxRetries = 5;
    int succeedAfterAttempts = 3;
    try (MockClientPoolImpl mockClientPool =
        new MockClientPoolImpl(2, RetryableException.class, true, maxRetries)) {
      // initial the client pool with a client, so that we can verify the client is replaced
      MockClient firstClient = mockClientPool.newClient();
      mockClientPool.clients().add(firstClient);

      int actions = mockClientPool.run(client -> client.succeedAfter(succeedAfterAttempts));
      assertThat(actions)
          .as("There should be exactly one successful action invocation")
          .isEqualTo(1);
      assertThat(mockClientPool.reconnectionAttempts()).isEqualTo(succeedAfterAttempts - 1);
      assertThat(mockClientPool.peekFirst().equals(firstClient)).isFalse();
    }
  }

  @Test
  public void testRetriesExhaustedAndSurfacesFailure() {
    int maxRetries = 3;
    int succeedAfterAttempts = 5;
    try (MockClientPoolImpl mockClientPool =
        new MockClientPoolImpl(2, RetryableException.class, true, maxRetries)) {
      assertThatThrownBy(
              () -> mockClientPool.run(client -> client.succeedAfter(succeedAfterAttempts)))
          .isInstanceOf(RetryableException.class);
      assertThat(mockClientPool.reconnectionAttempts()).isEqualTo(maxRetries);
    }
  }

  @Test
  public void testNoRetryingNonRetryableException() {
    try (MockClientPoolImpl mockClientPool =
        new MockClientPoolImpl(2, RetryableException.class, true, 3)) {
      assertThatThrownBy(() -> mockClientPool.run(MockClient::failWithNonRetryable, true))
          .isInstanceOf(NonRetryableException.class);
      assertThat(mockClientPool.reconnectionAttempts()).isEqualTo(0);
    }
  }

  @Test
  public void testNoRetryingWhenDisabled() {
    try (MockClientPoolImpl mockClientPool =
        new MockClientPoolImpl(2, RetryableException.class, false, 3)) {
      assertThatThrownBy(() -> mockClientPool.run(client -> client.succeedAfter(3)))
          .isInstanceOf(RetryableException.class);
      assertThat(mockClientPool.reconnectionAttempts()).isEqualTo(0);
    }
  }

  static class RetryableException extends RuntimeException {}

  static class NonRetryableException extends RuntimeException {}

  static class MockClient {
    boolean closed = false;
    int actions = 0;
    int retryableFailures = 0;

    MockClient() {}

    MockClient(int retryableFailures) {
      this.retryableFailures = retryableFailures;
    }

    public void close() {
      closed = true;
    }

    public int successfulAction() {
      actions++;
      return actions;
    }

    int succeedAfter(int succeedAfterAttempts) {
      if (retryableFailures == succeedAfterAttempts - 1) {
        return successfulAction();
      }

      retryableFailures++;
      throw new RetryableException();
    }

    int failWithNonRetryable() {
      throw new NonRetryableException();
    }
  }

  static class MockClientPoolImpl extends ClientPoolImpl<MockClient, Exception> {

    private int reconnectionAttempts;

    MockClientPoolImpl(
        int poolSize,
        Class<? extends Exception> reconnectExc,
        boolean retryByDefault,
        int numRetries) {
      super(poolSize, reconnectExc, retryByDefault, numRetries);
    }

    @Override
    protected MockClient newClient() {
      return new MockClient();
    }

    @Override
    protected MockClient reconnect(MockClient client) {
      reconnectionAttempts++;
      return new MockClient(reconnectionAttempts);
    }

    protected MockClient peekFirst() {
      return clients().peekFirst();
    }

    @Override
    protected void close(MockClient client) {
      client.close();
    }

    int reconnectionAttempts() {
      return reconnectionAttempts;
    }
  }
}
