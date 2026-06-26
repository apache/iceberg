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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.junit.jupiter.api.Test;

public class TestTasks {

  @Test
  public void attemptCounterIsIncreasedOnRetries() {
    Counter counter = new DefaultMetricsContext().counter("counter");

    final int retries = 10;

    Tasks.foreach(IntStream.range(0, 10))
        .countAttempts(counter)
        .exponentialBackoff(0, 0, 5000, 0)
        .retry(retries)
        .onlyRetryOn(RuntimeException.class)
        .run(
            x -> {
              // don't throw on the last retry
              if (counter.value() <= retries) {
                throw new RuntimeException();
              }
            });

    assertThat(counter.value()).isEqualTo(retries + 1);
  }

  @Test
  public void attemptCounterIsIncreasedWithoutRetries() {
    Counter counter = new DefaultMetricsContext().counter("counter");

    Tasks.foreach(IntStream.range(0, 10)).countAttempts(counter).run(x -> {});

    assertThat(counter.value()).isOne();
  }

  @Test
  public void retryExhaustedReportsAttemptLimit() {
    RuntimeException failure = new RuntimeException("failed");
    RuntimeException wrapped = new RuntimeException("wrapped");
    AtomicReference<Exception> capturedFailure = new AtomicReference<>();
    AtomicReference<Tasks.RetryExhaustionReason> capturedReason = new AtomicReference<>();

    Throwable thrown =
        catchThrowable(
            () ->
                Tasks.foreach(1)
                    .retry(1)
                    .exponentialBackoff(0, 0, 5000, 0)
                    .onlyRetryOn(RuntimeException.class)
                    .onRetryExhausted(
                        (exception, reason) -> {
                          capturedFailure.set(exception);
                          capturedReason.set(reason);
                          return wrapped;
                        })
                    .run(
                        x -> {
                          throw failure;
                        }));

    assertThat(thrown).isSameAs(wrapped);
    assertThat(capturedFailure.get()).isSameAs(failure);
    assertThat(capturedReason.get()).isEqualTo(Tasks.RetryExhaustionReason.ATTEMPT_LIMIT);
  }

  @Test
  public void retryExhaustedHandlerCannotBeNull() {
    Throwable thrown = catchThrowable(() -> Tasks.foreach(1).onRetryExhausted(null));

    assertThat(thrown)
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Handler cannot be null");
  }

  @Test
  public void retryExhaustedReportsTimeout() {
    RuntimeException wrapped = new RuntimeException("wrapped");
    AtomicReference<Tasks.RetryExhaustionReason> capturedReason = new AtomicReference<>();

    Throwable thrown =
        catchThrowable(
            () ->
                Tasks.foreach(1)
                    .retry(2)
                    .exponentialBackoff(0, 0, 1, 0)
                    .onlyRetryOn(RuntimeException.class)
                    .onRetryExhausted(
                        (exception, reason) -> {
                          capturedReason.set(reason);
                          return wrapped;
                        })
                    .run(
                        x -> {
                          sleep(5);
                          throw new RuntimeException("failed");
                        }));

    assertThat(thrown).isSameAs(wrapped);
    assertThat(capturedReason.get()).isEqualTo(Tasks.RetryExhaustionReason.TIMEOUT);
  }

  @Test
  public void retryExhaustedReportsAttemptLimitAndTimeout() {
    RuntimeException wrapped = new RuntimeException("wrapped");
    AtomicReference<Tasks.RetryExhaustionReason> capturedReason = new AtomicReference<>();

    Throwable thrown =
        catchThrowable(
            () ->
                Tasks.foreach(1)
                    .retry(1)
                    .exponentialBackoff(0, 0, 1, 0)
                    .onlyRetryOn(RuntimeException.class)
                    .onRetryExhausted(
                        (exception, reason) -> {
                          capturedReason.set(reason);
                          return wrapped;
                        })
                    .run(
                        x -> {
                          sleep(5);
                          throw new RuntimeException("failed");
                        }));

    assertThat(thrown).isSameAs(wrapped);
    assertThat(capturedReason.get())
        .isEqualTo(Tasks.RetryExhaustionReason.ATTEMPT_LIMIT_AND_TIMEOUT);
  }

  @Test
  public void retryExhaustedHandlerIsOptIn() {
    RuntimeException failure = new RuntimeException("failed");
    AtomicInteger attempts = new AtomicInteger(0);

    Throwable thrown =
        catchThrowable(
            () ->
                Tasks.foreach(1)
                    .retry(1)
                    .exponentialBackoff(0, 0, 5000, 0)
                    .onlyRetryOn(RuntimeException.class)
                    .run(
                        x -> {
                          attempts.incrementAndGet();
                          throw failure;
                        }));

    assertThat(thrown).isSameAs(failure);
    assertThat(attempts.get()).isEqualTo(2);
  }

  @Test
  public void retryExhaustedDoesNotWrapNonRetryableException() {
    RuntimeException failure = new RuntimeException("failed");
    RuntimeException wrapped = new RuntimeException("wrapped");
    AtomicInteger attempts = new AtomicInteger(0);

    Throwable thrown =
        catchThrowable(
            () ->
                Tasks.foreach(1)
                    .retry(1)
                    .exponentialBackoff(0, 0, 5000, 0)
                    .onlyRetryOn(IllegalArgumentException.class)
                    .onRetryExhausted((exception, reason) -> wrapped)
                    .run(
                        x -> {
                          attempts.incrementAndGet();
                          throw failure;
                        }));

    assertThat(thrown).isSameAs(failure);
    assertThat(attempts.get()).isOne();
  }

  @Test
  public void retryExhaustedCanWrapAsCommitFailedException() {
    CommitFailedException failure = new CommitFailedException("failed");

    Throwable thrown =
        catchThrowable(
            () ->
                Tasks.foreach(1)
                    .retry(0)
                    .exponentialBackoff(0, 0, 5000, 0)
                    .onlyRetryOn(CommitFailedException.class)
                    .onRetryExhausted(
                        (exception, reason) -> new CommitFailedException(exception, "wrapped"))
                    .run(
                        x -> {
                          throw failure;
                        }));

    assertThat(thrown).isInstanceOf(CommitFailedException.class);
    assertThat(thrown).hasMessage("wrapped");
    assertThat(thrown.getCause()).isSameAs(failure);
  }

  private static void sleep(long millis) {
    try {
      TimeUnit.MILLISECONDS.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
