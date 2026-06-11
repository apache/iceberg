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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
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
  public void retryExhaustionIncludesReason() {
    assertThatThrownBy(
            () ->
                Tasks.foreach(Collections.singleton(1))
                    .retry(1)
                    .exponentialBackoff(0, 0, 5000, 0)
                    .onlyRetryOn(IllegalStateException.class)
                    .run(
                        item -> {
                          throw new IllegalStateException("Retryable failure");
                        }))
        .isInstanceOf(Tasks.RetryExhaustedException.class)
        .hasCauseInstanceOf(IllegalStateException.class)
        .satisfies(
            thrown ->
                assertThat(((Tasks.RetryExhaustedException) thrown).reason())
                    .isEqualTo(Tasks.RetryExhaustedException.Reason.RETRY_LIMIT_EXCEEDED));
  }

  @Test
  public void retryExhaustionDoesNotWrapNonRetryableFailure() {
    AtomicInteger attempts = new AtomicInteger();

    assertThatThrownBy(
            () ->
                Tasks.foreach(Collections.singleton(1))
                    .retry(1)
                    .exponentialBackoff(0, 0, 5000, 0)
                    .onlyRetryOn(IllegalStateException.class)
                    .run(
                        item -> {
                          if (attempts.incrementAndGet() == 1) {
                            throw new IllegalStateException("Retryable failure");
                          }

                          throw new IllegalArgumentException("Not retryable");
                        }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Not retryable");

    assertThat(attempts).hasValue(2);
  }
}
