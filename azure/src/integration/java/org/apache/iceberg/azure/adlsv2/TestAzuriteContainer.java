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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ContainerFetchException;
import org.testcontainers.containers.ContainerLaunchException;

public class TestAzuriteContainer {

  private static final int MAX_ATTEMPTS = 5;

  @Test
  public void succeedsWithoutRetryWhenStartSucceeds() {
    AtomicInteger calls = new AtomicInteger();

    AzuriteContainer.startWithRetry(calls::incrementAndGet, MAX_ATTEMPTS, Duration.ZERO);

    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  public void retriesUntilStartSucceeds() {
    AtomicInteger calls = new AtomicInteger();
    Runnable start =
        () -> {
          if (calls.incrementAndGet() < 3) {
            throw new ContainerFetchException("transient", new RuntimeException("404"));
          }
        };

    AzuriteContainer.startWithRetry(start, MAX_ATTEMPTS, Duration.ZERO);

    assertThat(calls.get()).isEqualTo(3);
  }

  @Test
  public void rethrowsLastFailureAfterExhaustingAttempts() {
    AtomicInteger calls = new AtomicInteger();
    ContainerFetchException fetchFailure =
        new ContainerFetchException("transient", new RuntimeException("404"));
    Runnable start =
        () -> {
          calls.incrementAndGet();
          throw fetchFailure;
        };

    assertThatThrownBy(() -> AzuriteContainer.startWithRetry(start, MAX_ATTEMPTS, Duration.ZERO))
        .isSameAs(fetchFailure);
    assertThat(calls.get()).isEqualTo(MAX_ATTEMPTS);
  }

  @Test
  public void doesNotRetryNonImageFetchFailures() {
    AtomicInteger calls = new AtomicInteger();
    ContainerLaunchException launchFailure =
        new ContainerLaunchException("wait strategy timed out");
    Runnable start =
        () -> {
          calls.incrementAndGet();
          throw launchFailure;
        };

    assertThatThrownBy(() -> AzuriteContainer.startWithRetry(start, MAX_ATTEMPTS, Duration.ZERO))
        .isSameAs(launchFailure);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  public void retriesFetchFailureWrappedInLaunchException() {
    AtomicInteger calls = new AtomicInteger();
    Runnable start =
        () -> {
          if (calls.incrementAndGet() < 2) {
            throw new ContainerLaunchException(
                "startup failed",
                new ContainerFetchException("transient", new RuntimeException("404")));
          }
        };

    AzuriteContainer.startWithRetry(start, MAX_ATTEMPTS, Duration.ZERO);

    assertThat(calls.get()).isEqualTo(2);
  }
}
