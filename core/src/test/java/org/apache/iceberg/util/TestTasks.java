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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTasks {

  @AfterEach
  void clearInterruptStatus() {
    Thread.interrupted();
  }

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

  @ParameterizedTest
  @MethodSource("interruptions")
  void tasksAreNotRetriedWhenFailureIsCausedByInterruption(Exception interruption) {
    Counter counter = new DefaultMetricsContext().counter("counter");

    assertThatThrownBy(
            () ->
                Tasks.foreach(1)
                    .countAttempts(counter)
                    .retry(3)
                    .run(
                        x -> {
                          throw interruption;
                        },
                        Exception.class))
        .isSameAs(interruption);

    assertThat(counter.value()).as("Interrupted task should not be retried").isOne();
    assertThat(Thread.currentThread().isInterrupted())
        .as("Interrupt status should be restored")
        .isTrue();
  }

  private static Stream<Exception> interruptions() {
    return Stream.of(
        new InterruptedException("interrupted"),
        new InterruptedIOException("interrupted"),
        new ClosedByInterruptException(),
        new RuntimeException("failed to read manifest", new InterruptedException("interrupted")),
        new UncheckedIOException(new InterruptedIOException("interrupted")),
        new RuntimeException(
            "failed to read manifest",
            new IOException("failed to open stream", new ClosedByInterruptException())));
  }

  @ParameterizedTest
  @MethodSource("transientFailures")
  void tasksAreRetriedWhenFailureIsNotCausedByInterruption(Exception failure) {
    Counter counter = new DefaultMetricsContext().counter("counter");

    assertThatThrownBy(
            () ->
                Tasks.foreach(1)
                    .countAttempts(counter)
                    .exponentialBackoff(0, 0, 5000, 0)
                    .retry(3)
                    .run(
                        x -> {
                          throw failure;
                        },
                        Exception.class))
        .isSameAs(failure);

    assertThat(counter.value()).isEqualTo(4);
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }

  private static Stream<Exception> transientFailures() {
    return Stream.of(
        new RuntimeException("failed to read manifest", new IOException("connection reset")),
        // SocketTimeoutException extends InterruptedIOException but is not an interrupt
        new SocketTimeoutException("read timed out"),
        new RuntimeException("failed to read manifest", new SocketTimeoutException("timed out")));
  }

  @Test
  void parallelTasksAreNotRetriedWhenTheCallerIsInterrupted() throws InterruptedException {
    int taskCount = 3;
    Counter counter = new DefaultMetricsContext().counter("counter");
    CountDownLatch allTasksBlocked = new CountDownLatch(taskCount);
    CountDownLatch allTasksInterrupted = new CountDownLatch(taskCount);

    ExecutorService taskPool = Executors.newFixedThreadPool(taskCount);
    ExecutorService callerPool = Executors.newSingleThreadExecutor();
    try {
      Future<?> caller =
          callerPool.submit(
              () ->
                  Tasks.range(taskCount)
                      .countAttempts(counter)
                      .retry(3)
                      .suppressFailureWhenFinished()
                      .executeWith(taskPool)
                      .run(
                          index -> {
                            allTasksBlocked.countDown();
                            try {
                              Thread.sleep(TimeUnit.MINUTES.toMillis(10));
                            } catch (InterruptedException e) {
                              // blocking reads surface an interrupt as a wrapped failure without
                              // preserving the interrupt status
                              allTasksInterrupted.countDown();
                              throw new RuntimeException("failed to read", e);
                            }
                          }));

      assertThat(allTasksBlocked.await(5, TimeUnit.SECONDS)).isTrue();

      // interrupts the caller while it waits in Tasks.waitFor, which cancels every running task
      caller.cancel(true);

      assertThat(allTasksInterrupted.await(5, TimeUnit.SECONDS))
          .as("Each task should be interrupted individually")
          .isTrue();

      // a retried task never releases its thread: it sleeps for the retry backoff and then blocks
      // again, so an idle pool means the interrupted tasks stopped
      CountDownLatch poolIsIdle = new CountDownLatch(1);
      taskPool.submit(poolIsIdle::countDown);

      assertThat(poolIsIdle.await(5, TimeUnit.SECONDS))
          .as("Interrupted tasks should release their threads instead of retrying")
          .isTrue();
      assertThat(counter.value())
          .as("None of the interrupted tasks should be retried")
          .isEqualTo(taskCount);
    } finally {
      taskPool.shutdownNow();
      callerPool.shutdownNow();
    }
  }
}
