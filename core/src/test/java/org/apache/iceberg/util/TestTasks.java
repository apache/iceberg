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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTasks {

  private RecordingExecutor executor;

  @BeforeEach
  public void before() {
    executor = new RecordingExecutor(2);
  }

  @AfterEach
  public void after() {
    executor.shutdownNow();
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

    try {
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
    } finally {
      Thread.interrupted();
    }
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

  @Test
  public void parallelRunReturnsWhenTasksFinish() {
    int rounds = 100;
    AtomicInteger finished = new AtomicInteger();

    long start = System.nanoTime();
    for (int round = 0; round < rounds; round++) {
      Tasks.range(4)
          .executeWith(executor)
          .run(
              x -> {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
                finished.incrementAndGet();
              });
      assertThat(finished).hasValue((round + 1) * 4);
    }
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    // polling with a 10 ms sleep takes at least 10 ms per round
    assertThat(elapsedMillis).isLessThan(rounds * 10L);
  }

  @Test
  public void parallelRunRethrowsTaskError() {
    Thread waitingThread = Thread.currentThread();

    assertThatThrownBy(
            () ->
                Tasks.range(2)
                    .executeWith(executor)
                    .run(
                        x -> {
                          if (x == 0) {
                            awaitBlocked(waitingThread);
                            throw new Error("task failed");
                          }
                        }))
        .isExactlyInstanceOf(Error.class)
        .hasMessage("task failed");
  }

  @Test
  public void parallelRunIgnoresCancelledFuture() throws InterruptedException {
    Thread waitingThread = Thread.currentThread();
    CountDownLatch release = new CountDownLatch(1);

    boolean succeeded =
        Tasks.range(2)
            .executeWith(executor)
            .run(
                x -> {
                  if (x == 0) {
                    release.await(10, TimeUnit.SECONDS);
                  } else {
                    awaitBlocked(waitingThread);
                    executor.futures().get(0).cancel(false);
                    release.countDown();
                  }
                },
                InterruptedException.class);

    assertThat(succeeded).isTrue();
    assertThat(executor.futures().get(0).isCancelled()).isTrue();
  }

  @Test
  public void parallelRunCancelsFuturesOnInterrupt() {
    Thread waitingThread = Thread.currentThread();
    CountDownLatch neverReleased = new CountDownLatch(1);

    assertThatThrownBy(
            () ->
                Tasks.range(4)
                    .executeWith(executor)
                    .run(
                        x -> {
                          if (x == 0) {
                            awaitBlocked(waitingThread);
                            waitingThread.interrupt();
                          }
                          neverReleased.await(10, TimeUnit.SECONDS);
                        },
                        InterruptedException.class))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("InterruptedException")
        .hasCauseInstanceOf(InterruptedException.class);

    assertThat(Thread.interrupted()).isTrue();
    assertThat(executor.futures()).hasSize(4).allMatch(Future::isCancelled);
  }

  private static void awaitBlocked(Thread thread) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    // FutureTask.get() parks with the future as the blocker
    while (!(LockSupport.getBlocker(thread) instanceof Future)) {
      if (System.nanoTime() - deadline > 0) {
        throw new AssertionError(
            "Timed out waiting for " + thread.getName() + " to block on a future");
      }
      Thread.onSpinWait();
    }
  }

  private static class RecordingExecutor extends ThreadPoolExecutor {
    private final List<Future<?>> futures = new CopyOnWriteArrayList<>();

    RecordingExecutor(int threads) {
      super(threads, threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    List<Future<?>> futures() {
      return futures;
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
      RunnableFuture<T> future = super.newTaskFor(runnable, value);
      futures.add(future);
      return future;
    }
  }
}
