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
package org.apache.iceberg.metrics;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Generalized Timer interface for creating telemetry related instances for measuring duration of
 * operations.
 */
public interface Timer {

  /**
   * The number of times {@link Timer#time(Duration)} was called.
   *
   * @return The number of times {@link Timer#time(Duration)} was called.
   */
  long count();

  /**
   * The total duration that was recorded.
   *
   * @return The total duration that was recorded.
   */
  Duration totalDuration();

  /**
   * Starts the timer and returns a {@link Timed} instance. Call {@link Timed#stop()} to complete
   * the timing.
   *
   * @return A {@link Timed} instance with the start time recorded.
   */
  Timed start();

  /**
   * The {@link TimeUnit} of the timer.
   *
   * @return The {@link TimeUnit} of the timer.
   */
  default TimeUnit unit() {
    return TimeUnit.NANOSECONDS;
  }

  /**
   * Records a custom amount in the given time unit.
   *
   * @param amount The amount to record
   * @param unit The time unit of the amount
   */
  void record(long amount, TimeUnit unit);

  /**
   * The duration to record
   *
   * @param duration The duration to record
   */
  default void time(Duration duration) {
    record(duration.toNanos(), TimeUnit.NANOSECONDS);
  }

  /**
   * Executes and measures the given {@link Runnable} instance.
   *
   * @param runnable The {@link Runnable} to execute and measure.
   */
  void time(Runnable runnable);

  /**
   * Executes and measures the given {@link Callable} and returns its result.
   *
   * @param callable The {@link Callable} to execute and measure.
   * @param <T> The type of the {@link Callable}
   * @return The result of the underlying {@link Callable}.
   * @throws Exception In case the {@link Callable} fails.
   */
  <T> T timeCallable(Callable<T> callable) throws Exception;

  /**
   * Gets the result from the given {@link Supplier} and measures its execution time.
   *
   * @param supplier The {@link Supplier} to execute and measure.
   * @param <T> The type of the {@link Supplier}.
   * @return The result of the underlying {@link Supplier}.
   */
  <T> T time(Supplier<T> supplier);

  /**
   * Determines whether this timer is a NOOP timer.
   *
   * @return Whether this timer is a NOOP timer.
   */
  default boolean isNoop() {
    return NOOP.equals(this);
  }

  /**
   * A timing sample that carries internal state about the Timer's start position. The timing can be
   * completed by calling {@link Timed#stop()}.
   */
  interface Timed extends AutoCloseable {
    /** Stops the timer and records the total duration up until {@link Timer#start()} was called. */
    void stop();

    @Override
    default void close() {
      stop();
    }

    Timed NOOP = () -> {};
  }

  Timer NOOP =
      new Timer() {
        @Override
        public Timed start() {
          return Timed.NOOP;
        }

        @Override
        public long count() {
          throw new UnsupportedOperationException("NOOP timer has no count");
        }

        @Override
        public Duration totalDuration() {
          throw new UnsupportedOperationException("NOOP timer has no duration");
        }

        @Override
        public TimeUnit unit() {
          throw new UnsupportedOperationException("NOOP timer has no unit");
        }

        @Override
        public void record(long amount, TimeUnit unit) {}

        @Override
        public void time(Runnable runnable) {}

        @Override
        public <T> T timeCallable(Callable<T> callable) throws Exception {
          return callable.call();
        }

        @Override
        public <T> T time(Supplier<T> supplier) {
          return supplier.get();
        }

        @Override
        public String toString() {
          return "NOOP timer";
        }
      };
}
