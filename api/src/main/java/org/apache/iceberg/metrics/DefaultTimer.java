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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Stopwatch;

/**
 * A default {@link Timer} implementation that uses a {@link Stopwatch} instance internally to
 * measure time.
 */
public class DefaultTimer implements Timer {
  private final TimeUnit timeUnit;
  private final LongAdder count = new LongAdder();
  private final LongAdder totalTime = new LongAdder();

  public DefaultTimer(TimeUnit timeUnit) {
    Preconditions.checkArgument(null != timeUnit, "Invalid time unit: null");
    this.timeUnit = timeUnit;
  }

  @Override
  public long count() {
    return count.longValue();
  }

  @Override
  public Duration totalDuration() {
    return Duration.ofNanos(totalTime.longValue());
  }

  @Override
  public Timed start() {
    return new DefaultTimed(this, timeUnit);
  }

  @Override
  public void record(long amount, TimeUnit unit) {
    Preconditions.checkArgument(amount >= 0, "Cannot record %s %s: must be >= 0", amount, unit);
    this.totalTime.add(TimeUnit.NANOSECONDS.convert(amount, unit));
    this.count.increment();
  }

  @Override
  public <T> T time(Supplier<T> supplier) {
    Timed timed = start();
    try {
      return supplier.get();
    } finally {
      timed.stop();
    }
  }

  @Override
  public <T> T timeCallable(Callable<T> callable) throws Exception {
    Timed timed = start();
    try {
      return callable.call();
    } finally {
      timed.stop();
    }
  }

  @Override
  public void time(Runnable runnable) {
    Timed timed = start();
    try {
      runnable.run();
    } finally {
      timed.stop();
    }
  }

  @Override
  public TimeUnit unit() {
    return timeUnit;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(DefaultTimer.class)
        .add("duration", totalDuration())
        .add("count", count)
        .add("timeUnit", timeUnit)
        .toString();
  }

  private static class DefaultTimed implements Timed {
    private final Timer timer;
    private final TimeUnit defaultTimeUnit;
    private final AtomicReference<Stopwatch> stopwatchRef = new AtomicReference<>();

    private DefaultTimed(Timer timer, TimeUnit defaultTimeUnit) {
      this.timer = timer;
      this.defaultTimeUnit = defaultTimeUnit;
      stopwatchRef.compareAndSet(null, Stopwatch.createStarted());
    }

    @Override
    public void stop() {
      Stopwatch stopwatch = stopwatchRef.getAndSet(null);
      Preconditions.checkState(null != stopwatch, "stop() called multiple times");
      timer.record(stopwatch.stop().elapsed(defaultTimeUnit), defaultTimeUnit);
    }
  }
}
