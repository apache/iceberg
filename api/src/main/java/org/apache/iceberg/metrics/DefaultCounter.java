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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** A default {@link Counter} implementation that uses an {@link AtomicLong} to count events. */
public class DefaultCounter implements Counter {
  public static final Counter NOOP =
      new DefaultCounter(Unit.UNDEFINED) {
        @Override
        public void increment() {}

        @Override
        public void increment(long amount) {}

        @Override
        public long value() {
          throw new UnsupportedOperationException("NOOP counter has no value");
        }
      };

  private final LongAdder counter;
  private final MetricsContext.Unit unit;

  DefaultCounter(MetricsContext.Unit unit) {
    Preconditions.checkArgument(null != unit, "Invalid count unit: null");
    this.unit = unit;
    this.counter = new LongAdder();
  }

  @Override
  public void increment() {
    increment(1L);
  }

  @Override
  public void increment(long amount) {
    Math.addExact(counter.longValue(), amount);
    counter.add(amount);
  }

  @Override
  public long value() {
    return counter.longValue();
  }

  @Override
  public String toString() {
    return String.format("{%s=%s}", unit().displayName(), value());
  }

  @Override
  public MetricsContext.Unit unit() {
    return unit;
  }

}
