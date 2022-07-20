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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * A default {@link org.apache.iceberg.metrics.MetricsContext.Counter} implementation that uses an {@link Integer} to
 * count events.
 */
class IntCounter implements MetricsContext.Counter<Integer> {
  private final AtomicInteger counter;

  IntCounter() {
    this.counter = new AtomicInteger(0);
  }

  @Override
  public void increment() {
    increment(1);
  }

  @Override
  public void increment(Integer amount) {
    counter.updateAndGet(val -> Math.addExact(val, amount));
  }

  @Override
  public Optional<Integer> count() {
    return Optional.of(counter.get());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("counter", counter)
        .toString();
  }
}
