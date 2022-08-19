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

import org.apache.iceberg.metrics.MetricsContext.Unit;

/** Generalized Counter interface for creating telemetry-related instances when counting events. */
public interface Counter {

  /** Increment the counter by 1. */
  void increment();

  /**
   * Increment the counter by the provided amount.
   *
   * @param amount to be incremented.
   */
  default void increment(int amount) {
    increment((long) amount);
  }

  /**
   * Increment the counter by the provided amount.
   *
   * @param amount to be incremented.
   */
  void increment(long amount);

  /**
   * Reports the current count.
   *
   * @return The current count.
   */
  long value();

  /**
   * The unit of the counter.
   *
   * @return The unit of the counter.
   */
  default Unit unit() {
    return Unit.COUNT;
  }

  /**
   * Determines whether this counter is a NOOP counter.
   *
   * @return Whether this counter is a NOOP counter.
   */
  default boolean isNoop() {
    return DefaultCounter.NOOP.equals(this);
  }
}
