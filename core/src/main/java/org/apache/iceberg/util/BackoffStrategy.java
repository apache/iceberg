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

import java.util.Map;

/**
 * Strategy that decides how long to wait between retry attempts in {@link Tasks}.
 *
 * <p>{@link Tasks} always retries through a {@code BackoffStrategy}. When none is supplied, the
 * built-in {@link ExponentialBackoffStrategy} is used. Call sites typically resolve the strategy
 * via {@link BackoffStrategies#from(java.util.Map, long, long, double)} so operators can supply a
 * custom implementation by setting the {@link BackoffStrategies#STRATEGY_IMPL} property to the
 * fully-qualified name of a class implementing this interface with a public no-arg constructor.
 *
 * <p>The total retry duration ({@code commit.retry.total-timeout-ms} and similar) and the maximum
 * number of attempts remain {@link Tasks} concerns; a strategy only computes the per-attempt wait.
 *
 * <p>Implementations must be thread-safe: the parallel {@link Tasks} execution path shares a single
 * strategy instance across worker threads, and a single instance may be reused across many retried
 * items.
 */
public interface BackoffStrategy {

  /**
   * Returns the time in milliseconds to wait before the given retry attempt.
   *
   * @param attempt the 1-based attempt number that is about to be retried; callers always pass a
   *     value {@code >= 1} (the first retry, after the initial failure, is attempt 1), and
   *     strategies need not handle {@code 0} or negative inputs
   * @return the wait time in milliseconds, including any jitter the strategy chooses to apply
   */
  long computeBackoff(int attempt);

  /**
   * Initializes the strategy from catalog or table properties.
   *
   * <p>Called once immediately after construction when the strategy is loaded reflectively by
   * {@link BackoffStrategies}. The default implementation does nothing.
   *
   * @param properties the properties the strategy was selected from
   */
  default void initialize(Map<String, String> properties) {}
}
