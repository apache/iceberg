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
import org.apache.iceberg.common.DynConstructors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves and instantiates pluggable {@link BackoffStrategy} implementations. */
public class BackoffStrategies {
  private static final Logger LOG = LoggerFactory.getLogger(BackoffStrategies.class);

  /**
   * Property naming the {@link BackoffStrategy} implementation to use for retries. The value is a
   * fully-qualified class name with a public no-arg constructor. The same key is honored for every
   * retry site (commit, lock, status check, scan, token refresh, file cleanup). When unset, the
   * built-in exponential backoff is used.
   */
  public static final String STRATEGY_IMPL = "retry.strategy-impl";

  private BackoffStrategies() {}

  /**
   * Resolves the configured strategy from the given properties.
   *
   * @param properties properties that may contain {@link #STRATEGY_IMPL}
   * @return the configured strategy, or {@code null} when {@link #STRATEGY_IMPL} is not set (the
   *     caller then keeps the built-in exponential backoff)
   */
  public static BackoffStrategy from(Map<String, String> properties) {
    String impl = properties == null ? null : properties.get(STRATEGY_IMPL);
    if (impl == null) {
      return null;
    }

    return loadBackoffStrategy(impl, properties);
  }

  /**
   * Resolves the configured strategy from the given properties, falling back to a default {@link
   * ExponentialBackoffStrategy} configured from the provided min/max/scale parameters when {@link
   * #STRATEGY_IMPL} is not set. Never returns {@code null}.
   *
   * @param properties properties that may contain {@link #STRATEGY_IMPL}; may be {@code null}
   * @param defaultMinSleepTimeMs min sleep for the default exponential fallback
   * @param defaultMaxSleepTimeMs max sleep for the default exponential fallback
   * @param defaultScaleFactor scale factor for the default exponential fallback
   * @return the configured custom strategy if {@link #STRATEGY_IMPL} is set, otherwise a fresh
   *     {@link ExponentialBackoffStrategy} with the given parameters
   */
  public static BackoffStrategy from(
      Map<String, String> properties,
      long defaultMinSleepTimeMs,
      long defaultMaxSleepTimeMs,
      double defaultScaleFactor) {
    BackoffStrategy custom = from(properties);
    return custom != null
        ? custom
        : new ExponentialBackoffStrategy(
            defaultMinSleepTimeMs, defaultMaxSleepTimeMs, defaultScaleFactor);
  }

  /**
   * Loads and initializes a {@link BackoffStrategy} by class name.
   *
   * @param impl fully-qualified class name of a {@link BackoffStrategy} with a no-arg constructor
   * @param properties properties passed to {@link BackoffStrategy#initialize(Map)}
   * @return an initialized strategy instance
   */
  public static BackoffStrategy loadBackoffStrategy(String impl, Map<String, String> properties) {
    LOG.info("Loading custom BackoffStrategy implementation: {}", impl);
    DynConstructors.Ctor<BackoffStrategy> ctor;
    try {
      ctor =
          DynConstructors.builder(BackoffStrategy.class)
              .loader(BackoffStrategies.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize BackoffStrategy, missing no-arg constructor: %s", impl),
          e);
    }

    BackoffStrategy strategy;
    try {
      strategy = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize BackoffStrategy, %s does not implement BackoffStrategy.", impl),
          e);
    }

    strategy.initialize(properties);
    return strategy;
  }
}
