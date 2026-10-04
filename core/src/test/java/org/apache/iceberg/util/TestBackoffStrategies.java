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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestBackoffStrategies {

  @Test
  public void defaultExponentialBackoffMatchesLegacyFormula() {
    long min = 100;
    long max = 60_000;
    double scale = 2.0;
    ExponentialBackoffStrategy backoff = new ExponentialBackoffStrategy(min, max, scale);

    for (int attempt = 1; attempt <= 12; attempt++) {
      int delayMs = (int) Math.min(min * Math.pow(scale, attempt - 1), (double) max);
      long jitterBound = Math.max(1, (int) (delayMs * 0.1));
      long wait = backoff.computeBackoff(attempt);
      assertThat(wait)
          .isGreaterThanOrEqualTo((long) delayMs)
          .isLessThan((long) delayMs + jitterBound);
    }
  }

  @Test
  public void defaultExponentialBackoffRespectsMaxCap() {
    long max = 1000;
    ExponentialBackoffStrategy backoff = new ExponentialBackoffStrategy(100, max, 2.0);

    long wait = backoff.computeBackoff(50); // attempt large enough to exceed the cap
    long jitterBound = Math.max(1, (int) (max * 0.1));
    assertThat(wait).isGreaterThanOrEqualTo(max).isLessThan(max + jitterBound);
  }

  @Test
  public void fromReturnsNullWhenKeyAbsent() {
    assertThat(BackoffStrategies.from(null)).isNull();
    assertThat(BackoffStrategies.from(ImmutableMap.of())).isNull();
    assertThat(BackoffStrategies.from(ImmutableMap.of("other", "value"))).isNull();
  }

  @Test
  public void fromLoadsAndInitializesConfiguredStrategy() {
    Map<String, String> properties =
        ImmutableMap.of(
            BackoffStrategies.STRATEGY_IMPL,
            FixedBackoffStrategy.class.getName(),
            "delay-ms",
            "42");

    BackoffStrategy strategy = BackoffStrategies.from(properties);

    assertThat(strategy).isInstanceOf(FixedBackoffStrategy.class);
    assertThat(strategy.computeBackoff(1)).isEqualTo(42L);
    assertThat(strategy.computeBackoff(9)).isEqualTo(42L);
  }

  @Test
  public void loadBackoffStrategyRejectsMissingNoArgConstructor() {
    assertThatThrownBy(
            () ->
                BackoffStrategies.loadBackoffStrategy(
                    NoNoArgStrategy.class.getName(), ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing no-arg constructor");
  }

  @Test
  public void loadBackoffStrategyRejectsNonStrategyClass() {
    assertThatThrownBy(
            () -> BackoffStrategies.loadBackoffStrategy(String.class.getName(), ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not implement BackoffStrategy");
  }

  @Test
  public void loadBackoffStrategyRejectsUnknownClass() {
    assertThatThrownBy(
            () ->
                BackoffStrategies.loadBackoffStrategy(
                    "org.apache.iceberg.util.NotAClass", ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("BackoffStrategy");
  }

  @Test
  public void loadBackoffStrategyPropagatesInitializeFailures() {
    assertThatThrownBy(
            () ->
                BackoffStrategies.loadBackoffStrategy(
                    ThrowingInitStrategy.class.getName(), ImmutableMap.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("init failed");
  }

  @Test
  public void fromWithDefaultsReturnsExponentialWhenKeyAbsent() {
    BackoffStrategy strategy = BackoffStrategies.from(ImmutableMap.of(), 100, 1000, 2.0);

    assertThat(strategy).isInstanceOf(ExponentialBackoffStrategy.class);
    long wait = strategy.computeBackoff(1);
    assertThat(wait).isGreaterThanOrEqualTo(100L).isLessThan(110L);
  }

  @Test
  public void fromWithDefaultsReturnsCustomWhenKeySet() {
    Map<String, String> properties =
        ImmutableMap.of(
            BackoffStrategies.STRATEGY_IMPL,
            FixedBackoffStrategy.class.getName(),
            "delay-ms",
            "42");

    BackoffStrategy strategy = BackoffStrategies.from(properties, 100, 1000, 2.0);

    assertThat(strategy).isInstanceOf(FixedBackoffStrategy.class);
    assertThat(strategy.computeBackoff(1)).isEqualTo(42L);
  }

  @Test
  public void fromWithDefaultsAcceptsNullProperties() {
    BackoffStrategy strategy = BackoffStrategies.from(null, 100, 1000, 2.0);

    assertThat(strategy).isInstanceOf(ExponentialBackoffStrategy.class);
    long wait = strategy.computeBackoff(1);
    assertThat(wait).isGreaterThanOrEqualTo(100L).isLessThan(110L);
  }

  /** Test strategy with a public no-arg constructor that records its initialize properties. */
  public static class FixedBackoffStrategy implements BackoffStrategy {
    private long delayMs = 0;

    @Override
    public void initialize(Map<String, String> properties) {
      this.delayMs = Long.parseLong(properties.getOrDefault("delay-ms", "0"));
    }

    @Override
    public long computeBackoff(int attempt) {
      return delayMs;
    }
  }

  /** Test strategy without a no-arg constructor. */
  public static class NoNoArgStrategy implements BackoffStrategy {
    public NoNoArgStrategy(String required) {}

    @Override
    public long computeBackoff(int attempt) {
      return 0;
    }
  }

  /** Test strategy whose initialize() throws, to verify the loader propagates the failure. */
  public static class ThrowingInitStrategy implements BackoffStrategy {
    @Override
    public void initialize(Map<String, String> properties) {
      throw new IllegalStateException("init failed");
    }

    @Override
    public long computeBackoff(int attempt) {
      return 0;
    }
  }
}
