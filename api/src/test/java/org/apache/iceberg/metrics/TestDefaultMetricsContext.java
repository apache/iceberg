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
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestDefaultMetricsContext {

  @Test
  public void unsupportedCounter() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    Assertions.assertThatThrownBy(() -> metricsContext.counter("test", Double.class, MetricsContext.Unit.COUNT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Counter for type java.lang.Double is not supported");
  }

  @Test
  public void intCounter() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    MetricsContext.Counter<Integer> counter = metricsContext.counter("test", Integer.class, MetricsContext.Unit.COUNT);
    counter.increment(5);
    Assertions.assertThat(counter.count()).isPresent().get().isEqualTo(5);
  }

  @Test
  public void intCounterOverflow() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    MetricsContext.Counter<Integer> counter = metricsContext.counter("test", Integer.class, MetricsContext.Unit.COUNT);
    counter.increment(Integer.MAX_VALUE);
    Assertions.assertThatThrownBy(counter::increment)
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("integer overflow");
    Assertions.assertThat(counter.count()).isPresent().get().isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void longCounter() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    MetricsContext.Counter<Long> counter = metricsContext.counter("test", Long.class, MetricsContext.Unit.COUNT);
    counter.increment(5L);
    Assertions.assertThat(counter.count()).isPresent().get().isEqualTo(5L);
  }

  @Test
  public void longCounterOverflow() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    MetricsContext.Counter<Long> counter = metricsContext.counter("test", Long.class, MetricsContext.Unit.COUNT);
    counter.increment(Long.MAX_VALUE);
    Assertions.assertThatThrownBy(counter::increment)
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("long overflow");
    Assertions.assertThat(counter.count()).isPresent().get().isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void timer() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    Timer timer = metricsContext.timer("test", TimeUnit.MICROSECONDS);
    timer.record(10, TimeUnit.MINUTES);
    Assertions.assertThat(timer.totalDuration()).isEqualTo(Duration.ofMinutes(10L));
  }
}
