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
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestDefaultCounter {

  @Test
  public void nullCheck() {
    Assertions.assertThatThrownBy(() -> new DefaultMetricsContext().counter("test", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid count unit: null");
  }

  @Test
  public void noop() {
    Assertions.assertThat(DefaultCounter.NOOP.unit()).isEqualTo(Unit.UNDEFINED);
    Assertions.assertThat(DefaultCounter.NOOP.isNoop()).isTrue();
    Assertions.assertThatThrownBy(DefaultCounter.NOOP::value)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("NOOP counter has no value");
  }

  @Test
  public void count() {
    Counter counter = new DefaultCounter(Unit.BYTES);
    counter.increment();
    counter.increment(5L);
    Assertions.assertThat(counter.value()).isEqualTo(6L);
    Assertions.assertThat(counter.unit()).isEqualTo(MetricsContext.Unit.BYTES);
    Assertions.assertThat(counter.isNoop()).isFalse();
  }

  @Test
  public void counterOverflow() {
    Counter counter = new DefaultCounter(MetricsContext.Unit.COUNT);
    counter.increment(Long.MAX_VALUE);
    Assertions.assertThatThrownBy(counter::increment)
        .isInstanceOf(ArithmeticException.class)
        .hasMessage("long overflow");
    Assertions.assertThat(counter.value()).isEqualTo(Long.MAX_VALUE);
  }
}
