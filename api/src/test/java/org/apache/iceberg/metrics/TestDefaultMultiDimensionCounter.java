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

public class TestDefaultMultiDimensionCounter {
  static final String COUNTER_NAME1 = "counter-name1";
  static final String COUNTER_NAME2 = "counter-name2";
  static final DefaultMetricTag KEY1 = new DefaultMetricTag("key1");
  static final DefaultMetricTag KEY1_UPPER = new DefaultMetricTag("KEY1");

  @Test
  public void nullCheck() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    Assertions.assertThatThrownBy(() -> metricsContext.multiCounter(null, Unit.COUNT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid name: null");

    Assertions.assertThatThrownBy(() -> new DefaultMetricsContext().multiCounter("test", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid count unit: null");
  }

  @Test
  public void noop() {
    Assertions.assertThat(DefaultMultiDimensionCounter.NOOP.unit()).isEqualTo(Unit.UNDEFINED);
    Assertions.assertThat(DefaultMultiDimensionCounter.NOOP.isNoop()).isTrue();
    Assertions.assertThatThrownBy(
            () -> DefaultMultiDimensionCounter.NOOP.value(new DefaultMetricTag("key")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("NOOP multi-dimension counter has no value");
    Assertions.assertThatThrownBy(DefaultMultiDimensionCounter.NOOP::name)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("NOOP multi-dimension counter has no name");
    Assertions.assertThatThrownBy(DefaultMultiDimensionCounter.NOOP::metricTags)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("NOOP multi-dimension counter does not have keys");
  }

  @Test
  public void countWithSingleDimension() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    MultiDimensionCounter counter1 =
        new DefaultMultiDimensionCounter(COUNTER_NAME1, Unit.BYTES, metricsContext);
    Assertions.assertThat(counter1.name()).isEqualTo(COUNTER_NAME1);
    Assertions.assertThat(counter1.unit()).isEqualTo(MetricsContext.Unit.BYTES);
    Assertions.assertThat(counter1.isNoop()).isFalse();

    counter1.increment(KEY1);
    counter1.increment(KEY1, 2L);
    Assertions.assertThat(counter1.value(KEY1)).isEqualTo(3L);

    counter1.increment(KEY1_UPPER, 10L);
    Assertions.assertThat(counter1.value(KEY1)).isEqualTo(3L);
    Assertions.assertThat(counter1.value(KEY1_UPPER)).isEqualTo(10L);

    // Test counter with different name but using same keys
    MultiDimensionCounter counter2 =
        new DefaultMultiDimensionCounter(COUNTER_NAME2, Unit.BYTES, metricsContext);
    Assertions.assertThat(counter2.name()).isEqualTo(COUNTER_NAME2);
    counter2.increment(KEY1, 100L);
    Assertions.assertThat(counter2.value(KEY1)).isEqualTo(100L);
    Assertions.assertThat(counter1.value(KEY1)).isEqualTo(3L);

    // Test counter with same name and same keys
    MultiDimensionCounter counter3 =
        new DefaultMultiDimensionCounter(COUNTER_NAME1, Unit.BYTES, metricsContext);
    Assertions.assertThat(counter3.name()).isEqualTo(COUNTER_NAME1);
    counter3.increment(KEY1, 1000L);
    Assertions.assertThat(counter3.value(KEY1)).isEqualTo(1000L);
    Assertions.assertThat(counter1.value(KEY1)).isEqualTo(3L);

    Assertions.assertThatThrownBy(() -> counter1.increment(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key: null");
  }

  @Test
  public void countWithMultiDimensions() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    MultiDimensionCounter counter = metricsContext.multiCounter(COUNTER_NAME1, Unit.COUNT, 3);
    final MetricTag key3D = new DefaultMetricTag("1d_key", "2d_key", "3d_key");

    counter.increment(key3D);
    Assertions.assertThat(counter.value(key3D)).isEqualTo(1);

    counter.increment(key3D, 10L);
    Assertions.assertThat(counter.value(key3D)).isEqualTo(11L);

    // The order of dimensions matter
    final MetricTag key3DMixed = new DefaultMetricTag("2d_key", "3d_key", "1d_key");
    counter.increment(key3DMixed, 100L);
    Assertions.assertThat(counter.value(key3DMixed)).isEqualTo(100L);
    Assertions.assertThat(counter.value(key3D)).isEqualTo(11L);
  }

  @Test
  public void dimensionMismatch() {
    MetricsContext metricsContext = new DefaultMetricsContext();
    MultiDimensionCounter counter1 = metricsContext.multiCounter(COUNTER_NAME1, Unit.COUNT);

    Assertions.assertThatThrownBy(() -> counter1.increment(new DefaultMetricTag("key1", "key2")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Number of dimensions mismatch between the provided tag and counter-name1. 2 vs 1");

    Assertions.assertThatThrownBy(() -> counter1.increment(new DefaultMetricTag("key1", "key2"), 5))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Number of dimensions mismatch between the provided tag and counter-name1. 2 vs 1");

    Assertions.assertThatThrownBy(() -> counter1.value(new DefaultMetricTag("key1", "key2")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Number of dimensions mismatch between the provided tag and counter-name1. 2 vs 1");

    MultiDimensionCounter counter2 = metricsContext.multiCounter(COUNTER_NAME1, Unit.COUNT, 3);
    Assertions.assertThatThrownBy(() -> counter2.increment(new DefaultMetricTag("key1")))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Number of dimensions mismatch between the provided tag and counter-name1. 1 vs 3");
  }
}
