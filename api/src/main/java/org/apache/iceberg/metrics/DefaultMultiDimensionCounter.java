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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class DefaultMultiDimensionCounter implements MultiDimensionCounter {
  public static final DefaultMultiDimensionCounter NOOP =
      new DefaultMultiDimensionCounter("NOOP-MultiDimensionCounter", Unit.UNDEFINED, null) {
        @Override
        public String toString() {
          return "NOOP multi-dimension counter";
        }

        @Override
        public void increment(MetricTag tag) {}

        @Override
        public void increment(MetricTag tag, long amount) {}

        @Override
        public String name() {
          throw new UnsupportedOperationException("NOOP multi-dimension counter has no name");
        }

        @Override
        public Set<String> metricTags() {
          throw new UnsupportedOperationException(
              "NOOP multi-dimension counter does not have keys");
        }

        @Override
        public Unit unit() {
          return Unit.UNDEFINED;
        }

        @Override
        public long value(MetricTag tag) {
          throw new UnsupportedOperationException("NOOP multi-dimension counter has no value");
        }

        @Override
        public boolean isNoop() {
          return true;
        }
      };

  private Map<String, Counter> counters = Maps.newConcurrentMap();
  private final String name;
  private final MetricsContext metricsContext;
  private final Unit unit;
  private final int numDimensions;

  public DefaultMultiDimensionCounter(String name, Unit unit, MetricsContext metricsContext) {
    this(name, unit, metricsContext, 1);
  }

  public DefaultMultiDimensionCounter(
      String name, Unit unit, MetricsContext metricsContext, int numDimensions) {
    Preconditions.checkArgument(null != name, "Invalid name: null");
    Preconditions.checkArgument(null != unit, "Invalid count unit: null");

    this.name = name;
    this.metricsContext = metricsContext;
    this.unit = unit;
    this.numDimensions = numDimensions;
  }

  @Override
  public void increment(MetricTag tag) {
    increment(tag, 1);
  }

  @Override
  public void increment(MetricTag tag, long amount) {
    Preconditions.checkArgument(null != tag, "Invalid key: null");
    validateNumOfDimensions(tag);
    counters
        .computeIfAbsent(
            tag.get(),
            s -> {
              return metricsContext.counter(counterId(s));
            })
        .increment(amount);
  }

  private String counterId(String key) {
    return name + "-" + key;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Set<String> metricTags() {
    return counters.keySet();
  }

  @Override
  public Unit unit() {
    return unit;
  }

  @Override
  public long value(MetricTag tag) {
    validateNumOfDimensions(tag);
    if (!counters.containsKey(tag.get())) {
      return 0;
    }
    return counters.get(tag.get()).value();
  }

  private void validateNumOfDimensions(MetricTag tag) {
    if (tag.numDimensions() != numDimensions) {
      throw new RuntimeException(
          String.format(
              "Number of dimensions mismatch between the provided tag and %s. %s vs %s",
              name, tag.numDimensions(), numDimensions));
    }
  }
}
