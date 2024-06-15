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

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Generalized interface for creating telemetry related instances for tracking operations.
 * Implementations must take into account usage considerations like thread safety and serialization.
 */
public interface MetricsContext extends Serializable {
  enum Unit {
    UNDEFINED("undefined"),
    BYTES("bytes"),
    COUNT("count");

    private final String displayName;

    Unit(String displayName) {
      this.displayName = displayName;
    }

    public String displayName() {
      return displayName;
    }

    public static Unit fromDisplayName(String displayName) {
      Preconditions.checkArgument(null != displayName, "Invalid unit: null");
      try {
        return Unit.valueOf(displayName.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format("Invalid unit: %s", displayName), e);
      }
    }
  }

  default void initialize(Map<String, String> properties) {}

  /**
   * @deprecated will be removed in 2.0.0, use {@link org.apache.iceberg.metrics.Counter} instead.
   */
  @Deprecated
  interface Counter<T extends Number> {
    /** Increment the counter by a single whole number value (i.e. 1). */
    void increment();

    /**
     * Increment the counter by the provided amount.
     *
     * @param amount to be incremented
     */
    void increment(T amount);

    /**
     * Reporting count is optional if the counter is reporting externally.
     *
     * @return current count if available
     * @deprecated Use {@link Counter#value()}
     */
    @Deprecated
    default Optional<T> count() {
      return Optional.empty();
    }

    /**
     * Reports the current count.
     *
     * @return The current count
     */
    default T value() {
      throw new UnsupportedOperationException("Count is not supported.");
    }

    /**
     * The unit of the counter.
     *
     * @return The unit of the counter.
     */
    default Unit unit() {
      return Unit.UNDEFINED;
    }
  }

  /**
   * Get a named counter of a specific type. Metric implementations may impose restrictions on what
   * types are supported for specific counters.
   *
   * @param name name of the metric
   * @param type numeric type of the counter value
   * @param unit the unit designation of the metric
   * @return a counter implementation
   * @deprecated will be removed in 2.0.0, use {@link MetricsContext#counter(String, Unit)} instead.
   */
  @Deprecated
  default <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
    throw new UnsupportedOperationException("Counter is not supported.");
  }

  /**
   * Get a named counter.
   *
   * @param name The name of the counter
   * @param unit The unit designation of the counter
   * @return a {@link org.apache.iceberg.metrics.Counter} implementation
   */
  default org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
    throw new UnsupportedOperationException("Counter is not supported.");
  }

  /**
   * Get a named counter using {@link Unit#COUNT}
   *
   * @param name The name of the counter
   * @return a {@link org.apache.iceberg.metrics.Counter} implementation
   */
  default org.apache.iceberg.metrics.Counter counter(String name) {
    return counter(name, Unit.COUNT);
  }

  /**
   * Get a named multi-dimension counter
   *
   * @param name The name of the counter
   * @param unit The unit designation of the counter
   * @return a {@link MultiDimensionCounter} implementation
   */
  default MultiDimensionCounter multiCounter(String name, Unit unit) {
    throw new UnsupportedOperationException("MultiDimensionCounter is not supported.");
  }

  /**
   * Get a named multi-dimension counter
   *
   * @param name The name of the counter
   * @param unit The unit designation of the counter
   * @param numDimensions The number of dimensions in the multi-dimension counter
   * @return a {@link MultiDimensionCounter} implementation
   */
  default MultiDimensionCounter multiCounter(String name, Unit unit, int numDimensions) {
    throw new UnsupportedOperationException("MultiDimensionCounter is not supported.");
  }

  /**
   * Get a named timer.
   *
   * @param name name of the metric
   * @param unit the time unit designation of the metric
   * @return a timer implementation
   */
  default Timer timer(String name, TimeUnit unit) {
    throw new UnsupportedOperationException("Timer is not supported.");
  }

  default Histogram histogram(String name) {
    throw new UnsupportedOperationException("Histogram is not supported.");
  }

  /**
   * Utility method for producing no metrics.
   *
   * @return a non-recording metrics context
   */
  static MetricsContext nullMetrics() {
    return new MetricsContext() {

      @Override
      public Timer timer(String name, TimeUnit unit) {
        return Timer.NOOP;
      }

      @Override
      @SuppressWarnings("unchecked")
      public <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
        if (Integer.class.equals(type)) {
          return (Counter<T>)
              ((DefaultCounter) org.apache.iceberg.metrics.DefaultCounter.NOOP).asIntCounter();
        }

        if (Long.class.equals(type)) {
          return (Counter<T>)
              ((DefaultCounter) org.apache.iceberg.metrics.DefaultCounter.NOOP).asLongCounter();
        }

        throw new IllegalArgumentException(
            String.format("Counter for type %s is not supported", type.getName()));
      }

      @Override
      public org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
        return org.apache.iceberg.metrics.DefaultCounter.NOOP;
      }

      @Override
      public MultiDimensionCounter multiCounter(String name, Unit unit) {
        return DefaultMultiDimensionCounter.NOOP;
      }
    };
  }
}
