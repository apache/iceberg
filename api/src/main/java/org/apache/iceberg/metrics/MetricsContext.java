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
import java.util.Map;
import java.util.Optional;

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
  }

  default void initialize(Map<String, String> properties) {}

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
     */
    default Optional<T> count() {
      return Optional.empty();
    }

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
   */
  default <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
    throw new UnsupportedOperationException("Counter is not supported.");
  }

  /**
   * Utility method for producing no metrics.
   *
   * @return a non-recording metrics context
   */
  static MetricsContext nullMetrics() {
    return new MetricsContext() {
      @Override
      public <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
        return new Counter<T>() {
          @Override
          public void increment() {}

          @Override
          public void increment(T amount) {}
        };
      }
    };
  }
}
