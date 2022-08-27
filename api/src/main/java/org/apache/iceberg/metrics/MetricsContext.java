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
import java.util.concurrent.TimeUnit;

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
      return Unit.valueOf(displayName.toUpperCase(Locale.ROOT));
    }
  }

  default void initialize(Map<String, String> properties) {}

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
      public org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
        return org.apache.iceberg.metrics.DefaultCounter.NOOP;
      }
    };
  }
}
