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

import java.util.Set;
import org.apache.iceberg.metrics.MetricsContext.Unit;

/** Generalized multi-dimension Counter interface. */
public interface MultiDimensionCounter {

  /** Increment the counter under a given key by 1. */
  void increment(MetricTag tag);

  /** Increment the counter under a given key by the provided amount. */
  void increment(MetricTag tag, long amount);

  /**
   * The name of the multi-dimension counter.
   *
   * @return The name of the multi-dimension counter.
   */
  String name();

  /**
   * The keys used by this multi-dimension counter.
   *
   * @return The tags used by this multi-dimension counter.
   */
  Set<String> metricTags();

  /**
   * The unit of the counter.
   *
   * @return The unit of the counter.
   */
  Unit unit();

  /**
   * Reports the current count associated by the given key.
   *
   * @return The current count associated by the given key.
   */
  long value(MetricTag tag);

  /**
   * Determines whether this counter is a NOOP counter.
   *
   * @return Whether this counter is a NOOP counter.
   */
  default boolean isNoop() {
    return false;
  }
}
