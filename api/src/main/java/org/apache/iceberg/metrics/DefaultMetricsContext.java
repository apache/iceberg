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

import java.util.concurrent.TimeUnit;

/** A default {@link MetricsContext} implementation that uses native Java counters/timers. */
public class DefaultMetricsContext implements MetricsContext {

  @Override
  @Deprecated
  public <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
    if (Integer.class.equals(type)) {
      return (Counter<T>) new IntCounter(name, unit);
    }

    if (Long.class.equals(type)) {
      return (Counter<T>) new LongCounter(name, unit);
    }
    throw new IllegalArgumentException(
        String.format("Counter for type %s is not supported", type.getName()));
  }

  @Override
  public Timer timer(String name, TimeUnit unit) {
    return new DefaultTimer(name, unit);
  }

  @Override
  public org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
    return new DefaultCounter(name, unit);
  }
}
