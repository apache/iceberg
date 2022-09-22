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
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MultiDimensionCounterResult {
  public abstract Map<String, Long> counterResults();

  public abstract String name();

  public abstract Unit unit();

  @Value.Derived
  public long value(String key) {
    return counterResults().getOrDefault(key, 0L);
  }

  @Value.Derived
  public long totalValue() {
    return counterResults().values().stream().reduce(0L, Long::sum);
  }

  static MultiDimensionCounterResult fromCounter(MultiDimensionCounter multiCounter) {
    Preconditions.checkArgument(null != multiCounter, "Invalid counter: null");
    if (multiCounter.isNoop()) {
      return null;
    }

    Map<String, Long> result = Maps.newHashMap();
    for (String counterKey : multiCounter.metricTags()) {
      result.put(counterKey, multiCounter.value(new DefaultMetricTag(counterKey)));
    }
    return ImmutableMultiDimensionCounterResult.builder()
        .name(multiCounter.name())
        .unit(multiCounter.unit())
        .counterResults(result)
        .build();
  }

  static MultiDimensionCounterResult of(String name, Unit unit, Map<String, Long> counterResults) {
    return ImmutableMultiDimensionCounterResult.builder()
        .name(name)
        .unit(unit)
        .counterResults(counterResults)
        .build();
  }
}
