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
package org.apache.iceberg.aws.s3;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Test implementation of MetricsContext that maintains isolated counters for each test instance.
 * Used instead of Hadoop metrics to avoid shared state between test runs
 */
public class TestMetricsContext implements MetricsContext {
  private final Map<String, org.apache.iceberg.metrics.Counter> counters = Maps.newConcurrentMap();

  @Override
  public org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
    return counters.computeIfAbsent(name, k -> new LocalCounter());
  }

  private static class LocalCounter implements org.apache.iceberg.metrics.Counter {
    private final AtomicLong count = new AtomicLong(0);

    @Override
    public void increment() {
      increment(1L);
    }

    @Override
    public void increment(long amount) {
      count.addAndGet(amount);
    }

    @Override
    public long value() {
      return count.get();
    }
  }
}
