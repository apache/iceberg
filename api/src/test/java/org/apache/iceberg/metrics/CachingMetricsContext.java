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
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link MetricsContext} that returns the same {@link Counter} instance for a given name, so that
 * tests can observe the counters that the code under test increments. {@link DefaultMetricsContext}
 * allocates a fresh counter on every {@link #counter(String, Unit)} call, which would otherwise
 * hand the test and the code under test two independent counters.
 */
public class CachingMetricsContext extends DefaultMetricsContext {
  private final Map<String, Counter> counters = new ConcurrentHashMap<>();

  @Override
  public Counter counter(String name, Unit unit) {
    return counters.computeIfAbsent(name, ignored -> super.counter(name, unit));
  }
}
