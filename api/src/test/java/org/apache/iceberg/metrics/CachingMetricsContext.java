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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** A {@link MetricsContext} that hands out the same counter per name so tests can read it back. */
public class CachingMetricsContext extends DefaultMetricsContext {
  // The counter type is fully qualified: the inherited nested MetricsContext.Counter would
  // otherwise shadow the top-level org.apache.iceberg.metrics.Counter within this package.
  private final Map<String, org.apache.iceberg.metrics.Counter> counters = Maps.newConcurrentMap();

  @Override
  public org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
    return counters.computeIfAbsent(name, ignored -> super.counter(name, unit));
  }
}
