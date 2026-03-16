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

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.immutables.value.Value;

@Value.Immutable
public abstract class CacheMetricsReport implements MetricsReport {
  public abstract long hitCount();

  public abstract long missCount();

  public abstract long evictionCount();

  public static CacheMetricsReport of(CacheStats stats) {
    return ImmutableCacheMetricsReport.builder()
        .hitCount(stats.hitCount())
        .missCount(stats.missCount())
        .evictionCount(stats.evictionCount())
        .build();
  }
}
