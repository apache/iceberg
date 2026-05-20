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

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.junit.jupiter.api.Test;

public class TestCacheMetricsReport {
  @Test
  public void testNoInputStats() {
    CacheMetricsReport cacheMetrics = CacheMetricsReport.of(Caffeine.newBuilder().build().stats());

    assertThat(cacheMetrics.hitCount()).isZero();
    assertThat(cacheMetrics.missCount()).isZero();
    assertThat(cacheMetrics.evictionCount()).isZero();
  }

  @Test
  public void testCacheMetricsFromCaffeineCache() {
    int maxTotalWeight = 300;

    Cache<Integer, Integer> inputCache =
        Caffeine.newBuilder()
            .maximumWeight(maxTotalWeight)
            .weigher((Weigher<Integer, Integer>) (key, value) -> value * 100)
            .recordStats()
            .build();

    inputCache.get(1, key -> key);
    inputCache.get(1, key -> key);
    inputCache.get(2, key -> key);
    inputCache.get(3, key -> key); // This evicts the other entries due to max weight

    inputCache.cleanUp();

    CacheMetricsReport cacheMetrics = CacheMetricsReport.of(inputCache.stats());

    assertThat(cacheMetrics.hitCount()).isOne();
    assertThat(cacheMetrics.missCount()).isEqualTo(3);
    assertThat(cacheMetrics.evictionCount()).isEqualTo(2);
  }
}
