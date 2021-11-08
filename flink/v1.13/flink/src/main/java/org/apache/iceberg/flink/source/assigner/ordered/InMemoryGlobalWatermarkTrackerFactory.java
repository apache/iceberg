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

package org.apache.iceberg.flink.source.assigner.ordered;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * GlobalWatermarkTracker Factory that caches the issued trackers. The caching is required because
 * we could have multiple Iceberg Sources (one for each kafka source) trying to create the
 * WatermarkTracker independently in parallel. This implementation ensures that there's only one
 * instance for a given partition type.
 */
public class InMemoryGlobalWatermarkTrackerFactory implements GlobalWatermarkTracker.Factory {

  private static final Map<String, GlobalWatermarkTracker<?>> trackerCache = Maps.newHashMap();
  // private final Supplier<Registry> registrySupplier;

  @SuppressWarnings("unchecked")
  public synchronized <PartitionT> GlobalWatermarkTracker<PartitionT> apply(String name) {
    return (GlobalWatermarkTracker<PartitionT>)
        trackerCache.computeIfAbsent(
            name, dontCare -> new InMemoryGlobalWatermarkTracker<>());
  }

  @VisibleForTesting
  static void clear() {
    trackerCache.clear();
  }
}
