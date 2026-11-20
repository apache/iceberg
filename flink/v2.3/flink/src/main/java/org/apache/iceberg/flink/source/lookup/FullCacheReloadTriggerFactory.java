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
package org.apache.iceberg.flink.source.lookup;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.TimedCacheReloadTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public class FullCacheReloadTriggerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(FullCacheReloadTriggerFactory.class);

  private static final List<ConfigOption<?>> RELOAD_OPTIONS =
      List.of(
          LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL,
          LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE,
          LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME,
          LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS);

  public static @Nullable CacheReloadTrigger create(Configuration lookupConf) {
    if (!lookupConf.contains(LookupOptions.FULL_CACHE_RELOAD_STRATEGY)) {
      warnIgnoredOptions(lookupConf);
      return null;
    }

    Configuration triggerConf = new Configuration(lookupConf);
    triggerConf.set(LookupOptions.CACHE_TYPE, LookupCacheType.FULL);

    return switch (lookupConf.get(LookupOptions.FULL_CACHE_RELOAD_STRATEGY)) {
      case PERIODIC -> PeriodicCacheReloadTrigger.fromConfig(triggerConf);
      case TIMED -> TimedCacheReloadTrigger.fromConfig(triggerConf);
    };
  }

  private static void warnIgnoredOptions(Configuration lookupConf) {
    List<String> ignored =
        RELOAD_OPTIONS.stream()
            .filter(lookupConf::contains)
            .map(ConfigOption::key)
            .collect(Collectors.toList());
    if (!ignored.isEmpty()) {
      LOG.warn(
          "Full cache will not be reloaded: {} is not set, so {} will be ignored",
          LookupOptions.FULL_CACHE_RELOAD_STRATEGY.key(),
          ignored);
    }
  }

  private FullCacheReloadTriggerFactory() {}
}
