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

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.TimedCacheReloadTrigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

@Internal
public class FullCacheReloadTriggerFactory {

  public static @Nullable CacheReloadTrigger create(Configuration lookupConf) {
    Set<String> configuredKeys = lookupConf.toMap().keySet();
    ReloadStrategy reloadStrategy = reloadStrategy(lookupConf, configuredKeys);
    if (reloadStrategy == null) {
      return null;
    }

    Configuration triggerConf = new Configuration(lookupConf);
    triggerConf.set(LookupOptions.CACHE_TYPE, LookupCacheType.FULL);
    triggerConf.set(LookupOptions.FULL_CACHE_RELOAD_STRATEGY, reloadStrategy);

    return switch (reloadStrategy) {
      case PERIODIC -> periodicTrigger(triggerConf, configuredKeys);
      case TIMED -> timedTrigger(triggerConf, configuredKeys);
    };
  }

  private static PeriodicCacheReloadTrigger periodicTrigger(
      Configuration triggerConf, Set<String> configuredKeys) {
    Preconditions.checkArgument(
        configuredKeys.contains(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key()),
        "%s must be set when %s is %s",
        LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key(),
        LookupOptions.FULL_CACHE_RELOAD_STRATEGY.key(),
        ReloadStrategy.PERIODIC);

    return PeriodicCacheReloadTrigger.fromConfig(triggerConf);
  }

  private static TimedCacheReloadTrigger timedTrigger(
      Configuration triggerConf, Set<String> configuredKeys) {
    Preconditions.checkArgument(
        configuredKeys.contains(LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME.key()),
        "%s must be set when %s is %s",
        LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME.key(),
        LookupOptions.FULL_CACHE_RELOAD_STRATEGY.key(),
        ReloadStrategy.TIMED);

    return TimedCacheReloadTrigger.fromConfig(triggerConf);
  }

  private static @Nullable ReloadStrategy reloadStrategy(
      Configuration lookupConf, Set<String> configuredKeys) {
    if (configuredKeys.contains(LookupOptions.FULL_CACHE_RELOAD_STRATEGY.key())) {
      return lookupConf.get(LookupOptions.FULL_CACHE_RELOAD_STRATEGY);
    }

    if (configuredKeys.contains(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key())) {
      return ReloadStrategy.PERIODIC;
    }

    if (configuredKeys.contains(LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME.key())
        || configuredKeys.contains(LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS.key())) {
      return ReloadStrategy.TIMED;
    }

    return null;
  }

  private FullCacheReloadTriggerFactory() {}
}
