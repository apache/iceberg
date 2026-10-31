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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.TimedCacheReloadTrigger;
import org.junit.jupiter.api.Test;

class TestFullCacheReloadTriggerFactory {

  @Test
  void reloadIsDisabledWhenNoReloadOptionIsConfigured() {
    assertThat(FullCacheReloadTriggerFactory.create(new Configuration())).isNull();
  }

  @Test
  void periodicReloadIsEnabledByInterval() {
    Configuration lookupConf = new Configuration();
    lookupConf.set(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL, Duration.ofMinutes(5));

    assertThat(FullCacheReloadTriggerFactory.create(lookupConf))
        .isInstanceOf(PeriodicCacheReloadTrigger.class);
  }

  @Test
  void timedReloadIsEnabledByLocalIsoTime() {
    Configuration lookupConf = new Configuration();
    lookupConf.set(LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME, "10:15");

    assertThat(FullCacheReloadTriggerFactory.create(lookupConf))
        .isInstanceOf(TimedCacheReloadTrigger.class);
  }

  @Test
  void timedReloadIsEnabledByOffsetIsoTime() {
    Configuration lookupConf = new Configuration();
    lookupConf.set(LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME, "10:15+07:00");

    assertThat(FullCacheReloadTriggerFactory.create(lookupConf))
        .isInstanceOf(TimedCacheReloadTrigger.class);
  }

  @Test
  void reloadStrategyOverridesTheStrategyOfTheConfiguredOptions() {
    Configuration lookupConf = new Configuration();
    lookupConf.set(LookupOptions.FULL_CACHE_RELOAD_STRATEGY, ReloadStrategy.TIMED);
    lookupConf.set(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL, Duration.ofMinutes(5));
    lookupConf.set(LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME, "10:15");

    assertThat(FullCacheReloadTriggerFactory.create(lookupConf))
        .isInstanceOf(TimedCacheReloadTrigger.class);
  }

  @Test
  void periodicReloadRequiresInterval() {
    Configuration lookupConf = new Configuration();
    lookupConf.set(LookupOptions.FULL_CACHE_RELOAD_STRATEGY, ReloadStrategy.PERIODIC);

    assertThatThrownBy(() -> FullCacheReloadTriggerFactory.create(lookupConf))
        .as("The periodic strategy needs an interval")
        .hasMessageContaining(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key());
  }

  @Test
  void timedReloadRequiresIsoTime() {
    Configuration lookupConf = new Configuration();
    lookupConf.set(LookupOptions.FULL_CACHE_RELOAD_STRATEGY, ReloadStrategy.TIMED);

    assertThatThrownBy(() -> FullCacheReloadTriggerFactory.create(lookupConf))
        .as("The timed strategy needs the time of day to reload at")
        .hasMessageContaining(LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME.key());
  }
}
