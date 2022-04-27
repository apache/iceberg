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

package org.apache.iceberg.flink.source;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class FlinkLookupOptions implements Serializable {
  public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS = ConfigOptions.key("lookup.cache.max-rows")
      .longType()
      .defaultValue(-1L)
      .withDescription(
          "The max number of rows of lookup cache, over this value, the oldest rows will be eliminated." +
              "\"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is specified.");
  public static final ConfigOption<Duration> LOOKUP_CACHE_TTL = ConfigOptions.key("lookup.cache.ttl")
      .durationType()
      .defaultValue(Duration.ofSeconds(10L))
      .withDescription("The cache time to live.");
  public static final ConfigOption<Boolean> LOOKUP_CACHE_IGNORE_EMPTY = ConfigOptions.key("lookup.cache.ignore-empty")
      .booleanType()
      .defaultValue(true)
      .withDescription("Ignore empty rows of lookup cache.");
  public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions.key("lookup.max-retries")
      .intType()
      .defaultValue(3)
      .withDescription("The max retry times if lookup failed.");
  public static final ConfigOption<Integer> LOOKUP_BASE_RETRY_BACKOFF_MILLS = ConfigOptions.key(
      "lookup.base-retry-backoff-mills")
      .intType()
      .defaultValue(100)
      .withDescription("The min retry-backoff times if lookup failed.");
  public static final ConfigOption<Integer> LOOKUP_MAX_RETRY_BACKOFF_MILLS = ConfigOptions.key(
      "lookup.max-retry-backoff-mills")
      .intType()
      .defaultValue(10000)
      .withDescription("The max retry-backoff times if lookup failed.");

  private final long cacheMaxSize;
  private final long cacheExpireMs;
  private final boolean cacheIgnoreEmpty;
  private final int maxRetries;
  private final int baseRetryBackoffMills;
  private final int maxRetryBackoffMills;

  FlinkLookupOptions(long cacheMaxSize,
      long cacheExpireMs,
      boolean cacheIgnoreEmpty,
      int maxRetries,
      int baseRetryBackoffMills,
      int maxRetryBackoffMills) {
    this.cacheMaxSize = cacheMaxSize;
    this.cacheExpireMs = cacheExpireMs;
    this.cacheIgnoreEmpty = cacheIgnoreEmpty;
    this.maxRetries = maxRetries;
    this.baseRetryBackoffMills = baseRetryBackoffMills;
    this.maxRetryBackoffMills = maxRetryBackoffMills;
  }

  public long getCacheMaxSize() {
    return cacheMaxSize;
  }

  public long getCacheExpireMs() {
    return cacheExpireMs;
  }

  public boolean isCacheIgnoreEmpty() {
    return cacheIgnoreEmpty;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public int getBaseRetryBackoffMills() {
    return baseRetryBackoffMills;
  }

  public int getMaxRetryBackoffMills() {
    return maxRetryBackoffMills;
  }

  public static FlinkLookupOptions valueOf(Map<String, String> tableOptions) {
    Configuration configuration = new Configuration();
    tableOptions.forEach(configuration::setString);

    long cacheMaxSize = configuration.get(LOOKUP_CACHE_MAX_ROWS);
    boolean cacheIgnoreEmpty = configuration.get(LOOKUP_CACHE_IGNORE_EMPTY).booleanValue();
    long cacheExpireMs = configuration.get(LOOKUP_CACHE_TTL).toMillis();
    Preconditions.checkArgument(cacheExpireMs > 0, LOOKUP_CACHE_TTL.key() + " cannot be less than or equal 0");

    int maxRetries = configuration.get(LOOKUP_MAX_RETRIES).intValue();
    int baseRetryBackoffMills = configuration.get(LOOKUP_BASE_RETRY_BACKOFF_MILLS).intValue();
    int maxRetryBackoffMills = configuration.get(LOOKUP_MAX_RETRY_BACKOFF_MILLS).intValue();
    Preconditions.checkArgument(maxRetries >= 0, LOOKUP_MAX_RETRIES.key() + " cannot be less than 0");
    Preconditions.checkArgument(baseRetryBackoffMills >= 0,
        LOOKUP_BASE_RETRY_BACKOFF_MILLS.key() + " cannot be less than or equal 0");
    Preconditions.checkArgument(maxRetryBackoffMills >= 0,
        LOOKUP_MAX_RETRY_BACKOFF_MILLS.key() + " cannot be less than or equal 0");

    return new FlinkLookupOptions(cacheMaxSize, cacheExpireMs, cacheIgnoreEmpty,
        maxRetries, baseRetryBackoffMills, maxRetryBackoffMills);
  }
}
