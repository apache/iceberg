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
package org.apache.iceberg.aws.lakeformation;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

class LakeFormationCredentialsCache
    implements LoadingCache<String, LakeFormationTemporaryCredentials> {
  private final LoadingCache<String, LakeFormationTemporaryCredentials> cache;
  private final Clock clock;
  private final Duration expiryLeadTimeForRefresh;

  LakeFormationCredentialsCache(
      LoadingCache<String, LakeFormationTemporaryCredentials> cache,
      long cacheRefreshLeadTimeInMillis) {
    this.cache = Preconditions.checkNotNull(cache, "Cache cannot be null");
    this.clock = Clock.systemUTC();
    this.expiryLeadTimeForRefresh = Duration.ofMillis(cacheRefreshLeadTimeInMillis);
  }

  public boolean needsRefresh(LakeFormationTemporaryCredentials credentials) {
    if (credentials == null) {
      return true;
    }

    return credentials
        .sessionCredentialsExpiration()
        .minus(expiryLeadTimeForRefresh)
        .isBefore(clock.instant());
  }

  @Override
  public LakeFormationTemporaryCredentials get(String key) {
    LakeFormationTemporaryCredentials credentials = cache.getIfPresent(key);
    if (needsRefresh(credentials)) {
      refresh(key);
    }
    return cache.get(key);
  }

  @Override
  public Map<String, LakeFormationTemporaryCredentials> getAll(Iterable<? extends String> keys) {
    return cache.getAll(keys);
  }

  @Override
  public void refresh(String key) {
    cache.refresh(key);
  }

  @Override
  public ConcurrentMap<String, LakeFormationTemporaryCredentials> asMap() {
    return cache.asMap();
  }

  @Override
  public void cleanUp() {
    cache.cleanUp();
  }

  @Override
  public @NonNull Policy<String, LakeFormationTemporaryCredentials> policy() {
    return null;
  }

  @Nullable
  @Override
  public LakeFormationTemporaryCredentials getIfPresent(Object key) {
    return cache.getIfPresent(key);
  }

  @Override
  public @org.checkerframework.checker.nullness.qual.Nullable LakeFormationTemporaryCredentials get(
      @NonNull String key,
      @NonNull
          Function<? super String, ? extends LakeFormationTemporaryCredentials> mappingFunction) {
    return null;
  }

  @Override
  public Map<String, LakeFormationTemporaryCredentials> getAllPresent(Iterable<?> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void put(String key, LakeFormationTemporaryCredentials value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends String, ? extends LakeFormationTemporaryCredentials> map) {
    cache.putAll(map);
  }

  @Override
  public void invalidate(Object key) {
    cache.invalidate(key);
  }

  @Override
  public void invalidateAll(Iterable<?> keys) {
    cache.invalidateAll(keys);
  }

  @Override
  public void invalidateAll() {
    cache.invalidateAll();
  }

  @Override
  public @NonNegative long estimatedSize() {
    return 0;
  }

  @Override
  public CacheStats stats() {
    return cache.stats();
  }
}
