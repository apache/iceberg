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
package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.Optional;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * A wrapper around CachingCatalog that provides accessor methods to test the underlying cache,
 * without making those fields public in the CachingCatalog itself.
 */
public class TestableCachingCatalog extends CachingCatalog {

  public static TestableCachingCatalog wrap(
      Catalog catalog, Duration expirationInterval, Ticker ticker) {
    return new TestableCachingCatalog(
        catalog,
        true /* caseSensitive */,
        expirationInterval,
        ticker,
        Duration.ofMillis(
            CatalogProperties.CACHE_EXPIRATION_EXPIRE_AFTER_WRITE_INTERVAL_MS_DEFAULT));
  }

  public static TestableCachingCatalog wrap(
      Catalog catalog,
      Duration expirationInterval,
      Ticker ticker,
      Duration expireAfterWriteInterval) {
    return new TestableCachingCatalog(
        catalog, true /* caseSensitive */, expirationInterval, ticker, expireAfterWriteInterval);
  }

  private final Duration cacheExpirationInterval;
  private final Duration expireAfterWriteInterval;

  TestableCachingCatalog(
      Catalog catalog,
      boolean caseSensitive,
      Duration expirationInterval,
      Ticker ticker,
      Duration expireAfterWriteInterval) {
    super(
        catalog,
        caseSensitive,
        expirationInterval.toMillis(),
        ticker,
        expireAfterWriteInterval.toMillis());
    this.cacheExpirationInterval = expirationInterval;
    this.expireAfterWriteInterval = expireAfterWriteInterval;
  }

  public Cache<TableIdentifier, Table> cache() {
    // cleanUp must be called as tests apply assertions directly on the underlying map, but metadata
    // table
    // map entries are cleaned up asynchronously.
    tableCache.cleanUp();
    return tableCache;
  }

  public boolean isCacheExpirationEnabled() {
    return tableCache.policy().expireAfterAccess().isPresent()
        || tableCache.policy().expireAfterWrite().isPresent();
  }

  public Optional<Duration> ageOf(TableIdentifier identifier) {
    return tableCache.policy().expireAfterAccess().get().ageOf(identifier);
  }

  public Optional<Duration> remainingAgeFor(TableIdentifier identifier) {
    return ageOf(identifier).map(cacheExpirationInterval::minus);
  }
}
