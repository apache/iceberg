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
package org.apache.iceberg.rest;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RESTTableCache implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RESTTableCache.class);

  @Value.Immutable
  interface SessionIdTableId {
    String sessionId();

    TableIdentifier tableIdentifier();

    static SessionIdTableId of(String sessionId, TableIdentifier ident) {
      return ImmutableSessionIdTableId.builder()
          .sessionId(sessionId)
          .tableIdentifier(ident)
          .build();
    }
  }

  @Value.Immutable
  interface TableSupplierWithETag {
    Supplier<BaseTable> tableSupplier();

    String eTag();

    static TableSupplierWithETag of(Supplier<BaseTable> tableSupplier, String eTag) {
      return ImmutableTableSupplierWithETag.builder()
          .tableSupplier(tableSupplier)
          .eTag(eTag)
          .build();
    }
  }

  private final Cache<SessionIdTableId, TableSupplierWithETag> tableCache;

  RESTTableCache(Map<String, String> props) {
    this(props, Ticker.systemTicker());
  }

  @VisibleForTesting
  RESTTableCache(Map<String, String> props, Ticker ticker) {
    long expireAfterWriteMS =
        PropertyUtil.propertyAsLong(
            props,
            RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS,
            RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS_DEFAULT);
    Preconditions.checkArgument(
        expireAfterWriteMS > 0, "Invalid expire after write: zero or negative");

    long numEntries =
        PropertyUtil.propertyAsLong(
            props,
            RESTCatalogProperties.TABLE_CACHE_MAX_ENTRIES,
            RESTCatalogProperties.TABLE_CACHE_MAX_ENTRIES_DEFAULT);
    Preconditions.checkArgument(numEntries >= 0, "Invalid max entries: negative");

    tableCache =
        Caffeine.newBuilder()
            .maximumSize(numEntries)
            .expireAfterWrite(Duration.ofMillis(expireAfterWriteMS))
            .removalListener(
                (compositeKey, table, cause) ->
                    LOG.debug("Evicted {} from table cache ({})", compositeKey, cause))
            .recordStats()
            .ticker(ticker)
            .build();
  }

  public TableSupplierWithETag getIfPresent(String sessionId, TableIdentifier identifier) {
    SessionIdTableId cacheKey = SessionIdTableId.of(sessionId, identifier);
    return tableCache.getIfPresent(cacheKey);
  }

  public void put(
      String sessionId,
      TableIdentifier identifier,
      Supplier<BaseTable> tableSupplier,
      String eTag) {
    tableCache.put(
        SessionIdTableId.of(sessionId, identifier), TableSupplierWithETag.of(tableSupplier, eTag));
  }

  public void invalidate(String sessionId, TableIdentifier identifier) {
    SessionIdTableId cacheKey = SessionIdTableId.of(sessionId, identifier);
    tableCache.invalidate(cacheKey);
  }

  @VisibleForTesting
  Cache<SessionIdTableId, TableSupplierWithETag> tableCache() {
    return tableCache;
  }

  @Override
  public void close() {
    tableCache.invalidateAll();
    tableCache.cleanUp();
  }
}
