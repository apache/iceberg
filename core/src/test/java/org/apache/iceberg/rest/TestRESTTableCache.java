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

import static org.apache.iceberg.rest.RESTTableCache.SessionIdTableId;
import static org.apache.iceberg.rest.RESTTableCache.TableCacheEntry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.util.FakeTicker;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestRESTTableCache {

  private static final String SESSION_ID = SessionCatalog.SessionContext.createEmpty().sessionId();
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("ns", TABLE_NAME);
  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newTableMetadata(
          new Schema(), PartitionSpec.unpartitioned(), "file:///tmp/test", ImmutableMap.of());
  private static final Map<String, String> TABLE_CONF = ImmutableMap.of("conf1", "abcd");
  private static final Credential CREDENTIAL =
      ImmutableCredential.builder().prefix("pre").putConfig("conf2", "xyz").build();
  private static final List<Credential> CREDENTIALS = ImmutableList.of(CREDENTIAL);
  private static final RESTClient TABLE_CLIENT = Mockito.mock(RESTClient.class);
  private static final String ETAG = "d7sa6das";
  private static final Duration HALF_OF_TABLE_EXPIRATION =
      Duration.ofMillis(RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS_DEFAULT)
          .dividedBy(2);

  private void putEntry(RESTTableCache cache, String sessionId, TableIdentifier identifier) {
    cache.put(sessionId, identifier, TABLE_METADATA, TABLE_CLIENT, TABLE_CONF, CREDENTIALS, ETAG);
  }

  @Test
  public void invalidProperties() {
    assertThatThrownBy(
            () ->
                new RESTTableCache(
                    Map.of(RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS, "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid expire after write: zero or negative");

    assertThatThrownBy(
            () ->
                new RESTTableCache(
                    Map.of(RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS, "-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid expire after write: zero or negative");

    assertThatThrownBy(
            () -> new RESTTableCache(Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_ENTRIES, "-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid max entries: negative");
  }

  @Test
  public void basicPutAndGet() {
    RESTTableCache cache = new RESTTableCache(Map.of());
    putEntry(cache, SESSION_ID, TABLE_IDENTIFIER);

    assertThat(cache.cache().asMap()).hasSize(1);
    assertThat(cache.cache().asMap())
        .containsKeys(SessionIdTableId.of(SESSION_ID, TABLE_IDENTIFIER));

    TableCacheEntry entry = cache.getIfPresent(SESSION_ID, TABLE_IDENTIFIER);

    assertThat(entry.tableMetadata()).isSameAs(TABLE_METADATA);
    assertThat(entry.tableClient()).isSameAs(TABLE_CLIENT);
    assertThat(entry.tableConf()).isEqualTo(TABLE_CONF);
    assertThat(entry.credentials()).isEqualTo(CREDENTIALS);
    assertThat(entry.eTag()).isSameAs(ETAG);
  }

  @Test
  public void notFoundInCache() {
    RESTTableCache cache = new RESTTableCache(Map.of());
    putEntry(cache, SESSION_ID, TABLE_IDENTIFIER);

    assertThat(cache.getIfPresent("some_id", TABLE_IDENTIFIER)).isNull();
    assertThat(cache.getIfPresent(SESSION_ID, TableIdentifier.of("ns", "other_table"))).isNull();
  }

  @Test
  public void tableInMultipleSessions() {
    RESTTableCache cache = new RESTTableCache(Map.of());
    String otherSessionId = "sessionID2";
    putEntry(cache, SESSION_ID, TABLE_IDENTIFIER);
    putEntry(cache, otherSessionId, TABLE_IDENTIFIER);

    assertThat(cache.cache().asMap()).hasSize(2);

    cache.invalidate(SESSION_ID, TABLE_IDENTIFIER);
    TableCacheEntry entry = cache.getIfPresent(otherSessionId, TABLE_IDENTIFIER);
    cache.cache().cleanUp();

    assertThat(cache.cache().asMap()).hasSize(1);
    assertThat(cache.getIfPresent(SESSION_ID, TABLE_IDENTIFIER)).isNull();
    assertThat(entry.tableMetadata()).isSameAs(TABLE_METADATA);
    assertThat(entry.tableClient()).isSameAs(TABLE_CLIENT);
    assertThat(entry.eTag()).isSameAs(ETAG);
  }

  @Test
  public void maxEntriesReached() {
    RESTTableCache cache = new RESTTableCache(Map.of());
    // Add more items than the max limit
    for (int i = 0; i < RESTCatalogProperties.TABLE_CACHE_MAX_ENTRIES_DEFAULT + 10; ++i) {
      putEntry(cache, SESSION_ID, TableIdentifier.of("ns", "tbl" + i));
    }
    cache.cache().cleanUp();

    assertThat(cache.cache().asMap())
        .hasSize(RESTCatalogProperties.TABLE_CACHE_MAX_ENTRIES_DEFAULT);
  }

  @Test
  public void configureMaxEntriesReached() {
    RESTTableCache cache =
        new RESTTableCache(Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_ENTRIES, "1"));
    TableIdentifier otherTableIdentifier = TableIdentifier.of("ns", "other_table");
    putEntry(cache, SESSION_ID, TABLE_IDENTIFIER);
    putEntry(cache, SESSION_ID, otherTableIdentifier);
    cache.cache().cleanUp();

    assertThat(cache.cache().asMap()).hasSize(1);
    assertThat(cache.getIfPresent(SESSION_ID, otherTableIdentifier)).isNotNull();
    assertThat(cache.getIfPresent(SESSION_ID, TABLE_IDENTIFIER)).isNull();
  }

  @Test
  public void cacheTurnedOff() {
    RESTTableCache cache =
        new RESTTableCache(Map.of(RESTCatalogProperties.TABLE_CACHE_MAX_ENTRIES, "0"));
    putEntry(cache, SESSION_ID, TABLE_IDENTIFIER);
    cache.cache().cleanUp();

    assertThat(cache.cache().asMap()).isEmpty();
  }

  @Test
  public void entryExpires() {
    FakeTicker ticker = new FakeTicker();
    RESTTableCache cache = new RESTTableCache(Map.of(), ticker);
    putEntry(cache, SESSION_ID, TABLE_IDENTIFIER);

    SessionIdTableId cacheKey = SessionIdTableId.of(SESSION_ID, TABLE_IDENTIFIER);
    assertThat(cache.cache().policy().expireAfterAccess()).isNotPresent();
    assertThat(cache.cache().policy().expireAfterWrite().get().ageOf(cacheKey))
        .isPresent()
        .get()
        .isEqualTo(Duration.ZERO);

    ticker.advance(HALF_OF_TABLE_EXPIRATION);

    assertThat(cache.cache().asMap()).containsOnlyKeys(cacheKey);
    assertThat(cache.cache().policy().expireAfterWrite().get().ageOf(cacheKey))
        .isPresent()
        .get()
        .isEqualTo(HALF_OF_TABLE_EXPIRATION);

    ticker.advance(HALF_OF_TABLE_EXPIRATION.plus(Duration.ofSeconds(10)));
    cache.cache().cleanUp();

    assertThat(cache.cache().asMap()).doesNotContainKey(cacheKey);
  }

  @Test
  public void configureExpiration() {
    FakeTicker ticker = new FakeTicker();
    Duration expirationInterval = Duration.ofSeconds(30);
    RESTTableCache cache =
        new RESTTableCache(
            Map.of(
                RESTCatalogProperties.TABLE_CACHE_EXPIRE_AFTER_WRITE_MS,
                String.valueOf(expirationInterval.toMillis())),
            ticker);
    putEntry(cache, SESSION_ID, TABLE_IDENTIFIER);

    assertThat(cache.getIfPresent(SESSION_ID, TABLE_IDENTIFIER)).isNotNull();

    ticker.advance(expirationInterval.plus(Duration.ofSeconds(10)));
    cache.cache().cleanUp();

    assertThat(cache.getIfPresent(SESSION_ID, TABLE_IDENTIFIER)).isNull();
  }
}
