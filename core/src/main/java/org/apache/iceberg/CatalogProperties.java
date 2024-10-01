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

import java.util.concurrent.TimeUnit;

public class CatalogProperties {

  private CatalogProperties() {}

  public static final String CATALOG_IMPL = "catalog-impl";
  public static final String FILE_IO_IMPL = "io-impl";
  public static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";
  public static final String WAREHOUSE_LOCATION = "warehouse";
  public static final String TABLE_DEFAULT_PREFIX = "table-default.";
  public static final String TABLE_OVERRIDE_PREFIX = "table-override.";
  public static final String METRICS_REPORTER_IMPL = "metrics-reporter-impl";

  /**
   * Controls whether the catalog will cache table entries upon load.
   *
   * <p>If {@link #CACHE_EXPIRATION_INTERVAL_MS} is set to zero, this value will be ignored and the
   * cache will be disabled.
   */
  public static final String CACHE_ENABLED = "cache-enabled";

  public static final boolean CACHE_ENABLED_DEFAULT = true;

  /** Controls whether the caching catalog will cache table entries using case sensitive keys. */
  public static final String CACHE_CASE_SENSITIVE = "cache.case-sensitive";

  public static final boolean CACHE_CASE_SENSITIVE_DEFAULT = true;

  /**
   * Controls the duration for which entries in the catalog are cached.
   *
   * <p>Behavior of specific values of cache.expiration-interval-ms:
   *
   * <ul>
   *   <li>Zero - Caching and cache expiration are both disabled
   *   <li>Negative Values - Cache expiration is turned off and entries expire only on refresh etc
   *   <li>Positive Values - Cache entries expire if not accessed via the cache after this many
   *       milliseconds
   * </ul>
   */
  public static final String CACHE_EXPIRATION_INTERVAL_MS = "cache.expiration-interval-ms";

  public static final long CACHE_EXPIRATION_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(30);
  public static final long CACHE_EXPIRATION_INTERVAL_MS_OFF = -1;

  /**
   * Controls whether to use caching during manifest reads or not.
   *
   * <p>Enabling manifest file caching require the following configuration constraints to be true:
   *
   * <ul>
   *   <li>{@link #IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS} must be a non-negative value.
   *   <li>{@link #IO_MANIFEST_CACHE_MAX_TOTAL_BYTES} must be a positive value.
   *   <li>{@link #IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH} must be a positive value.
   * </ul>
   */
  public static final String IO_MANIFEST_CACHE_ENABLED = "io.manifest.cache-enabled";

  public static final boolean IO_MANIFEST_CACHE_ENABLED_DEFAULT = false;

  /**
   * Controls the maximum duration for which an entry stays in the manifest cache.
   *
   * <p>Must be a non-negative value. Following are specific behaviors of this config:
   *
   * <ul>
   *   <li>Zero - Cache entries expires only if it gets evicted due to memory pressure from {@link
   *       #IO_MANIFEST_CACHE_MAX_TOTAL_BYTES} setting.
   *   <li>Positive Values - Cache entries expire if not accessed via the cache after this many
   *       milliseconds
   * </ul>
   */
  public static final String IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS =
      "io.manifest.cache.expiration-interval-ms";

  public static final long IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT =
      TimeUnit.SECONDS.toMillis(60);

  /**
   * Controls the maximum total amount of bytes to cache in manifest cache.
   *
   * <p>Must be a positive value.
   */
  public static final String IO_MANIFEST_CACHE_MAX_TOTAL_BYTES =
      "io.manifest.cache.max-total-bytes";

  public static final long IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT = 100 * 1024 * 1024;

  /**
   * Controls the maximum length of file to be considered for caching.
   *
   * <p>An {@link org.apache.iceberg.io.InputFile} will not be cached if the length is longer than
   * this limit. Must be a positive value.
   */
  public static final String IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH =
      "io.manifest.cache.max-content-length";

  public static final long IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT = 8 * 1024 * 1024;

  public static final String URI = "uri";
  public static final String CLIENT_POOL_SIZE = "clients";
  public static final int CLIENT_POOL_SIZE_DEFAULT = 2;
  public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS =
      "client.pool.cache.eviction-interval-ms";
  public static final long CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT =
      TimeUnit.MINUTES.toMillis(5);

  /**
   * A comma separated list of elements used, in addition to the {@link #URI}, to compose the key of
   * the client pool cache.
   *
   * <p>Supported key elements in a Catalog are implementation-dependent.
   */
  public static final String CLIENT_POOL_CACHE_KEYS = "client-pool-cache-keys";

  public static final String LOCK_IMPL = "lock-impl";

  public static final String LOCK_HEARTBEAT_INTERVAL_MS = "lock.heartbeat-interval-ms";
  public static final long LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(3);

  public static final String LOCK_HEARTBEAT_TIMEOUT_MS = "lock.heartbeat-timeout-ms";
  public static final long LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT = TimeUnit.SECONDS.toMillis(15);

  public static final String LOCK_HEARTBEAT_THREADS = "lock.heartbeat-threads";
  public static final int LOCK_HEARTBEAT_THREADS_DEFAULT = 4;

  public static final String LOCK_ACQUIRE_INTERVAL_MS = "lock.acquire-interval-ms";
  public static final long LOCK_ACQUIRE_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(5);

  public static final String LOCK_ACQUIRE_TIMEOUT_MS = "lock.acquire-timeout-ms";
  public static final long LOCK_ACQUIRE_TIMEOUT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(3);

  public static final String LOCK_TABLE = "lock.table";

  public static final String APP_ID = "app-id";
  public static final String USER = "user";

  public static final String AUTH_SESSION_TIMEOUT_MS = "auth.session-timeout-ms";
  public static final long AUTH_SESSION_TIMEOUT_MS_DEFAULT = TimeUnit.HOURS.toMillis(1);

  public static final String ENCRYPTION_KMS_TYPE = "encryption.kms-type";
  public static final String ENCRYPTION_KMS_IMPL = "encryption.kms-impl";
}
