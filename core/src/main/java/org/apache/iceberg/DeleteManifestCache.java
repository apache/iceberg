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
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Cache for parsed delete file entries from manifests to avoid redundant manifest parsing across
 * changelog scans.
 *
 * <p>This cache is thread-safe and uses LRU eviction with size and time-based expiration.
 */
class DeleteManifestCache {
  private static final int DEFAULT_MAX_ENTRIES = 1000;
  private static final int DEFAULT_EXPIRE_AFTER_ACCESS_MINUTES = 30;

  private static final DeleteManifestCache INSTANCE =
      new DeleteManifestCache(DEFAULT_MAX_ENTRIES, DEFAULT_EXPIRE_AFTER_ACCESS_MINUTES);

  private final Cache<ManifestCacheKey, List<DeleteFile>> cache;

  private DeleteManifestCache(int maxEntries, int expireAfterAccessMinutes) {
    this.cache =
        Caffeine.newBuilder()
            .maximumSize(maxEntries)
            .expireAfterAccess(expireAfterAccessMinutes, TimeUnit.MINUTES)
            .recordStats()
            .build();
  }

  static DeleteManifestCache instance() {
    return INSTANCE;
  }

  /**
   * Get cached delete files for a manifest.
   *
   * @param manifest the manifest file
   * @return list of delete files if cached, null otherwise
   */
  List<DeleteFile> get(ManifestFile manifest) {
    ManifestCacheKey key = ManifestCacheKey.of(manifest);
    return cache.getIfPresent(key);
  }

  /**
   * Cache delete files for a manifest.
   *
   * @param manifest the manifest file
   * @param deleteFiles the list of delete files to cache
   */
  void put(ManifestFile manifest, List<DeleteFile> deleteFiles) {
    ManifestCacheKey key = ManifestCacheKey.of(manifest);
    // Store an immutable copy to prevent external modifications
    cache.put(key, ImmutableList.copyOf(deleteFiles));
  }

  /**
   * Get cache statistics for monitoring.
   *
   * @return cache stats
   */
  com.github.benmanes.caffeine.cache.stats.CacheStats stats() {
    return cache.stats();
  }

  /** Invalidate all cache entries (useful for testing or explicit cache clearing). */
  void invalidateAll() {
    cache.invalidateAll();
  }

  /** Cache key based on manifest identity and content. */
  private static class ManifestCacheKey {
    private final String path;
    private final long length;
    private final Long snapshotId;
    private final int partitionSpecId;

    private ManifestCacheKey(String path, long length, Long snapshotId, int partitionSpecId) {
      this.path = path;
      this.length = length;
      this.snapshotId = snapshotId;
      this.partitionSpecId = partitionSpecId;
    }

    static ManifestCacheKey of(ManifestFile manifest) {
      return new ManifestCacheKey(
          manifest.path(), manifest.length(), manifest.snapshotId(), manifest.partitionSpecId());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ManifestCacheKey that = (ManifestCacheKey) o;
      return length == that.length
          && partitionSpecId == that.partitionSpecId
          && Objects.equals(path, that.path)
          && Objects.equals(snapshotId, that.snapshotId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(path, length, snapshotId, partitionSpecId);
    }
  }

  /**
   * Builder for creating cache instances with custom configuration (useful for testing or
   * specialized use cases).
   */
  static class Builder {
    private int maxEntries = DEFAULT_MAX_ENTRIES;
    private int expireAfterAccessMinutes = DEFAULT_EXPIRE_AFTER_ACCESS_MINUTES;

    Builder maxEntries(int max) {
      this.maxEntries = max;
      return this;
    }

    Builder expireAfterAccessMinutes(int minutes) {
      this.expireAfterAccessMinutes = minutes;
      return this;
    }

    DeleteManifestCache build() {
      return new DeleteManifestCache(maxEntries, expireAfterAccessMinutes);
    }
  }

  static Builder builder() {
    return new Builder();
  }
}
