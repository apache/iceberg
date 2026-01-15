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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg Lookup 缓存组件，封装基于 Caffeine 的 LRU 缓存实现。
 *
 * <p>支持两种缓存模式：
 *
 * <ul>
 *   <li>PARTIAL 模式（点查缓存）：基于 LRU 策略的部分缓存，使用 Caffeine Cache
 *   <li>ALL 模式（全量缓存）：双缓冲机制，支持无锁刷新
 * </ul>
 *
 * <p>注意：缓存使用 {@link RowDataKey} 作为键，确保正确的 equals 和 hashCode 实现。
 */
@Internal
public class IcebergLookupCache implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergLookupCache.class);

  /** PARTIAL 模式下使用的 LRU 缓存，使用 RowDataKey 作为键 */
  private transient Cache<RowDataKey, List<RowData>> partialCache;

  /** ALL 模式下使用的双缓冲缓存（主缓存），使用 RowDataKey 作为键 */
  private final AtomicReference<Cache<RowDataKey, List<RowData>>> allCachePrimary;

  /** ALL 模式下使用的双缓冲缓存（备缓存），使用 RowDataKey 作为键 */
  private final AtomicReference<Cache<RowDataKey, List<RowData>>> allCacheSecondary;

  /** 缓存配置 */
  private final CacheConfig config;

  /** 缓存模式 */
  private final CacheMode cacheMode;

  /** 缓存模式枚举 */
  public enum CacheMode {
    /** 点查缓存模式，使用 LRU 策略 */
    PARTIAL,
    /** 全量缓存模式，使用双缓冲机制 */
    ALL
  }

  /** 缓存配置 */
  public static class CacheConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Duration ttl;
    private final long maxRows;

    private CacheConfig(Duration ttl, long maxRows) {
      this.ttl = ttl;
      this.maxRows = maxRows;
    }

    public Duration getTtl() {
      return ttl;
    }

    public long getMaxRows() {
      return maxRows;
    }

    public static Builder builder() {
      return new Builder();
    }

    /** Builder for CacheConfig */
    public static class Builder {
      private Duration ttl = Duration.ofMinutes(10);
      private long maxRows = 10000L;

      private Builder() {}

      public Builder ttl(Duration cacheTtl) {
        this.ttl = Preconditions.checkNotNull(cacheTtl, "TTL cannot be null");
        return this;
      }

      public Builder maxRows(long cacheMaxRows) {
        Preconditions.checkArgument(cacheMaxRows > 0, "maxRows must be positive");
        this.maxRows = cacheMaxRows;
        return this;
      }

      public CacheConfig build() {
        return new CacheConfig(ttl, maxRows);
      }
    }
  }

  /**
   * 创建 PARTIAL 模式的缓存实例
   *
   * @param config 缓存配置
   * @return 缓存实例
   */
  public static IcebergLookupCache createPartialCache(CacheConfig config) {
    return new IcebergLookupCache(CacheMode.PARTIAL, config);
  }

  /**
   * 创建 ALL 模式的缓存实例
   *
   * @param config 缓存配置
   * @return 缓存实例
   */
  public static IcebergLookupCache createAllCache(CacheConfig config) {
    return new IcebergLookupCache(CacheMode.ALL, config);
  }

  private IcebergLookupCache(CacheMode cacheMode, CacheConfig config) {
    this.cacheMode = Preconditions.checkNotNull(cacheMode, "Cache mode cannot be null");
    this.config = Preconditions.checkNotNull(config, "Cache config cannot be null");
    this.allCachePrimary = new AtomicReference<>();
    this.allCacheSecondary = new AtomicReference<>();
  }

  /** 初始化缓存，必须在使用前调用 */
  public void open() {
    if (cacheMode == CacheMode.PARTIAL) {
      this.partialCache = buildPartialCache();
      LOG.info(
          "Initialized PARTIAL lookup cache with ttl={}, maxRows={}",
          config.getTtl(),
          config.getMaxRows());
    } else {
      // ALL 模式下，初始化双缓冲
      this.allCachePrimary.set(buildAllCache());
      this.allCacheSecondary.set(buildAllCache());
      LOG.info("Initialized ALL lookup cache with double buffering");
    }
  }

  /** 关闭缓存，释放资源 */
  public void close() {
    if (partialCache != null) {
      partialCache.invalidateAll();
      partialCache = null;
    }
    Cache<RowDataKey, List<RowData>> primary = allCachePrimary.get();
    if (primary != null) {
      primary.invalidateAll();
      allCachePrimary.set(null);
    }
    Cache<RowDataKey, List<RowData>> secondary = allCacheSecondary.get();
    if (secondary != null) {
      secondary.invalidateAll();
      allCacheSecondary.set(null);
    }
    LOG.info("Closed lookup cache");
  }

  private Cache<RowDataKey, List<RowData>> buildPartialCache() {
    return Caffeine.newBuilder()
        .maximumSize(config.getMaxRows())
        .expireAfterWrite(config.getTtl())
        .build();
  }

  private Cache<RowDataKey, List<RowData>> buildAllCache() {
    // ALL 模式不限制大小，因为会加载全量数据
    return Caffeine.newBuilder().build();
  }

  /**
   * 从缓存中获取数据（PARTIAL 模式）
   *
   * @param key lookup 键（RowData）
   * @return 缓存中的数据，如果不存在返回 null
   */
  public List<RowData> get(RowData key) {
    Preconditions.checkState(cacheMode == CacheMode.PARTIAL, "get() is only for PARTIAL mode");
    Preconditions.checkNotNull(partialCache, "Cache not initialized, call open() first");
    return partialCache.getIfPresent(new RowDataKey(key));
  }

  /**
   * 向缓存中放入数据（PARTIAL 模式）
   *
   * @param key lookup 键（RowData）
   * @param value 数据列表
   */
  public void put(RowData key, List<RowData> value) {
    Preconditions.checkState(cacheMode == CacheMode.PARTIAL, "put() is only for PARTIAL mode");
    Preconditions.checkNotNull(partialCache, "Cache not initialized, call open() first");
    partialCache.put(new RowDataKey(key), value);
  }

  /**
   * 使指定键的缓存失效（PARTIAL 模式）
   *
   * @param key lookup 键（RowData）
   */
  public void invalidate(RowData key) {
    Preconditions.checkState(
        cacheMode == CacheMode.PARTIAL, "invalidate() is only for PARTIAL mode");
    Preconditions.checkNotNull(partialCache, "Cache not initialized, call open() first");
    partialCache.invalidate(new RowDataKey(key));
  }

  /** 使所有缓存失效 */
  public void invalidateAll() {
    if (cacheMode == CacheMode.PARTIAL && partialCache != null) {
      partialCache.invalidateAll();
    } else if (cacheMode == CacheMode.ALL) {
      Cache<RowDataKey, List<RowData>> primary = allCachePrimary.get();
      if (primary != null) {
        primary.invalidateAll();
      }
    }
  }

  /**
   * 从缓存中获取数据（ALL 模式）
   *
   * @param key lookup 键（RowData）
   * @return 缓存中的数据，如果不存在返回 null
   */
  public List<RowData> getFromAll(RowData key) {
    Preconditions.checkState(cacheMode == CacheMode.ALL, "getFromAll() is only for ALL mode");
    Cache<RowDataKey, List<RowData>> primary = allCachePrimary.get();
    Preconditions.checkNotNull(primary, "Cache not initialized, call open() first");
    RowDataKey wrappedKey = new RowDataKey(key);
    List<RowData> result = primary.getIfPresent(wrappedKey);
    LOG.debug("getFromAll: key={}, found={}", wrappedKey, result != null);
    return result;
  }

  /**
   * 刷新全量缓存（ALL 模式）
   *
   * <p>使用双缓冲机制，确保刷新期间查询不受影响：
   *
   * <ol>
   *   <li>将新数据加载到备缓存
   *   <li>原子交换主缓存和备缓存
   *   <li>清空旧的主缓存（现在是备缓存）
   * </ol>
   *
   * @param dataLoader 数据加载器，返回所有数据
   * @throws Exception 如果加载数据失败
   */
  public void refreshAll(Supplier<Collection<CacheEntry>> dataLoader) throws Exception {
    Preconditions.checkState(cacheMode == CacheMode.ALL, "refreshAll() is only for ALL mode");
    Preconditions.checkNotNull(allCachePrimary.get(), "Cache not initialized, call open() first");

    LOG.info("Starting full cache refresh with double buffering");

    try {
      // 获取备缓存
      Cache<RowDataKey, List<RowData>> secondary = allCacheSecondary.get();
      if (secondary == null) {
        secondary = buildAllCache();
        allCacheSecondary.set(secondary);
      }

      // 清空备缓存
      secondary.invalidateAll();

      // 加载新数据到备缓存
      Collection<CacheEntry> entries = dataLoader.get();
      for (CacheEntry entry : entries) {
        // 使用 RowDataKey 作为缓存的 key
        RowDataKey wrappedKey = new RowDataKey(entry.getKey());
        secondary.put(wrappedKey, entry.getValue());
        LOG.debug("Put to cache: key={}, valueCount={}", wrappedKey, entry.getValue().size());
      }

      LOG.info("Loaded {} entries to secondary cache", entries.size());

      // 原子交换主缓存和备缓存
      Cache<RowDataKey, List<RowData>> primary = allCachePrimary.get();
      allCachePrimary.set(secondary);
      allCacheSecondary.set(primary);

      // 清空旧的主缓存（现在是备缓存）
      primary.invalidateAll();

      LOG.info("Successfully refreshed full cache, swapped buffers");

    } catch (Exception e) {
      LOG.error("Failed to refresh full cache, keeping existing cache data", e);
      throw e;
    }
  }

  /**
   * 获取当前缓存大小
   *
   * @return 缓存中的条目数
   */
  public long size() {
    if (cacheMode == CacheMode.PARTIAL && partialCache != null) {
      return partialCache.estimatedSize();
    } else if (cacheMode == CacheMode.ALL) {
      Cache<RowDataKey, List<RowData>> primary = allCachePrimary.get();
      return primary != null ? primary.estimatedSize() : 0;
    }
    return 0;
  }

  /**
   * 获取缓存模式
   *
   * @return 缓存模式
   */
  public CacheMode getCacheMode() {
    return cacheMode;
  }

  /** 缓存条目，用于 ALL 模式的批量加载 */
  public static class CacheEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private final RowData key;
    private final List<RowData> value;

    public CacheEntry(RowData key, List<RowData> value) {
      this.key = key;
      this.value = value;
    }

    public RowData getKey() {
      return key;
    }

    public List<RowData> getValue() {
      return value;
    }
  }
}
