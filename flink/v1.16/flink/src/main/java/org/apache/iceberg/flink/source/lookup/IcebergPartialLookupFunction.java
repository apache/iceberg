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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg PARTIAL 模式同步 LookupFunction。
 *
 * <p>按需从 Iceberg 表查询数据，使用 LRU 缓存优化查询性能。
 *
 * <p>特性：
 *
 * <ul>
 *   <li>按需查询：仅在查询时按需从 Iceberg 表读取匹配的记录
 *   <li>LRU 缓存：查询结果缓存到内存，支持 TTL 过期和最大行数限制
 *   <li>谓词下推：将 Lookup 键条件下推到 Iceberg 文件扫描层
 *   <li>重试机制：支持配置最大重试次数
 * </ul>
 */
@Internal
public class IcebergPartialLookupFunction extends TableFunction<RowData> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergPartialLookupFunction.class);

  // 配置
  private final TableLoader tableLoader;
  private final Schema projectedSchema;
  private final int[] lookupKeyIndices;
  private final String[] lookupKeyNames;
  private final boolean caseSensitive;
  private final Duration cacheTtl;
  private final long cacheMaxRows;
  private final int maxRetries;

  // 运行时组件
  private transient IcebergLookupCache cache;
  private transient IcebergLookupReader reader;

  // Metrics
  private transient Counter lookupCounter;
  private transient Counter hitCounter;
  private transient Counter missCounter;
  private transient Counter retryCounter;
  private transient AtomicLong cacheSize;

  /**
   * 创建 IcebergPartialLookupFunction 实例
   *
   * @param tableLoader 表加载器
   * @param projectedSchema 投影后的 Schema
   * @param lookupKeyIndices Lookup 键在投影 Schema 中的索引
   * @param lookupKeyNames Lookup 键的字段名称
   * @param caseSensitive 是否区分大小写
   * @param cacheTtl 缓存 TTL
   * @param cacheMaxRows 缓存最大行数
   * @param maxRetries 最大重试次数
   */
  public IcebergPartialLookupFunction(
      TableLoader tableLoader,
      Schema projectedSchema,
      int[] lookupKeyIndices,
      String[] lookupKeyNames,
      boolean caseSensitive,
      Duration cacheTtl,
      long cacheMaxRows,
      int maxRetries) {
    this.tableLoader = Preconditions.checkNotNull(tableLoader, "TableLoader cannot be null");
    this.projectedSchema =
        Preconditions.checkNotNull(projectedSchema, "ProjectedSchema cannot be null");
    this.lookupKeyIndices =
        Preconditions.checkNotNull(lookupKeyIndices, "LookupKeyIndices cannot be null");
    this.lookupKeyNames =
        Preconditions.checkNotNull(lookupKeyNames, "LookupKeyNames cannot be null");
    this.caseSensitive = caseSensitive;
    this.cacheTtl = Preconditions.checkNotNull(cacheTtl, "CacheTtl cannot be null");
    this.cacheMaxRows = cacheMaxRows;
    this.maxRetries = maxRetries;

    Preconditions.checkArgument(lookupKeyIndices.length > 0, "At least one lookup key is required");
    Preconditions.checkArgument(
        lookupKeyIndices.length == lookupKeyNames.length,
        "LookupKeyIndices and LookupKeyNames must have the same length");
    Preconditions.checkArgument(cacheMaxRows > 0, "CacheMaxRows must be positive");
    Preconditions.checkArgument(maxRetries >= 0, "MaxRetries must be non-negative");
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);

    LOG.info(
        "Opening IcebergPartialLookupFunction with cacheTtl: {}, cacheMaxRows: {}, maxRetries: {}",
        cacheTtl,
        cacheMaxRows,
        maxRetries);

    // 初始化 Metrics
    initMetrics(context.getMetricGroup());

    // 初始化缓存
    this.cache =
        IcebergLookupCache.createPartialCache(
            IcebergLookupCache.CacheConfig.builder().ttl(cacheTtl).maxRows(cacheMaxRows).build());
    cache.open();

    // 初始化读取器
    this.reader =
        new IcebergLookupReader(
            tableLoader, projectedSchema, lookupKeyIndices, lookupKeyNames, caseSensitive);
    reader.open();

    LOG.info("IcebergPartialLookupFunction opened successfully");
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing IcebergPartialLookupFunction");

    // 关闭缓存
    if (cache != null) {
      cache.close();
    }

    // 关闭读取器
    if (reader != null) {
      reader.close();
    }

    super.close();
    LOG.info("IcebergPartialLookupFunction closed");
  }

  /**
   * Lookup 方法，被 Flink 调用执行维表关联
   *
   * @param keys Lookup 键值（可变参数）
   */
  public void eval(Object... keys) {
    lookupCounter.inc();

    // 构造 Lookup 键 RowData
    RowData lookupKey = buildLookupKey(keys);

    // 先查缓存
    List<RowData> cachedResults = cache.get(lookupKey);
    if (cachedResults != null) {
      hitCounter.inc();
      for (RowData result : cachedResults) {
        collect(result);
      }
      return;
    }

    missCounter.inc();

    // 缓存未命中，从 Iceberg 读取
    List<RowData> results = lookupWithRetry(lookupKey);

    // 更新缓存（即使结果为空也要缓存，避免重复查询不存在的键）
    cache.put(lookupKey, results != null ? results : Collections.emptyList());
    cacheSize.set(cache.size());

    // 输出结果
    if (results != null) {
      for (RowData result : results) {
        collect(result);
      }
    }
  }

  /** 初始化 Metrics */
  private void initMetrics(MetricGroup metricGroup) {
    MetricGroup lookupGroup = metricGroup.addGroup("iceberg").addGroup("lookup");

    this.lookupCounter = lookupGroup.counter("lookupCount");
    this.hitCounter = lookupGroup.counter("hitCount");
    this.missCounter = lookupGroup.counter("missCount");
    this.retryCounter = lookupGroup.counter("retryCount");

    this.cacheSize = new AtomicLong(0);
    lookupGroup.gauge("cacheSize", (Gauge<Long>) cacheSize::get);
  }

  /** 构建 Lookup 键 RowData */
  private RowData buildLookupKey(Object[] keys) {
    GenericRowData keyRow = new GenericRowData(keys.length);
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] instanceof String) {
        keyRow.setField(i, StringData.fromString((String) keys[i]));
      } else {
        keyRow.setField(i, keys[i]);
      }
    }
    return keyRow;
  }

  /**
   * 带重试机制的 Lookup 查询
   *
   * @param lookupKey Lookup 键
   * @return 查询结果列表
   */
  private List<RowData> lookupWithRetry(RowData lookupKey) {
    Exception lastException = null;

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          retryCounter.inc();
          LOG.debug("Retry attempt {} for lookup key: {}", attempt, lookupKey);
          // 简单的退避策略
          Thread.sleep(Math.min(100 * attempt, 1000));
        }

        return reader.lookup(lookupKey);

      } catch (Exception e) {
        lastException = e;
        LOG.warn(
            "Lookup failed for key: {}, attempt: {}/{}", lookupKey, attempt + 1, maxRetries + 1, e);
      }
    }

    // 所有重试都失败
    LOG.error(
        "All {} lookup attempts failed for key: {}", maxRetries + 1, lookupKey, lastException);

    // 返回空列表而不是抛出异常，以保持作业运行
    return Collections.emptyList();
  }
}
