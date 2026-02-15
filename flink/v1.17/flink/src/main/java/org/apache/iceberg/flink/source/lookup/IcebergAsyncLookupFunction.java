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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg PARTIAL 模式异步 LookupFunction。
 *
 * <p>使用异步 IO 执行 Lookup 查询以提高吞吐量。
 *
 * <p>特性：
 *
 * <ul>
 *   <li>异步查询：使用线程池异步执行 Lookup 查询
 *   <li>并发控制：支持配置最大并发请求数
 *   <li>LRU 缓存：查询结果缓存到内存，支持 TTL 过期和最大行数限制
 *   <li>重试机制：支持配置最大重试次数
 * </ul>
 */
@Internal
public class IcebergAsyncLookupFunction extends AsyncLookupFunction {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergAsyncLookupFunction.class);

  // 配置
  private final TableLoader tableLoader;
  private final Schema projectedSchema;
  private final int[] lookupKeyIndices;
  private final String[] lookupKeyNames;
  private final boolean caseSensitive;
  private final Duration cacheTtl;
  private final long cacheMaxRows;
  private final int maxRetries;
  private final int asyncCapacity;

  // 运行时组件
  private transient IcebergLookupCache cache;
  private transient IcebergLookupReader reader;
  private transient ExecutorService executorService;
  private transient Semaphore semaphore;

  // Metrics
  private transient Counter lookupCounter;
  private transient Counter hitCounter;
  private transient Counter missCounter;
  private transient Counter retryCounter;
  private transient Counter asyncTimeoutCounter;
  private transient AtomicLong cacheSize;
  private transient AtomicLong pendingRequests;

  /**
   * 创建 IcebergAsyncLookupFunction 实例
   *
   * @param tableLoader 表加载器
   * @param projectedSchema 投影后的 Schema
   * @param lookupKeyIndices Lookup 键在投影 Schema 中的索引
   * @param lookupKeyNames Lookup 键的字段名称
   * @param caseSensitive 是否区分大小写
   * @param cacheTtl 缓存 TTL
   * @param cacheMaxRows 缓存最大行数
   * @param maxRetries 最大重试次数
   * @param asyncCapacity 异步查询最大并发数
   */
  public IcebergAsyncLookupFunction(
      TableLoader tableLoader,
      Schema projectedSchema,
      int[] lookupKeyIndices,
      String[] lookupKeyNames,
      boolean caseSensitive,
      Duration cacheTtl,
      long cacheMaxRows,
      int maxRetries,
      int asyncCapacity) {
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
    this.asyncCapacity = asyncCapacity;

    Preconditions.checkArgument(lookupKeyIndices.length > 0, "At least one lookup key is required");
    Preconditions.checkArgument(
        lookupKeyIndices.length == lookupKeyNames.length,
        "LookupKeyIndices and LookupKeyNames must have the same length");
    Preconditions.checkArgument(cacheMaxRows > 0, "CacheMaxRows must be positive");
    Preconditions.checkArgument(maxRetries >= 0, "MaxRetries must be non-negative");
    Preconditions.checkArgument(asyncCapacity > 0, "AsyncCapacity must be positive");
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);

    LOG.info(
        "Opening IcebergAsyncLookupFunction with cacheTtl: {}, cacheMaxRows: {}, maxRetries: {}, asyncCapacity: {}",
        cacheTtl,
        cacheMaxRows,
        maxRetries,
        asyncCapacity);

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

    // 初始化线程池
    this.executorService =
        Executors.newFixedThreadPool(
            Math.min(asyncCapacity, Runtime.getRuntime().availableProcessors() * 2),
            new ThreadFactoryBuilder()
                .setNameFormat("iceberg-async-lookup-%d")
                .setDaemon(true)
                .build());

    // 初始化信号量用于并发控制
    this.semaphore = new Semaphore(asyncCapacity);

    LOG.info("IcebergAsyncLookupFunction opened successfully");
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing IcebergAsyncLookupFunction");

    // 关闭线程池
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    // 关闭缓存
    if (cache != null) {
      cache.close();
    }

    // 关闭读取器
    if (reader != null) {
      reader.close();
    }

    super.close();
    LOG.info("IcebergAsyncLookupFunction closed");
  }

  /**
   * 异步 Lookup 方法，被 Flink 调用执行维表关联
   *
   * @param keyRow Lookup 键 RowData
   * @return 异步结果 CompletableFuture
   */
  @Override
  public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
    lookupCounter.inc();
    pendingRequests.incrementAndGet();

    // 提取 Lookup 键
    RowData lookupKey = extractLookupKey(keyRow);

    // 先查缓存
    List<RowData> cachedResults = cache.get(lookupKey);
    if (cachedResults != null) {
      hitCounter.inc();
      pendingRequests.decrementAndGet();
      return CompletableFuture.completedFuture(cachedResults);
    }

    missCounter.inc();

    // 创建异步 Future
    CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();

    // 异步执行查询
    executorService.execute(
        () -> {
          boolean acquired = false;
          try {
            // 获取信号量，控制并发
            acquired = semaphore.tryAcquire(30, TimeUnit.SECONDS);
            if (!acquired) {
              asyncTimeoutCounter.inc();
              LOG.warn("Async lookup timed out waiting for semaphore for key: {}", lookupKey);
              future.complete(Collections.emptyList());
              return;
            }

            // 执行带重试的查询
            List<RowData> results = lookupWithRetry(lookupKey);

            // 更新缓存
            cache.put(lookupKey, results != null ? results : Collections.emptyList());
            cacheSize.set(cache.size());

            // 完成 Future
            future.complete(results != null ? results : Collections.emptyList());

          } catch (Exception e) {
            LOG.error("Async lookup failed for key: {}", lookupKey, e);
            future.complete(Collections.emptyList());
          } finally {
            if (acquired) {
              semaphore.release();
            }
            pendingRequests.decrementAndGet();
          }
        });

    return future;
  }

  /** 初始化 Metrics */
  private void initMetrics(MetricGroup metricGroup) {
    MetricGroup lookupGroup = metricGroup.addGroup("iceberg").addGroup("lookup");

    this.lookupCounter = lookupGroup.counter("lookupCount");
    this.hitCounter = lookupGroup.counter("hitCount");
    this.missCounter = lookupGroup.counter("missCount");
    this.retryCounter = lookupGroup.counter("retryCount");
    this.asyncTimeoutCounter = lookupGroup.counter("asyncTimeoutCount");

    this.cacheSize = new AtomicLong(0);
    this.pendingRequests = new AtomicLong(0);

    lookupGroup.gauge("cacheSize", (Gauge<Long>) cacheSize::get);
    lookupGroup.gauge("pendingRequests", (Gauge<Long>) pendingRequests::get);
  }

  /** 从输入 RowData 中提取 Lookup 键 */
  private RowData extractLookupKey(RowData keyRow) {
    // keyRow 已经是 Lookup 键，直接返回
    // 但需要复制以避免重用问题
    int arity = keyRow.getArity();
    GenericRowData copy = new GenericRowData(arity);
    for (int i = 0; i < arity; i++) {
      if (!keyRow.isNullAt(i)) {
        // 简单复制，对于复杂类型可能需要深拷贝
        copy.setField(i, getFieldValue(keyRow, i));
      }
    }
    return copy;
  }

  /** 获取字段值 */
  private Object getFieldValue(RowData row, int index) {
    if (row.isNullAt(index)) {
      return null;
    }

    // 这里需要根据实际类型来获取值
    // 由于我们不知道具体类型，尝试使用 GenericRowData 的通用方法
    if (row instanceof GenericRowData) {
      return ((GenericRowData) row).getField(index);
    }

    // 对于其他类型，尝试常见类型
    Object result = tryGetString(row, index);
    if (result != null) {
      return result;
    }

    result = tryGetInt(row, index);
    if (result != null) {
      return result;
    }

    result = tryGetLong(row, index);
    if (result != null) {
      return result;
    }

    LOG.warn("Unable to get field value at index {}", index);
    return null;
  }

  private Object tryGetString(RowData row, int index) {
    try {
      return row.getString(index);
    } catch (Exception e) {
      LOG.trace("Not a String at index {}", index, e);
      return null;
    }
  }

  private Object tryGetInt(RowData row, int index) {
    try {
      return row.getInt(index);
    } catch (Exception e) {
      LOG.trace("Not an Int at index {}", index, e);
      return null;
    }
  }

  private Object tryGetLong(RowData row, int index) {
    try {
      return row.getLong(index);
    } catch (Exception e) {
      LOG.trace("Not a Long at index {}", index, e);
      return null;
    }
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
          LOG.debug("Retry attempt {} for async lookup key: {}", attempt, lookupKey);
          // 简单的退避策略
          Thread.sleep(Math.min(100 * attempt, 1000));
        }

        return reader.lookup(lookupKey);

      } catch (Exception e) {
        lastException = e;
        LOG.warn(
            "Async lookup failed for key: {}, attempt: {}/{}",
            lookupKey,
            attempt + 1,
            maxRetries + 1,
            e);
      }
    }

    // 所有重试都失败
    LOG.error(
        "All {} async lookup attempts failed for key: {}",
        maxRetries + 1,
        lookupKey,
        lastException);

    // 返回空列表而不是抛出异常，以保持作业运行
    return Collections.emptyList();
  }
}
