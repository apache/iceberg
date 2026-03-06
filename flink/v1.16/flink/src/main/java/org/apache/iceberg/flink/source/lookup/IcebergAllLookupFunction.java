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

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg ALL mode LookupFunction.
 *
 * <p>Load the entire Iceberg table into memory at job startup, and refresh periodically at
 * configured intervals.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Load all table data into memory at startup
 *   <li>Reload latest data periodically based on configured reload-interval
 *   <li>Use double buffering mechanism to ensure queries are not affected during refresh
 *   <li>Retain existing cache data and log errors on refresh failure
 * </ul>
 */
@Internal
public class IcebergAllLookupFunction extends TableFunction<RowData> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergAllLookupFunction.class);

  // 配置
  private final TableLoader tableLoader;
  private final Schema projectedSchema;
  private final int[] lookupKeyIndices;
  private final String[] lookupKeyNames;
  private final boolean caseSensitive;
  private final Duration reloadInterval;

  // 运行时组件
  private transient IcebergLookupCache cache;
  private transient IcebergLookupReader reader;
  private transient ScheduledExecutorService reloadExecutor;

  // Metrics
  private transient Counter lookupCounter;
  private transient Counter hitCounter;
  private transient Counter missCounter;
  private transient Counter refreshCounter;
  private transient Counter refreshFailedCounter;
  private transient AtomicLong cacheSize;
  private transient AtomicLong lastRefreshTime;

  /**
   * 创建 IcebergAllLookupFunction 实例
   *
   * @param tableLoader 表加载器
   * @param projectedSchema 投影后的 Schema
   * @param lookupKeyIndices Lookup 键在投影 Schema 中的索引
   * @param lookupKeyNames Lookup 键的字段名称
   * @param caseSensitive 是否区分大小写
   * @param reloadInterval 缓存刷新间隔
   */
  public IcebergAllLookupFunction(
      TableLoader tableLoader,
      Schema projectedSchema,
      int[] lookupKeyIndices,
      String[] lookupKeyNames,
      boolean caseSensitive,
      Duration reloadInterval) {
    this.tableLoader = Preconditions.checkNotNull(tableLoader, "TableLoader cannot be null");
    this.projectedSchema =
        Preconditions.checkNotNull(projectedSchema, "ProjectedSchema cannot be null");
    this.lookupKeyIndices =
        Preconditions.checkNotNull(lookupKeyIndices, "LookupKeyIndices cannot be null");
    this.lookupKeyNames =
        Preconditions.checkNotNull(lookupKeyNames, "LookupKeyNames cannot be null");
    this.caseSensitive = caseSensitive;
    this.reloadInterval =
        Preconditions.checkNotNull(reloadInterval, "ReloadInterval cannot be null");

    Preconditions.checkArgument(lookupKeyIndices.length > 0, "At least one lookup key is required");
    Preconditions.checkArgument(
        lookupKeyIndices.length == lookupKeyNames.length,
        "LookupKeyIndices and LookupKeyNames must have the same length");
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);

    LOG.info("Opening IcebergAllLookupFunction with reload interval: {}", reloadInterval);

    // 初始化 Metrics
    initMetrics(context.getMetricGroup());

    // 初始化缓存
    this.cache =
        IcebergLookupCache.createAllCache(
            IcebergLookupCache.CacheConfig.builder()
                .ttl(Duration.ofDays(365)) // ALL 模式不使用 TTL
                .maxRows(Long.MAX_VALUE)
                .build());
    cache.open();

    // 初始化读取器
    this.reader =
        new IcebergLookupReader(
            tableLoader, projectedSchema, lookupKeyIndices, lookupKeyNames, caseSensitive);
    reader.open();

    // 首次全量加载
    loadAllData();

    // 启动定期刷新任务
    startReloadScheduler();

    LOG.info("IcebergAllLookupFunction opened successfully");
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing IcebergAllLookupFunction");

    // 停止定期刷新任务
    if (reloadExecutor != null && !reloadExecutor.isShutdown()) {
      reloadExecutor.shutdown();
      try {
        if (!reloadExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          reloadExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        reloadExecutor.shutdownNow();
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
    LOG.info("IcebergAllLookupFunction closed");
  }

  /**
   * Lookup method, called by Flink to execute dimension table join
   *
   * @param keys lookup key values (variable arguments)
   */
  public void eval(Object... keys) {
    lookupCounter.inc();

    // Build lookup key RowData
    RowData lookupKey = buildLookupKey(keys);

    // Add debug logging
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Lookup eval: keys={}, keyTypes={}, lookupKey={}, cacheSize={}",
          java.util.Arrays.toString(keys),
          getKeyTypes(keys),
          lookupKey,
          cache.size());
    }

    // Query from cache
    List<RowData> results = cache.getFromAll(lookupKey);

    if (results != null && !results.isEmpty()) {
      hitCounter.inc();
      LOG.debug("Lookup hit: key={}, resultCount={}", lookupKey, results.size());
      for (RowData result : results) {
        collect(result);
      }
    } else {
      missCounter.inc();
      // In ALL mode, cache miss means data does not exist, no additional query needed
      LOG.warn("Lookup miss: key={}, cacheSize={}", lookupKey, cache.size());
    }
  }

  /** Get key type information for debugging */
  private String getKeyTypes(Object[] keys) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < keys.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(keys[i] == null ? "null" : keys[i].getClass().getSimpleName());
    }
    sb.append("]");
    return sb.toString();
  }

  /** Initialize metrics */
  private void initMetrics(MetricGroup metricGroup) {
    MetricGroup lookupGroup = metricGroup.addGroup("iceberg").addGroup("lookup");

    this.lookupCounter = lookupGroup.counter("lookupCount");
    this.hitCounter = lookupGroup.counter("hitCount");
    this.missCounter = lookupGroup.counter("missCount");
    this.refreshCounter = lookupGroup.counter("refreshCount");
    this.refreshFailedCounter = lookupGroup.counter("refreshFailedCount");

    this.cacheSize = new AtomicLong(0);
    this.lastRefreshTime = new AtomicLong(0);

    lookupGroup.gauge("cacheSize", (Gauge<Long>) cacheSize::get);
    lookupGroup.gauge("lastRefreshTime", (Gauge<Long>) lastRefreshTime::get);
  }

  /** Build lookup key RowData */
  private RowData buildLookupKey(Object[] keys) {
    org.apache.flink.table.data.GenericRowData keyRow =
        new org.apache.flink.table.data.GenericRowData(keys.length);
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] instanceof String) {
        keyRow.setField(i, org.apache.flink.table.data.StringData.fromString((String) keys[i]));
      } else {
        keyRow.setField(i, keys[i]);
      }
    }
    return keyRow;
  }

  /** Load all data into cache */
  private void loadAllData() {
    LOG.info("Starting full data load...");
    long startTime = System.currentTimeMillis();

    try {
      cache.refreshAll(
          () -> {
            try {
              return reader.readAll();
            } catch (IOException e) {
              throw new RuntimeException("Failed to read all data from Iceberg table", e);
            }
          });

      long duration = System.currentTimeMillis() - startTime;
      cacheSize.set(cache.size());
      lastRefreshTime.set(System.currentTimeMillis());
      refreshCounter.inc();

      LOG.info("Full data load completed in {} ms, cache size: {}", duration, cache.size());

    } catch (Exception e) {
      refreshFailedCounter.inc();
      LOG.error("Failed to load full data, will retry on next scheduled refresh", e);
      throw new RuntimeException("Failed to load full data from Iceberg table", e);
    }
  }

  /** Refresh cache data */
  private void refreshData() {
    LOG.info("Starting scheduled cache refresh...");
    long startTime = System.currentTimeMillis();

    try {
      cache.refreshAll(
          () -> {
            try {
              return reader.readAll();
            } catch (IOException e) {
              throw new RuntimeException("Failed to read all data from Iceberg table", e);
            }
          });

      long duration = System.currentTimeMillis() - startTime;
      cacheSize.set(cache.size());
      lastRefreshTime.set(System.currentTimeMillis());
      refreshCounter.inc();

      LOG.info("Cache refresh completed in {} ms, cache size: {}", duration, cache.size());

    } catch (Exception e) {
      refreshFailedCounter.inc();
      LOG.error("Failed to refresh cache, keeping existing data", e);
      // Do not throw exception, keep existing cache to continue serving
    }
  }

  /** Start periodic refresh scheduler */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void startReloadScheduler() {
    this.reloadExecutor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("iceberg-lookup-reload-%d")
                .setDaemon(true)
                .build());

    long intervalMillis = reloadInterval.toMillis();

    reloadExecutor.scheduleAtFixedRate(
        this::refreshData,
        intervalMillis, // First refresh happens after interval
        intervalMillis,
        TimeUnit.MILLISECONDS);

    LOG.info("Started reload scheduler with interval: {} ms", intervalMillis);
  }
}
