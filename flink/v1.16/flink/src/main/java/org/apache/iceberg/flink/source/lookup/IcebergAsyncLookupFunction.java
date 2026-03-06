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
 * Iceberg PARTIAL mode async LookupFunction.
 *
 * <p>Use async IO to execute lookup queries to improve throughput.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Async query: Use thread pool to execute lookup queries asynchronously
 *   <li>Concurrency control: Support configuring max concurrent requests
 *   <li>LRU cache: Cache query results in memory with TTL expiration and max rows limit
 *   <li>Retry mechanism: Support configuring max retry attempts
 * </ul>
 */
@Internal
public class IcebergAsyncLookupFunction extends AsyncLookupFunction {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IcebergAsyncLookupFunction.class);

  // Configuration
  private final TableLoader tableLoader;
  private final Schema projectedSchema;
  private final int[] lookupKeyIndices;
  private final String[] lookupKeyNames;
  private final boolean caseSensitive;
  private final Duration cacheTtl;
  private final long cacheMaxRows;
  private final int maxRetries;
  private final int asyncCapacity;

  // Runtime components
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
   * Create an IcebergAsyncLookupFunction instance
   *
   * @param tableLoader table loader
   * @param projectedSchema projected schema
   * @param lookupKeyIndices indices of lookup keys in projected schema
   * @param lookupKeyNames field names of lookup keys
   * @param caseSensitive whether case sensitive
   * @param cacheTtl cache TTL
   * @param cacheMaxRows max cache rows
   * @param maxRetries max retry attempts
   * @param asyncCapacity max concurrent async queries
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

    // Initialize metrics
    initMetrics(context.getMetricGroup());

    // Initialize cache
    this.cache =
        IcebergLookupCache.createPartialCache(
            IcebergLookupCache.CacheConfig.builder().ttl(cacheTtl).maxRows(cacheMaxRows).build());
    cache.open();

    // Initialize reader
    this.reader =
        new IcebergLookupReader(
            tableLoader, projectedSchema, lookupKeyIndices, lookupKeyNames, caseSensitive);
    reader.open();

    // Initialize thread pool
    this.executorService =
        Executors.newFixedThreadPool(
            Math.min(asyncCapacity, Runtime.getRuntime().availableProcessors() * 2),
            new ThreadFactoryBuilder()
                .setNameFormat("iceberg-async-lookup-%d")
                .setDaemon(true)
                .build());

    // Initialize semaphore for concurrency control
    this.semaphore = new Semaphore(asyncCapacity);

    LOG.info("IcebergAsyncLookupFunction opened successfully");
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing IcebergAsyncLookupFunction");

    // Shutdown thread pool
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

    // Close cache
    if (cache != null) {
      cache.close();
    }

    // Close reader
    if (reader != null) {
      reader.close();
    }

    super.close();
    LOG.info("IcebergAsyncLookupFunction closed");
  }

  /**
   * Async lookup method, called by Flink to execute dimension table join
   *
   * @param keyRow lookup key RowData
   * @return async result CompletableFuture
   */
  @Override
  public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
    lookupCounter.inc();
    pendingRequests.incrementAndGet();

    // Extract lookup key
    RowData lookupKey = extractLookupKey(keyRow);

    // Check cache first
    List<RowData> cachedResults = cache.get(lookupKey);
    if (cachedResults != null) {
      hitCounter.inc();
      pendingRequests.decrementAndGet();
      return CompletableFuture.completedFuture(cachedResults);
    }

    missCounter.inc();

    // Create async future
    CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();

    // Execute query asynchronously
    executorService.execute(
        () -> {
          boolean acquired = false;
          try {
            // Acquire semaphore to control concurrency
            acquired = semaphore.tryAcquire(30, TimeUnit.SECONDS);
            if (!acquired) {
              asyncTimeoutCounter.inc();
              LOG.warn("Async lookup timed out waiting for semaphore for key: {}", lookupKey);
              future.complete(Collections.emptyList());
              return;
            }

            // Execute query with retry
            List<RowData> results = lookupWithRetry(lookupKey);

            // Update cache
            cache.put(lookupKey, results != null ? results : Collections.emptyList());
            cacheSize.set(cache.size());

            // Complete future
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

  /** Initialize metrics */
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

  /** Extract lookup key from input RowData */
  private RowData extractLookupKey(RowData keyRow) {
    // keyRow is already the lookup key, return directly
    // But need to copy to avoid reuse issues
    int arity = keyRow.getArity();
    GenericRowData copy = new GenericRowData(arity);
    for (int i = 0; i < arity; i++) {
      if (!keyRow.isNullAt(i)) {
        // Simple copy, for complex types may need deep copy
        copy.setField(i, getFieldValue(keyRow, i));
      }
    }
    return copy;
  }

  /** Get field value */
  private Object getFieldValue(RowData row, int index) {
    if (row.isNullAt(index)) {
      return null;
    }

    // Need to get value based on actual type
    // Since we don't know the specific type, try using GenericRowData's generic methods
    if (row instanceof GenericRowData) {
      return ((GenericRowData) row).getField(index);
    }

    // For other types, try common types
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
   * Lookup query with retry mechanism
   *
   * @param lookupKey lookup key
   * @return query result list
   */
  private List<RowData> lookupWithRetry(RowData lookupKey) {
    Exception lastException = null;

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          retryCounter.inc();
          LOG.debug("Retry attempt {} for async lookup key: {}", attempt, lookupKey);
          // Simple backoff strategy
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

    // All retries failed
    LOG.error(
        "All {} async lookup attempts failed for key: {}",
        maxRetries + 1,
        lookupKey,
        lastException);

    // Return empty list instead of throwing exception to keep job running
    return Collections.emptyList();
  }
}
