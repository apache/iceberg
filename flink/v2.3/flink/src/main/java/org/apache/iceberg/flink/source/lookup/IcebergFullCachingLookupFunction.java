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
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkRowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A full caching lookup function that lazily loads the whole Iceberg dimension table on the first
 * lookup and serves subsequent lookups from the cache. The cache can be refreshed periodically with
 * the {@code lookup.full-cache.periodic-reload.interval} option.
 */
public class IcebergFullCachingLookupFunction extends LookupFunction {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFullCachingLookupFunction.class);

  private final TableLoader tableLoader;

  private final String[] projectedColumns;

  private final RowType projectedRowType;

  private final int[] lookupKeyIndices;

  private final List<Expression> pushedFilters;

  private final Duration refreshInterval;

  private final LookupCacheType cacheType;

  private final boolean caseSensitive;

  private final RowType keyRowType;

  private final @Nullable String configuredCacheDirectory;

  private transient Table table;
  private transient IcebergLookupReader reader;
  private transient RowData.FieldGetter[] lookupKeyGetters;
  private transient RowData.FieldGetter[] cacheKeyGetters;
  private transient RowData.FieldGetter[] rowFieldGetters;
  private transient TypeSerializer[] fieldSerializers;
  private transient volatile IcebergLookupCache cache;
  private transient volatile boolean closed;
  private transient ScheduledExecutorService refreshExecutor;

  private final ReentrantLock cacheLock = new ReentrantLock();

  public IcebergFullCachingLookupFunction(
      TableLoader tableLoader,
      String[] projectedColumns,
      RowType projectedRowType,
      int[] keyIndices,
      List<Expression> pushedFilters,
      @Nullable Duration refreshInterval,
      LookupCacheType cacheType,
      boolean caseSensitive,
      @Nullable String cacheDirectory) {
    Preconditions.checkArgument(
        cacheType != LookupCacheType.ROCKSDB
            || (cacheDirectory != null && !cacheDirectory.isEmpty()),
        "RocksDB lookup cache requires %s to be set when %s is %s.",
        IcebergLookupOptions.ROCKSDB_CACHE_DIR.key(),
        IcebergLookupOptions.CACHE_TYPE.key(),
        LookupCacheType.ROCKSDB);

    this.tableLoader = tableLoader;
    this.projectedColumns = projectedColumns;
    this.projectedRowType = projectedRowType;
    this.lookupKeyIndices = keyIndices;
    this.pushedFilters = pushedFilters == null ? ImmutableList.of() : pushedFilters;
    this.refreshInterval = refreshInterval;
    this.cacheType = cacheType;
    this.caseSensitive = caseSensitive;
    this.keyRowType = keyRowType(projectedRowType, keyIndices);
    this.configuredCacheDirectory = cacheDirectory;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    LOG.info(
        "IcebergFullCachingLookupFunction opening lazily, cacheType={}, projected fields={}, keyIndices={}",
        cacheType,
        Arrays.toString(projectedColumns),
        Arrays.toString(lookupKeyIndices));

    tableLoader.open();
    this.table = tableLoader.loadTable();

    Schema tableSchema = table.schema();
    Types.NestedField[] projectedFields = new Types.NestedField[projectedColumns.length];
    for (int i = 0; i < projectedColumns.length; i++) {
      Types.NestedField field = tableSchema.findField(projectedColumns[i]);
      Preconditions.checkArgument(
          field != null, "Cannot find column '%s' in table schema", projectedColumns[i]);
      projectedFields[i] = field;
    }

    Schema icebergProjection = new Schema(projectedFields);

    this.lookupKeyGetters = new RowData.FieldGetter[lookupKeyIndices.length];
    this.cacheKeyGetters = new RowData.FieldGetter[lookupKeyIndices.length];
    for (int i = 0; i < lookupKeyIndices.length; i++) {
      int projectedIndex = lookupKeyIndices[i];
      LogicalType type = projectedRowType.getTypeAt(projectedIndex);
      this.lookupKeyGetters[i] = FlinkRowData.createFieldGetter(type, i);
      this.cacheKeyGetters[i] = FlinkRowData.createFieldGetter(type, projectedIndex);
    }

    this.rowFieldGetters = new RowData.FieldGetter[projectedRowType.getFieldCount()];
    for (int i = 0; i < projectedRowType.getFieldCount(); i++) {
      this.rowFieldGetters[i] = FlinkRowData.createFieldGetter(projectedRowType.getTypeAt(i), i);
    }

    this.fieldSerializers =
        projectedRowType.getChildren().stream()
            .map(InternalSerializers::create)
            .toArray(TypeSerializer[]::new);

    String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    this.reader =
        new IcebergLookupReader(
            table, icebergProjection, pushedFilters, caseSensitive, nameMapping);
    this.cache = null;
    this.closed = false;
    if (refreshInterval != null && !refreshInterval.isZero() && !refreshInterval.isNegative()) {
      startRefreshExecutor();
    }
  }

  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    ensureCacheLoaded();

    RowData key = extractLookupKey(keyRow, lookupKeyGetters);
    cacheLock.lock();
    try {
      IcebergLookupCache current = this.cache;
      if (current == null) {
        return Collections.emptyList();
      }

      List<RowData> hit = current.get(key);
      return hit == null ? Collections.emptyList() : hit;
    } finally {
      cacheLock.unlock();
    }
  }

  @Override
  public void close() throws Exception {
    closed = true;
    if (refreshExecutor != null) {
      refreshExecutor.shutdownNow();
    }

    try {
      synchronized (this) {
        if (tableLoader != null) {
          tableLoader.close();
        }
      }

      IcebergLookupCache current;
      cacheLock.lock();
      try {
        current = this.cache;
        this.cache = null;
      } finally {
        cacheLock.unlock();
      }

      closeQuietly(current);
      super.close();
    } finally {
      refreshExecutor = null;
    }
  }

  private void startRefreshExecutor() {
    long intervalMillis = Math.max(refreshInterval.toMillis(), 1L);
    refreshExecutor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "iceberg-full-cache-refresh");
              thread.setDaemon(true);
              return thread;
            });

    refreshExecutor.scheduleWithFixedDelay(
        () -> {
          try {
            reloadCache("scheduled");
          } catch (Exception e) {
            LOG.warn("Failed to reload Iceberg full lookup cache, keep using previous cache", e);
          }
        },
        intervalMillis,
        intervalMillis,
        TimeUnit.MILLISECONDS);
  }

  private void ensureCacheLoaded() throws IOException {
    if (cache != null) {
      return;
    }

    synchronized (this) {
      if (cache == null) {
        reloadCache("initial");
      }
    }
  }

  private synchronized void reloadCache(String reason) throws IOException {
    if (closed) {
      return;
    }

    LOG.info(
        "IcebergFullCachingLookupFunction {} loading started, cacheType={}, projected fields={}, pushedFilters={}",
        reason,
        cacheType,
        Arrays.toString(projectedColumns),
        pushedFilters);

    table.refresh();
    IcebergLookupCache next = createCache();
    int[] rowCnt = {0};
    long start = System.currentTimeMillis();
    try {
      reader.read(
          row -> {
            RowData copied = copyRow(row);
            next.add(extractLookupKey(copied, cacheKeyGetters), copied);
            rowCnt[0]++;
          });
      next.completeLoad();
    } catch (RuntimeException | IOException e) {
      closeQuietly(next);
      throw e;
    }

    IcebergLookupCache previous;
    cacheLock.lock();
    try {
      if (closed) {
        closeQuietly(next);
        return;
      }

      previous = this.cache;
      this.cache = next;
    } finally {
      cacheLock.unlock();
    }

    closeQuietly(previous);

    LOG.info(
        "IcebergFullCachingLookupFunction {} loading finished, cacheType={}, rows={}, cost={} ms",
        reason,
        cacheType,
        rowCnt[0],
        System.currentTimeMillis() - start);
  }

  private IcebergLookupCache createCache() throws IOException {
    return switch (cacheType) {
      case MEMORY -> new InMemoryLookupCache();
      case ROCKSDB ->
          RocksDBLookupCache.create(
              Paths.get(configuredCacheDirectory, "iceberg-lookup-cache-" + UUID.randomUUID()),
              keyRowType,
              projectedRowType);
    };
  }

  private static void closeQuietly(@Nullable IcebergLookupCache cache) {
    if (cache == null) {
      return;
    }

    try {
      cache.close();
    } catch (RuntimeException e) {
      LOG.warn("Failed to close Iceberg lookup cache", e);
    }
  }

  private static RowType keyRowType(RowType projectedRowType, int[] keyIndices) {
    LogicalType[] types = new LogicalType[keyIndices.length];
    String[] names = new String[keyIndices.length];
    for (int i = 0; i < keyIndices.length; i++) {
      types[i] = projectedRowType.getTypeAt(keyIndices[i]);
      names[i] = projectedRowType.getFieldNames().get(keyIndices[i]);
    }

    return RowType.of(types, names);
  }

  private RowData copyRow(RowData row) {
    return RowDataUtil.clone(
        row,
        new GenericRowData(projectedRowType.getFieldCount()),
        projectedRowType,
        fieldSerializers,
        rowFieldGetters);
  }

  private static RowData extractLookupKey(RowData row, RowData.FieldGetter[] keyGetters) {
    GenericRowData key = new GenericRowData(keyGetters.length);
    for (int i = 0; i < keyGetters.length; i++) {
      key.setField(i, keyGetters[i].getFieldOrNull(row));
    }

    return key;
  }
}
