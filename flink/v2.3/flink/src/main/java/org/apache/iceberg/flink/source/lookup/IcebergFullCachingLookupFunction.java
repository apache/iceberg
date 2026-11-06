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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkRowData;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lookup function that serves every lookup from an in-memory cache of the rows of an Iceberg
 * table, after projection and pushed-down filters are applied.
 *
 * <p>The cache is loaded from the current snapshot either when the function is opened or on the
 * first lookup, depending on {@link IcebergLookupOptions#FULL_CACHE_EAGER_LOAD}. When a {@link
 * CacheReloadTrigger} is configured, the cache is reloaded on the trigger's schedule: a reload that
 * finds no new snapshot keeps the loaded cache, and a reload that fails fails the next lookup
 * instead of serving the previously loaded cache.
 */
@Internal
public class IcebergFullCachingLookupFunction extends LookupFunction {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFullCachingLookupFunction.class);

  private static final String METRIC_GROUP = "cache";
  private static final long UNINITIALIZED = -1L;

  private final TableLoader tableLoader;
  private final RowType projectedRowType;
  private final int[] lookupKeyIndexes;
  private final List<Expression> pushedFilters;
  private final boolean caseSensitive;
  private final boolean eagerLoad;
  private final @Nullable CacheReloadTrigger reloadTrigger;

  private transient Table table;
  private transient IcebergLookupReader reader;
  private transient RowDataSerializer rowSerializer;
  private transient RowDataSerializer keySerializer;
  private transient RowData.FieldGetter[] keyGetters;
  private transient volatile InMemoryLookupCache cache;
  private transient volatile Throwable reloadFailure;
  private transient volatile boolean closed;

  private transient Counter missCounter;
  private transient Counter loadCounter;
  private transient Counter loadFailuresCounter;
  private transient volatile long currentSnapshotId;
  private transient volatile long cachedRecords;
  private transient volatile long latestLoadTimeMs;

  public IcebergFullCachingLookupFunction(
      TableLoader tableLoader,
      RowType projectedRowType,
      int[] lookupKeyIndexes,
      List<Expression> pushedFilters,
      boolean caseSensitive,
      boolean eagerLoad,
      @Nullable CacheReloadTrigger reloadTrigger) {
    this.tableLoader = tableLoader;
    this.projectedRowType = projectedRowType;
    this.lookupKeyIndexes = lookupKeyIndexes;
    this.pushedFilters = pushedFilters;
    this.caseSensitive = caseSensitive;
    this.eagerLoad = eagerLoad;
    this.reloadTrigger = reloadTrigger;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    LOG.info(
        "IcebergFullCachingLookupFunction opening, projected fields={}, lookupKeyIndexes={}",
        projectedRowType.getFieldNames(),
        Arrays.toString(lookupKeyIndexes));

    MetricGroup metricGroup = context.getMetricGroup().addGroup(METRIC_GROUP);
    this.missCounter = metricGroup.counter("missCount");
    this.loadCounter = metricGroup.counter("loadCount");
    this.loadFailuresCounter = metricGroup.counter("numLoadFailures");
    this.currentSnapshotId = IcebergLookupReader.CURRENT_SNAPSHOT;
    this.cachedRecords = 0L;
    this.latestLoadTimeMs = UNINITIALIZED;
    this.reloadFailure = null;
    metricGroup.gauge("snapshotId", () -> currentSnapshotId);
    metricGroup.gauge("numCachedRecords", () -> cachedRecords);
    metricGroup.gauge("latestLoadTime", () -> latestLoadTimeMs);

    tableLoader.open();
    this.table = tableLoader.loadTable();

    Schema icebergProjection =
        FlinkSchemaUtil.convert(table.schema(), FlinkSchemaUtil.toResolvedSchema(projectedRowType));

    createAccessors();

    String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    this.reader =
        new IcebergLookupReader(
            table, icebergProjection, pushedFilters, caseSensitive, nameMapping);

    if (eagerLoad) {
      loadCache();
    }

    if (reloadTrigger != null) {
      reloadTrigger.open(new ReloadContext(this));
    }
  }

  /**
   * Returns the cached rows for the key. The returned collection and its rows are shared with the
   * cache, so they must not be modified.
   */
  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    checkReloadFailure();

    InMemoryLookupCache currentCache = cache;
    if (currentCache == null) {
      loadCache();
      currentCache = cache;
    }

    List<RowData> hit = currentCache.get(keySerializer.toBinaryRow(keyRow));
    if (hit == null) {
      missCounter.inc();
      return Collections.emptyList();
    }

    return hit;
  }

  @Override
  public void close() throws Exception {
    this.closed = true;

    try {
      if (reloadTrigger != null) {
        reloadTrigger.close();
      }
    } finally {
      try {
        tableLoader.close();
      } finally {
        if (cache != null) {
          cache.close();
        }

        super.close();
      }
    }
  }

  private CompletableFuture<Void> triggerReload() {
    if (cache == null || closed) {
      return CompletableFuture.completedFuture(null);
    }

    try {
      loadCache();
      return CompletableFuture.completedFuture(null);
    } catch (Throwable failure) {
      if (closed) {
        return CompletableFuture.completedFuture(null);
      }

      loadFailuresCounter.inc();
      this.reloadFailure = failure;
      LOG.error(
          "IcebergFullCachingLookupFunction reload failed, the job will fail on the next lookup "
              + "instead of serving the previously loaded cache",
          failure);
      return CompletableFuture.failedFuture(failure);
    }
  }

  private synchronized void loadCache() throws IOException {
    if (closed) {
      return;
    }

    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    long snapshotId =
        snapshot == null ? IcebergLookupReader.CURRENT_SNAPSHOT : snapshot.snapshotId();

    if (cache != null && snapshotId == currentSnapshotId) {
      LOG.info(
          "IcebergFullCachingLookupFunction skipping reload, snapshot {} is already cached",
          snapshot == null ? "none" : snapshotId);
      return;
    }

    LOG.info(
        "IcebergFullCachingLookupFunction loading started, snapshot={}, committedAt={}, projected fields={}, pushedFilters={}",
        snapshot == null ? "none" : snapshotId,
        snapshot == null ? "n/a" : snapshot.timestampMillis(),
        projectedRowType.getFieldNames(),
        pushedFilters);

    InMemoryLookupCache loaded = new InMemoryLookupCache();
    long rowCnt = 0L;
    long start = System.currentTimeMillis();
    try (CloseableIterable<RowData> rows = reader.read(snapshotId)) {
      for (RowData row : rows) {
        loaded.add(extractKey(row), rowSerializer.toBinaryRow(row).copy());
        rowCnt++;
      }
    }

    long loadTimeMs = System.currentTimeMillis() - start;

    if (closed) {
      return;
    }

    this.cache = loaded;
    this.currentSnapshotId = snapshotId;
    this.cachedRecords = rowCnt;
    this.latestLoadTimeMs = loadTimeMs;
    loadCounter.inc();

    LOG.info(
        "IcebergFullCachingLookupFunction loading finished, snapshot={}, rows={}, cost={} ms",
        snapshot == null ? "none" : snapshotId,
        rowCnt,
        loadTimeMs);
  }

  private BinaryRowData extractKey(RowData row) {
    GenericRowData key = new GenericRowData(keyGetters.length);
    for (int i = 0; i < keyGetters.length; i++) {
      key.setField(i, keyGetters[i].getFieldOrNull(row));
    }

    return keySerializer.toBinaryRow(key).copy();
  }

  private void createAccessors() {
    LogicalType[] keyTypes = new LogicalType[lookupKeyIndexes.length];
    this.keyGetters = new RowData.FieldGetter[lookupKeyIndexes.length];
    for (int i = 0; i < lookupKeyIndexes.length; i++) {
      int projectedIndex = lookupKeyIndexes[i];
      keyTypes[i] = projectedRowType.getTypeAt(projectedIndex);
      this.keyGetters[i] = FlinkRowData.createFieldGetter(keyTypes[i], projectedIndex);
    }

    this.rowSerializer = new RowDataSerializer(projectedRowType);
    this.keySerializer = new RowDataSerializer(keyTypes);
  }

  private void checkReloadFailure() throws IOException {
    Throwable failure = reloadFailure;
    if (failure != null) {
      throw new IOException(
          "Lookup cache reload failed, failing the job instead of serving the previously loaded cache",
          failure);
    }
  }

  private record ReloadContext(IcebergFullCachingLookupFunction function) implements CacheReloadTrigger.Context {

    @Override
      public long currentProcessingTime() {
        return System.currentTimeMillis();
      }

      @Override
      public long currentWatermark() {
        throw new UnsupportedOperationException(
                "Watermarks are currently unsupported in cache reload triggers.");
      }

      @Override
      public CompletableFuture<Void> triggerReload() {
        return function.triggerReload();
      }
    }
}
