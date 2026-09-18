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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
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
 * A full caching lookup function: the whole projected Iceberg dimension table is loaded into an
 * in-memory cache, and every lookup is served from that cache.
 *
 * <p>When a {@link CacheReloadTrigger} is configured, the cache is reloaded by that trigger, and a
 * cache that was loaded successfully replaces the previous one atomically. A failed reload fails
 * the job on the next lookup instead of serving the previously loaded cache, so stale data is never
 * served silently.
 */
public class IcebergFullCachingLookupFunction extends LookupFunction
    implements CacheReloadTrigger.Context {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFullCachingLookupFunction.class);

  private static final String METRIC_GROUP = "icebergLookupCache";
  private static final long UNKNOWN = -1L;

  private final TableLoader tableLoader;
  private final RowType projectedRowType;
  private final int[] lookupKeyIndices;
  private final List<Expression> pushedFilters;
  private final boolean caseSensitive;
  private final boolean eagerLoad;
  private final @Nullable CacheReloadTrigger reloadTrigger;

  private transient Table table;
  private transient IcebergLookupReader reader;
  private transient RowData.FieldGetter[] lookupKeyGetters;
  private transient RowData.FieldGetter[] cacheKeyGetters;
  private transient RowData.FieldGetter[] rowFieldGetters;
  private transient TypeSerializer[] fieldSerializers;
  private transient volatile IcebergLookupCache cache;
  private transient volatile Throwable reloadFailure;

  private transient Counter cacheHitCounter;
  private transient Counter cacheMissCounter;
  private transient Counter reloadSuccessCounter;
  private transient Counter reloadFailureCounter;
  private transient volatile long currentSnapshotId;
  private transient volatile int cachedRows;

  public IcebergFullCachingLookupFunction(
      TableLoader tableLoader,
      RowType projectedRowType,
      int[] keyIndices,
      List<Expression> pushedFilters,
      boolean caseSensitive,
      boolean eagerLoad,
      @Nullable CacheReloadTrigger reloadTrigger) {
    this.tableLoader = tableLoader;
    this.projectedRowType = projectedRowType;
    this.lookupKeyIndices = keyIndices;
    this.pushedFilters = pushedFilters == null ? ImmutableList.of() : pushedFilters;
    this.caseSensitive = caseSensitive;
    this.eagerLoad = eagerLoad;
    this.reloadTrigger = reloadTrigger;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    LOG.info(
        "IcebergFullCachingLookupFunction opening, projected fields={}, keyIndices={}",
        projectedRowType.getFieldNames(),
        Arrays.toString(lookupKeyIndices));

    registerMetrics(context);
    resetState();

    tableLoader.open();
    this.table = tableLoader.loadTable();

    Schema tableSchema = table.schema();
    List<String> projectedColumns = projectedRowType.getFieldNames();
    Types.NestedField[] projectedFields = new Types.NestedField[projectedColumns.size()];
    for (int i = 0; i < projectedColumns.size(); i++) {
      String column = projectedColumns.get(i);
      Types.NestedField field = tableSchema.findField(column);
      Preconditions.checkArgument(field != null, "Cannot find column '%s' in table schema", column);
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

    if (eagerLoad) {
      loadCache();
    }

    if (reloadTrigger != null) {
      reloadTrigger.open(this);
    }
  }

  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    checkReloadFailure();

    IcebergLookupCache currentCache = cache;
    if (currentCache == null) {
      loadCache();
      currentCache = cache;
    }

    List<RowData> hit = currentCache.get(extractLookupKey(keyRow, lookupKeyGetters));
    if (hit == null) {
      cacheMissCounter.inc();
      return Collections.emptyList();
    }

    cacheHitCounter.inc();
    return hit;
  }

  @Override
  public void close() throws Exception {
    if (reloadTrigger != null) {
      reloadTrigger.close();
    }

    tableLoader.close();

    IcebergLookupCache currentCache = cache;
    if (currentCache != null) {
      currentCache.close();
    }

    super.close();
  }

  @Override
  public CompletableFuture<Void> triggerReload() {
    if (cache == null) {
      return CompletableFuture.completedFuture(null);
    }

    try {
      loadCache();
      reloadSuccessCounter.inc();
      return CompletableFuture.completedFuture(null);
    } catch (Exception failure) {
      reloadFailureCounter.inc();
      this.reloadFailure = failure;
      LOG.error(
          "IcebergFullCachingLookupFunction reload failed, the job will fail on the next lookup "
              + "instead of serving the previously loaded cache",
          failure);
      return CompletableFuture.failedFuture(failure);
    }
  }

  @Override
  public long currentProcessingTime() {
    return System.currentTimeMillis();
  }

  @Override
  public long currentWatermark() {
    throw new UnsupportedOperationException(
        "Watermarks are currently unsupported in cache reload triggers.");
  }

  private void loadCache() throws IOException {
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    long snapshotId =
        snapshot == null ? IcebergLookupReader.CURRENT_SNAPSHOT : snapshot.snapshotId();

    if (cache != null && snapshotId == currentSnapshotId) {
      LOG.info(
          "IcebergFullCachingLookupFunction skipping reload, snapshot {} is already cached",
          snapshotId == IcebergLookupReader.CURRENT_SNAPSHOT ? "none" : snapshotId);
      return;
    }

    LOG.info(
        "IcebergFullCachingLookupFunction loading started, snapshot={}, committedAt={}, projected fields={}, pushedFilters={}",
        snapshotId == IcebergLookupReader.CURRENT_SNAPSHOT ? "none" : snapshotId,
        snapshot == null ? "n/a" : snapshot.timestampMillis(),
        projectedRowType.getFieldNames(),
        pushedFilters);

    IcebergLookupCache loaded = new InMemoryLookupCache();
    int[] rowCnt = {0};
    long start = System.currentTimeMillis();
    reader.read(
        snapshotId,
        row -> {
          RowData copied = copyRow(row);
          loaded.add(extractLookupKey(copied, cacheKeyGetters), copied);
          rowCnt[0]++;
        });
    loaded.completeLoad();

    this.currentSnapshotId = snapshotId;
    this.cachedRows = rowCnt[0];
    this.cache = loaded;

    LOG.info(
        "IcebergFullCachingLookupFunction loading finished, snapshot={}, rows={}, cost={} ms",
        snapshotId == IcebergLookupReader.CURRENT_SNAPSHOT ? "none" : snapshotId,
        rowCnt[0],
        System.currentTimeMillis() - start);
  }

  private void checkReloadFailure() throws IOException {
    Throwable failure = reloadFailure;
    if (failure != null) {
      throw new IOException(
          "Lookup cache reload failed, failing the job instead of serving the previously loaded cache",
          failure);
    }
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

  private void registerMetrics(FunctionContext context) {
    MetricGroup group = context.getMetricGroup().addGroup(METRIC_GROUP);
    this.cacheHitCounter = group.counter("cacheHit");
    this.cacheMissCounter = group.counter("cacheMiss");
    this.reloadSuccessCounter = group.counter("reloadSuccess");
    this.reloadFailureCounter = group.counter("reloadFailure");
    group.gauge("snapshotId", () -> currentSnapshotId);
    group.gauge("cachedRows", () -> cachedRows);
  }

  private void resetState() {
    this.currentSnapshotId = UNKNOWN;
    this.cachedRows = 0;
    this.reloadFailure = null;
  }
}
