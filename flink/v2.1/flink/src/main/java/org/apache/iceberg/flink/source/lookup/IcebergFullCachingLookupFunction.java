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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkRowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergFullCachingLookupFunction extends LookupFunction {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFullCachingLookupFunction.class);

  private final TableLoader tableLoader;

  private final String[] projectedColumns;

  private final RowType projectedRowType;

  private final int[] lookupKeyIndices;

  private final List<Expression> pushedFilters;

  private final Duration refreshInterval;

  private transient Table table;
  private transient IcebergLookUpReader reader;
  private transient RowData.FieldGetter[] lookupKeyGetters;
  private transient RowData.FieldGetter[] cacheKeyGetters;
  private transient RowData.FieldGetter[] rowFieldGetters;
  private transient TypeSerializer[] fieldSerializers;
  private transient volatile Map<RowData, List<RowData>> cache;
  private transient volatile boolean closed;
  private transient ScheduledExecutorService refreshExecutor;

  public IcebergFullCachingLookupFunction(
      TableLoader tableLoader,
      String[] projectedColumns,
      RowType projectedRowType,
      int[] keyIndices,
      List<Expression> pushedFilters,
      Duration refreshInterval) {
    this.tableLoader = tableLoader;
    this.projectedColumns = projectedColumns;
    this.projectedRowType = projectedRowType;
    this.lookupKeyIndices = keyIndices;
    this.pushedFilters = pushedFilters == null ? ImmutableList.of() : pushedFilters;
    this.refreshInterval = refreshInterval;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    LOG.info(
        "IcebergFullCachingLookupFunction opening lazily, projected fields={}, keyIndices={}",
        Arrays.toString(projectedColumns),
        Arrays.toString(lookupKeyIndices));

    tableLoader.open();
    this.table = tableLoader.loadTable();
    Schema icebergProjection = table.schema().select(projectedColumns);

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

    this.reader = new IcebergLookUpReader(table, icebergProjection, pushedFilters, false, null);
    this.cache = null;
    this.closed = false;
    if (hasPositiveRefreshInterval(refreshInterval)) {
      startRefreshExecutor();
    }
  }

  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    ensureCacheLoaded();

    List<RowData> hit = cache.get(extractLookupKey(keyRow, lookupKeyGetters));
    return hit == null ? Collections.emptyList() : hit;
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

        if (cache != null) {
          cache.clear();
        }
      }

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
        this::reloadCacheSafely, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
  }

  private void reloadCacheSafely() {
    try {
      reloadCache("scheduled");
    } catch (Exception e) {
      LOG.warn("Failed to reload Iceberg full lookup cache, keep using previous cache", e);
    }
  }

  private static boolean hasPositiveRefreshInterval(Duration refreshInterval) {
    return refreshInterval != null && !refreshInterval.isZero() && !refreshInterval.isNegative();
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
        "IcebergFullCachingLookupFunction {} loading started, projected fields={}, pushedFilters={}",
        reason,
        Arrays.toString(projectedColumns),
        pushedFilters);

    table.refresh();
    Map<RowData, List<RowData>> next = Maps.newHashMap();
    int[] rowCnt = {0};
    long start = System.currentTimeMillis();
    reader.read(
        null,
        row -> {
          RowData copied = copyRow(row);
          RowData key = extractLookupKey(copied, cacheKeyGetters);
          next.computeIfAbsent(key, k -> Lists.newLinkedList()).add(copied);
          rowCnt[0]++;
        });

    if (!closed) {
      this.cache = next;
    }

    LOG.info(
        "IcebergFullCachingLookupFunction {} loading finished, rows={}, distinctKeys={}, cost={} ms",
        reason,
        rowCnt[0],
        next.size(),
        System.currentTimeMillis() - start);
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
