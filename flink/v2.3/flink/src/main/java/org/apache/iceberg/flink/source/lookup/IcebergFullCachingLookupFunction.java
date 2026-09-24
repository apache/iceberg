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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
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
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A full caching lookup function: the whole projected Iceberg dimension table is loaded into an
 * in-memory cache on the first lookup, and every lookup is served from that cache.
 */
@Internal
public class IcebergFullCachingLookupFunction extends LookupFunction {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFullCachingLookupFunction.class);

  private static final String METRIC_GROUP = "icebergLookupCache";

  private final TableLoader tableLoader;
  private final RowType projectedRowType;
  private final int[] lookupKeyIndices;
  private final List<Expression> pushedFilters;
  private final boolean caseSensitive;
  private final boolean eagerLoad;

  private transient Table table;
  private transient IcebergLookupReader reader;
  private transient RowData.FieldGetter[] lookupKeyGetters;
  private transient RowData.FieldGetter[] cacheKeyGetters;
  private transient RowData.FieldGetter[] rowFieldGetters;
  private transient TypeSerializer[] fieldSerializers;
  private transient InMemoryLookupCache cache;

  private transient Counter lookupMissCounter;
  private transient volatile long currentSnapshotId;
  private transient volatile int cachedRows;

  public IcebergFullCachingLookupFunction(
      TableLoader tableLoader,
      RowType projectedRowType,
      int[] keyIndices,
      List<Expression> pushedFilters,
      boolean caseSensitive,
      boolean eagerLoad) {
    this.tableLoader = tableLoader;
    this.projectedRowType = projectedRowType;
    this.lookupKeyIndices = keyIndices;
    this.pushedFilters = pushedFilters == null ? ImmutableList.of() : pushedFilters;
    this.caseSensitive = caseSensitive;
    this.eagerLoad = eagerLoad;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    LOG.info(
        "IcebergFullCachingLookupFunction opening, projected fields={}, keyIndices={}",
        projectedRowType.getFieldNames(),
        Arrays.toString(lookupKeyIndices));

    MetricGroup metricGroup = context.getMetricGroup().addGroup(METRIC_GROUP);
    this.lookupMissCounter = metricGroup.counter("lookupMiss");
    this.currentSnapshotId = IcebergLookupReader.CURRENT_SNAPSHOT;
    this.cachedRows = 0;
    metricGroup.gauge("snapshotId", () -> currentSnapshotId);
    metricGroup.gauge("cachedRows", () -> cachedRows);

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
  }

  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    if (cache == null) {
      loadCache();
    }

    List<RowData> hit = cache.get(extractLookupKey(keyRow, lookupKeyGetters));
    if (hit == null) {
      lookupMissCounter.inc();
      return Collections.emptyList();
    }

    return hit;
  }

  @Override
  public void close() throws Exception {
    tableLoader.close();

    if (cache != null) {
      cache.close();
    }

    super.close();
  }

  private void loadCache() throws IOException {
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    long snapshotId =
        snapshot == null ? IcebergLookupReader.CURRENT_SNAPSHOT : snapshot.snapshotId();

    LOG.info(
        "IcebergFullCachingLookupFunction loading started, snapshot={}, committedAt={}, projected fields={}, pushedFilters={}",
        snapshot == null ? "none" : snapshotId,
        snapshot == null ? "n/a" : snapshot.timestampMillis(),
        projectedRowType.getFieldNames(),
        pushedFilters);

    InMemoryLookupCache loaded = new InMemoryLookupCache();
    int[] rowCnt = {0};
    long start = System.currentTimeMillis();
    reader.read(
        snapshotId,
        row -> {
          RowData copied = copyRow(row);
          loaded.add(extractLookupKey(copied, cacheKeyGetters), copied);
          rowCnt[0]++;
        });

    this.cache = loaded;
    this.currentSnapshotId = snapshotId;
    this.cachedRows = rowCnt[0];

    LOG.info(
        "IcebergFullCachingLookupFunction loading finished, snapshot={}, rows={}, cost={} ms",
        snapshot == null ? "none" : snapshot.snapshotId(),
        rowCnt[0],
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

  private void createAccessors() {
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
  }
}
