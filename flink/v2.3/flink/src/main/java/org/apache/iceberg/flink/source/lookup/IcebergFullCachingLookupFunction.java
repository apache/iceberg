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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
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
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkRowData;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
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
  private final int[] lookupKeyIndexes;
  private final List<Expression> pushedFilters;
  private final boolean caseSensitive;
  private final boolean eagerLoad;

  private transient Table table;
  private transient IcebergLookupReader reader;
  private transient RowDataSerializer rowSerializer;
  private transient RowDataSerializer keySerializer;
  private transient RowData.FieldGetter[] keyGetters;
  private transient InMemoryLookupCache cache;

  private transient Counter lookupMissCounter;
  private transient volatile long currentSnapshotId;
  private transient volatile int cachedRows;

  public IcebergFullCachingLookupFunction(
      TableLoader tableLoader,
      RowType projectedRowType,
      int[] lookupKeyIndexes,
      List<Expression> pushedFilters,
      boolean caseSensitive,
      boolean eagerLoad) {
    Preconditions.checkNotNull(pushedFilters, "Pushed filters should not be null");

    this.tableLoader = tableLoader;
    this.projectedRowType = projectedRowType;
    this.lookupKeyIndexes = lookupKeyIndexes;
    this.pushedFilters = pushedFilters;
    this.caseSensitive = caseSensitive;
    this.eagerLoad = eagerLoad;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    LOG.info(
        "IcebergFullCachingLookupFunction opening, projected fields={}, lookupKeyIndexes={}",
        projectedRowType.getFieldNames(),
        Arrays.toString(lookupKeyIndexes));

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

    List<RowData> hit = cache.get(keySerializer.toBinaryRow(keyRow));
    if (hit == null) {
      lookupMissCounter.inc();
      return Collections.emptyList();
    }

    return hit;
  }

  @Override
  public void close() throws Exception {
    try {
      tableLoader.close();
    } finally {
      if (cache != null) {
        cache.close();
      }

      super.close();
    }
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

    InMemoryLookupCache loaded = createCache(snapshot);
    int rowCnt = 0;
    long start = System.currentTimeMillis();
    try (CloseableIterable<RowData> rows = reader.read(snapshotId)) {
      for (RowData row : rows) {
        loaded.add(extractKey(row), rowSerializer.toBinaryRow(row).copy());
        rowCnt++;
      }
    }

    this.cache = loaded;
    this.currentSnapshotId = snapshotId;
    this.cachedRows = rowCnt;

    LOG.info(
        "IcebergFullCachingLookupFunction loading finished, snapshot={}, rows={}, cost={} ms",
        snapshot == null ? "none" : snapshotId,
        rowCnt,
        System.currentTimeMillis() - start);
  }

  private InMemoryLookupCache createCache(Snapshot snapshot) {
    if (snapshot != null && pushedFilters.isEmpty()) {
      Long totalRecords =
          PropertyUtil.propertyAsNullableLong(
              snapshot.summary(), SnapshotSummary.TOTAL_RECORDS_PROP);
      if (totalRecords != null) {
        return new InMemoryLookupCache((int) Math.min(totalRecords, Integer.MAX_VALUE));
      }
    }

    return new InMemoryLookupCache();
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
}
