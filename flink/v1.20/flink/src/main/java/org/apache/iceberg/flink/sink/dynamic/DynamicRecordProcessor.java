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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.Collections;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

@Internal
class DynamicRecordProcessor<T> extends ProcessFunction<T, DynamicRecordInternal>
    implements Collector<DynamicRecord> {
  static final String DYNAMIC_TABLE_UPDATE_STREAM = "dynamic-table-update-stream";

  private final DynamicRecordConverter<T> converter;
  private final CatalogLoader catalogLoader;
  private final boolean immediateUpdate;
  private final int cacheMaximumSize;
  private final long cacheRefreshMs;

  private transient TableDataCache tableCache;
  private transient DynamicKeySelector selector;
  private transient TableUpdater updater;
  private transient OutputTag<DynamicRecordInternal> updateStream;
  private transient Collector<DynamicRecordInternal> collector;
  private transient Context context;

  DynamicRecordProcessor(
      DynamicRecordConverter<T> converter,
      CatalogLoader catalogLoader,
      boolean immediateUpdate,
      int cacheMaximumSize,
      long cacheRefreshMs) {
    this.converter = converter;
    this.catalogLoader = catalogLoader;
    this.immediateUpdate = immediateUpdate;
    this.cacheMaximumSize = cacheMaximumSize;
    this.cacheRefreshMs = cacheRefreshMs;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    Catalog catalog = catalogLoader.loadCatalog();
    this.tableCache = new TableDataCache(catalog, cacheMaximumSize, cacheRefreshMs);
    this.selector =
        new DynamicKeySelector(
            cacheMaximumSize, getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks());
    if (immediateUpdate) {
      updater = new TableUpdater(tableCache, catalog);
    }

    updateStream =
        new OutputTag<>(
            DYNAMIC_TABLE_UPDATE_STREAM,
            new DynamicRecordInternalType(catalogLoader, true, cacheMaximumSize)) {};

    converter.open(openContext);
  }

  @Override
  public void processElement(T element, Context ctx, Collector<DynamicRecordInternal> out)
      throws Exception {
    this.context = ctx;
    this.collector = out;
    converter.convert(element, this);
  }

  @Override
  public void collect(DynamicRecord data) {
    boolean exists = tableCache.exists(data.tableIdentifier()).f0;
    String foundBranch = exists ? tableCache.branch(data.tableIdentifier(), data.branch()) : null;

    Tuple2<Schema, CompareSchemasVisitor.Result> foundSchema =
        exists
            ? tableCache.schema(data.tableIdentifier(), data.schema())
            : TableDataCache.NOT_FOUND;

    PartitionSpec foundSpec = exists ? tableCache.spec(data.tableIdentifier(), data.spec()) : null;

    if (!exists
        || foundBranch == null
        || foundSpec == null
        || foundSchema.f1 == CompareSchemasVisitor.Result.INCOMPATIBLE) {
      if (immediateUpdate) {
        Tuple3<Schema, CompareSchemasVisitor.Result, PartitionSpec> newData =
            updater.update(data.tableIdentifier(), data.branch(), data.schema(), data.spec());
        emit(collector, data, newData.f0, newData.f1, newData.f2);
      } else {
        int writerKey =
            selector.getKey(
                new DynamicKeySelector.Input(
                    data,
                    foundSchema.f0 != null ? foundSchema.f0 : data.schema(),
                    foundSpec != null ? foundSpec : data.spec(),
                    data.rowData()));
        context.output(
            updateStream,
            new DynamicRecordInternal(
                data.tableIdentifier().toString(),
                data.branch(),
                data.schema(),
                data.spec(),
                writerKey,
                data.rowData(),
                data.upsertMode(),
                getEqualityFieldIds(data.equalityFields(), data.schema())));
      }
    } else {
      emit(collector, data, foundSchema.f0, foundSchema.f1, foundSpec);
    }
  }

  private void emit(
      Collector<DynamicRecordInternal> out,
      DynamicRecord data,
      Schema schema,
      CompareSchemasVisitor.Result result,
      PartitionSpec spec) {
    RowData rowData =
        result == CompareSchemasVisitor.Result.SAME
            ? data.rowData()
            : RowDataEvolver.convert(data.rowData(), data.schema(), schema);
    int writerKey = selector.getKey(new DynamicKeySelector.Input(data, schema, spec, rowData));
    String tableName = data.tableIdentifier().toString();
    out.collect(
        new DynamicRecordInternal(
            tableName,
            data.branch(),
            schema,
            spec,
            writerKey,
            rowData,
            data.upsertMode(),
            getEqualityFieldIds(data.equalityFields(), schema)));
  }

  static List<Integer> getEqualityFieldIds(List<String> equalityFields, Schema schema) {
    if (equalityFields == null || equalityFields.isEmpty()) {
      if (!schema.identifierFieldIds().isEmpty()) {
        return Lists.newArrayList(schema.identifierFieldIds());
      } else {
        return Collections.emptyList();
      }
    }
    List<Integer> equalityFieldIds = Lists.newArrayListWithCapacity(equalityFields.size());
    for (String equalityField : equalityFields) {
      Types.NestedField field = schema.findField(equalityField);
      Preconditions.checkNotNull(
          field, "Equality field %s does not exist in schema", equalityField);
      equalityFieldIds.add(field.fieldId());
    }
    return equalityFieldIds;
  }

  @Override
  public void close() {}
}
