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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;

@Internal
class DynamicRecordProcessor<T> extends ProcessFunction<T, DynamicRecordInternal>
    implements Collector<DynamicRecord> {
  @VisibleForTesting
  static final String DYNAMIC_TABLE_UPDATE_STREAM = "dynamic-table-update-stream";

  private final DynamicRecordGenerator<T> generator;
  private final CatalogLoader catalogLoader;
  private final boolean immediateUpdate;
  private final boolean dropUnusedColumns;
  private final int cacheMaximumSize;
  private final long cacheRefreshMs;
  private final int inputSchemasPerTableCacheMaximumSize;
  private final TableCreator tableCreator;

  private transient TableMetadataCache tableCache;
  private transient HashKeyGenerator hashKeyGenerator;
  private transient TableUpdater updater;
  private transient OutputTag<DynamicRecordInternal> updateStream;
  private transient Collector<DynamicRecordInternal> collector;
  private transient Context context;

  DynamicRecordProcessor(
      DynamicRecordGenerator<T> generator,
      CatalogLoader catalogLoader,
      boolean immediateUpdate,
      boolean dropUnusedColumns,
      int cacheMaximumSize,
      long cacheRefreshMs,
      int inputSchemasPerTableCacheMaximumSize,
      TableCreator tableCreator) {
    this.generator = generator;
    this.catalogLoader = catalogLoader;
    this.immediateUpdate = immediateUpdate;
    this.dropUnusedColumns = dropUnusedColumns;
    this.cacheMaximumSize = cacheMaximumSize;
    this.cacheRefreshMs = cacheRefreshMs;
    this.inputSchemasPerTableCacheMaximumSize = inputSchemasPerTableCacheMaximumSize;
    this.tableCreator = tableCreator;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    Catalog catalog = catalogLoader.loadCatalog();
    this.tableCache =
        new TableMetadataCache(
            catalog, cacheMaximumSize, cacheRefreshMs, inputSchemasPerTableCacheMaximumSize);
    this.hashKeyGenerator =
        new HashKeyGenerator(
            cacheMaximumSize, getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks());
    if (immediateUpdate) {
      updater = new TableUpdater(tableCache, catalog, dropUnusedColumns);
    } else {
      updateStream =
          new OutputTag<>(
              DYNAMIC_TABLE_UPDATE_STREAM,
              new DynamicRecordInternalType(catalogLoader, true, cacheMaximumSize)) {};
    }

    generator.open(openContext);
  }

  @Override
  public void processElement(T element, Context ctx, Collector<DynamicRecordInternal> out)
      throws Exception {
    this.context = ctx;
    this.collector = out;
    generator.generate(element, this);
  }

  @Override
  public void collect(DynamicRecord data) {
    boolean exists = tableCache.exists(data.tableIdentifier()).f0;
    String foundBranch = exists ? tableCache.branch(data.tableIdentifier(), data.branch()) : null;

    TableMetadataCache.ResolvedSchemaInfo foundSchema =
        exists
            ? tableCache.schema(data.tableIdentifier(), data.schema(), dropUnusedColumns)
            : TableMetadataCache.NOT_FOUND;

    PartitionSpec foundSpec = exists ? tableCache.spec(data.tableIdentifier(), data.spec()) : null;

    if (!exists
        || foundBranch == null
        || foundSpec == null
        || foundSchema.compareResult() == CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED) {
      if (immediateUpdate) {
        Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> newData =
            updater.update(
                data.tableIdentifier(), data.branch(), data.schema(), data.spec(), tableCreator);
        emit(
            collector,
            data,
            newData.f0.resolvedTableSchema(),
            newData.f0.recordConverter(),
            newData.f1);
      } else {
        int writerKey =
            hashKeyGenerator.generateKey(
                data,
                foundSchema.resolvedTableSchema() != null
                    ? foundSchema.resolvedTableSchema()
                    : data.schema(),
                foundSpec != null ? foundSpec : data.spec(),
                data.rowData());
        context.output(
            updateStream,
            new DynamicRecordInternal(
                data.tableIdentifier().toString(),
                data.branch(),
                data.schema(),
                data.rowData(),
                data.spec(),
                writerKey,
                data.upsertMode(),
                DynamicSinkUtil.getEqualityFieldIds(data.equalityFields(), data.schema())));
      }
    } else {
      emit(
          collector,
          data,
          foundSchema.resolvedTableSchema(),
          foundSchema.recordConverter(),
          foundSpec);
    }
  }

  private void emit(
      Collector<DynamicRecordInternal> out,
      DynamicRecord data,
      Schema schema,
      DataConverter recordConverter,
      PartitionSpec spec) {
    RowData rowData = (RowData) recordConverter.convert(data.rowData());
    int writerKey = hashKeyGenerator.generateKey(data, schema, spec, rowData);
    String tableName = data.tableIdentifier().toString();
    out.collect(
        new DynamicRecordInternal(
            tableName,
            data.branch(),
            schema,
            rowData,
            spec,
            writerKey,
            data.upsertMode(),
            DynamicSinkUtil.getEqualityFieldIds(data.equalityFields(), schema)));
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
