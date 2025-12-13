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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;

/**
 * An optional operator to perform table updates for tables (e.g. schema update) in a non-concurrent
 * way. Records must be keyed / routed to this operator by table name to ensure non-concurrent
 * updates. The operator itself forwards the record after updating schema / spec of the table. The
 * update is also reflected in the record.
 */
@Internal
class DynamicTableUpdateOperator
    extends RichMapFunction<DynamicRecordInternal, DynamicRecordInternal> {
  private final CatalogLoader catalogLoader;
  private final boolean dropUnusedColumns;
  private final int cacheMaximumSize;
  private final long cacheRefreshMs;
  private final int inputSchemasPerTableCacheMaximumSize;
  private final TableCreator tableCreator;

  private transient TableUpdater updater;

  DynamicTableUpdateOperator(
      CatalogLoader catalogLoader,
      boolean dropUnusedColumns,
      int cacheMaximumSize,
      long cacheRefreshMs,
      int inputSchemasPerTableCacheMaximumSize,
      TableCreator tableCreator) {
    this.catalogLoader = catalogLoader;
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
    this.updater =
        new TableUpdater(
            new TableMetadataCache(
                catalog, cacheMaximumSize, cacheRefreshMs, inputSchemasPerTableCacheMaximumSize),
            catalog,
            dropUnusedColumns);
  }

  @Override
  public DynamicRecordInternal map(DynamicRecordInternal data) throws Exception {
    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> newData =
        updater.update(
            TableIdentifier.parse(data.tableName()),
            data.branch(),
            data.schema(),
            data.spec(),
            tableCreator);
    TableMetadataCache.ResolvedSchemaInfo compareInfo = newData.f0;

    data.setSchema(compareInfo.resolvedTableSchema());
    data.setSpec(newData.f1);

    RowData newRowData = (RowData) newData.f0.recordConverter().convert(data.rowData());
    data.setRowData(newRowData);

    return data;
  }
}
