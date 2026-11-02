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

import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

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
  private final boolean caseSensitive;

  private transient TableUpdater updater;

  DynamicTableUpdateOperator(
      CatalogLoader catalogLoader, TableCreator tableCreator, FlinkDynamicSinkConf configuration) {
    this.catalogLoader = catalogLoader;
    this.tableCreator = tableCreator;

    this.cacheMaximumSize = configuration.cacheMaxSize();
    this.cacheRefreshMs = configuration.cacheRefreshMs();
    this.inputSchemasPerTableCacheMaximumSize = configuration.inputSchemasPerTableCacheMaxSize();
    this.caseSensitive = configuration.caseSensitive();
    this.dropUnusedColumns = configuration.dropUnusedColumns();
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    Catalog catalog = catalogLoader.loadCatalog();
    this.updater =
        new TableUpdater(
            new TableMetadataCache(
                catalog,
                cacheMaximumSize,
                cacheRefreshMs,
                inputSchemasPerTableCacheMaximumSize,
                caseSensitive,
                dropUnusedColumns),
            catalog,
            caseSensitive,
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
    Schema tableSchema = compareInfo.resolvedTableSchema();

    data.setEqualityFieldIds(remapFieldIds(data.equalityFields(), data.schema(), tableSchema));
    data.setSchema(tableSchema);
    data.setSpec(newData.f1);

    RowData newRowData = (RowData) newData.f0.recordConverter().convert(data.rowData());
    data.setRowData(newRowData);

    return data;
  }

  private Set<Integer> remapFieldIds(Set<Integer> fieldIds, Schema fromSchema, Schema toSchema) {
    if (fieldIds == null || fieldIds.isEmpty()) {
      return fieldIds;
    }

    Set<Integer> remappedIds = Sets.newHashSetWithExpectedSize(fieldIds.size());
    for (Integer fieldId : fieldIds) {
      String name = fromSchema.findColumnName(fieldId);
      Types.NestedField field =
          caseSensitive ? toSchema.findField(name) : toSchema.caseInsensitiveFindField(name);
      Preconditions.checkNotNull(field, "Equality field %s does not exist in schema", name);
      remappedIds.add(field.fieldId());
    }

    return remappedIds;
  }
}
