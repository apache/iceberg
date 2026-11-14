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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
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
 *
 * <p>When an error stream is configured on the sink, a record whose update fails is routed to the
 * {@link DynamicRecordProcessor#ERROR_STREAM} side output if the configured {@link
 * DynamicRecordExceptionFilter} whitelists the failure; otherwise the failure propagates and fails
 * the job.
 */
@Internal
class DynamicTableUpdateOperator
    extends ProcessFunction<DynamicRecordInternal, DynamicRecordInternal> {
  private final CatalogLoader catalogLoader;
  private final boolean dropUnusedColumns;
  private final int cacheMaximumSize;
  private final long cacheRefreshMs;
  private final int inputSchemasPerTableCacheMaximumSize;
  private final TableCreator tableCreator;
  private final boolean caseSensitive;
  private final DynamicRecordExceptionFilter errorFilter;

  private transient TableUpdater updater;
  private transient OutputTag<Tuple2<DynamicRecordInternal, Exception>> errorStream;

  DynamicTableUpdateOperator(
      CatalogLoader catalogLoader,
      TableCreator tableCreator,
      FlinkDynamicSinkConf configuration,
      DynamicRecordExceptionFilter errorFilter) {
    this.catalogLoader = catalogLoader;
    this.tableCreator = tableCreator;
    this.cacheMaximumSize = configuration.cacheMaxSize();
    this.cacheRefreshMs = configuration.cacheRefreshMs();
    this.inputSchemasPerTableCacheMaximumSize = configuration.inputSchemasPerTableCacheMaxSize();
    this.caseSensitive = configuration.caseSensitive();
    this.dropUnusedColumns = configuration.dropUnusedColumns();
    this.errorFilter = errorFilter;
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

    if (errorFilter != null) {
      errorStream =
          new OutputTag<>(
              DynamicRecordProcessor.ERROR_STREAM,
              new TupleTypeInfo<>(
                  new DynamicRecordInternalType(
                      catalogLoader, true, cacheMaximumSize, inputSchemasPerTableCacheMaximumSize),
                  TypeInformation.of(Exception.class)));
    }
  }

  @Override
  public void processElement(
      DynamicRecordInternal record, Context ctx, Collector<DynamicRecordInternal> out)
      throws Exception {
    try {
      out.collect(processRecord(record));
    } catch (RuntimeException e) {
      // Divert only failures the filter whitelists; otherwise propagate so Flink can retry.
      if (errorFilter != null && errorFilter.shouldDivert(e)) {
        ctx.output(errorStream, Tuple2.of(record, e));
      } else {
        throw e;
      }
    }
  }

  private DynamicRecordInternal processRecord(DynamicRecordInternal data) {
    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> newData =
        updater.update(
            TableIdentifier.parse(data.tableName()),
            data.branch(),
            data.schema(),
            data.spec(),
            tableCreator);
    TableMetadataCache.ResolvedSchemaInfo compareInfo = newData.f0;
    Schema tableSchema = compareInfo.resolvedTableSchema();

    Set<Integer> equalityFieldIds =
        remapFieldIds(data.equalityFields(), data.schema(), tableSchema);
    // The writer falls back to the identifier fields of the table when the record has none.
    Set<Integer> effectiveEqualityFieldIds =
        equalityFieldIds.isEmpty() ? tableSchema.identifierFieldIds() : equalityFieldIds;
    DynamicSinkUtil.validateUpsert(data.upsertMode(), newData.f1, effectiveEqualityFieldIds);
    RowData newRowData = (RowData) newData.f0.recordConverter().convert(data.rowData());

    // Mutate only after nothing can fail anymore, so that a diverted record is the unmodified
    // input.
    data.setEqualityFieldIds(equalityFieldIds);
    data.setSchema(tableSchema);
    data.setSpec(newData.f1);
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
