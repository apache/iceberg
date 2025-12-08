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

import java.time.Clock;
import java.util.Map;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TableMetadataCache is responsible for caching table metadata to avoid hitting the catalog too
 * frequently. We store table identifier, schema, partition spec, and a set of past schema
 * comparison results of the active table schema against the last input schemas.
 */
@Internal
class TableMetadataCache {

  private static final Logger LOG = LoggerFactory.getLogger(TableMetadataCache.class);
  private static final Tuple2<Boolean, Exception> EXISTS = Tuple2.of(true, null);
  private static final Tuple2<Boolean, Exception> NOT_EXISTS = Tuple2.of(false, null);
  static final ResolvedSchemaInfo NOT_FOUND =
      new ResolvedSchemaInfo(
          null, CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED, DataConverter.identity());

  private final Catalog catalog;
  private final long refreshMs;
  private final Clock cacheRefreshClock;
  private final int inputSchemasPerTableCacheMaximumSize;
  private final Map<TableIdentifier, CacheItem> tableCache;

  TableMetadataCache(
      Catalog catalog, int maximumSize, long refreshMs, int inputSchemasPerTableCacheMaximumSize) {
    this(catalog, maximumSize, refreshMs, inputSchemasPerTableCacheMaximumSize, Clock.systemUTC());
  }

  @VisibleForTesting
  TableMetadataCache(
      Catalog catalog,
      int maximumSize,
      long refreshMs,
      int inputSchemasPerTableCacheMaximumSize,
      Clock cacheRefreshClock) {
    this.catalog = catalog;
    this.refreshMs = refreshMs;
    this.cacheRefreshClock = cacheRefreshClock;
    this.inputSchemasPerTableCacheMaximumSize = inputSchemasPerTableCacheMaximumSize;
    this.tableCache = new LRUCache<>(maximumSize);
  }

  Tuple2<Boolean, Exception> exists(TableIdentifier identifier) {
    CacheItem cached = tableCache.get(identifier);
    if (cached != null && Boolean.TRUE.equals(cached.tableExists)) {
      return EXISTS;
    } else if (needsRefresh(cached, true)) {
      return refreshTable(identifier);
    } else {
      return NOT_EXISTS;
    }
  }

  String branch(TableIdentifier identifier, String branch) {
    return branch(identifier, branch, true);
  }

  ResolvedSchemaInfo schema(TableIdentifier identifier, Schema input, boolean dropUnusedColumns) {
    return schema(identifier, input, true, dropUnusedColumns);
  }

  PartitionSpec spec(TableIdentifier identifier, PartitionSpec spec) {
    return spec(identifier, spec, true);
  }

  void update(TableIdentifier identifier, Table table) {
    tableCache.put(
        identifier,
        new CacheItem(
            cacheRefreshClock.millis(),
            true,
            table.refs().keySet(),
            table.schemas(),
            table.specs(),
            inputSchemasPerTableCacheMaximumSize));
  }

  private String branch(TableIdentifier identifier, String branch, boolean allowRefresh) {
    CacheItem cached = tableCache.get(identifier);
    if (cached != null && cached.tableExists && cached.branches.contains(branch)) {
      return branch;
    }

    if (needsRefresh(cached, allowRefresh)) {
      refreshTable(identifier);
      return branch(identifier, branch, false);
    } else {
      return null;
    }
  }

  private ResolvedSchemaInfo schema(
      TableIdentifier identifier, Schema input, boolean allowRefresh, boolean dropUnusedColumns) {
    CacheItem cached = tableCache.get(identifier);
    Schema compatible = null;
    if (cached != null && cached.tableExists) {
      // This only works if the {@link Schema#equals(Object)} returns true for the old schema
      // and a new schema. Performance is paramount as this code is on the hot path. Every other
      // way for comparing 2 schemas were performing worse than the
      // {@link CompareByNameVisitor#visit(Schema, Schema, boolean)}, so caching was useless.
      ResolvedSchemaInfo lastResult = cached.inputSchemas.get(input);
      if (lastResult != null) {
        return lastResult;
      }

      for (Map.Entry<Integer, Schema> tableSchema : cached.tableSchemas.entrySet()) {
        CompareSchemasVisitor.Result result =
            CompareSchemasVisitor.visit(input, tableSchema.getValue(), true, dropUnusedColumns);
        if (result == CompareSchemasVisitor.Result.SAME) {
          ResolvedSchemaInfo newResult =
              new ResolvedSchemaInfo(
                  tableSchema.getValue(),
                  CompareSchemasVisitor.Result.SAME,
                  DataConverter.identity());
          cached.inputSchemas.put(input, newResult);
          return newResult;
        } else if (compatible == null
            && result == CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED) {
          compatible = tableSchema.getValue();
        }
      }
    }

    if (needsRefresh(cached, allowRefresh)) {
      refreshTable(identifier);
      return schema(identifier, input, false, dropUnusedColumns);
    } else if (compatible != null) {
      ResolvedSchemaInfo newResult =
          new ResolvedSchemaInfo(
              compatible,
              CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED,
              DataConverter.get(
                  FlinkSchemaUtil.convert(input), FlinkSchemaUtil.convert(compatible)));
      cached.inputSchemas.put(input, newResult);
      return newResult;
    } else if (cached != null && cached.tableExists) {
      cached.inputSchemas.put(input, NOT_FOUND);
      return NOT_FOUND;
    } else {
      return NOT_FOUND;
    }
  }

  private PartitionSpec spec(TableIdentifier identifier, PartitionSpec spec, boolean allowRefresh) {
    CacheItem cached = tableCache.get(identifier);
    if (cached != null && cached.tableExists) {
      for (PartitionSpec tableSpec : cached.specs.values()) {
        if (PartitionSpecEvolution.checkCompatibility(tableSpec, spec)) {
          return tableSpec;
        }
      }
    }

    if (needsRefresh(cached, allowRefresh)) {
      refreshTable(identifier);
      return spec(identifier, spec, false);
    } else {
      return null;
    }
  }

  private Tuple2<Boolean, Exception> refreshTable(TableIdentifier identifier) {
    try {
      Table table = catalog.loadTable(identifier);
      update(identifier, table);
      return EXISTS;
    } catch (NoSuchTableException e) {
      LOG.debug("Table doesn't exist {}", identifier, e);
      tableCache.put(
          identifier, new CacheItem(cacheRefreshClock.millis(), false, null, null, null, 1));
      return Tuple2.of(false, e);
    }
  }

  private boolean needsRefresh(CacheItem cacheItem, boolean allowRefresh) {
    return allowRefresh
        && (cacheItem == null
            || cacheRefreshClock.millis() - cacheItem.createdTimestampMillis > refreshMs);
  }

  public void invalidate(TableIdentifier identifier) {
    tableCache.remove(identifier);
  }

  /** Handles timeout for missing items only. Caffeine performance causes noticeable delays. */
  static class CacheItem {
    private final long createdTimestampMillis;
    private final boolean tableExists;
    private final Set<String> branches;
    private final Map<Integer, Schema> tableSchemas;
    private final Map<Integer, PartitionSpec> specs;
    private final Map<Schema, ResolvedSchemaInfo> inputSchemas;

    private CacheItem(
        long createdTimestampMillis,
        boolean tableExists,
        Set<String> branches,
        Map<Integer, Schema> tableSchemas,
        Map<Integer, PartitionSpec> specs,
        int inputSchemaCacheMaximumSize) {
      this.createdTimestampMillis = createdTimestampMillis;
      this.tableExists = tableExists;
      this.branches = branches;
      this.tableSchemas = tableSchemas;
      this.specs = specs;
      this.inputSchemas =
          new LRUCache<>(inputSchemaCacheMaximumSize, CacheItem::inputSchemaEvictionListener);
    }

    private static void inputSchemaEvictionListener(
        Map.Entry<Schema, ResolvedSchemaInfo> evictedEntry) {
      LOG.warn(
          "Performance degraded as records with different schema is generated for the same table. "
              + "Likely the DynamicRecord.schema is not reused. "
              + "Reuse the same instance if the record schema is the same to improve performance");
    }

    @VisibleForTesting
    Map<Schema, ResolvedSchemaInfo> inputSchemas() {
      return inputSchemas;
    }
  }

  static class ResolvedSchemaInfo {
    private final Schema resolvedTableSchema;
    private final CompareSchemasVisitor.Result compareResult;
    private final DataConverter recordConverter;

    ResolvedSchemaInfo(
        Schema tableSchema,
        CompareSchemasVisitor.Result compareResult,
        DataConverter recordConverter) {
      this.resolvedTableSchema = tableSchema;
      this.compareResult = compareResult;
      this.recordConverter = recordConverter;
    }

    Schema resolvedTableSchema() {
      return resolvedTableSchema;
    }

    CompareSchemasVisitor.Result compareResult() {
      return compareResult;
    }

    DataConverter recordConverter() {
      return recordConverter;
    }
  }

  @VisibleForTesting
  Map<TableIdentifier, CacheItem> getInternalCache() {
    return tableCache;
  }
}
