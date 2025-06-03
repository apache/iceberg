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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.LinkedHashMap;
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
  private static final int MAX_SCHEMA_COMPARISON_RESULTS_TO_CACHE = 10;
  private static final Tuple2<Boolean, Exception> EXISTS = Tuple2.of(true, null);
  private static final Tuple2<Boolean, Exception> NOT_EXISTS = Tuple2.of(false, null);
  static final Tuple2<Schema, CompareSchemasVisitor.Result> NOT_FOUND =
      Tuple2.of(null, CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);

  private final Catalog catalog;
  private final long refreshMs;
  private final Cache<TableIdentifier, CacheItem> cache;

  TableMetadataCache(Catalog catalog, int maximumSize, long refreshMs) {
    this.catalog = catalog;
    this.refreshMs = refreshMs;
    this.cache = Caffeine.newBuilder().maximumSize(maximumSize).build();
  }

  Tuple2<Boolean, Exception> exists(TableIdentifier identifier) {
    CacheItem cached = cache.getIfPresent(identifier);
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

  Tuple2<Schema, CompareSchemasVisitor.Result> schema(TableIdentifier identifier, Schema input) {
    return schema(identifier, input, true);
  }

  PartitionSpec spec(TableIdentifier identifier, PartitionSpec spec) {
    return spec(identifier, spec, true);
  }

  void update(TableIdentifier identifier, Table table) {
    cache.put(
        identifier,
        new CacheItem(true, table.refs().keySet(), new SchemaInfo(table.schemas()), table.specs()));
  }

  private String branch(TableIdentifier identifier, String branch, boolean allowRefresh) {
    CacheItem cached = cache.getIfPresent(identifier);
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

  private Tuple2<Schema, CompareSchemasVisitor.Result> schema(
      TableIdentifier identifier, Schema input, boolean allowRefresh) {
    CacheItem cached = cache.getIfPresent(identifier);
    Schema compatible = null;
    if (cached != null && cached.tableExists) {
      // This only works if the {@link Schema#equals(Object)} returns true for the old schema
      // and a new schema. Performance is paramount as this code is on the hot path. Every other
      // way for comparing 2 schemas were performing worse than the
      // {@link CompareByNameVisitor#visit(Schema, Schema, boolean)}, so caching was useless.
      Tuple2<Schema, CompareSchemasVisitor.Result> lastResult =
          cached.schema.lastResults.get(input);
      if (lastResult != null) {
        return lastResult;
      }

      for (Map.Entry<Integer, Schema> tableSchema : cached.schema.schemas.entrySet()) {
        CompareSchemasVisitor.Result result =
            CompareSchemasVisitor.visit(input, tableSchema.getValue(), true);
        if (result == CompareSchemasVisitor.Result.SAME) {
          Tuple2<Schema, CompareSchemasVisitor.Result> newResult =
              Tuple2.of(tableSchema.getValue(), CompareSchemasVisitor.Result.SAME);
          cached.schema.update(input, newResult);
          return newResult;
        } else if (compatible == null
            && result == CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED) {
          compatible = tableSchema.getValue();
        }
      }
    }

    if (needsRefresh(cached, allowRefresh)) {
      refreshTable(identifier);
      return schema(identifier, input, false);
    } else if (compatible != null) {
      Tuple2<Schema, CompareSchemasVisitor.Result> newResult =
          Tuple2.of(compatible, CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED);
      cached.schema.update(input, newResult);
      return newResult;
    } else if (cached != null && cached.tableExists) {
      cached.schema.update(input, NOT_FOUND);
      return NOT_FOUND;
    } else {
      return NOT_FOUND;
    }
  }

  private PartitionSpec spec(TableIdentifier identifier, PartitionSpec spec, boolean allowRefresh) {
    CacheItem cached = cache.getIfPresent(identifier);
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
      cache.put(identifier, new CacheItem(false, null, null, null));
      return Tuple2.of(false, e);
    }
  }

  private boolean needsRefresh(CacheItem cacheItem, boolean allowRefresh) {
    return allowRefresh
        && (cacheItem == null || cacheItem.created + refreshMs > System.currentTimeMillis());
  }

  public void invalidate(TableIdentifier identifier) {
    cache.invalidate(identifier);
  }

  /** Handles timeout for missing items only. Caffeine performance causes noticeable delays. */
  static class CacheItem {
    private final long created = System.currentTimeMillis();

    private final boolean tableExists;
    private final Set<String> branches;
    private final SchemaInfo schema;
    private final Map<Integer, PartitionSpec> specs;

    private CacheItem(
        boolean tableExists,
        Set<String> branches,
        SchemaInfo schema,
        Map<Integer, PartitionSpec> specs) {
      this.tableExists = tableExists;
      this.branches = branches;
      this.schema = schema;
      this.specs = specs;
    }

    @VisibleForTesting
    SchemaInfo getSchemaInfo() {
      return schema;
    }
  }

  /**
   * Stores precalculated results for {@link CompareSchemasVisitor#visit(Schema, Schema, boolean)}
   * in the cache.
   */
  static class SchemaInfo {
    private final Map<Integer, Schema> schemas;
    private final Map<Schema, Tuple2<Schema, CompareSchemasVisitor.Result>> lastResults;

    private SchemaInfo(Map<Integer, Schema> schemas) {
      this.schemas = schemas;
      this.lastResults = new LimitedLinkedHashMap<>();
    }

    private void update(
        Schema newLastSchema, Tuple2<Schema, CompareSchemasVisitor.Result> newLastResult) {
      lastResults.put(newLastSchema, newLastResult);
    }

    @VisibleForTesting
    Tuple2<Schema, CompareSchemasVisitor.Result> getLastResult(Schema schema) {
      return lastResults.get(schema);
    }
  }

  @SuppressWarnings("checkstyle:IllegalType")
  private static class LimitedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      boolean remove = size() > MAX_SCHEMA_COMPARISON_RESULTS_TO_CACHE;
      if (remove) {
        LOG.warn(
            "Performance degraded as records with different schema is generated for the same table. "
                + "Likely the DynamicRecord.schema is not reused. "
                + "Reuse the same instance if the record schema is the same to improve performance");
      }

      return remove;
    }
  }

  @VisibleForTesting
  Cache<TableIdentifier, CacheItem> getInternalCache() {
    return cache;
  }
}
