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
import java.util.List;
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
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
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
  private final boolean caseSensitive;
  private final boolean dropUnusedColumns;

  TableMetadataCache(
      Catalog catalog,
      int maximumSize,
      long refreshMs,
      int inputSchemasPerTableCacheMaximumSize,
      boolean caseSensitive,
      boolean dropUnusedColumns) {
    this(
        catalog,
        maximumSize,
        refreshMs,
        inputSchemasPerTableCacheMaximumSize,
        caseSensitive,
        dropUnusedColumns,
        Clock.systemUTC());
  }

  @VisibleForTesting
  TableMetadataCache(
      Catalog catalog,
      int maximumSize,
      long refreshMs,
      int inputSchemasPerTableCacheMaximumSize,
      boolean caseSensitive,
      boolean dropUnusedColumns,
      Clock cacheRefreshClock) {
    this.catalog = catalog;
    this.refreshMs = refreshMs;
    this.inputSchemasPerTableCacheMaximumSize = inputSchemasPerTableCacheMaximumSize;
    this.tableCache = new LRUCache<>(maximumSize);
    this.caseSensitive = caseSensitive;
    this.dropUnusedColumns = dropUnusedColumns;
    this.cacheRefreshClock = cacheRefreshClock;
  }

  Tuple2<Boolean, Exception> exists(TableIdentifier identifier) {
    CacheItem cached = tableCache.get(identifier);
    if (cached != null && Boolean.TRUE.equals(cached.tableExists)) {
      return EXISTS;
    } else if (needsRefresh(identifier, cached, true)) {
      return refreshTable(identifier);
    } else {
      return NOT_EXISTS;
    }
  }

  String branch(TableIdentifier identifier, String branch) {
    return branch(identifier, branch, true);
  }

  ResolvedSchemaInfo schema(TableIdentifier identifier, Schema input) {
    return schema(identifier, input, true);
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
            schemaTiers(
                orderSchemas(table.schemas(), table.schema().schemaId()),
                table.schema(),
                caseSensitive),
            table.specs(),
            inputSchemasPerTableCacheMaximumSize));
  }

  private String branch(TableIdentifier identifier, String branch, boolean allowRefresh) {
    CacheItem cached = tableCache.get(identifier);
    if (cached != null && cached.tableExists && cached.branches.contains(branch)) {
      return branch;
    }

    if (needsRefresh(identifier, cached, allowRefresh)) {
      refreshTable(identifier);
      return branch(identifier, branch, false);
    } else {
      return null;
    }
  }

  private ResolvedSchemaInfo schema(
      TableIdentifier identifier, Schema input, boolean allowRefresh) {
    CacheItem cached = tableCache.get(identifier);
    if (cached != null && cached.tableExists) {
      // This only works if the {@link Schema#equals(Object)} returns true for the old schema
      // and a new schema. Performance is paramount as this code is on the hot path. Every other
      // way for comparing 2 schemas were performing worse than the
      // {@link CompareByNameVisitor#visit(Schema, Schema, boolean)}, so caching was useless.
      ResolvedSchemaInfo lastResult = cached.inputSchemas.get(input);
      if (lastResult == null) {
        ResolvedSchemaInfo newResult = resolveSchema(identifier, cached, input, allowRefresh);
        if (newResult == null) {
          refreshTable(identifier);
          return schema(identifier, input, false);
        }

        cached.inputSchemas.put(input, newResult);
        return newResult;
      } else if (lastResult != NOT_FOUND) {
        return lastResult;
      }
    }

    if (needsRefresh(identifier, cached, allowRefresh)) {
      refreshTable(identifier);
      return schema(identifier, input, false);
    }

    return NOT_FOUND;
  }

  /** Returns null if the table metadata must be refreshed before resolving the input schema. */
  private ResolvedSchemaInfo resolveSchema(
      TableIdentifier identifier, CacheItem cached, Schema input, boolean allowRefresh) {
    Schema compatible = null;
    // The schemas of a tier are only used if no schema of the previous tiers matched the input.
    // Within a tier, the first schema which is the same as the input wins, otherwise the first
    // schema which the input can be converted to. See schemaTiers for the order of the schemas.
    for (List<Schema> tableSchemas : cached.schemaTiers) {
      for (Schema tableSchema : tableSchemas) {
        CompareSchemasVisitor.Result result =
            CompareSchemasVisitor.visit(input, tableSchema, caseSensitive, dropUnusedColumns);
        if (result == CompareSchemasVisitor.Result.SAME) {
          return new ResolvedSchemaInfo(
              tableSchema, CompareSchemasVisitor.Result.SAME, DataConverter.identity());
        } else if (compatible == null
            && result == CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED) {
          compatible = tableSchema;
        }
      }

      if (compatible != null) {
        break;
      }
    }

    if (needsRefresh(identifier, cached, allowRefresh)) {
      return null;
    }

    if (compatible == null) {
      return NOT_FOUND;
    }

    return new ResolvedSchemaInfo(
        compatible,
        CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED,
        DataConverter.get(
            FlinkSchemaUtil.convert(input), FlinkSchemaUtil.convert(compatible), caseSensitive));
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

    if (needsRefresh(identifier, cached, allowRefresh)) {
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
    } catch (NoSuchTableException | NoSuchNamespaceException e) {
      LOG.debug("Table or namespace doesn't exist {}", identifier, e);
      tableCache.put(
          identifier, new CacheItem(cacheRefreshClock.millis(), false, null, null, null, 1));
      return Tuple2.of(false, e);
    }
  }

  private boolean needsRefresh(
      TableIdentifier identifier, CacheItem cacheItem, boolean allowRefresh) {
    if (!allowRefresh) {
      return false;
    }

    if (cacheItem == null) {
      return true;
    }

    long nowMillis = cacheRefreshClock.millis();
    long timeElapsedMillis = nowMillis - cacheItem.createdTimestampMillis;
    if (timeElapsedMillis > refreshMs) {
      LOG.info("Refreshing table metadata for {} after {} millis", identifier, timeElapsedMillis);
      return true;
    }

    return false;
  }

  public void invalidate(TableIdentifier identifier) {
    tableCache.remove(identifier);
  }

  /**
   * Orders the schemas with the current schema first, followed by the rest from newest to oldest.
   * The current schema is not necessarily the one with the highest id, for example after a
   * rollback.
   */
  static Map<Integer, Schema> orderSchemas(Map<Integer, Schema> tableSchemas, int currentSchemaId) {
    Map<Integer, Schema> ordered = Maps.newLinkedHashMapWithExpectedSize(tableSchemas.size());
    Schema current = tableSchemas.get(currentSchemaId);
    if (current != null) {
      ordered.put(currentSchemaId, current);
    }

    tableSchemas.entrySet().stream()
        .filter(e -> e.getKey() != currentSchemaId)
        .sorted(Map.Entry.<Integer, Schema>comparingByKey().reversed())
        .forEach(e -> ordered.put(e.getKey(), e.getValue()));
    return ordered;
  }

  /**
   * Splits the ordered schemas into two tiers, keeping their order within each tier.
   *
   * <p>The second tier holds the schemas which use a field name that belongs to a different field
   * id in the current schema, for example after a column was dropped and added again, or renamed
   * and its name reused. Data written with such a schema is stored under field ids which are not
   * read through the current schema, so these schemas are only used when no schema of the first
   * tier matches the input.
   */
  static List<List<Schema>> schemaTiers(
      Map<Integer, Schema> orderedSchemas, Schema currentSchema, boolean caseSensitive) {
    List<Schema> preferred = Lists.newArrayList();
    List<Schema> fallback = Lists.newArrayList();
    for (Schema schema : orderedSchemas.values()) {
      if (reusesFieldNames(schema, currentSchema, caseSensitive)) {
        fallback.add(schema);
      } else {
        preferred.add(schema);
      }
    }

    return ImmutableList.of(preferred, fallback);
  }

  private static boolean reusesFieldNames(
      Schema schema, Schema currentSchema, boolean caseSensitive) {
    for (Map.Entry<Integer, String> field : TypeUtil.indexNameById(schema.asStruct()).entrySet()) {
      Types.NestedField currentField =
          caseSensitive
              ? currentSchema.findField(field.getValue())
              : currentSchema.caseInsensitiveFindField(field.getValue());
      if (currentField != null && currentField.fieldId() != field.getKey()) {
        return true;
      }
    }

    return false;
  }

  /** Handles timeout for missing items only. Caffeine performance causes noticeable delays. */
  static class CacheItem {
    private final long createdTimestampMillis;
    private final boolean tableExists;
    private final Set<String> branches;
    private final List<List<Schema>> schemaTiers;
    private final Map<Integer, PartitionSpec> specs;
    private final Map<Schema, ResolvedSchemaInfo> inputSchemas;

    private CacheItem(
        long createdTimestampMillis,
        boolean tableExists,
        Set<String> branches,
        List<List<Schema>> schemaTiers,
        Map<Integer, PartitionSpec> specs,
        int inputSchemaCacheMaximumSize) {
      this.createdTimestampMillis = createdTimestampMillis;
      this.tableExists = tableExists;
      this.branches = branches;
      this.schemaTiers = schemaTiers;
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
