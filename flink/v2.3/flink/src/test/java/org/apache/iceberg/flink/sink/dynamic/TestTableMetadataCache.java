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

import static org.apache.iceberg.TestHelpers.waitUntilAfter;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestTableMetadataCache extends TestFlinkIcebergSinkBase {

  private static final boolean CASE_SENSITIVE = true;
  private static final boolean CASE_INSENSITIVE = false;

  private static final boolean DROP_COLUMNS = true;
  private static final boolean PRESERVE_COLUMNS = false;

  static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  static final Schema SCHEMA2 =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "extra", Types.StringType.get()));

  static final Schema SCHEMA_UPPERCASE =
      new Schema(
          Types.NestedField.optional(1, "ID", Types.IntegerType.get()),
          Types.NestedField.optional(2, "DATA", Types.StringType.get()));

  static final Schema SCHEMA_MIXEDCASE =
      new Schema(
          Types.NestedField.optional(1, "Id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "Data", Types.StringType.get()));

  @Test
  void testCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);

    Schema schema1 = cache.schema(tableIdentifier, SCHEMA).resolvedTableSchema();
    assertThat(schema1.sameSchema(SCHEMA)).isTrue();
    assertThat(
            cache.schema(tableIdentifier, SerializationUtils.clone(SCHEMA)).resolvedTableSchema())
        .isEqualTo(schema1);

    assertThat(cache.schema(tableIdentifier, SCHEMA2)).isEqualTo(TableMetadataCache.NOT_FOUND);

    schema1 = cache.schema(tableIdentifier, SCHEMA).resolvedTableSchema();
    assertThat(
            cache.schema(tableIdentifier, SerializationUtils.clone(SCHEMA)).resolvedTableSchema())
        .isEqualTo(schema1);
  }

  @Test
  void testCacheInvalidationAfterSchemaChange() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, PRESERVE_COLUMNS);

    Schema schema1 = cache.schema(tableIdentifier, SCHEMA).resolvedTableSchema();
    assertThat(schema1.sameSchema(SCHEMA)).isTrue();

    catalog.dropTable(tableIdentifier);
    catalog.createTable(tableIdentifier, SCHEMA2);
    tableUpdater.update(
        tableIdentifier,
        SnapshotRef.MAIN_BRANCH,
        SCHEMA2,
        PartitionSpec.unpartitioned(),
        TableCreator.DEFAULT);

    Schema schema2 = cache.schema(tableIdentifier, SCHEMA2).resolvedTableSchema();
    assertThat(schema2.sameSchema(SCHEMA2)).isTrue();
  }

  @Test
  void testCachingDisabled() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 0, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);

    assertThat(cache.getInternalCache()).isEmpty();
  }

  @Test
  void testNoCacheRefreshingBeforeRefreshIntervalElapses() {
    // Create table
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    Table table = catalog.createTable(tableIdentifier, SCHEMA2);

    // Init cache
    TableMetadataCache cache =
        new TableMetadataCache(
            catalog,
            10,
            100L,
            10,
            CASE_INSENSITIVE,
            PRESERVE_COLUMNS,
            Clock.fixed(Instant.now(), ZoneId.systemDefault()));
    cache.update(tableIdentifier, table);

    // Cache schema
    Schema schema = cache.schema(tableIdentifier, SCHEMA2).resolvedTableSchema();
    assertThat(schema.sameSchema(SCHEMA2)).isTrue();

    // Cache schema with fewer fields
    TableMetadataCache.ResolvedSchemaInfo schemaInfo = cache.schema(tableIdentifier, SCHEMA);
    assertThat(schemaInfo.resolvedTableSchema().sameSchema(SCHEMA2)).isTrue();
    assertThat(schemaInfo.compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED);

    // Assert both schemas are in cache
    TableMetadataCache.CacheItem cacheItem = cache.getInternalCache().get(tableIdentifier);
    assertThat(cacheItem).isNotNull();
    assertThat(cacheItem.inputSchemas()).containsKeys(SCHEMA, SCHEMA2);
  }

  @Test
  void refreshesUnresolvedSchemaAfterRefreshInterval() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    Table table = catalog.createTable(tableIdentifier, SCHEMA);
    table.newAppend().commit();

    long refreshMs = 10L;
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, refreshMs, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    cache.update(tableIdentifier, table);
    assertThat(cache.schema(tableIdentifier, SCHEMA2).compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);

    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    waitUntilAfter(System.currentTimeMillis() + refreshMs);

    assertThat(cache.schema(tableIdentifier, SCHEMA2).compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.SAME);
  }

  @Test
  void testNoSuchNamespaceExceptionHandling() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.of("nonexistent_namespace", "myTable");
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);

    TableMetadataCache.ResolvedSchemaInfo result = cache.schema(tableIdentifier, SCHEMA);

    assertThat(result).isEqualTo(TableMetadataCache.NOT_FOUND);
    assertThat(cache.getInternalCache().get(tableIdentifier)).isNotNull();
  }

  @Test
  void testNoSuchTableExceptionHandling() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.nonexistent_table");
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);

    TableMetadataCache.ResolvedSchemaInfo result = cache.schema(tableIdentifier, SCHEMA);

    assertThat(result).isEqualTo(TableMetadataCache.NOT_FOUND);
    assertThat(cache.getInternalCache().get(tableIdentifier)).isNotNull();
  }

  @Test
  void testCaseInsensitiveCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_INSENSITIVE, PRESERVE_COLUMNS);

    Schema schema1 = cache.schema(tableIdentifier, SCHEMA).resolvedTableSchema();
    assertThat(schema1.sameSchema(SCHEMA)).isTrue();

    Schema schemaUpperCase = cache.schema(tableIdentifier, SCHEMA_UPPERCASE).resolvedTableSchema();
    assertThat(schemaUpperCase).isEqualTo(schema1);

    Schema schemaMixedCase = cache.schema(tableIdentifier, SCHEMA_MIXEDCASE).resolvedTableSchema();
    assertThat(schemaMixedCase).isEqualTo(schema1);
  }

  @Test
  void testCaseSensitiveCachingDoesNotMatch() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);

    Schema schema1 = cache.schema(tableIdentifier, SCHEMA).resolvedTableSchema();
    assertThat(schema1.sameSchema(SCHEMA)).isTrue();

    assertThat(cache.schema(tableIdentifier, SCHEMA_UPPERCASE))
        .isEqualTo(TableMetadataCache.NOT_FOUND);

    assertThat(cache.schema(tableIdentifier, SCHEMA_MIXEDCASE))
        .isEqualTo(TableMetadataCache.NOT_FOUND);
  }

  @Test
  void testSchemaPrefersCurrentSchemaAfterDropAndReAddColumn() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    Table table = catalog.createTable(tableIdentifier, SCHEMA);
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    int retiredFieldId = table.schema().findField("extra").fieldId();
    table.updateSchema().deleteColumn("extra").commit();
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    int currentFieldId = table.schema().findField("extra").fieldId();
    assertThat(currentFieldId).isNotEqualTo(retiredFieldId);

    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);

    Schema resolved = cache.schema(tableIdentifier, table.schema()).resolvedTableSchema();

    assertThat(resolved.findField("extra").fieldId()).isEqualTo(currentFieldId);
  }

  @Test
  void testSchemaStillMatchesHistoricalSchemaWhenDroppedColumnIsNotReAdded() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    Table table = catalog.createTable(tableIdentifier, SCHEMA);
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    Schema withExtra = table.schema();
    table.updateSchema().deleteColumn("extra").commit();

    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);

    Schema resolved = cache.schema(tableIdentifier, withExtra).resolvedTableSchema();

    assertThat(resolved.findField("extra")).isNotNull();
  }

  @Test
  void testSchemasAreOrderedCurrentFirstThenNewestToOldest() {
    Map<Integer, Schema> schemas = Maps.newHashMap();
    for (int id = 0; id < 4; id++) {
      schemas.put(id, SCHEMA);
    }

    assertThat(TableMetadataCache.CacheItem.orderSchemas(schemas, 3).keySet())
        .containsExactly(3, 2, 1, 0);

    // after a rollback the current schema is not the one with the highest id
    assertThat(TableMetadataCache.CacheItem.orderSchemas(schemas, 1).keySet())
        .containsExactly(1, 3, 2, 0);
  }
}
