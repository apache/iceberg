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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTableUpdater extends TestFlinkIcebergSinkBase {

  static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  static final Schema SCHEMA2 =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "extra", Types.StringType.get()));

  @Test
  void testTableCreation(@TempDir Path tempDir) {
    // Location for tables is not configurable for hadoop catalogs
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("catalog", Map.of());
    catalog.createNamespace(Namespace.of("myNamespace"));
    TableIdentifier tableIdentifier = TableIdentifier.parse("myNamespace.myTable");
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, false);

    String locationOverride = tempDir.toString() + "/custom-path";
    Map<String, String> tableProperties = Map.of("key", "value");
    TableCreator tableCreator =
        (catalog1, identifier, schema, spec) ->
            catalog1.createTable(identifier, schema, spec, locationOverride, tableProperties);

    tableUpdater.update(
        tableIdentifier, "main", SCHEMA, PartitionSpec.unpartitioned(), tableCreator);
    assertThat(catalog.tableExists(tableIdentifier)).isTrue();
    assertThat(catalog.loadTable(tableIdentifier).properties().get("key")).isEqualTo("value");
    assertThat(catalog.loadTable(tableIdentifier).location()).isEqualTo(locationOverride);
    TableMetadataCache.ResolvedSchemaInfo cachedSchema =
        cache.schema(tableIdentifier, SCHEMA, false);
    assertThat(cachedSchema.resolvedTableSchema().sameSchema(SCHEMA)).isTrue();
  }

  @Test
  void testTableAlreadyExists() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, false);

    // Make the table non-existent in cache
    cache.exists(tableIdentifier);
    // Create the table
    catalog.createTable(tableIdentifier, SCHEMA);
    // Make sure that the cache is invalidated and the table refreshed without an error
    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> result =
        tableUpdater.update(
            tableIdentifier, "main", SCHEMA, PartitionSpec.unpartitioned(), TableCreator.DEFAULT);
    assertThat(result.f0.resolvedTableSchema().sameSchema(SCHEMA)).isTrue();
    assertThat(result.f0.compareResult()).isEqualTo(CompareSchemasVisitor.Result.SAME);
    assertThat(result.f1).isEqualTo(PartitionSpec.unpartitioned());
  }

  @Test
  void testBranchCreationAndCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, false);

    catalog.createTable(tableIdentifier, SCHEMA);
    tableUpdater.update(
        tableIdentifier, "myBranch", SCHEMA, PartitionSpec.unpartitioned(), TableCreator.DEFAULT);
    TableMetadataCache.CacheItem cacheItem = cache.getInternalCache().get(tableIdentifier);
    assertThat(cacheItem).isNotNull();

    tableUpdater.update(
        tableIdentifier, "myBranch", SCHEMA, PartitionSpec.unpartitioned(), TableCreator.DEFAULT);
    assertThat(cache.getInternalCache()).contains(Map.entry(tableIdentifier, cacheItem));
  }

  @Test
  void testSpecCreation() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, false);

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("data", 10).build();
    tableUpdater.update(tableIdentifier, "main", SCHEMA, spec, TableCreator.DEFAULT);

    Table table = catalog.loadTable(tableIdentifier);
    assertThat(table).isNotNull();
    assertThat(table.spec()).isEqualTo(spec);
  }

  @Test
  void testInvalidateOldCacheEntryOnUpdate() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    cache.schema(tableIdentifier, SCHEMA, false);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, false);

    Schema updated =
        tableUpdater
            .update(
                tableIdentifier,
                "main",
                SCHEMA2,
                PartitionSpec.unpartitioned(),
                TableCreator.DEFAULT)
            .f0
            .resolvedTableSchema();
    assertThat(updated.sameSchema(SCHEMA2)).isTrue();
    assertThat(
            cache.schema(tableIdentifier, SCHEMA2, false).resolvedTableSchema().sameSchema(SCHEMA2))
        .isTrue();
  }

  @Test
  void testLastResultInvalidation() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, false);

    // Initialize cache
    tableUpdater.update(
        tableIdentifier, "main", SCHEMA, PartitionSpec.unpartitioned(), TableCreator.DEFAULT);

    // Update table behind the scenes
    catalog.dropTable(tableIdentifier);
    catalog.createTable(tableIdentifier, SCHEMA2);

    // Cache still stores the old information
    assertThat(cache.schema(tableIdentifier, SCHEMA2, false).compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);

    assertThat(
            tableUpdater
                .update(
                    tableIdentifier,
                    "main",
                    SCHEMA2,
                    PartitionSpec.unpartitioned(),
                    TableCreator.DEFAULT)
                .f0
                .compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.SAME);

    // Last result cache should be cleared
    assertThat(cache.getInternalCache().get(tableIdentifier).inputSchemas())
        .doesNotContainKey(SCHEMA2);
  }

  @Test
  void testDropUnusedColumns() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);

    final boolean dropUnusedColumns = true;
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, dropUnusedColumns);

    catalog.createTable(tableIdentifier, SCHEMA2);

    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> result =
        tableUpdater.update(
            tableIdentifier, "main", SCHEMA, PartitionSpec.unpartitioned(), TableCreator.DEFAULT);

    assertThat(result.f0.compareResult()).isEqualTo(CompareSchemasVisitor.Result.SAME);
    Schema tableSchema = catalog.loadTable(tableIdentifier).schema();
    assertThat(tableSchema.columns()).hasSize(2);
    assertThat(tableSchema.findField("id")).isNotNull();
    assertThat(tableSchema.findField("data")).isNotNull();
    assertThat(tableSchema.findField("extra")).isNull();
  }
}
