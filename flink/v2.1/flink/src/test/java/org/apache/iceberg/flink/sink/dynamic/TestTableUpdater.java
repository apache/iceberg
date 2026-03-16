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
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestTableUpdater extends TestFlinkIcebergSinkBase {

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

  @Test
  void testTableCreation(@TempDir Path tempDir) {
    // Location for tables is not configurable for hadoop catalogs
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("catalog", Map.of());
    catalog.createNamespace(Namespace.of("myNamespace"));
    TableIdentifier tableIdentifier = TableIdentifier.parse("myNamespace.myTable");
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, PRESERVE_COLUMNS);

    String locationOverride = tempDir.toString() + "/custom-path";
    Map<String, String> tableProperties = Map.of("key", "value");
    TableCreator tableCreator =
        (catalog1, identifier, schema, spec) ->
            catalog1.createTable(identifier, schema, spec, locationOverride, tableProperties);

    tableUpdater.update(
        tableIdentifier,
        SnapshotRef.MAIN_BRANCH,
        SCHEMA,
        PartitionSpec.unpartitioned(),
        tableCreator);
    assertThat(catalog.tableExists(tableIdentifier)).isTrue();
    assertThat(catalog.loadTable(tableIdentifier).properties().get("key")).isEqualTo("value");
    assertThat(catalog.loadTable(tableIdentifier).location()).isEqualTo(locationOverride);
    TableMetadataCache.ResolvedSchemaInfo cachedSchema = cache.schema(tableIdentifier, SCHEMA);
    assertThat(cachedSchema.resolvedTableSchema().sameSchema(SCHEMA)).isTrue();
  }

  @Test
  void testTableAlreadyExists() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, PRESERVE_COLUMNS);

    // Make the table non-existent in cache
    cache.exists(tableIdentifier);
    // Create the table
    catalog.createTable(tableIdentifier, SCHEMA);
    // Make sure that the cache is invalidated and the table refreshed without an error
    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> result =
        tableUpdater.update(
            tableIdentifier,
            SnapshotRef.MAIN_BRANCH,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            TableCreator.DEFAULT);
    assertThat(result.f0.resolvedTableSchema().sameSchema(SCHEMA)).isTrue();
    assertThat(result.f0.compareResult()).isEqualTo(CompareSchemasVisitor.Result.SAME);
    assertThat(result.f1).isEqualTo(PartitionSpec.unpartitioned());
  }

  @Test
  void testBranchCreationAndCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, PRESERVE_COLUMNS);

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
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, PRESERVE_COLUMNS);

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("data", 10).build();
    tableUpdater.update(
        tableIdentifier, SnapshotRef.MAIN_BRANCH, SCHEMA, spec, TableCreator.DEFAULT);

    Table table = catalog.loadTable(tableIdentifier);
    assertThat(table).isNotNull();
    assertThat(table.spec()).isEqualTo(spec);
  }

  @Test
  void testInvalidateOldCacheEntryOnUpdate() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    cache.schema(tableIdentifier, SCHEMA);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, PRESERVE_COLUMNS);

    Schema updated =
        tableUpdater
            .update(
                tableIdentifier,
                SnapshotRef.MAIN_BRANCH,
                SCHEMA2,
                PartitionSpec.unpartitioned(),
                TableCreator.DEFAULT)
            .f0
            .resolvedTableSchema();
    assertThat(updated.sameSchema(SCHEMA2)).isTrue();
    assertThat(cache.schema(tableIdentifier, SCHEMA2).resolvedTableSchema().sameSchema(SCHEMA2))
        .isTrue();
  }

  @Test
  void testLastResultInvalidation() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, PRESERVE_COLUMNS);

    // Initialize cache
    tableUpdater.update(
        tableIdentifier,
        SnapshotRef.MAIN_BRANCH,
        SCHEMA,
        PartitionSpec.unpartitioned(),
        TableCreator.DEFAULT);

    // Update table behind the scenes
    catalog.dropTable(tableIdentifier);
    catalog.createTable(tableIdentifier, SCHEMA2);

    // Cache still stores the old information
    assertThat(cache.schema(tableIdentifier, SCHEMA2).compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);

    assertThat(
            tableUpdater
                .update(
                    tableIdentifier,
                    SnapshotRef.MAIN_BRANCH,
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCaseSensitivity(boolean caseSensitive) {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, caseSensitive, DROP_COLUMNS);

    TableUpdater tableUpdater = new TableUpdater(cache, catalog, caseSensitive, DROP_COLUMNS);

    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "extra", Types.StringType.get()));

    catalog.createTable(tableIdentifier, schema);

    Schema schemaWithUpperCase =
        new Schema(
            Types.NestedField.optional(1, "Id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "Data", Types.StringType.get()),
            Types.NestedField.optional(3, "Extra", Types.StringType.get()));

    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> result =
        tableUpdater.update(
            tableIdentifier,
            SnapshotRef.MAIN_BRANCH,
            schemaWithUpperCase,
            PartitionSpec.unpartitioned(),
            TableCreator.DEFAULT);

    assertThat(result.f0.compareResult()).isEqualTo(CompareSchemasVisitor.Result.SAME);

    Schema tableSchema = catalog.loadTable(tableIdentifier).schema();
    if (caseSensitive) {
      assertThat(tableSchema.columns()).hasSize(3);
      assertThat(tableSchema.findField("Id")).isNotNull();
      assertThat(tableSchema.findField("Data")).isNotNull();
      assertThat(tableSchema.findField("Extra")).isNotNull();
    } else {
      assertThat(tableSchema.sameSchema(schema)).isTrue();
    }
  }

  @Test
  void testDropUnusedColumns() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, DROP_COLUMNS);

    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, DROP_COLUMNS);

    catalog.createTable(tableIdentifier, SCHEMA2);

    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> result =
        tableUpdater.update(
            tableIdentifier,
            SnapshotRef.MAIN_BRANCH,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            TableCreator.DEFAULT);

    assertThat(result.f0.compareResult()).isEqualTo(CompareSchemasVisitor.Result.SAME);
    Schema tableSchema = catalog.loadTable(tableIdentifier).schema();
    assertThat(tableSchema.columns()).hasSize(2);
    assertThat(tableSchema.findField("id")).isNotNull();
    assertThat(tableSchema.findField("data")).isNotNull();
    assertThat(tableSchema.findField("extra")).isNull();
  }

  @Test
  void testNamespaceAndTableCreation() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    SupportsNamespaces namespaceCatalog = (SupportsNamespaces) catalog;
    TableIdentifier tableIdentifier = TableIdentifier.of("new_namespace", "myTable");
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, PRESERVE_COLUMNS);

    assertThat(namespaceCatalog.namespaceExists(Namespace.of("new_namespace"))).isFalse();
    assertThat(catalog.tableExists(tableIdentifier)).isFalse();

    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> result =
        tableUpdater.update(
            tableIdentifier,
            SnapshotRef.MAIN_BRANCH,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            TableCreator.DEFAULT);

    assertThat(namespaceCatalog.namespaceExists(Namespace.of("new_namespace"))).isTrue();

    assertThat(catalog.tableExists(tableIdentifier)).isTrue();
    assertThat(result.f0.resolvedTableSchema().sameSchema(SCHEMA)).isTrue();
    assertThat(result.f0.compareResult()).isEqualTo(CompareSchemasVisitor.Result.SAME);
  }

  @Test
  void testTableCreationWithExistingNamespace() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    SupportsNamespaces namespaceCatalog = (SupportsNamespaces) catalog;
    Namespace namespace = Namespace.of("existing_namespace");
    namespaceCatalog.createNamespace(namespace);

    TableIdentifier tableIdentifier = TableIdentifier.of("existing_namespace", "myTable");
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10, CASE_SENSITIVE, PRESERVE_COLUMNS);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, CASE_SENSITIVE, PRESERVE_COLUMNS);

    assertThat(namespaceCatalog.namespaceExists(namespace)).isTrue();
    assertThat(catalog.tableExists(tableIdentifier)).isFalse();

    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> result =
        tableUpdater.update(
            tableIdentifier,
            SnapshotRef.MAIN_BRANCH,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            TableCreator.DEFAULT);

    assertThat(namespaceCatalog.namespaceExists(namespace)).isTrue();
    assertThat(catalog.tableExists(tableIdentifier)).isTrue();
    assertThat(result.f0.resolvedTableSchema().sameSchema(SCHEMA)).isTrue();
    assertThat(result.f0.compareResult()).isEqualTo(CompareSchemasVisitor.Result.SAME);
  }
}
