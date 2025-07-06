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

import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

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
  void testTableCreation() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog);

    tableUpdater.update(tableIdentifier, "main", SCHEMA, PartitionSpec.unpartitioned());
    assertThat(catalog.tableExists(tableIdentifier)).isTrue();

    TableMetadataCache.ResolvedSchemaInfo cachedSchema = cache.schema(tableIdentifier, SCHEMA);
    assertThat(cachedSchema.resolvedTableSchema().sameSchema(SCHEMA)).isTrue();
  }

  @Test
  void testTableAlreadyExists() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog);

    // Make the table non-existent in cache
    cache.exists(tableIdentifier);
    // Create the table
    catalog.createTable(tableIdentifier, SCHEMA);
    // Make sure that the cache is invalidated and the table refreshed without an error
    Tuple2<TableMetadataCache.ResolvedSchemaInfo, PartitionSpec> result =
        tableUpdater.update(tableIdentifier, "main", SCHEMA, PartitionSpec.unpartitioned());
    assertThat(result.f0.resolvedTableSchema().sameSchema(SCHEMA)).isTrue();
    assertThat(result.f0.compareResult()).isEqualTo(CompareSchemasVisitor.Result.SAME);
    assertThat(result.f1).isEqualTo(PartitionSpec.unpartitioned());
  }

  @Test
  void testBranchCreationAndCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog);

    catalog.createTable(tableIdentifier, SCHEMA);
    tableUpdater.update(tableIdentifier, "myBranch", SCHEMA, PartitionSpec.unpartitioned());
    TableMetadataCache.CacheItem cacheItem = cache.getInternalCache().get(tableIdentifier);
    assertThat(cacheItem).isNotNull();

    tableUpdater.update(tableIdentifier, "myBranch", SCHEMA, PartitionSpec.unpartitioned());
    assertThat(cache.getInternalCache()).contains(Map.entry(tableIdentifier, cacheItem));
  }

  @Test
  void testSpecCreation() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("myTable");
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog);

    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("data", 10).build();
    tableUpdater.update(tableIdentifier, "main", SCHEMA, spec);

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
    cache.schema(tableIdentifier, SCHEMA);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog);

    Schema updated =
        tableUpdater
            .update(tableIdentifier, "main", SCHEMA2, PartitionSpec.unpartitioned())
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
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog);

    // Initialize cache
    tableUpdater.update(tableIdentifier, "main", SCHEMA, PartitionSpec.unpartitioned());

    // Update table behind the scenes
    catalog.dropTable(tableIdentifier);
    catalog.createTable(tableIdentifier, SCHEMA2);

    // Cache still stores the old information
    assertThat(cache.schema(tableIdentifier, SCHEMA2).compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED);

    assertThat(
            tableUpdater
                .update(tableIdentifier, "main", SCHEMA2, PartitionSpec.unpartitioned())
                .f0
                .compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.SAME);

    // Last result cache should be cleared
    assertThat(cache.getInternalCache().get(tableIdentifier).inputSchemas())
        .doesNotContainKey(SCHEMA2);
  }
}
