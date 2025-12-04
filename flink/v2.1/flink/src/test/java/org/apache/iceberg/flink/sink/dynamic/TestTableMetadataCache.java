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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestTableMetadataCache extends TestFlinkIcebergSinkBase {

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
  void testCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);

    Schema schema1 = cache.schema(tableIdentifier, SCHEMA, false).resolvedTableSchema();
    assertThat(schema1.sameSchema(SCHEMA)).isTrue();
    assertThat(
            cache
                .schema(tableIdentifier, SerializationUtils.clone(SCHEMA), false)
                .resolvedTableSchema())
        .isEqualTo(schema1);

    assertThat(cache.schema(tableIdentifier, SCHEMA2, false))
        .isEqualTo(TableMetadataCache.NOT_FOUND);

    schema1 = cache.schema(tableIdentifier, SCHEMA, false).resolvedTableSchema();
    assertThat(
            cache
                .schema(tableIdentifier, SerializationUtils.clone(SCHEMA), false)
                .resolvedTableSchema())
        .isEqualTo(schema1);
  }

  @Test
  void testCacheInvalidationAfterSchemaChange() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog, false);

    Schema schema1 = cache.schema(tableIdentifier, SCHEMA, false).resolvedTableSchema();
    assertThat(schema1.sameSchema(SCHEMA)).isTrue();

    catalog.dropTable(tableIdentifier);
    catalog.createTable(tableIdentifier, SCHEMA2);
    tableUpdater.update(
        tableIdentifier, "main", SCHEMA2, PartitionSpec.unpartitioned(), TableCreator.DEFAULT);

    Schema schema2 = cache.schema(tableIdentifier, SCHEMA2, false).resolvedTableSchema();
    assertThat(schema2.sameSchema(SCHEMA2)).isTrue();
  }

  @Test
  void testCachingDisabled() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache = new TableMetadataCache(catalog, 0, Long.MAX_VALUE, 10);

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
            catalog, 10, 100L, 10, Clock.fixed(Instant.now(), ZoneId.systemDefault()));
    cache.update(tableIdentifier, table);

    // Cache schema
    Schema schema = cache.schema(tableIdentifier, SCHEMA2, false).resolvedTableSchema();
    assertThat(schema.sameSchema(SCHEMA2)).isTrue();

    // Cache schema with fewer fields
    TableMetadataCache.ResolvedSchemaInfo schemaInfo = cache.schema(tableIdentifier, SCHEMA, false);
    assertThat(schemaInfo.resolvedTableSchema().sameSchema(SCHEMA2)).isTrue();
    assertThat(schemaInfo.compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED);

    // Assert both schemas are in cache
    TableMetadataCache.CacheItem cacheItem = cache.getInternalCache().get(tableIdentifier);
    assertThat(cacheItem).isNotNull();
    assertThat(cacheItem.inputSchemas()).containsKeys(SCHEMA, SCHEMA2);
  }
}
