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
import org.apache.commons.lang3.SerializationUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
  void testSchemaCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);

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
  void testSpecCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");

    // Create table with initial partition spec
    PartitionSpec initialSpec = PartitionSpec.builderFor(SCHEMA).bucket("data", 10).build();
    catalog.createTable(tableIdentifier, SCHEMA, initialSpec);
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);

    // Test spec caching - first call should load from catalog
    PartitionSpec cachedSpec1 = cache.spec(tableIdentifier, initialSpec);
    assertThat(cachedSpec1).isNotNull();
    assertThat(cachedSpec1).isEqualTo(initialSpec);

    // Second call should return cached result
    PartitionSpec cachedSpec2 = cache.spec(tableIdentifier, initialSpec);
    assertThat(cachedSpec2).isSameAs(cachedSpec1);

    // Test with different but compatible spec
    PartitionSpec compatibleSpec = PartitionSpec.builderFor(SCHEMA).bucket("data", 10).build();
    PartitionSpec cachedCompatibleSpec = cache.spec(tableIdentifier, compatibleSpec);
    assertThat(cachedCompatibleSpec).isEqualTo(initialSpec);

    // Test with incompatible spec - should return null
    PartitionSpec incompatibleSpec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    PartitionSpec cachedIncompatibleSpec = cache.spec(tableIdentifier, incompatibleSpec);
    assertThat(cachedIncompatibleSpec).isNull();
  }

  @Test
  void testPropertiesCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");

    // Create table with some properties
    Map<String, String> initialProperties = Maps.newHashMap();
    initialProperties.put("write.format.default", "parquet");
    initialProperties.put("write.parquet.compression-codec", "snappy");
    catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned(), initialProperties);

    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);

    // Test properties caching - first call should load from catalog
    Map<String, String> cachedProperties = cache.properties(tableIdentifier);
    assertThat(cachedProperties).isNotNull();
    assertThat(cachedProperties).containsAllEntriesOf(initialProperties);

    // Second call should return cached result
    Map<String, String> cachedProperties2 = cache.properties(tableIdentifier);
    assertThat(cachedProperties2).isSameAs(cachedProperties);

    // Test with non-existent table
    TableIdentifier nonExistentTable = TableIdentifier.parse("default.nonExistentTable");
    Map<String, String> nonExistentProperties = cache.properties(nonExistentTable);
    assertThat(nonExistentProperties).isNull();
  }

  @Test
  void testCacheInvalidationAfterSchemaChange() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SCHEMA);
    TableMetadataCache cache = new TableMetadataCache(catalog, 10, Long.MAX_VALUE, 10);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog);

    Schema schema1 = cache.schema(tableIdentifier, SCHEMA).resolvedTableSchema();
    assertThat(schema1.sameSchema(SCHEMA)).isTrue();

    catalog.dropTable(tableIdentifier);
    catalog.createTable(tableIdentifier, SCHEMA2);
    tableUpdater.update(tableIdentifier, "main", SCHEMA2, PartitionSpec.unpartitioned());

    Schema schema2 = cache.schema(tableIdentifier, SCHEMA2).resolvedTableSchema();
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
}
