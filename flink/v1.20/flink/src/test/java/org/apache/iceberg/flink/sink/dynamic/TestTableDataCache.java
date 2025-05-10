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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.sink.TestFlinkIcebergSinkBase;
import org.junit.jupiter.api.Test;

public class TestTableDataCache extends TestFlinkIcebergSinkBase {

  @Test
  void testCaching() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SimpleDataUtil.SCHEMA);
    TableDataCache cache = new TableDataCache(catalog, 10, Long.MAX_VALUE);

    Schema schema1 = cache.schema(tableIdentifier, SimpleDataUtil.SCHEMA).f0;
    assertThat(schema1.sameSchema(SimpleDataUtil.SCHEMA)).isTrue();
    assertThat(cache.schema(tableIdentifier, SerializationUtils.clone(SimpleDataUtil.SCHEMA)).f0)
        .isEqualTo(schema1);

    assertThat(cache.schema(tableIdentifier, SimpleDataUtil.SCHEMA2))
        .isEqualTo(TableDataCache.NOT_FOUND);

    schema1 = cache.schema(tableIdentifier, SimpleDataUtil.SCHEMA).f0;
    assertThat(cache.schema(tableIdentifier, SerializationUtils.clone(SimpleDataUtil.SCHEMA)).f0)
        .isEqualTo(schema1);
  }

  @Test
  void testCacheInvalidationAfterSchemaChange() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SimpleDataUtil.SCHEMA);
    TableDataCache cache = new TableDataCache(catalog, 10, Long.MAX_VALUE);
    TableUpdater tableUpdater = new TableUpdater(cache, catalog);

    Schema schema1 = cache.schema(tableIdentifier, SimpleDataUtil.SCHEMA).f0;
    assertThat(schema1.sameSchema(SimpleDataUtil.SCHEMA)).isTrue();

    catalog.dropTable(tableIdentifier);
    catalog.createTable(tableIdentifier, SimpleDataUtil.SCHEMA2);
    tableUpdater.update(
        tableIdentifier, "main", SimpleDataUtil.SCHEMA2, PartitionSpec.unpartitioned());

    Schema schema2 = cache.schema(tableIdentifier, SimpleDataUtil.SCHEMA2).f0;
    assertThat(schema2.sameSchema(SimpleDataUtil.SCHEMA2)).isTrue();
  }

  @Test
  void testCachingDisabled() {
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    catalog.createTable(tableIdentifier, SimpleDataUtil.SCHEMA);
    TableDataCache cache = new TableDataCache(catalog, 0, Long.MAX_VALUE);

    // Cleanup routine doesn't run after every write
    cache.getInternalCache().cleanUp();
    assertThat(cache.getInternalCache().estimatedSize()).isEqualTo(0);
  }
}
