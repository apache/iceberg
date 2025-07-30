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
import java.time.ZoneOffset;
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
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, Clock.systemUTC(), 10);

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
        new TableMetadataCache(catalog, 10, Long.MAX_VALUE, Clock.systemUTC(), 10);
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
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 0, Long.MAX_VALUE, Clock.systemUTC(), 10);

    assertThat(cache.getInternalCache()).isEmpty();
  }

  @Test
  void testNoCacheRefreshBeforeRefreshIntervalElapses() {
    // Create table
    Catalog catalog = CATALOG_EXTENSION.catalog();
    TableIdentifier tableIdentifier = TableIdentifier.parse("default.myTable");
    Table table = catalog.createTable(tableIdentifier, SCHEMA2);

    // Create test clock
    long firstEpochMillis = 1L;
    MutableClock testClock = new MutableClock(ZoneOffset.UTC, firstEpochMillis);

    // Init cache
    long refreshIntervalMillis = 100L;
    TableMetadataCache cache =
        new TableMetadataCache(catalog, 10, refreshIntervalMillis, testClock, 10);
    cache.update(tableIdentifier, table);

    // Cache schema
    Schema schema = cache.schema(tableIdentifier, SCHEMA2).resolvedTableSchema();
    assertThat(schema.sameSchema(SCHEMA2)).isTrue();

    // Progress clock to timestamp before refresh interval
    long secondEpochMilli = 2L;
    testClock.setEpochMilli(secondEpochMilli);

    // Cache schema with fewer fields
    TableMetadataCache.ResolvedSchemaInfo schemaInfo = cache.schema(tableIdentifier, SCHEMA);
    assertThat(schemaInfo.resolvedTableSchema().sameSchema(SCHEMA2)).isTrue();
    assertThat(schemaInfo.compareResult())
        .isEqualTo(CompareSchemasVisitor.Result.DATA_CONVERSION_NEEDED);

    // Assert both schemas are in cache
    assertThat(cache.getResolvedSchemaInfo(tableIdentifier, SCHEMA)).isNotNull();
    assertThat(cache.getResolvedSchemaInfo(tableIdentifier, SCHEMA2)).isNotNull();
  }

  private static class MutableClock extends Clock {
    private final ZoneId zoneId;
    private long epochMilli;

    MutableClock(ZoneId zoneId, long epochMilli) {
      this.zoneId = zoneId;
      this.epochMilli = epochMilli;
    }

    void setEpochMilli(long epochMilli) {
      this.epochMilli = epochMilli;
    }

    public ZoneId getZone() {
      return this.zoneId;
    }

    public Clock withZone(ZoneId zone) {
      return zone.equals(this.zoneId) ? this : new MutableClock(zone, this.epochMilli);
    }

    public Instant instant() {
      return Instant.ofEpochMilli(this.epochMilli);
    }
  }
}
