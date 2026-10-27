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

import static org.apache.iceberg.types.Types.DoubleType;
import static org.apache.iceberg.types.Types.LongType;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.iceberg.types.Types.StringType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.function.Supplier;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestTableSerializerCache {

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION = new HadoopCatalogExtension("db", "table");

  Schema schema1 = new Schema(23, required(1, "id", LongType.get()));

  Schema schema2 =
      new Schema(
          42,
          required(1, "id", LongType.get()),
          optional(2, "data", StringType.get()),
          optional(3, "double", DoubleType.get()));

  TableSerializerCache cache = new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 10);

  @Test
  void testFullSchemaCaching() {
    Supplier<RowDataSerializer> creator1a = () -> cache.serializer("table", schema1);
    Supplier<RowDataSerializer> creator1b = () -> cache.serializer("table", schema2);
    Supplier<RowDataSerializer> creator2 = () -> cache.serializer("table2", schema2);

    RowDataSerializer serializer1a = creator1a.get();
    RowDataSerializer serializer1b = creator1b.get();
    RowDataSerializer serializer2 = creator2.get();
    assertThat(serializer1a).isNotSameAs(serializer1b).isNotSameAs(serializer2);

    assertThat(serializer1a).isSameAs(creator1a.get());
    assertThat(serializer1b).isSameAs(creator1b.get());
    assertThat(serializer2).isSameAs(creator2.get());
  }

  @Test
  void testSerializerById() {
    LoadCountingCatalogLoader catalogLoader =
        new LoadCountingCatalogLoader(CATALOG_EXTENSION.catalogLoader());
    cache = new TableSerializerCache(catalogLoader, 10);

    Schema schema1Copy = new Schema(schema1.schemaId(), required(1, "id", LongType.get()));
    Supplier<RowDataSerializer> creator1a = () -> cache.serializerById("table", schema1);
    Supplier<RowDataSerializer> creator1b = () -> cache.serializerById("table", schema1Copy);
    Supplier<RowDataSerializer> creator2 = () -> cache.serializerById("table", schema2);
    Supplier<RowDataSerializer> creator3 = () -> cache.serializerById("table2", schema1);

    RowDataSerializer serializer1a = creator1a.get();
    RowDataSerializer serializer1b = creator1b.get();
    RowDataSerializer serializer2 = creator2.get();
    RowDataSerializer serializer3 = creator3.get();

    assertThat(serializer1a).isNotSameAs(serializer2).isNotSameAs(serializer3);
    assertThat(serializer1b).isSameAs(serializer1a);
    assertThat(catalogLoader.loadCount()).isEqualTo(0);
  }

  @Test
  void testCachingWithSchemaLookup() {
    CatalogLoader catalogLoader = CATALOG_EXTENSION.catalogLoader();
    cache = new TableSerializerCache(catalogLoader, 10);

    Catalog catalog = catalogLoader.loadCatalog();
    Table table = catalog.createTable(TableIdentifier.of("table"), schema1);

    Tuple3<RowDataSerializer, Schema, PartitionSpec> serializerWithSchemaAndSpec =
        cache.serializerWithSchemaAndSpec(
            "table", table.schema().schemaId(), PartitionSpec.unpartitioned().specId());
    assertThat(serializerWithSchemaAndSpec).isNotNull();
    assertThat(serializerWithSchemaAndSpec.f0).isNotNull();
    assertThat(serializerWithSchemaAndSpec.f1.sameSchema(table.schema())).isTrue();
    assertThat(serializerWithSchemaAndSpec.f2).isEqualTo(table.spec());

    Tuple3<RowDataSerializer, Schema, PartitionSpec> serializerWithSchemaAndSpec2 =
        cache.serializerWithSchemaAndSpec(
            "table", table.schema().schemaId(), PartitionSpec.unpartitioned().specId());

    assertThat(serializerWithSchemaAndSpec.f0).isSameAs(serializerWithSchemaAndSpec2.f0);
    assertThat(serializerWithSchemaAndSpec.f1).isSameAs(serializerWithSchemaAndSpec2.f1);
    assertThat(serializerWithSchemaAndSpec.f2).isSameAs(serializerWithSchemaAndSpec2.f2);
  }

  @Test
  void testSchemaLookupWithSchemaEvolution() {
    Table table = CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table"), schema1);
    cache = new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 10);
    int specId = table.spec().specId();
    int schemaId1 = table.schema().schemaId();
    Supplier<RowDataSerializer> creator1a =
        () -> cache.serializerWithSchemaAndSpec("table", schemaId1, specId).f0;

    table.updateSchema().addColumn("data", StringType.get()).commit();

    int schemaId2 = table.schema().schemaId();
    Supplier<RowDataSerializer> creator2 =
        () -> cache.serializerWithSchemaAndSpec("table", schemaId2, specId).f0;
    Supplier<RowDataSerializer> creator1b =
        () -> cache.serializerWithSchemaAndSpec("table", schemaId1, specId).f0;

    assertThat(creator2.get()).isNotSameAs(creator1a.get());
    assertThat(creator1b.get()).isSameAs(creator1a.get());
  }

  @Test
  void testSchemaSerializerEviction() {
    cache = new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 1);
    Supplier<RowDataSerializer> creator1 = () -> cache.serializer("table", schema1);
    Supplier<RowDataSerializer> creator2 = () -> cache.serializer("table", schema2);

    RowDataSerializer serializer1 = creator1.get();
    assertThat(creator1.get()).isSameAs(serializer1);

    // Only one schema fits, so caching schema2 evicts the serializer for schema1
    RowDataSerializer serializer2 = creator2.get();
    assertThat(creator2.get()).isSameAs(serializer2);
    assertThat(creator1.get()).isNotSameAs(serializer1);
  }

  @Test
  void testSerializerByIdSharedWithSchemaLookup() {
    Table table = CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table"), schema1);
    cache = new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 10);
    int schemaId = table.schema().schemaId();
    int specId = table.spec().specId();
    Supplier<RowDataSerializer> creator1 = () -> cache.serializerById("table", table.schema());
    Supplier<RowDataSerializer> creator2 =
        () -> cache.serializerWithSchemaAndSpec("table", schemaId, specId).f0;

    RowDataSerializer serializer1 = creator1.get();

    assertThat(creator2.get()).isSameAs(serializer1);
    assertThat(creator1.get()).isSameAs(serializer1);
  }

  @Test
  void testSchemaLookupWithUnknownSchemaId() {
    Table table = CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table"), schema1);
    cache = new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 10);
    int specId = table.spec().specId();

    assertThatThrownBy(() -> cache.serializerWithSchemaAndSpec("table", 999, specId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("SchemaId:999 not found in table table. Expected SchemaId:999 to exist.");
  }

  @Test
  void testSchemaLookupWithUnknownSpecId() {
    Table table = CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table"), schema1);
    cache = new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 10);
    int schemaId = table.schema().schemaId();

    assertThatThrownBy(() -> cache.serializerWithSchemaAndSpec("table", schemaId, 999))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("SpecId:999 not found in table table. Expected SpecId:999 to exist.");
  }

  @Test
  void testCacheEviction() {
    cache = new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 0);
    assertThat(cache.maximumSize()).isEqualTo(0);

    Supplier<RowDataSerializer> creator1 = () -> cache.serializer("table", schema1);
    Supplier<RowDataSerializer> creator2 = () -> cache.serializer("table2", schema2);

    RowDataSerializer serializer1 = creator1.get();
    RowDataSerializer serializer2 = creator2.get();

    cache.getCache().clear();
    assertThat(serializer1).isNotSameAs(creator1.get());
    assertThat(serializer2).isNotSameAs(creator2.get());
  }

  @Test
  void testCacheSize() {
    cache = new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 1000);
    assertThat(cache.maximumSize()).isEqualTo(1000);
  }

  @Test
  void testReusesCatalogAcrossLookups() {
    Table table = CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table"), schema1);
    Table table2 = CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table2"), schema2);

    LoadCountingCatalogLoader catalogLoader =
        new LoadCountingCatalogLoader(CATALOG_EXTENSION.catalogLoader());
    cache = new TableSerializerCache(catalogLoader, 10);

    // schema/spec ids are unknown, so both lookups miss the cache and need the catalog
    cache.serializerWithSchemaAndSpec(
        "table", table.schema().schemaId(), PartitionSpec.unpartitioned().specId());
    cache.serializerWithSchemaAndSpec(
        "table2", table2.schema().schemaId(), PartitionSpec.unpartitioned().specId());

    assertThat(catalogLoader.loadCount()).isEqualTo(1);
  }

  private static class LoadCountingCatalogLoader implements CatalogLoader {
    private final CatalogLoader delegate;
    private int loadCount = 0;

    private LoadCountingCatalogLoader(CatalogLoader delegate) {
      this.delegate = delegate;
    }

    private int loadCount() {
      return loadCount;
    }

    @Override
    public Catalog loadCatalog() {
      loadCount += 1;
      return delegate.loadCatalog();
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public CatalogLoader clone() {
      return this;
    }
  }
}
