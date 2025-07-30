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

public class TestTableSerializerCache {

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
    Supplier<RowDataSerializer> creator1a =
        () -> cache.serializer("table", schema1, PartitionSpec.unpartitioned());
    Supplier<RowDataSerializer> creator1b =
        () -> cache.serializer("table", schema2, PartitionSpec.unpartitioned());
    Supplier<RowDataSerializer> creator2 =
        () -> cache.serializer("table2", schema2, PartitionSpec.unpartitioned());

    RowDataSerializer serializer1a = creator1a.get();
    RowDataSerializer serializer1b = creator1b.get();
    RowDataSerializer serializer2 = creator2.get();
    assertThat(serializer1a).isNotSameAs(serializer1b).isNotSameAs(serializer2);

    assertThat(serializer1a).isSameAs(creator1a.get());
    assertThat(serializer1b).isSameAs(creator1b.get());
    assertThat(serializer2).isSameAs(creator2.get());
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
  void testCacheEviction() {
    cache = new TableSerializerCache(CATALOG_EXTENSION.catalogLoader(), 0);
    assertThat(cache.maximumSize()).isEqualTo(0);

    Supplier<RowDataSerializer> creator1 =
        () -> cache.serializer("table", schema1, PartitionSpec.unpartitioned());
    Supplier<RowDataSerializer> creator2 =
        () -> cache.serializer("table2", schema2, PartitionSpec.unpartitioned());

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
}
