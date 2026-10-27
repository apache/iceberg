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

import java.io.Serializable;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * A Cache which holds Flink's {@link RowDataSerializer} for a given table name and schema. This
 * avoids re-creating the serializer for a given table schema for every incoming record.
 *
 * <p>There is an additional optimization built into this class: Users do not have to supply the
 * full schema / spec, but can also provide their id. This avoids transferring the schema / spec for
 * every record. If the id is unknown, the schema / spec will be retrieved from the catalog.
 *
 * <p>Note that the caller must ensure that ids are only used for known schemas / specs. The id
 * optimization must not be used in the update path.
 */
@Internal
class TableSerializerCache implements Serializable {

  private final CatalogLoader catalogLoader;
  private final int maximumSize;

  // Intentionally not closed; the catalog is reused for the serializer's lifetime.
  private transient Catalog catalog;
  private transient Map<String, SerializerInfo> serializers;

  TableSerializerCache(CatalogLoader catalogLoader, int maximumSize) {
    this.catalogLoader = catalogLoader;
    this.maximumSize = maximumSize;
  }

  RowDataSerializer serializer(String tableName, Schema schema) {
    return serializerInfo(tableName).serializerBySchema(schema);
  }

  /**
   * Resolves the serializer when the caller already holds the schema and spec along with their ids.
   * The serializer is looked up by schema id, so it is not rebuilt when the {@link Schema} instance
   * changes.
   *
   * <p>The id-keyed serializers are shared with {@link #serializerWithSchemaAndSpec}, so the schema
   * must come from the table. Do not use this for user-supplied schemas, whose ids are not reliable
   * and could collide with the table's schemas.
   */
  RowDataSerializer serializerById(String tableName, Schema schema) {
    return serializerInfo(tableName).serializerBySchemaId(schema);
  }

  Tuple3<RowDataSerializer, Schema, PartitionSpec> serializerWithSchemaAndSpec(
      String tableName, Integer schemaId, Integer specId) {
    SerializerInfo info = serializerInfo(tableName);
    Schema schema = info.schemas.get(schemaId);
    PartitionSpec spec = info.specs.get(specId);

    if (schema == null || spec == null) {
      info.update();
      schema = info.schemas.get(schemaId);
      spec = info.specs.get(specId);
    }

    Preconditions.checkArgument(
        schema != null,
        "SchemaId:%s not found in table %s. Expected SchemaId:%s to exist.",
        schemaId,
        tableName,
        schemaId);
    Preconditions.checkArgument(
        spec != null,
        "SpecId:%s not found in table %s. Expected SpecId:%s to exist.",
        specId,
        tableName,
        specId);

    return Tuple3.of(info.serializerBySchemaId(schema), schema, spec);
  }

  private SerializerInfo serializerInfo(String tableName) {
    if (serializers == null) {
      this.serializers = new LRUCache<>(maximumSize);
    }

    return serializers.computeIfAbsent(tableName, SerializerInfo::new);
  }

  CatalogLoader catalogLoader() {
    return catalogLoader;
  }

  int maximumSize() {
    return maximumSize;
  }

  private class SerializerInfo {
    private final String tableName;
    private final Map<Schema, RowDataSerializer> serializersBySchema;
    private final Map<Integer, RowDataSerializer> serializersBySchemaId;
    private Map<Integer, Schema> schemas;
    private Map<Integer, PartitionSpec> specs;

    SerializerInfo(String tableName) {
      this.tableName = tableName;
      this.serializersBySchema = new LRUCache<>(maximumSize);
      this.serializersBySchemaId = Maps.newHashMapWithExpectedSize(2);
      this.schemas = Maps.newHashMapWithExpectedSize(1);
      this.specs = Maps.newHashMapWithExpectedSize(0);
    }

    private RowDataSerializer serializerBySchema(Schema schema) {
      return serializersBySchema.computeIfAbsent(schema, this::createSerializer);
    }

    private RowDataSerializer serializerBySchemaId(Schema schema) {
      return serializersBySchemaId.computeIfAbsent(
          schema.schemaId(), id -> createSerializer(schema));
    }

    private RowDataSerializer createSerializer(Schema schema) {
      return new RowDataSerializer(FlinkSchemaUtil.convert(schema));
    }

    private void update() {
      if (catalog == null) {
        catalog = catalogLoader.loadCatalog();
      }

      Table table = catalog.loadTable(TableIdentifier.parse(tableName));
      schemas = table.schemas();
      specs = table.specs();
    }
  }

  @VisibleForTesting
  Map<String, SerializerInfo> getCache() {
    return serializers;
  }
}
