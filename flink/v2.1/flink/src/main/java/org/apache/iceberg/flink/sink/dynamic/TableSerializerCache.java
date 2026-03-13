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
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
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
  private transient Map<String, SerializerInfo> serializers;

  TableSerializerCache(CatalogLoader catalogLoader, int maximumSize) {
    this.catalogLoader = catalogLoader;
    this.maximumSize = maximumSize;
  }

  RowDataSerializer serializer(String tableName, Schema schema, PartitionSpec spec) {
    return serializer(tableName, schema, spec, null, null).f0;
  }

  Tuple3<RowDataSerializer, Schema, PartitionSpec> serializerWithSchemaAndSpec(
      String tableName, Integer schemaId, Integer specId) {
    return serializer(tableName, null, null, schemaId, specId);
  }

  private Tuple3<RowDataSerializer, Schema, PartitionSpec> serializer(
      String tableName,
      @Nullable Schema unknownSchema,
      @Nullable PartitionSpec unknownSpec,
      @Nullable Integer schemaId,
      @Nullable Integer specId) {
    Preconditions.checkState(
        (unknownSchema == null && unknownSpec == null) ^ (schemaId == null && specId == null),
        "Either the full schema/spec or their ids must be provided.");

    if (serializers == null) {
      // We need to initialize the cache at the first time
      this.serializers = new LRUCache<>(maximumSize);
    }

    SerializerInfo info = serializers.computeIfAbsent(tableName, SerializerInfo::new);
    Schema schema = unknownSchema != null ? unknownSchema : info.schemas.get(schemaId);
    PartitionSpec spec = unknownSpec != null ? unknownSpec : info.specs.get(specId);

    if (schema == null || spec == null) {
      info.update();
      schema = info.schemas.get(schemaId);
      spec = info.specs.get(specId);
    }

    RowDataSerializer serializer =
        info.serializers.computeIfAbsent(
            schema, s -> new RowDataSerializer(FlinkSchemaUtil.convert(s)));

    return Tuple3.of(serializer, schema, spec);
  }

  CatalogLoader catalogLoader() {
    return catalogLoader;
  }

  int maximumSize() {
    return maximumSize;
  }

  private class SerializerInfo {
    private final String tableName;
    private final Map<Schema, RowDataSerializer> serializers;
    private Map<Integer, Schema> schemas;
    private Map<Integer, PartitionSpec> specs;

    SerializerInfo(String tableName) {
      this.tableName = tableName;
      this.serializers = Maps.newHashMapWithExpectedSize(2);
      this.schemas = Maps.newHashMapWithExpectedSize(1);
      this.specs = Maps.newHashMapWithExpectedSize(0);
    }

    private void update() {
      Table table = catalogLoader.loadCatalog().loadTable(TableIdentifier.parse(tableName));
      schemas = table.schemas();
      specs = table.specs();
    }
  }

  @VisibleForTesting
  Map<String, SerializerInfo> getCache() {
    return serializers;
  }
}
