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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.Serializable;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

@Internal
class RowDataSerializerCache implements Serializable {
  private final CatalogLoader catalogLoader;
  private final int maximumSize;
  private transient Cache<String, SerializerInfo> serializers;

  RowDataSerializerCache(CatalogLoader catalogLoader, int maximumSize) {
    this.catalogLoader = catalogLoader;
    this.maximumSize = maximumSize;
  }

  Tuple3<RowDataSerializer, Schema, PartitionSpec> serializer(
      String tableName,
      Schema unknownSchema,
      PartitionSpec unknownSpec,
      Integer schemaId,
      Integer specId) {
    if (serializers == null) {
      // We need to initialize the cache at the first time
      this.serializers = Caffeine.newBuilder().maximumSize(maximumSize).build();
    }

    SerializerInfo info = serializers.get(tableName, SerializerInfo::new);
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
      this.schemas = Maps.newHashMapWithExpectedSize(0);
      this.specs = Maps.newHashMapWithExpectedSize(0);
    }

    private void update() {
      Table table = catalogLoader.loadCatalog().loadTable(TableIdentifier.parse(tableName));
      schemas = table.schemas();
      specs = table.specs();
    }
  }
}
