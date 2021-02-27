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

package org.apache.iceberg.mr.hive;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.SerializationUtil;


class HiveIcebergConfigUtil {

  private static final String TABLE_SCHEMA_MAP = "iceberg.mr.table.schema.map";


  private HiveIcebergConfigUtil() {
  }

  /**
   * Copies schema provided by schemaSupplier into the configuration.
   *
   * While copying, the schema is added into a map(tablename -> schema). This map is serialized and set as the
   * value for the key TABLE_SCHEMA_MAP.
   */
  static void copySchemaToConf(Supplier<Schema> schemaSupplier, Configuration configuration, Properties tblProperties) {
    String tableName = tblProperties.getProperty(Catalogs.NAME);
    Map<String, String> tableToSchema =
        Optional.ofNullable(configuration.get(TABLE_SCHEMA_MAP))
            .map(x -> (HashMap<String, String>) SerializationUtil.deserializeFromBase64(x))
            .orElseGet(() -> new HashMap<>());
    if (!tableToSchema.containsKey(tableName)) {
      tableToSchema.put(tableName, SchemaParser.toJson(schemaSupplier.get()));
    }
    configuration.set(TABLE_SCHEMA_MAP, SerializationUtil.serializeToBase64(tableToSchema));
  }

  /**
   * Gets schema from the configuration.
   *
   * tblProperties is consulted to get the tablename. This tablename is looked up in the serialized map present in
   * TABLE_SCHEMA_MAP configuration. The returned json schema is then parsed and returned.
   */
  static Optional<Schema> getSchemaFromConf(@Nullable Configuration configuration, Properties tblProperties) {
    String tableName = tblProperties.getProperty(Catalogs.NAME);
    return Optional.ofNullable(configuration)
        .map(c -> c.get(TABLE_SCHEMA_MAP))
        .map(x -> (HashMap<String, String>) SerializationUtil.deserializeFromBase64(x))
        .map(map -> map.get(tableName))
        .map(SchemaParser::fromJson);
  }

}
