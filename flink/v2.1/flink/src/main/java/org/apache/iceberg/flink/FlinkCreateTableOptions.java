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
package org.apache.iceberg.flink;

import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

class FlinkCreateTableOptions {

  private FlinkCreateTableOptions() {}

  public static final ConfigOption<String> CATALOG_NAME =
      ConfigOptions.key("catalog-name")
          .stringType()
          .noDefaultValue()
          .withDescription("Catalog name");

  public static final ConfigOption<String> CATALOG_TYPE =
      ConfigOptions.key(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE)
          .stringType()
          .noDefaultValue()
          .withDescription("Catalog type, the optional types are: custom, hadoop, hive.");

  public static final ConfigOption<String> CATALOG_DATABASE =
      ConfigOptions.key("catalog-database")
          .stringType()
          .defaultValue(FlinkCatalogFactory.DEFAULT_DATABASE_NAME)
          .withDescription("Database name managed in the iceberg catalog.");

  public static final ConfigOption<String> CATALOG_TABLE =
      ConfigOptions.key("catalog-table")
          .stringType()
          .noDefaultValue()
          .withDescription("Table name managed in the underlying iceberg catalog and database.");

  public static final ConfigOption<Boolean> USE_DYNAMIC_ICEBERG_SINK =
      ConfigOptions.key("use-dynamic-iceberg-sink")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Whether to use dynamic iceberg sink for routing data to multiple tables. "
                  + "When enabled, a single sink instance can dynamically route records to different "
                  + "Iceberg tables based on the logic defined in the DynamicRecordGenerator implementation. "
                  + "Requires 'dynamic-record-generator-impl' to be specified. "
                  + "Default is false (uses standard static sink behavior).");

  public static final ConfigOption<String> DYNAMIC_RECORD_GENERATOR_IMPL =
      ConfigOptions.key("dynamic-record-generator-impl")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Implementation of DynamicTableRecordGenerator class when use-dynamic-iceberg-sink is enabled.");

  public static final String SRC_CATALOG_PROPS_KEY = "src-catalog";
  public static final String CONNECTOR_PROPS_KEY = "connector";
  public static final String LOCATION_KEY = "location";

  static String toJson(
      String catalogName, String catalogDb, String catalogTable, Map<String, String> catalogProps) {
    return JsonUtil.generate(
        gen -> {
          gen.writeStartObject();
          gen.writeStringField(CATALOG_NAME.key(), catalogName);
          gen.writeStringField(CATALOG_DATABASE.key(), catalogDb);
          gen.writeStringField(CATALOG_TABLE.key(), catalogTable);

          String catalogType = catalogProps.get(CATALOG_TYPE.key());
          if (catalogType != null) {
            gen.writeStringField(CATALOG_TYPE.key(), catalogType);
          }

          String catalogImpl = catalogProps.get(CatalogProperties.CATALOG_IMPL);
          if (catalogImpl != null) {
            gen.writeStringField(CatalogProperties.CATALOG_IMPL, catalogImpl);
          }

          gen.writeEndObject();
        },
        false);
  }

  static Map<String, String> fromJson(String createTableOptions) {
    return JsonUtil.parse(
        createTableOptions,
        node -> {
          ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
          node.fieldNames()
              .forEachRemaining(field -> properties.put(field, JsonUtil.getString(field, node)));
          return properties.build();
        });
  }
}
