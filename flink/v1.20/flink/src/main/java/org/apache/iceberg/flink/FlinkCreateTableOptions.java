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
import org.apache.iceberg.util.JsonUtil;

public class FlinkCreateTableOptions {
  String catalog_name;
  String catalog_db;
  String catalog_table;
  Map<String, String> catalog_props;

  private FlinkCreateTableOptions(
      String catalog_name, String catalog_db, String catalog_table, Map<String, String> props) {
    this.catalog_name = catalog_name;
    this.catalog_db = catalog_db;
    this.catalog_table = catalog_table;
    this.catalog_props = props;
  }

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

  public static final ConfigOption<Map<String, String>> CATALOG_PROPS =
      ConfigOptions.key("catalog-props")
          .mapType()
          .noDefaultValue()
          .withDescription("Properties for the underlying catalog for iceberg table.");

  public static final String SRC_CATALOG_PROPS_KEY = "src-catalog";

  static String toJson(
      String catalog_name, String catalog_db, String catalog_table, Map<String, String> props) {
    return JsonUtil.generate(
        gen -> {
          gen.writeStartObject();
          gen.writeStringField(CATALOG_NAME.key(), catalog_name);
          gen.writeStringField(CATALOG_DATABASE.key(), catalog_db);
          gen.writeStringField(CATALOG_TABLE.key(), catalog_table);
          JsonUtil.writeStringMap(CATALOG_PROPS.key(), props, gen);
          gen.writeEndObject();
        },
        false);
  }

  static FlinkCreateTableOptions fromJson(String createTableOptions) {
    return JsonUtil.parse(
        createTableOptions,
        node -> {
          String catalog_name = JsonUtil.getString(CATALOG_NAME.key(), node);
          String catalog_db = JsonUtil.getString(CATALOG_DATABASE.key(), node);
          String catalog_table = JsonUtil.getString(CATALOG_TABLE.key(), node);
          Map<String, String> catalog_props = JsonUtil.getStringMap(CATALOG_PROPS.key(), node);

          return new FlinkCreateTableOptions(
              catalog_name, catalog_db, catalog_table, catalog_props);
        });
  }
}
