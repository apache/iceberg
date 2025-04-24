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

class FlinkCreateTableOptions {
  private final String catalogName;
  private final String catalogDb;
  private final String catalogTable;
  private final Map<String, String> catalogProps;

  private FlinkCreateTableOptions(
      String catalogName, String catalogDb, String catalogTable, Map<String, String> props) {
    this.catalogName = catalogName;
    this.catalogDb = catalogDb;
    this.catalogTable = catalogTable;
    this.catalogProps = props;
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
          JsonUtil.writeStringMap(CATALOG_PROPS.key(), catalogProps, gen);
          gen.writeEndObject();
        },
        false);
  }

  static FlinkCreateTableOptions fromJson(String createTableOptions) {
    return JsonUtil.parse(
        createTableOptions,
        node -> {
          String catalogName = JsonUtil.getString(CATALOG_NAME.key(), node);
          String catalogDb = JsonUtil.getString(CATALOG_DATABASE.key(), node);
          String catalogTable = JsonUtil.getString(CATALOG_TABLE.key(), node);
          Map<String, String> catalogProps = JsonUtil.getStringMap(CATALOG_PROPS.key(), node);

          return new FlinkCreateTableOptions(catalogName, catalogDb, catalogTable, catalogProps);
        });
  }

  String catalogName() {
    return catalogName;
  }

  String catalogDb() {
    return catalogDb;
  }

  String catalogTable() {
    return catalogTable;
  }

  Map<String, String> catalogProps() {
    return catalogProps;
  }
}
