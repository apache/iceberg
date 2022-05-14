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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;

/** {@link ConfigOption}s for {@link FlinkCatalog}. */
public class FlinkCatalogFactoryOptions {

  public static final String IDENTIFIER = "iceberg";

  public static final ConfigOption<String> DEFAULT_DATABASE =
      ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
          .stringType()
          .defaultValue(FlinkCatalog.DEFAULT_DATABASE)
          .withDescription("Default database name managed in the iceberg catalog.");

  public static final ConfigOption<Boolean> CACHE_ENABLED =
      ConfigOptions.key("cache-enabled").booleanType().defaultValue(true);

  public static final ConfigOption<String> BASE_NAMESPACE =
      ConfigOptions.key("base-namespace").stringType().noDefaultValue();

  public static final ConfigOption<String> CATALOG_IMPL =
      ConfigOptions.key(CatalogProperties.CATALOG_IMPL).stringType().noDefaultValue();

  public static final ConfigOption<String> ICEBERG_CATALOG_TYPE =
      ConfigOptions.key("catalog-type")
          .stringType()
          .defaultValue(CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE)
          .withDescription("Catalog type, the optional types are: custom, hadoop, hive.");

  public static final ConfigOption<String> HIVE_CONF_DIR =
      ConfigOptions.key("hive-conf-dir").stringType().noDefaultValue();

  private FlinkCatalogFactoryOptions() {
  }
}
