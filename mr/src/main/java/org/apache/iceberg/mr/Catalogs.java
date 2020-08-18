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

package org.apache.iceberg.mr;

import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public final class Catalogs {

  private static final String HADOOP = "hadoop";
  private static final String HIVE = "hive";

  private static final String NAME = "name";
  private static final String LOCATION = "location";

  private Catalogs() {}

  /**
   * Load an Iceberg table using the catalog and table identifier (or table path) specified by the configuration.
   * Catalog resolution happens in this order:
   * 1. Custom catalog if specified by {@link InputFormatConfig#CATALOG_LOADER_CLASS}
   * 2. Hadoop or Hive catalog if specified by {@link InputFormatConfig#CATALOG}
   * 3. Hadoop Tables
   * @param conf a Hadoop conf
   * @return an Iceberg table
   */
  public static Table loadTable(Configuration conf) {
    return loadTable(conf, conf.get(InputFormatConfig.TABLE_IDENTIFIER), conf.get(InputFormatConfig.TABLE_LOCATION));
  }

  // For use in HiveIcebergSerDe and HiveIcebergStorageHandler
  public static Table loadTable(Configuration conf, Properties props) {
    return loadTable(conf, props.getProperty(NAME), props.getProperty(LOCATION));
  }

  private static Table loadTable(Configuration conf, String tableIdentifier, String tableLocation) {
    Optional<Catalog> catalog = loadCatalog(conf);

    if (catalog.isPresent()) {
      Preconditions.checkArgument(tableIdentifier != null, "Table identifier not set");
      return catalog.get().loadTable(TableIdentifier.parse(tableIdentifier));
    }

    Preconditions.checkArgument(tableLocation != null, "Table location not set");
    return new HadoopTables(conf).load(tableLocation);
  }

  @VisibleForTesting
  static Optional<Catalog> loadCatalog(Configuration conf) {
    String catalogLoaderClass = conf.get(InputFormatConfig.CATALOG_LOADER_CLASS);

    if (catalogLoaderClass != null) {
      CatalogLoader loader = (CatalogLoader) DynConstructors.builder(CatalogLoader.class)
              .impl(catalogLoaderClass)
              .build()
              .newInstance();
      return Optional.of(loader.load(conf));
    }

    String catalogName = conf.get(InputFormatConfig.CATALOG);

    if (catalogName != null) {
      switch (catalogName.toLowerCase()) {
        case HADOOP:
          String warehouseLocation = conf.get(InputFormatConfig.HADOOP_CATALOG_WAREHOUSE_LOCATION);

          if (warehouseLocation != null) {
            return Optional.of(new HadoopCatalog(conf, warehouseLocation));
          }

          return Optional.of(new HadoopCatalog(conf));
        case HIVE:
          return Optional.of(HiveCatalogs.loadCatalog(conf));
        default:
          throw new NoSuchNamespaceException("Catalog " + catalogName + " is not supported.");
      }
    }

    return Optional.empty();
  }
}
