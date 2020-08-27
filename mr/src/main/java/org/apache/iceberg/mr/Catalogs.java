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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Catalogs {
  private static final Logger LOG = LoggerFactory.getLogger(Catalogs.class);

  private static final String HADOOP = "hadoop";
  private static final String HIVE = "hive";

  private static final String NAME = "name";
  private static final String LOCATION = "location";

  private Catalogs() {
  }

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

    Table table;
    if (catalog.isPresent()) {
      Preconditions.checkArgument(tableIdentifier != null, "Table identifier not set");
      table = catalog.get().loadTable(TableIdentifier.parse(tableIdentifier));
    } else {
      Preconditions.checkArgument(tableLocation != null, "Table location not set");
      table = new HadoopTables(conf).load(tableLocation);
    }

    LOG.info("Table loaded by catalog: [{}]", table);
    return table;
  }

  @VisibleForTesting
  static Optional<Catalog> loadCatalog(Configuration conf) {
    String catalogLoaderClass = conf.get(InputFormatConfig.CATALOG_LOADER_CLASS);

    if (catalogLoaderClass != null) {
      CatalogLoader loader = (CatalogLoader) DynConstructors.builder(CatalogLoader.class)
              .impl(catalogLoaderClass)
              .build()
              .newInstance();
      Catalog catalog = loader.load(conf);
      LOG.info("Dynamic catalog is used: [{}]", catalog);
      return Optional.of(catalog);
    }

    String catalogName = conf.get(InputFormatConfig.CATALOG);

    if (catalogName != null) {
      Catalog catalog;
      switch (catalogName.toLowerCase()) {
        case HADOOP:
          String warehouseLocation = conf.get(InputFormatConfig.HADOOP_CATALOG_WAREHOUSE_LOCATION);

          catalog = (warehouseLocation != null) ? new HadoopCatalog(conf, warehouseLocation) : new HadoopCatalog(conf);
          break;
        case HIVE:
          catalog = HiveCatalogs.loadCatalog(conf);
          break;
        default:
          throw new NoSuchNamespaceException("Catalog " + catalogName + " is not supported.");
      }

      LOG.info("Catalog is used: [{}]", catalog);
      return Optional.of(catalog);
    }

    LOG.info("No catalog is used");
    return Optional.empty();
  }
}
