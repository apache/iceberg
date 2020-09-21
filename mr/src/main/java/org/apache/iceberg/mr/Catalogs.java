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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
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

    if (catalog.isPresent()) {
      Preconditions.checkArgument(tableIdentifier != null, "Table identifier not set");
      return catalog.get().loadTable(TableIdentifier.parse(tableIdentifier));
    }

    Preconditions.checkArgument(tableLocation != null, "Table location not set");
    return new HadoopTables(conf).load(tableLocation);
  }

  public static Table createTable(Configuration conf, Properties props) {
    String schemaString = props.getProperty(InputFormatConfig.TABLE_SCHEMA);
    Preconditions.checkNotNull(schemaString, "Table schema not set");
    Schema schema = SchemaParser.fromJson(props.getProperty(InputFormatConfig.TABLE_SCHEMA));

    String specString = props.getProperty(InputFormatConfig.PARTITION_SPEC);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    if (specString != null) {
      spec = PartitionSpecParser.fromJson(schema, specString);
    }

    String location = props.getProperty(LOCATION);

    Map<String, String> map = new HashMap<>(props.size());
    props.forEach((k, v) -> map.put(k.toString(), v.toString()));

    Optional<Catalog> catalog = loadCatalog(conf);

    if (catalog.isPresent()) {
      String name = props.getProperty(NAME);
      Preconditions.checkNotNull(name, "Table identifier not set");
      return catalog.get().createTable(TableIdentifier.parse(name), schema, spec, location, map);
    }

    Preconditions.checkNotNull(location, "Table location not set");
    return new HadoopTables(conf).create(schema, spec, map, location);
  }

  public static boolean dropTable(Configuration conf, Properties props) {
    String location = props.getProperty(LOCATION);

    Optional<Catalog> catalog = loadCatalog(conf);

    if (catalog.isPresent()) {
      String name = props.getProperty(NAME);
      Preconditions.checkNotNull(name, "Table identifier not set");
      return catalog.get().dropTable(TableIdentifier.parse(name));
    }

    Preconditions.checkNotNull(location, "Table location not set");
    return new HadoopTables(conf).dropTable(location);
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
      LOG.info("Loaded catalog {} using {}", catalog, catalogLoaderClass);
      return Optional.of(catalog);
    }

    String catalogName = conf.get(InputFormatConfig.CATALOG);

    if (catalogName != null) {
      Catalog catalog;
      switch (catalogName.toLowerCase()) {
        case HADOOP:
          String warehouseLocation = conf.get(InputFormatConfig.HADOOP_CATALOG_WAREHOUSE_LOCATION);

          catalog = (warehouseLocation != null) ? new HadoopCatalog(conf, warehouseLocation) : new HadoopCatalog(conf);
          LOG.info("Loaded Hadoop catalog {}", catalog);
          return Optional.of(catalog);
        case HIVE:
          catalog = HiveCatalogs.loadCatalog(conf);
          LOG.info("Loaded Hive Metastore catalog {}", catalog);
          return Optional.of(catalog);
        default:
          throw new NoSuchNamespaceException("Catalog %s is not supported.", catalogName);
      }
    }

    LOG.info("Catalog is not configured");
    return Optional.empty();
  }
}
