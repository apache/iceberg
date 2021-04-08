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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;

/**
 * Class for catalog resolution and accessing the common functions for {@link Catalog} API.
 * <p>
 * Catalog resolution happens in this order:
 * <ol>
 * <li>Custom catalog if specified by {@link InputFormatConfig#CATALOG_LOADER_CLASS}
 * <li>Hadoop or Hive catalog if specified by {@link InputFormatConfig#CATALOG}
 * <li>Hadoop Tables
 * </ol>
 */
public final class Catalogs {

  public static final String ICEBERG_DEFAULT_CATALOG_NAME = "default_iceberg";
  public static final String ICEBERG_HADOOP_TABLE_NAME = "location_based_table";

  private static final String HIVE_CATALOG_TYPE = "hive";
  private static final String HADOOP_CATALOG_TYPE = "hadoop";
  private static final String NO_CATALOG_TYPE = "no catalog";

  public static final String NAME = "name";
  public static final String LOCATION = "location";

  private static final Set<String> PROPERTIES_TO_REMOVE =
      ImmutableSet.of(InputFormatConfig.TABLE_SCHEMA, InputFormatConfig.PARTITION_SPEC, LOCATION, NAME,
              InputFormatConfig.CATALOG_NAME);

  private Catalogs() {
  }

  /**
   * Load an Iceberg table using the catalog and table identifier (or table path) specified by the configuration.
   * @param conf a Hadoop conf
   * @return an Iceberg table
   */
  public static Table loadTable(Configuration conf) {
    return loadTable(conf, conf.get(InputFormatConfig.TABLE_IDENTIFIER), conf.get(InputFormatConfig.TABLE_LOCATION),
            conf.get(InputFormatConfig.CATALOG_NAME));
  }

  /**
   * Load an Iceberg table using the catalog specified by the configuration.
   * <p>
   * The table identifier ({@link Catalogs#NAME}) and the catalog name ({@link InputFormatConfig#CATALOG_NAME}),
   * or table path ({@link Catalogs#LOCATION}) should be specified by the controlling properties.
   * <p>
   * Used by HiveIcebergSerDe and HiveIcebergStorageHandler
   * @param conf a Hadoop
   * @param props the controlling properties
   * @return an Iceberg table
   */
  public static Table loadTable(Configuration conf, Properties props) {
    return loadTable(conf, props.getProperty(NAME), props.getProperty(LOCATION),
            props.getProperty(InputFormatConfig.CATALOG_NAME));
  }

  private static Table loadTable(Configuration conf, String tableIdentifier, String tableLocation,
                                 String catalogName) {
    Optional<Catalog> catalog = loadCatalog(conf, catalogName);

    if (catalog.isPresent()) {
      Preconditions.checkArgument(tableIdentifier != null, "Table identifier not set");
      return catalog.get().loadTable(TableIdentifier.parse(tableIdentifier));
    }

    Preconditions.checkArgument(tableLocation != null, "Table location not set");
    return new HadoopTables(conf).load(tableLocation);
  }

  /**
   * Creates an Iceberg table using the catalog specified by the configuration.
   * <p>
   * The properties should contain the following values:
   * <ul>
   * <li>Table identifier ({@link Catalogs#NAME}) or table path ({@link Catalogs#LOCATION}) is required
   * <li>Table schema ({@link InputFormatConfig#TABLE_SCHEMA}) is required
   * <li>Partition specification ({@link InputFormatConfig#PARTITION_SPEC}) is optional. Table will be unpartitioned if
   *  not provided
   * </ul><p>
   * Other properties will be handled over to the Table creation. The controlling properties above will not be
   * propagated.
   * @param conf a Hadoop conf
   * @param props the controlling properties
   * @return the created Iceberg table
   */
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
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);

    // Create a table property map without the controlling properties
    Map<String, String> map = new HashMap<>(props.size());
    for (Object key : props.keySet()) {
      if (!PROPERTIES_TO_REMOVE.contains(key)) {
        map.put(key.toString(), props.get(key).toString());
      }
    }

    Optional<Catalog> catalog = loadCatalog(conf, catalogName);

    if (catalog.isPresent()) {
      String name = props.getProperty(NAME);
      Preconditions.checkNotNull(name, "Table identifier not set");
      return catalog.get().createTable(TableIdentifier.parse(name), schema, spec, location, map);
    }

    Preconditions.checkNotNull(location, "Table location not set");
    return new HadoopTables(conf).create(schema, spec, map, location);
  }

  /**
   * Drops an Iceberg table using the catalog specified by the configuration.
   * <p>
   * The table identifier ({@link Catalogs#NAME}) or table path ({@link Catalogs#LOCATION}) should be specified by
   * the controlling properties.
   * @param conf a Hadoop conf
   * @param props the controlling properties
   * @return the created Iceberg table
   */
  public static boolean dropTable(Configuration conf, Properties props) {
    String location = props.getProperty(LOCATION);
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);

    Optional<Catalog> catalog = loadCatalog(conf, catalogName);

    if (catalog.isPresent()) {
      String name = props.getProperty(NAME);
      Preconditions.checkNotNull(name, "Table identifier not set");
      return catalog.get().dropTable(TableIdentifier.parse(name));
    }

    Preconditions.checkNotNull(location, "Table location not set");
    return new HadoopTables(conf).dropTable(location);
  }

  /**
   * Returns true if HiveCatalog is used
   * @param conf a Hadoop conf
   * @param props the controlling properties
   * @return true if the Catalog is HiveCatalog
   */
  public static boolean hiveCatalog(Configuration conf, Properties props) {
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);
    return CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE.equalsIgnoreCase(getCatalogType(conf, catalogName));
  }

  @VisibleForTesting
  static Optional<Catalog> loadCatalog(Configuration conf, String catalogName) {
    String catalogType = getCatalogType(conf, catalogName);
    if (catalogType == null) {
      throw new NoSuchNamespaceException("Catalog definition for %s is not found.", catalogName);
    }

    if (NO_CATALOG_TYPE.equalsIgnoreCase(catalogType)) {
      return Optional.empty();
    } else {
      String name = catalogName == null ? ICEBERG_DEFAULT_CATALOG_NAME : catalogName;
      return Optional.of(CatalogUtil.buildIcebergCatalog(name,
              getCatalogProperties(conf, name, catalogType), conf));
    }
  }

  /**
   * Collect all the catalog specific configuration from the global hive configuration.
   * @param conf a Hadoop configuration
   * @param catalogName name of the catalog
   * @param catalogType type of the catalog
   * @return complete map of catalog properties
   */
  private static Map<String, String> getCatalogProperties(Configuration conf, String catalogName, String catalogType) {
    String keyPrefix = InputFormatConfig.CATALOG_CONFIG_PREFIX + catalogName;
    Map<String, String> catalogProperties = Streams.stream(conf.iterator())
            .filter(e -> e.getKey().startsWith(keyPrefix))
            .collect(Collectors.toMap(e -> e.getKey().substring(keyPrefix.length() + 1), Map.Entry::getValue));
    return addCatalogPropertiesIfMissing(conf, catalogType, catalogProperties);
  }

  /**
   * This method is used for backward-compatible catalog configuration.
   * Collect all the catalog specific configuration from the global hive configuration.
   * Note: this should be removed when the old catalog configuration is depracated.
   * @param conf global hive configuration
   * @param catalogType type of the catalog
   * @param catalogProperties pre-populated catalog properties
   * @return complete map of catalog properties
   */
  private static Map<String, String> addCatalogPropertiesIfMissing(Configuration conf, String catalogType,
                                                                   Map<String, String> catalogProperties) {
    catalogProperties.putIfAbsent(CatalogUtil.ICEBERG_CATALOG_TYPE, catalogType);
    if (catalogType.equalsIgnoreCase(HADOOP_CATALOG_TYPE)) {
      catalogProperties.putIfAbsent(CatalogProperties.WAREHOUSE_LOCATION,
              conf.get(InputFormatConfig.HADOOP_CATALOG_WAREHOUSE_LOCATION));
    }
    return catalogProperties;
  }

  /**
   * Return the catalog type based on the catalog name.
   * <p>
   * If the catalog name is provided get the catalog type from 'iceberg.catalog.<code>catalogName</code>.type' config.
   * In case the value of this property is null, return with no catalog definition (Hadoop Table)
   * </p>
   * <p>
   * If catalog name is null, check the global conf for 'iceberg.mr.catalog' property. If the value of the property is:
   * <ul>
   *     <li>null/hive -> Hive Catalog</li>
   *     <li>location -> Hadoop Table</li>
   *     <li>hadoop -> Hadoop Catalog</li>
   *     <li>any other value -> Custom Catalog</li>
   * </ul>
   * </p>
   * @param conf global hive configuration
   * @param catalogName name of the catalog
   * @return type of the catalog, can be null
   */
  private static String getCatalogType(Configuration conf, String catalogName) {
    if (catalogName != null) {
      String catalogType = conf.get(String.format(InputFormatConfig.CATALOG_TYPE_TEMPLATE, catalogName));
      if (catalogName.equals(ICEBERG_HADOOP_TABLE_NAME) || catalogType == null) {
        return NO_CATALOG_TYPE;
      } else {
        return catalogType;
      }
    } else {
      String catalogType = conf.get(InputFormatConfig.CATALOG);
      if (catalogType == null) {
        return HIVE_CATALOG_TYPE;
      } else if (catalogType.equals(LOCATION)) {
        return NO_CATALOG_TYPE;
      } else {
        return catalogType;
      }
    }
  }
}
