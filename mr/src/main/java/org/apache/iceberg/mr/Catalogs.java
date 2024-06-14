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
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;

/**
 * Class for catalog resolution and accessing the common functions for {@link Catalog} API.
 *
 * <p>If the catalog name is provided, get the catalog type from iceberg.catalog.<code>catalogName
 * </code>.type config.
 *
 * <p>In case the catalog name is {@link #ICEBERG_HADOOP_TABLE_NAME location_based_table}, type is
 * ignored and tables will be loaded using {@link HadoopTables}.
 *
 * <p>In case the value of catalog type is null, iceberg.catalog.<code>catalogName</code>
 * .catalog-impl config is used to determine the catalog implementation class.
 *
 * <p>If catalog name is null, get the catalog type from {@link CatalogUtil#ICEBERG_CATALOG_TYPE
 * catalog type} config:
 *
 * <ul>
 *   <li>hive: HiveCatalog
 *   <li>location: HadoopTables
 *   <li>hadoop: HadoopCatalog
 * </ul>
 */
public final class Catalogs {

  public static final String ICEBERG_DEFAULT_CATALOG_NAME = "default_iceberg";
  public static final String ICEBERG_HADOOP_TABLE_NAME = "location_based_table";
  public static final String NAME = "name";
  public static final String LOCATION = "location";

  private static final String NO_CATALOG_TYPE = "no catalog";
  private static final Set<String> PROPERTIES_TO_REMOVE =
      ImmutableSet.of(
          InputFormatConfig.TABLE_SCHEMA,
          InputFormatConfig.PARTITION_SPEC,
          LOCATION,
          NAME,
          InputFormatConfig.CATALOG_NAME);

  private Catalogs() {}

  /**
   * Load an Iceberg table using the catalog and table identifier (or table path) specified by the
   * configuration.
   *
   * @param conf a Hadoop conf
   * @return an Iceberg table
   */
  public static Table loadTable(Configuration conf) {
    return loadTable(
        conf,
        conf.get(InputFormatConfig.TABLE_IDENTIFIER),
        conf.get(InputFormatConfig.TABLE_LOCATION),
        conf.get(InputFormatConfig.CATALOG_NAME));
  }

  /**
   * Load an Iceberg table using the catalog specified by the configuration.
   *
   * <p>The table identifier ({@link Catalogs#NAME}) and the catalog name ({@link
   * InputFormatConfig#CATALOG_NAME}), or table path ({@link Catalogs#LOCATION}) should be specified
   * by the controlling properties.
   *
   * <p>Used by HiveIcebergSerDe and HiveIcebergStorageHandler
   *
   * @param conf a Hadoop
   * @param props the controlling properties
   * @return an Iceberg table
   */
  public static Table loadTable(Configuration conf, Properties props) {
    return loadTable(
        conf,
        props.getProperty(NAME),
        props.getProperty(LOCATION),
        props.getProperty(InputFormatConfig.CATALOG_NAME));
  }

  private static Table loadTable(
      Configuration conf, String tableIdentifier, String tableLocation, String catalogName) {
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
   *
   * <p>The properties should contain the following values:
   *
   * <ul>
   *   <li>Table identifier ({@link Catalogs#NAME}) or table path ({@link Catalogs#LOCATION}) is
   *       required
   *   <li>Table schema ({@link InputFormatConfig#TABLE_SCHEMA}) is required
   *   <li>Partition specification ({@link InputFormatConfig#PARTITION_SPEC}) is optional. Table
   *       will be unpartitioned if not provided
   * </ul>
   *
   * <p>Other properties will be handled over to the Table creation. The controlling properties
   * above will not be propagated.
   *
   * @param conf a Hadoop conf
   * @param props the controlling properties
   * @return the created Iceberg table
   */
  public static Table createTable(Configuration conf, Properties props) {
    String schemaString = props.getProperty(InputFormatConfig.TABLE_SCHEMA);
    Preconditions.checkNotNull(schemaString, "Table schema not set");
    Schema schema = SchemaParser.fromJson(schemaString);

    String specString = props.getProperty(InputFormatConfig.PARTITION_SPEC);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    if (specString != null) {
      spec = PartitionSpecParser.fromJson(schema, specString);
    }

    String location = props.getProperty(LOCATION);
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);

    // Create a table property map without the controlling properties
    Map<String, String> map = Maps.newHashMapWithExpectedSize(props.size());
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
   *
   * <p>The table identifier ({@link Catalogs#NAME}) or table path ({@link Catalogs#LOCATION})
   * should be specified by the controlling properties.
   *
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
   *
   * @param conf a Hadoop conf
   * @param props the controlling properties
   * @return true if the Catalog is HiveCatalog
   */
  public static boolean hiveCatalog(Configuration conf, Properties props) {
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);
    String catalogType = getCatalogType(conf, catalogName);
    if (catalogType != null) {
      return CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE.equalsIgnoreCase(catalogType);
    }
    catalogType = getCatalogType(conf, ICEBERG_DEFAULT_CATALOG_NAME);
    if (catalogType != null) {
      return CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE.equalsIgnoreCase(catalogType);
    }
    return getCatalogProperties(conf, catalogName, catalogType).get(CatalogProperties.CATALOG_IMPL)
        == null;
  }

  @VisibleForTesting
  static Optional<Catalog> loadCatalog(Configuration conf, String catalogName) {
    String catalogType = getCatalogType(conf, catalogName);
    if (NO_CATALOG_TYPE.equalsIgnoreCase(catalogType)) {
      return Optional.empty();
    } else {
      String name = catalogName == null ? ICEBERG_DEFAULT_CATALOG_NAME : catalogName;
      return Optional.of(
          CatalogUtil.buildIcebergCatalog(
              name, getCatalogProperties(conf, name, catalogType), conf));
    }
  }

  /**
   * Collect all the catalog specific configuration from the global hive configuration.
   *
   * @param conf a Hadoop configuration
   * @param catalogName name of the catalog
   * @param catalogType type of the catalog
   * @return complete map of catalog properties
   */
  private static Map<String, String> getCatalogProperties(
      Configuration conf, String catalogName, String catalogType) {
    String keyPrefix = InputFormatConfig.CATALOG_CONFIG_PREFIX + catalogName;

    return Streams.stream(conf.iterator())
        .filter(e -> e.getKey().startsWith(keyPrefix))
        .collect(
            Collectors.toMap(
                e -> e.getKey().substring(keyPrefix.length() + 1), Map.Entry::getValue));
  }

  /**
   * Return the catalog type based on the catalog name.
   *
   * <p>See {@link Catalogs} documentation for catalog type resolution strategy.
   *
   * @param conf global hive configuration
   * @param catalogName name of the catalog
   * @return type of the catalog, can be null
   */
  private static String getCatalogType(Configuration conf, String catalogName) {
    if (catalogName != null) {
      String catalogType =
          conf.get(
              InputFormatConfig.catalogPropertyConfigKey(
                  catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE));
      if (catalogName.equals(ICEBERG_HADOOP_TABLE_NAME)) {
        return NO_CATALOG_TYPE;
      } else {
        return catalogType;
      }
    } else {
      String catalogType = conf.get(CatalogUtil.ICEBERG_CATALOG_TYPE);
      if (catalogType != null && catalogType.equals(LOCATION)) {
        return NO_CATALOG_TYPE;
      } else {
        return catalogType;
      }
    }
  }
}
