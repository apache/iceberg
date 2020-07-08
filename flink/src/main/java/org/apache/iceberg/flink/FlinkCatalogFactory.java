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

import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * A Flink Catalog factory implementation that creates {@link FlinkCatalog}.
 * <p>
 * This supports the following catalog configuration options:
 * <ul>
 *   <li><tt>type</tt> - Flink catalog factory key, should be "iceberg"</li>
 *   <li><tt>catalog-type</tt> - iceberg catalog type, "hive" or "hadoop"</li>
 *   <li><tt>uri</tt> - the Hive Metastore URI (Hive catalog only)</li>
 *   <li><tt>clients</tt> - the Hive Client Pool Size (Hive catalog only)</li>
 *   <li><tt>warehouse</tt> - the warehouse path (Hadoop catalog only)</li>
 *   <li><tt>default-database</tt> - a database name to use as the default</li>
 *   <li><tt>base-namespace</tt> - a base namespace as the prefix for all databases (Hadoop catalog only)</li>
 * </ul>
 * <p>
 * To use a custom catalog that is not a Hive or Hadoop catalog, extend this class and override
 * {@link #buildIcebergCatalog(String, Map)}.
 */
public class FlinkCatalogFactory implements CatalogFactory {

  // Can not just use "type", it conflicts with CATALOG_TYPE.
  public static final String ICEBERG_CATALOG_TYPE = "catalog-type";
  public static final String HIVE_URI = "uri";
  public static final String HIVE_CLIENT_POOL_SIZE = "clients";
  public static final String HADOOP_WAREHOUSE_LOCATION = "warehouse";

  public static final String DEFAULT_DATABASE = "default-database";
  public static final String BASE_NAMESPACE = "base-namespace";

  /**
   * Build an Iceberg {@link org.apache.iceberg.catalog.Catalog} to be used by this Flink catalog adapter.
   *
   * @param name    Flink's catalog name
   * @param options Flink's catalog options
   * @return an Iceberg catalog
   */
  protected org.apache.iceberg.catalog.Catalog buildIcebergCatalog(String name, Map<String, String> options) {
    Configuration conf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
    return buildIcebergCatalog(name, options, conf);
  }

  /**
   * Build an Iceberg {@link org.apache.iceberg.catalog.Catalog} to be used by this Flink catalog adapter.
   *
   * @param name    Flink's catalog name
   * @param options Flink's catalog options
   * @param conf    Flink's hadoop configuration
   * @return an Iceberg catalog
   */
  protected org.apache.iceberg.catalog.Catalog buildIcebergCatalog(
      String name, Map<String, String> options, Configuration conf) {
    String catalogType = options.getOrDefault(ICEBERG_CATALOG_TYPE, "hive");
    switch (catalogType) {
      case "hive":
        int clientPoolSize = Integer.parseInt(options.getOrDefault(HIVE_CLIENT_POOL_SIZE, "2"));
        String uri = options.get(HIVE_URI);
        return new HiveCatalog(name, uri, clientPoolSize, conf);

      case "hadoop":
        String warehouseLocation = options.get(HADOOP_WAREHOUSE_LOCATION);
        return new HadoopCatalog(name, conf, warehouseLocation);

      default:
        throw new UnsupportedOperationException("Unknown catalog type: " + catalogType);
    }
  }

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = Maps.newHashMap();
    context.put(CatalogDescriptorValidator.CATALOG_TYPE, "iceberg");
    context.put(CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION, "1");
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    List<String> properties = Lists.newArrayList();
    properties.add(ICEBERG_CATALOG_TYPE);
    properties.add(HIVE_URI);
    properties.add(HIVE_CLIENT_POOL_SIZE);
    properties.add(HADOOP_WAREHOUSE_LOCATION);
    properties.add(DEFAULT_DATABASE);
    properties.add(BASE_NAMESPACE);
    return properties;
  }

  @Override
  public Catalog createCatalog(String name, Map<String, String> properties) {
    org.apache.iceberg.catalog.Catalog catalog = buildIcebergCatalog(name, properties);
    String defaultDatabase = properties.getOrDefault(DEFAULT_DATABASE, "default");
    String[] baseNamespace = properties.containsKey(BASE_NAMESPACE) ?
        Splitter.on('.').splitToList(properties.get(BASE_NAMESPACE)).toArray(new String[0]) :
        new String[0];
    boolean cacheEnabled = Boolean.parseBoolean(properties.getOrDefault("cache-enabled", "true"));
    return new FlinkCatalog(name, defaultDatabase, baseNamespace, catalog, cacheEnabled);
  }
}
