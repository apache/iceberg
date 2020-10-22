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

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
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
 * {@link #createCatalogLoader(String, Map, Configuration)}.
 */
public class FlinkCatalogFactory implements CatalogFactory {

  // Can not just use "type", it conflicts with CATALOG_TYPE.
  public static final String ICEBERG_CATALOG_TYPE = "catalog-type";
  public static final String HIVE_URI = "uri";
  public static final String HIVE_CLIENT_POOL_SIZE = "clients";
  public static final String HIVE_CONF_DIR = "hive-conf-dir";
  public static final String WAREHOUSE_LOCATION = "warehouse";

  public static final String DEFAULT_DATABASE = "default-database";
  public static final String BASE_NAMESPACE = "base-namespace";

  /**
   * Create an Iceberg {@link org.apache.iceberg.catalog.Catalog} loader to be used by this Flink catalog adapter.
   *
   * @param name       Flink's catalog name
   * @param properties Flink's catalog properties
   * @param hadoopConf Hadoop configuration for catalog
   * @return an Iceberg catalog loader
   */
  protected CatalogLoader createCatalogLoader(String name, Map<String, String> properties, Configuration hadoopConf) {
    String catalogType = properties.getOrDefault(ICEBERG_CATALOG_TYPE, "hive");
    switch (catalogType) {
      case "hive":
        // The values of properties 'uri', 'warehouse', 'hive-conf-dir' are allowed to be null, in that case it will
        // fallback to parse those values from hadoop configuration which is loaded from classpath.
        String uri = properties.get(HIVE_URI);
        String warehouse = properties.get(WAREHOUSE_LOCATION);
        int clientPoolSize = Integer.parseInt(properties.getOrDefault(HIVE_CLIENT_POOL_SIZE, "2"));
        String hiveConfDir = properties.get(HIVE_CONF_DIR);
        Configuration newHadoopConf = mergeHiveConf(hadoopConf, hiveConfDir);
        return CatalogLoader.hive(name, newHadoopConf, uri, warehouse, clientPoolSize);

      case "hadoop":
        String warehouseLocation = properties.get(WAREHOUSE_LOCATION);
        return CatalogLoader.hadoop(name, hadoopConf, warehouseLocation);

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
    properties.add(HIVE_CONF_DIR);
    properties.add(WAREHOUSE_LOCATION);
    properties.add(DEFAULT_DATABASE);
    properties.add(BASE_NAMESPACE);
    return properties;
  }

  @Override
  public Catalog createCatalog(String name, Map<String, String> properties) {
    return createCatalog(name, properties, clusterHadoopConf());
  }

  protected Catalog createCatalog(String name, Map<String, String> properties, Configuration hadoopConf) {
    CatalogLoader catalogLoader = createCatalogLoader(name, properties, hadoopConf);
    String defaultDatabase = properties.getOrDefault(DEFAULT_DATABASE, "default");
    String[] baseNamespace = properties.containsKey(BASE_NAMESPACE) ?
        Splitter.on('.').splitToList(properties.get(BASE_NAMESPACE)).toArray(new String[0]) :
        new String[0];
    boolean cacheEnabled = Boolean.parseBoolean(properties.getOrDefault("cache-enabled", "true"));
    return new FlinkCatalog(name, defaultDatabase, baseNamespace, catalogLoader, cacheEnabled);
  }

  private static Configuration mergeHiveConf(Configuration hadoopConf, String hiveConfDir) {
    Configuration newConf = new Configuration(hadoopConf);
    if (!Strings.isNullOrEmpty(hiveConfDir)) {
      Preconditions.checkState(Files.exists(Paths.get(hiveConfDir, "hive-site.xml")),
          "There should be a hive-site.xml file under the directory %s", hiveConfDir);
      newConf.addResource(new Path(hiveConfDir, "hive-site.xml"));
    } else {
      // If don't provide the hive-site.xml path explicitly, it will try to load resource from classpath. If still
      // couldn't load the configuration file, then it will throw exception in HiveCatalog.
      URL configFile = CatalogLoader.class.getClassLoader().getResource("hive-site.xml");
      if (configFile != null) {
        newConf.addResource(configFile);
      }
    }
    return newConf;
  }

  public static Configuration clusterHadoopConf() {
    return HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
  }
}
