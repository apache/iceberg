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
import java.util.Locale;
import java.util.Map;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

/**
 * A Flink Catalog factory implementation that creates {@link FlinkCatalog}.
 *
 * <p>This supports the following catalog configuration options:
 *
 * <ul>
 *   <li><code>type</code> - Flink catalog factory key, should be "iceberg"
 *   <li><code>catalog-type</code> - iceberg catalog type, "hive" or "hadoop"
 *   <li><code>uri</code> - the Hive Metastore URI (Hive catalog only)
 *   <li><code>clients</code> - the Hive Client Pool Size (Hive catalog only)
 *   <li><code>warehouse</code> - the warehouse path (Hadoop catalog only)
 *   <li><code>default-database</code> - a database name to use as the default
 *   <li><code>base-namespace</code> - a base namespace as the prefix for all databases (Hadoop
 *       catalog only)
 *   <li><code>cache-enabled</code> - whether to enable catalog cache
 * </ul>
 *
 * <p>To use a custom catalog that is not a Hive or Hadoop catalog, extend this class and override
 * {@link #createCatalogLoader(String, Map, Configuration)}.
 */
public class FlinkCatalogFactory implements CatalogFactory {

  // Can not just use "type", it conflicts with CATALOG_TYPE.
  public static final String ICEBERG_CATALOG_TYPE = "catalog-type";
  public static final String ICEBERG_CATALOG_TYPE_HADOOP = "hadoop";
  public static final String ICEBERG_CATALOG_TYPE_HIVE = "hive";

  public static final String HIVE_CONF_DIR = "hive-conf-dir";
  public static final String HADOOP_CONF_DIR = "hadoop-conf-dir";
  public static final String DEFAULT_DATABASE = "default-database";
  public static final String DEFAULT_DATABASE_NAME = "default";
  public static final String BASE_NAMESPACE = "base-namespace";

  public static final String TYPE = "type";
  public static final String PROPERTY_VERSION = "property-version";

  /**
   * Create an Iceberg {@link org.apache.iceberg.catalog.Catalog} loader to be used by this Flink
   * catalog adapter.
   *
   * @param name Flink's catalog name
   * @param properties Flink's catalog properties
   * @param hadoopConf Hadoop configuration for catalog
   * @return an Iceberg catalog loader
   */
  static CatalogLoader createCatalogLoader(
      String name, Map<String, String> properties, Configuration hadoopConf) {
    String catalogImpl = properties.get(CatalogProperties.CATALOG_IMPL);
    if (catalogImpl != null) {
      String catalogType = properties.get(ICEBERG_CATALOG_TYPE);
      Preconditions.checkArgument(
          catalogType == null,
          "Cannot create catalog %s, both catalog-type and catalog-impl are set: catalog-type=%s, catalog-impl=%s",
          name,
          catalogType,
          catalogImpl);
      return CatalogLoader.custom(name, properties, hadoopConf, catalogImpl);
    }

    String catalogType = properties.getOrDefault(ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HIVE);
    switch (catalogType.toLowerCase(Locale.ENGLISH)) {
      case ICEBERG_CATALOG_TYPE_HIVE:
        // The values of properties 'uri', 'warehouse', 'hive-conf-dir' are allowed to be null, in
        // that case it will
        // fallback to parse those values from hadoop configuration which is loaded from classpath.
        String hiveConfDir = properties.get(HIVE_CONF_DIR);
        String hadoopConfDir = properties.get(HADOOP_CONF_DIR);
        Configuration newHadoopConf = mergeHiveConf(hadoopConf, hiveConfDir, hadoopConfDir);
        return CatalogLoader.hive(name, newHadoopConf, properties);

      case ICEBERG_CATALOG_TYPE_HADOOP:
        return CatalogLoader.hadoop(name, hadoopConf, properties);

      default:
        throw new UnsupportedOperationException(
            "Unknown catalog-type: " + catalogType + " (Must be 'hive' or 'hadoop')");
    }
  }

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = Maps.newHashMap();
    context.put(TYPE, "iceberg");
    context.put(PROPERTY_VERSION, "1");
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    return ImmutableList.of("*");
  }

  @Override
  public Catalog createCatalog(String name, Map<String, String> properties) {
    return createCatalog(name, properties, clusterHadoopConf());
  }

  protected Catalog createCatalog(
      String name, Map<String, String> properties, Configuration hadoopConf) {
    CatalogLoader catalogLoader = createCatalogLoader(name, properties, hadoopConf);
    String defaultDatabase = properties.getOrDefault(DEFAULT_DATABASE, DEFAULT_DATABASE_NAME);

    Namespace baseNamespace = Namespace.empty();
    if (properties.containsKey(BASE_NAMESPACE)) {
      baseNamespace = Namespace.of(properties.get(BASE_NAMESPACE).split("\\."));
    }

    boolean cacheEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, CatalogProperties.CACHE_ENABLED, CatalogProperties.CACHE_ENABLED_DEFAULT);

    long cacheExpirationIntervalMs =
        PropertyUtil.propertyAsLong(
            properties,
            CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS,
            CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_OFF);
    Preconditions.checkArgument(
        cacheExpirationIntervalMs != 0,
        "%s is not allowed to be 0.",
        CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS);

    return new FlinkCatalog(
        name,
        defaultDatabase,
        baseNamespace,
        catalogLoader,
        cacheEnabled,
        cacheExpirationIntervalMs);
  }

  private static Configuration mergeHiveConf(
      Configuration hadoopConf, String hiveConfDir, String hadoopConfDir) {
    Configuration newConf = new Configuration(hadoopConf);
    if (!Strings.isNullOrEmpty(hiveConfDir)) {
      Preconditions.checkState(
          Files.exists(Paths.get(hiveConfDir, "hive-site.xml")),
          "There should be a hive-site.xml file under the directory %s",
          hiveConfDir);
      newConf.addResource(new Path(hiveConfDir, "hive-site.xml"));
    } else {
      // If don't provide the hive-site.xml path explicitly, it will try to load resource from
      // classpath. If still
      // couldn't load the configuration file, then it will throw exception in HiveCatalog.
      URL configFile = CatalogLoader.class.getClassLoader().getResource("hive-site.xml");
      if (configFile != null) {
        newConf.addResource(configFile);
      }
    }

    if (!Strings.isNullOrEmpty(hadoopConfDir)) {
      Preconditions.checkState(
          Files.exists(Paths.get(hadoopConfDir, "hdfs-site.xml")),
          "Failed to load Hadoop configuration: missing %s",
          Paths.get(hadoopConfDir, "hdfs-site.xml"));
      newConf.addResource(new Path(hadoopConfDir, "hdfs-site.xml"));
      Preconditions.checkState(
          Files.exists(Paths.get(hadoopConfDir, "core-site.xml")),
          "Failed to load Hadoop configuration: missing %s",
          Paths.get(hadoopConfDir, "core-site.xml"));
      newConf.addResource(new Path(hadoopConfDir, "core-site.xml"));
    }

    return newConf;
  }

  public static Configuration clusterHadoopConf() {
    return HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
  }
}
