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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * {@link #createCatalogLoader(String, Map, Configuration)}.
 */
public class FlinkCatalogFactory implements CatalogFactory {

  // Can not just use "type", it conflicts with CATALOG_TYPE.
  public static final String ICEBERG_CATALOG_TYPE = "catalog-type";

  public static final String HIVE_URI = "uri";
  public static final String HIVE_CLIENT_POOL_SIZE = "clients";
  public static final String HADOOP_WAREHOUSE_LOCATION = "warehouse";

  public static final String HIVE_CONF_DIR = "hive-conf-dir";
  public static final String HIVE_SITE_SCHEME_FILE = "file";
  public static final String HIVE_SITE_SCHEME_HDFS = "hdfs";

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
        int clientPoolSize = Integer.parseInt(properties.getOrDefault(HIVE_CLIENT_POOL_SIZE, "2"));
        String uri = properties.get(HIVE_URI);
        String warehouse = properties.get(HADOOP_WAREHOUSE_LOCATION);
        return CatalogLoader.hive(name, hadoopConf, uri, warehouse, clientPoolSize);

      case "hadoop":
        String warehouseLocation = properties.get(HADOOP_WAREHOUSE_LOCATION);
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
    properties.add(HADOOP_WAREHOUSE_LOCATION);
    properties.add(DEFAULT_DATABASE);
    properties.add(BASE_NAMESPACE);
    properties.add(HIVE_CONF_DIR);
    return properties;
  }

  @Override
  public Catalog createCatalog(String name, Map<String, String> properties) {
    Configuration configuration = clusterHadoopConf();
    String catalogType = properties.get(ICEBERG_CATALOG_TYPE);
    if (catalogType.equals("hive")) {
      loadHiveConf(configuration, properties);
    }
    return createCatalog(name, properties, configuration);
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

  public static Configuration clusterHadoopConf() {
    return HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
  }

  private void loadHiveConf(Configuration configuration, Map<String, String> properties) {
    String hiveConfDir = properties.get(HIVE_CONF_DIR);
    if (hiveConfDir != null) {
      Path path = new Path(hiveConfDir + File.separator + "hive-site.xml");
      String scheme = getScheme(path);
      // We can add more storage support later，like s3
      switch (scheme) {
        case HIVE_SITE_SCHEME_HDFS:
          downloadFromHdfs(configuration, path);
          break;
        case HIVE_SITE_SCHEME_FILE:
          loadLocalHiveConf(configuration, path);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported FileSystem for scheme :" + scheme);
      }
    } else {
      URL path = this.getClass().getClassLoader().getResource("hive-site.xml");
      configuration.addResource(path);
    }
  }

  private String getScheme(Path path) {
    String scheme = path.toUri().getScheme();
    if (scheme == null) {
      throw new UnsupportedOperationException(
          "the scheme of HIVE_CONF_DIR can not be null");
    } else {
      return scheme;
    }
  }

  private void loadLocalHiveConf(Configuration configuration, Path localHiveSitePath) {
    File file = new File(localHiveSitePath.toUri());
    if (!file.exists()) {
      throw new RuntimeException(localHiveSitePath + " doesn't exist. if in application mode ," +
          " please provide a remote path for hive-site.xml");
    } else {
      configuration.addResource(localHiveSitePath);
    }
  }

  private void downloadFromHdfs(Configuration configuration, Path hdfsHiveSitePath) {
    try {
      File tmpFile = File.createTempFile("hive-site.xml-", ".tmp");
      FileSystem fs = FileSystem.get(configuration);
      Path sourcePath = fs.makeQualified(hdfsHiveSitePath);
      if (!fs.exists(sourcePath)) {
        throw new IOException(sourcePath + " doesn't exist.");
      }
      Path destPath = new Path(tmpFile.getAbsolutePath());
      fs.copyToLocalFile(sourcePath, destPath);
      configuration.addResource(destPath);
    } catch (IOException e) {
      throw new RuntimeException("copy hive-site.xml  to local error ", e);
    }
  }
}
