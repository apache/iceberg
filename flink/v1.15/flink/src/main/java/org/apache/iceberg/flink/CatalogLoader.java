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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Serializable loader to load an Iceberg {@link Catalog}. */
public interface CatalogLoader extends Serializable {

  /**
   * Create a new catalog with the provided properties. NOTICE: for flink, we may initialize the
   * {@link CatalogLoader} at flink sql client side or job manager side, and then serialize this
   * catalog loader to task manager, finally deserialize it and create a new catalog at task manager
   * side.
   *
   * @return a newly created {@link Catalog}
   */
  Catalog loadCatalog();

  static CatalogLoader hadoop(String name, Object hadoopConf, Map<String, String> properties) {
    return new HadoopCatalogLoader(name, hadoopConf, properties);
  }

  static CatalogLoader hive(String name, Object hadoopConf, Map<String, String> properties) {
    // The values of properties 'uri', 'warehouse', 'hive-conf-dir' are allowed to be null, in
    // that case it will
    // fallback to parse those values from hadoop configuration which is loaded from classpath.
    String hiveConfDir = properties.get(FlinkCatalogFactoryOptions.HIVE_CONF_DIR.key());

    org.apache.hadoop.conf.Configuration newHadoopConf =
        HiveCatalogLoader.mergeHiveConf(
            (org.apache.hadoop.conf.Configuration) hadoopConf, hiveConfDir);
    return new HiveCatalogLoader(name, newHadoopConf, properties);
  }

  static CatalogLoader custom(
      String name, Map<String, String> properties, Object hadoopConf, String impl) {
    return new CustomCatalogLoader(name, properties, hadoopConf, impl);
  }

  static Object toSerializableConf(Object hadoopConf) {
    if (hadoopConf == null) {
      return null;
    }

    ClassLoader classLoader = CatalogLoader.class.getClassLoader();
    Class<?> configurationClass;
    try {
      configurationClass = classLoader.loadClass("org.apache.hadoop.conf.Configuration");
    } catch (ClassNotFoundException e) {
      return null;
    }

    ValidationException.check(
        configurationClass.isInstance(hadoopConf),
        "%s is not an instance of Configuration from the classloader for %s",
        hadoopConf,
        CatalogLoader.class);

    Class<?> serializableConfigurationClass;
    try {
      serializableConfigurationClass =
          classLoader.loadClass("org.apache.iceberg.hadoop.SerializableConfiguration");
    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException("Failed to load SerializableConfiguration", e);
    }

    Object serializableConfiguration;
    try {
      serializableConfiguration =
          serializableConfigurationClass.getConstructor(configurationClass).newInstance(hadoopConf);
    } catch (NoSuchMethodException |
        InstantiationException |
        IllegalAccessException |
        InvocationTargetException e) {
      throw new UnsupportedOperationException(
          "Failed to load Configuration.setConf after loading Configurable", e);
    }

    return serializableConfiguration;
  }

  static Object getConf(Object serializableConf) {
    if (serializableConf == null) {
      return null;
    }

    Class<?> serializableConfigurationClass;
    try {
      serializableConfigurationClass =
          CatalogLoader.class
              .getClassLoader()
              .loadClass("org.apache.iceberg.hadoop.SerializableConfiguration");
    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException("Failed to load SerializableConfiguration", e);
    }

    DynMethods.BoundMethod get;
    try {
      get =
          DynMethods.builder("get")
              .impl(serializableConfigurationClass)
              .buildChecked()
              .bind(serializableConf);
    } catch (NoSuchMethodException e) {
      // this shouldn't happen because Configurable was loaded and defines setConf
      throw new UnsupportedOperationException(
          "Failed to load Configuration.setConf after loading Configurable", e);
    }

    return get.invoke();
  }

  class HadoopCatalogLoader implements CatalogLoader {
    private final String catalogName;
    private final Object serializableHadoopConf;
    private final String warehouseLocation;
    private final Map<String, String> properties;

    private HadoopCatalogLoader(String catalogName, Object conf, Map<String, String> properties) {
      this.catalogName = catalogName;
      this.serializableHadoopConf = toSerializableConf(conf);
      this.warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
      this.properties = Maps.newHashMap(properties);
    }

    @Override
    public Catalog loadCatalog() {
      return CatalogUtil.loadCatalog(
          HadoopCatalog.class.getName(), catalogName, properties, getConf(serializableHadoopConf));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("catalogName", catalogName)
          .add("warehouseLocation", warehouseLocation)
          .toString();
    }
  }

  class HiveCatalogLoader implements CatalogLoader {
    private final String catalogName;
    private final Object serializableHadoopConf;
    private final String uri;
    private final String warehouse;
    private final int clientPoolSize;
    private final Map<String, String> properties;

    private HiveCatalogLoader(String catalogName, Object conf, Map<String, String> properties) {
      this.catalogName = catalogName;
      this.serializableHadoopConf = toSerializableConf(conf);
      this.uri = properties.get(CatalogProperties.URI);
      this.warehouse = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
      this.clientPoolSize =
          properties.containsKey(CatalogProperties.CLIENT_POOL_SIZE) ?
              Integer.parseInt(properties.get(CatalogProperties.CLIENT_POOL_SIZE)) :
              CatalogProperties.CLIENT_POOL_SIZE_DEFAULT;
      this.properties = Maps.newHashMap(properties);
    }

    static org.apache.hadoop.conf.Configuration mergeHiveConf(
        org.apache.hadoop.conf.Configuration hadoopConf, String hiveConfDir) {
      org.apache.hadoop.conf.Configuration newConf =
          new org.apache.hadoop.conf.Configuration(hadoopConf);
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
      return newConf;
    }

    @Override
    public Catalog loadCatalog() {
      return CatalogUtil.loadCatalog(
          HiveCatalog.class.getName(), catalogName, properties, getConf(serializableHadoopConf));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("catalogName", catalogName)
          .add("uri", uri)
          .add("warehouse", warehouse)
          .add("clientPoolSize", clientPoolSize)
          .toString();
    }
  }

  class CustomCatalogLoader implements CatalogLoader {

    private final Object serializableHadoopConf;
    private final Map<String, String> properties;
    private final String name;
    private final String impl;

    private CustomCatalogLoader(
        String name, Map<String, String> properties, Object conf, String impl) {
      this.serializableHadoopConf = toSerializableConf(conf);
      this.properties = Maps.newHashMap(properties); // wrap into a hashmap for serialization
      this.name = name;
      this.impl =
          Preconditions.checkNotNull(
              impl, "Cannot initialize custom Catalog, impl class name is null");
    }

    @Override
    public Catalog loadCatalog() {
      return CatalogUtil.loadCatalog(impl, name, properties, getConf(serializableHadoopConf));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("name", name).add("impl", impl).toString();
    }
  }
}
