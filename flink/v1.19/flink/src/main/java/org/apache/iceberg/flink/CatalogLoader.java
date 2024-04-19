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
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;

/** Serializable loader to load an Iceberg {@link Catalog}. */
public interface CatalogLoader extends Serializable, Cloneable {

  /**
   * Create a new catalog with the provided properties. NOTICE: for flink, we may initialize the
   * {@link CatalogLoader} at flink sql client side or job manager side, and then serialize this
   * catalog loader to task manager, finally deserialize it and create a new catalog at task manager
   * side.
   *
   * @return a newly created {@link Catalog}
   */
  Catalog loadCatalog();

  /** Clone a CatalogLoader. */
  @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
  CatalogLoader clone();

  static CatalogLoader hadoop(String name, Object hadoopConf, Map<String, String> properties) {
    return new HadoopCatalogLoader(name, hadoopConf, properties);
  }

  static CatalogLoader hive(String name, Object hadoopConf, Map<String, String> properties) {
    return new HiveCatalogLoader(name, hadoopConf, properties);
  }

  static CatalogLoader rest(String name, Object hadoopConf, Map<String, String> properties) {
    return new RESTCatalogLoader(name, hadoopConf, properties);
  }

  static CatalogLoader custom(
      String name, Map<String, String> properties, Object hadoopConf, String impl) {
    return new CustomCatalogLoader(name, properties, hadoopConf, impl);
  }

  class HadoopCatalogLoader implements CatalogLoader {
    private final String catalogName;
    private final SerializableConfiguration hadoopConf;
    private final String warehouseLocation;
    private final Map<String, String> properties;

    private HadoopCatalogLoader(String catalogName, Object conf, Map<String, String> properties) {
      this.catalogName = catalogName;
      this.hadoopConf = new SerializableConfiguration(conf);
      this.warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
      this.properties = Maps.newHashMap(properties);
    }

    @Override
    public Catalog loadCatalog() {
      return CatalogUtil.loadCatalog(
          HadoopCatalog.class.getName(), catalogName, properties, hadoopConf.get());
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public CatalogLoader clone() {
      return new HadoopCatalogLoader(catalogName, hadoopConf.getClone(), properties);
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
    private final SerializableConfiguration hadoopConf;
    private final String uri;
    private final String warehouse;
    private final int clientPoolSize;
    private final Map<String, String> properties;

    private HiveCatalogLoader(String catalogName, Object conf, Map<String, String> properties) {
      this.catalogName = catalogName;
      this.hadoopConf = new SerializableConfiguration(conf);
      this.uri = properties.get(CatalogProperties.URI);
      this.warehouse = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
      this.clientPoolSize =
          properties.containsKey(CatalogProperties.CLIENT_POOL_SIZE)
              ? Integer.parseInt(properties.get(CatalogProperties.CLIENT_POOL_SIZE))
              : CatalogProperties.CLIENT_POOL_SIZE_DEFAULT;
      this.properties = Maps.newHashMap(properties);
    }

    @Override
    public Catalog loadCatalog() {
      return CatalogUtil.loadCatalog(
          HiveCatalog.class.getName(), catalogName, properties, hadoopConf.get());
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public CatalogLoader clone() {
      return new HiveCatalogLoader(catalogName, hadoopConf.getClone(), properties);
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

  class RESTCatalogLoader implements CatalogLoader {
    private final String catalogName;
    private final SerializableConfiguration hadoopConf;
    private final Map<String, String> properties;

    private RESTCatalogLoader(String catalogName, Object conf, Map<String, String> properties) {
      this.catalogName = catalogName;
      if (conf != null) {
        this.hadoopConf = new SerializableConfiguration(conf);
      } else {
        this.hadoopConf = null;
      }
      this.properties = Maps.newHashMap(properties);
    }

    @Override
    public Catalog loadCatalog() {
      Object conf = null;
      if (hadoopConf != null) {
        conf = hadoopConf.get();
      }
      return CatalogUtil.loadCatalog(RESTCatalog.class.getName(), catalogName, properties, conf);
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public CatalogLoader clone() {
      Object conf = null;
      if (hadoopConf != null) {
        conf = hadoopConf.getClone();
      }
      return new RESTCatalogLoader(catalogName, conf, properties);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("catalogName", catalogName)
          .add("properties", properties)
          .toString();
    }
  }

  class CustomCatalogLoader implements CatalogLoader {

    private final SerializableConfiguration hadoopConf;
    private final Map<String, String> properties;
    private final String name;
    private final String impl;

    private CustomCatalogLoader(
        String name, Map<String, String> properties, Object conf, String impl) {
      if (conf != null) {
        this.hadoopConf = new SerializableConfiguration(conf);
      } else {
        this.hadoopConf = null;
      }
      this.properties = Maps.newHashMap(properties); // wrap into a hashmap for serialization
      this.name = name;
      this.impl =
          Preconditions.checkNotNull(
              impl, "Cannot initialize custom Catalog, impl class name is null");
    }

    @Override
    public Catalog loadCatalog() {
      Object conf = null;
      if (hadoopConf != null) {
        conf = hadoopConf.get();
      }
      return CatalogUtil.loadCatalog(impl, name, properties, conf);
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public CatalogLoader clone() {
      Object conf = null;
      if (hadoopConf != null) {
        conf = hadoopConf.getClone();
      }
      return new CustomCatalogLoader(name, properties, conf, impl);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("name", name).add("impl", impl).toString();
    }
  }
}
