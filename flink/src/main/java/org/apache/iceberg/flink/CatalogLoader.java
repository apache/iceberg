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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Serializable loader to load an Iceberg {@link Catalog}.
 */
public interface CatalogLoader extends Serializable {

  /**
   * Create a new catalog with the provided properties. NOTICE: for flink, we may initialize the {@link CatalogLoader}
   * at flink sql client side or job manager side, and then serialize this catalog loader to task manager, finally
   * deserialize it and create a new catalog at task manager side.
   *
   * @return a newly created {@link Catalog}
   */
  Catalog loadCatalog();

  static CatalogLoader hadoop(String name, Configuration hadoopConf, String warehouseLocation) {
    return new HadoopCatalogLoader(name, hadoopConf, warehouseLocation);
  }

  static CatalogLoader hive(String name, Configuration hadoopConf, String uri, String warehouse, int clientPoolSize) {
    return new HiveCatalogLoader(name, hadoopConf, uri, warehouse, clientPoolSize);
  }

  class HadoopCatalogLoader implements CatalogLoader {
    private final String catalogName;
    private final SerializableConfiguration hadoopConf;
    private final String warehouseLocation;

    private HadoopCatalogLoader(String catalogName, Configuration conf, String warehouseLocation) {
      this.catalogName = catalogName;
      this.hadoopConf = new SerializableConfiguration(conf);
      this.warehouseLocation = warehouseLocation;
    }

    @Override
    public Catalog loadCatalog() {
      return new HadoopCatalog(catalogName, hadoopConf.get(), warehouseLocation);
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

    private HiveCatalogLoader(String catalogName, Configuration conf, String uri, String warehouse,
                              int clientPoolSize) {
      this.catalogName = catalogName;
      this.hadoopConf = new SerializableConfiguration(conf);
      this.uri = uri;
      this.warehouse = warehouse;
      this.clientPoolSize = clientPoolSize;
    }

    @Override
    public Catalog loadCatalog() {
      return new HiveCatalog(catalogName, uri, warehouse, clientPoolSize, hadoopConf.get());
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
}
