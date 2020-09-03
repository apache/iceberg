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
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Serializable loader to load an Iceberg {@link Catalog}.
 */
public interface CatalogLoader extends Serializable {

  Catalog loadCatalog(Configuration hadoopConf);

  static CatalogLoader hadoop(String name, String warehouseLocation) {
    return new HadoopCatalogLoader(name, warehouseLocation);
  }

  static CatalogLoader hive(String name, String uri, int clientPoolSize) {
    return new HiveCatalogLoader(name, uri, clientPoolSize);
  }

  class HadoopCatalogLoader implements CatalogLoader {
    private final String catalogName;
    private final String warehouseLocation;

    private HadoopCatalogLoader(String catalogName, String warehouseLocation) {
      this.catalogName = catalogName;
      this.warehouseLocation = warehouseLocation;
    }

    @Override
    public Catalog loadCatalog(Configuration hadoopConf) {
      return new HadoopCatalog(catalogName, hadoopConf, warehouseLocation);
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
    private final String uri;
    private final int clientPoolSize;

    private HiveCatalogLoader(String catalogName, String uri, int clientPoolSize) {
      this.catalogName = catalogName;
      this.uri = uri;
      this.clientPoolSize = clientPoolSize;
    }

    @Override
    public Catalog loadCatalog(Configuration hadoopConf) {
      return new HiveCatalog(catalogName, uri, clientPoolSize, hadoopConf);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("catalogName", catalogName)
          .add("uri", uri)
          .add("clientPoolSize", clientPoolSize)
          .toString();
    }
  }
}
