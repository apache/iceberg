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
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

/**
 * Serializable loader to load an Iceberg {@link Catalog}.
 */
public interface CatalogLoader extends Serializable {

  Catalog loadCatalog();

  static CatalogLoader hadoop(String name, Configuration hadoopConf, String warehouseLocation) {
    return new HadoopCatalogLoader(name, hadoopConf, warehouseLocation);
  }

  static CatalogLoader hive(String name, Configuration hadoopConf, String uri, String warehouse, int clientPoolSize) {
    return hive(name, hadoopConf, uri, warehouse, clientPoolSize, null);
  }

  static CatalogLoader hive(String name, Configuration hadoopConf, String uri, String warehouse,
                            int clientPoolSize, String hiveConfDir) {
    return new HiveCatalogLoader(name, hadoopConf, uri, warehouse, clientPoolSize, hiveConfDir);
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
    private final String hiveConfDir;

    private HiveCatalogLoader(String catalogName, Configuration conf, String uri, String warehouse, int clientPoolSize,
                              String hiveConfDir) {
      this.catalogName = catalogName;
      this.hadoopConf = new SerializableConfiguration(conf);
      this.uri = uri;
      this.warehouse = warehouse;
      this.clientPoolSize = clientPoolSize;
      this.hiveConfDir = hiveConfDir;
    }

    @Override
    public Catalog loadCatalog() {
      Configuration newConf = new Configuration(hadoopConf.get());
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
      return new HiveCatalog(catalogName, uri, warehouse, clientPoolSize, newConf);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("catalogName", catalogName)
          .add("uri", uri)
          .add("warehouse", warehouse)
          .add("clientPoolSize", clientPoolSize)
          .add("hiveConfDir", hiveConfDir)
          .toString();
    }
  }
}
