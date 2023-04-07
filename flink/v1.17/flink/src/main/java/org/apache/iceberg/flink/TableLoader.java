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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Serializable loader to load an Iceberg {@link Table}. Flink needs to get {@link Table} objects in
 * the cluster (for example, to get splits), not just on the client side. So we need an Iceberg
 * table loader to get the {@link Table} object.
 */
public interface TableLoader extends Closeable, Serializable, Cloneable {

  void open();

  Table loadTable();

  /** Clone a TableLoader */
  @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
  TableLoader clone();

  static TableLoader fromCatalog(CatalogLoader catalogLoader, TableIdentifier identifier) {
    return new CatalogTableLoader(catalogLoader, identifier);
  }

  static TableLoader fromHadoopTable(String location) {
    return fromHadoopTable(location, FlinkCatalogFactory.clusterHadoopConf());
  }

  static TableLoader fromHadoopTable(String location, Configuration hadoopConf) {
    return new HadoopTableLoader(location, hadoopConf);
  }

  class HadoopTableLoader implements TableLoader {

    private static final long serialVersionUID = 1L;

    private final String location;
    private final SerializableConfiguration hadoopConf;

    private transient HadoopTables tables;

    private HadoopTableLoader(String location, Configuration conf) {
      this.location = location;
      this.hadoopConf = new SerializableConfiguration(conf);
    }

    @Override
    public void open() {
      tables = new HadoopTables(hadoopConf.get());
    }

    @Override
    public Table loadTable() {
      FlinkEnvironmentContext.init();
      return tables.load(location);
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public TableLoader clone() {
      return new HadoopTableLoader(location, new Configuration(hadoopConf.get()));
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("location", location).toString();
    }
  }

  class CatalogTableLoader implements TableLoader {

    private static final long serialVersionUID = 1L;

    private final CatalogLoader catalogLoader;
    private final String identifier;

    private transient Catalog catalog;

    private CatalogTableLoader(CatalogLoader catalogLoader, TableIdentifier tableIdentifier) {
      this.catalogLoader = catalogLoader;
      this.identifier = tableIdentifier.toString();
    }

    @Override
    public void open() {
      catalog = catalogLoader.loadCatalog();
    }

    @Override
    public Table loadTable() {
      FlinkEnvironmentContext.init();
      return catalog.loadTable(TableIdentifier.parse(identifier));
    }

    @Override
    public void close() throws IOException {
      if (catalog instanceof Closeable) {
        ((Closeable) catalog).close();
      }
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public TableLoader clone() {
      return new CatalogTableLoader(catalogLoader.clone(), TableIdentifier.parse(identifier));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("tableIdentifier", identifier)
          .add("catalogLoader", catalogLoader)
          .toString();
    }
  }
}
