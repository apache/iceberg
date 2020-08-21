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

/**
 * Serializable loader to load an Iceberg {@link Table}.
 * Flink needs to get {@link Table} objects in the cluster (for example, to get splits), not just on the client side.
 * So we need an Iceberg table loader to get the {@link Table} object.
 */
public interface TableLoader extends Closeable, Serializable {

  void open(Configuration configuration);

  Table loadTable();

  static TableLoader fromCatalog(CatalogLoader catalogLoader, TableIdentifier identifier) {
    return new CatalogTableLoader(catalogLoader, identifier);
  }

  static TableLoader fromHadoopTable(String location) {
    return new HadoopTableLoader(location);
  }

  class HadoopTableLoader implements TableLoader {

    private static final long serialVersionUID = 1L;

    private final String location;
    private transient HadoopTables tables;

    private HadoopTableLoader(String location) {
      this.location = location;
    }

    @Override
    public void open(Configuration configuration) {
      tables = new HadoopTables(configuration);
    }

    @Override
    public Table loadTable() {
      return tables.load(location);
    }

    @Override
    public void close() {
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
    public void open(Configuration configuration) {
      catalog = catalogLoader.loadCatalog(configuration);
    }

    @Override
    public Table loadTable() {
      return catalog.loadTable(TableIdentifier.parse(identifier));
    }

    @Override
    public void close() throws IOException {
      if (catalog instanceof Closeable) {
        ((Closeable) catalog).close();
      }
    }
  }
}
