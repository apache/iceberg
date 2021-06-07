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

package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class TestCatalog implements Catalog, Configurable {

  private HadoopTables tables;
  private Configuration conf;
  private String warehouse;

  public TestCatalog() {
  }

  @Override
  public String name() {
    return "test-tables";
  }

  private String tablePath(TableIdentifier identifier) {
    return String.format("%s/%s", warehouse, identifier.name());
  }
  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema,
                           PartitionSpec spec, String location, Map<String, String> properties) {
    return tables.create(schema, spec, properties, tablePath(identifier));
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema,
                                               PartitionSpec spec, String location, Map<String, String> properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema,
                                                PartitionSpec spec, String location, Map<String, String> properties,
                                                boolean orCreate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return tables.dropTable(tablePath(identifier), purge);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    return tables.load(tablePath(identifier));
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    String uri = properties.get(CatalogProperties.URI);
    warehouse = properties.get("warehouse");
    Preconditions.checkArgument(uri != null,
        "Cannot initialize TestCatalog. The metastore connection uri must be set.");
    Preconditions.checkArgument(uri.contains("thrift"),
        "Cannot initialize TestCatalog. The metastore connection uri must use thrift as the scheme.");
    Preconditions.checkArgument(warehouse != null,
        "Cannot initialize TestCatalog. The base path for the catalog's warehouse directory must be set.");
    this.tables = new HadoopTables(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

}
