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
package org.apache.iceberg;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

public abstract class BaseMetastoreCatalog implements Catalog {
  private final Configuration conf;

  protected BaseMetastoreCatalog(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Table createTable(
      TableIdentifier tableIdentifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> tableProperties) {
    validateTableIdentifier(tableIdentifier);

    TableOperations ops = newTableOps(conf, tableIdentifier);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists: " + tableIdentifier);
    }

    String location = defaultWarehouseLocation(conf, tableIdentifier);
    TableMetadata metadata = TableMetadata.newTableMetadata(ops,
        schema,
        spec,
        location,
        tableProperties == null ? new HashMap<>() : tableProperties);
    ops.commit(null, metadata);

    return new BaseTable(ops, tableIdentifier.toString());
  }

  @Override
  public Table getTable(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);

    TableOperations ops = newTableOps(conf, tableIdentifier);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist: " + tableIdentifier.toString());
    }

    return new BaseTable(ops, tableIdentifier.toString());
  }

  public abstract TableOperations newTableOps(Configuration newConf, TableIdentifier tableIdentifier);

  protected String defaultWarehouseLocation(Configuration hadoopConf, TableIdentifier tableIdentifier) {
    String warehouseLocation = hadoopConf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(
        warehouseLocation,
        "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    return String.format(
        "%s/%s.db/%s",
        warehouseLocation,
        tableIdentifier.namespace().levels()[0],
        tableIdentifier.name());
  }

  protected static final void validateTableIdentifier(TableIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier.hasNamespace(), "metastore tables should have schema as namespace");
    Preconditions.checkArgument(tableIdentifier.namespace().levels().length == 1, "metastore tables should only have " +
        "schema name as namespace");
    String schemaName = tableIdentifier.namespace().levels()[0];
    Preconditions.checkArgument(schemaName != null && !schemaName.isEmpty(), "schema name can't be null or " +
        "empty");
    String tableName = tableIdentifier.name();
    Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "tableName can't be null or empty");
  }
}
