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
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

public abstract class BaseMetastoreTables implements Tables {
  private final Configuration conf;

  public BaseMetastoreTables(Configuration conf) {
    this.conf = conf;
  }

  protected abstract BaseMetastoreTableOperations newTableOps(Configuration newConf,
                                                              String database, String table);

  public Table load(String database, String table) {
    TableOperations ops = newTableOps(conf, database, table);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist: " + database + "." + table);
    }

    return new BaseTable(ops, database + "." + table);
  }

  public Table create(Schema schema, String database, String table) {
    return create(schema, PartitionSpec.unpartitioned(), database, table);
  }

  public Table create(Schema schema, PartitionSpec spec, String database, String table) {
    return create(schema, spec, ImmutableMap.of(), database, table);
  }

  public Table create(Schema schema, PartitionSpec spec, Map<String, String> properties,
                      String database, String table) {
    TableOperations ops = newTableOps(conf, database, table);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists: " + database + "." + table);
    }

    String location = defaultWarehouseLocation(conf, database, table);
    TableMetadata metadata = TableMetadata.newTableMetadata(ops, schema, spec, location, properties);
    ops.commit(null, metadata);

    return new BaseTable(ops, database + "." + table);
  }

  public Transaction beginCreate(Schema schema, PartitionSpec spec, String database, String table) {
    return beginCreate(schema, spec, ImmutableMap.of(), database, table);
  }

  public Transaction beginCreate(Schema schema, PartitionSpec spec, Map<String, String> properties,
                                 String database, String table) {
    TableOperations ops = newTableOps(conf, database, table);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists: " + database + "." + table);
    }

    String location = defaultWarehouseLocation(conf, database, table);
    TableMetadata metadata = TableMetadata.newTableMetadata(ops, schema, spec, location, properties);

    return Transactions.createTableTransaction(ops, metadata);
  }

  public Transaction beginReplace(Schema schema, PartitionSpec spec,
                                  String database, String table) {
    return beginReplace(schema, spec, ImmutableMap.of(), database, table);
  }

  public Transaction beginReplace(Schema schema, PartitionSpec spec, Map<String, String> properties,
                                  String database, String table) {
    TableOperations ops = newTableOps(conf, database, table);
    TableMetadata current = ops.current();

    TableMetadata metadata;
    if (current != null) {
      metadata = current.buildReplacement(schema, spec, properties);
      return Transactions.replaceTableTransaction(ops, metadata);
    } else {
      String location = defaultWarehouseLocation(conf, database, table);
      metadata = TableMetadata.newTableMetadata(ops, schema, spec, location, properties);
      return Transactions.createTableTransaction(ops, metadata);
    }
  }

  protected String defaultWarehouseLocation(Configuration hadoopConf,
                                            String database, String table) {
    String warehouseLocation = hadoopConf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(warehouseLocation,
        "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    return String.format("%s/%s.db/%s", warehouseLocation, database, table);
  }
}
