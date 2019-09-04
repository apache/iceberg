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

package org.apache.iceberg.hive;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;

public class HiveCatalog extends BaseMetastoreCatalog implements Closeable {

  private final HiveClientPool clients;
  private final Configuration conf;

  public HiveCatalog(Configuration conf) {
    this.clients = new HiveClientPool(2, conf);
    this.conf = conf;
  }

  @Override
  public org.apache.iceberg.Table createTable(
      TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
        "Missing database in table identifier: %s", identifier);
    return super.createTable(identifier, schema, spec, location, properties);
  }

  @Override
  public org.apache.iceberg.Table loadTable(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace().levels().length >= 1,
        "Missing database in table identifier: %s", identifier);
    return super.loadTable(identifier);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    Preconditions.checkArgument(identifier.namespace().levels().length == 1,
        "Missing database in table identifier: %s", identifier);
    String database = identifier.namespace().level(0);

    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    try {
      clients.run(client -> {
        client.dropTable(database, identifier.name(),
            false /* do not delete data */,
            false /* throw NoSuchObjectException if the table doesn't exist */);
        return null;
      });

      if (purge && lastMetadata != null) {
        dropTableData(ops.io(), lastMetadata);
      }

      return true;

    } catch (NoSuchObjectException e) {
      return false;

    } catch (TException e) {
      throw new RuntimeException("Failed to drop " + identifier.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to dropTable", e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    Preconditions.checkArgument(from.namespace().levels().length == 1,
        "Missing database in table identifier: %s", from);
    Preconditions.checkArgument(to.namespace().levels().length == 1,
        "Missing database in table identifier: %s", to);

    String toDatabase = to.namespace().level(0);
    String fromDatabase = from.namespace().level(0);
    String fromName = from.name();

    try {
      Table table = clients.run(client -> client.getTable(fromDatabase, fromName));
      table.setDbName(toDatabase);
      table.setTableName(to.name());

      clients.run(client -> {
        client.alter_table(fromDatabase, fromName, table);
        return null;
      });

    } catch (TException e) {
      throw new RuntimeException("Failed to rename " + from.toString() + " to " + to.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to rename", e);
    }
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new HiveTableOperations(conf, clients, dbName, tableName);
  }

  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String warehouseLocation = conf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(
        warehouseLocation,
        "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    return String.format(
        "%s/%s.db/%s",
        warehouseLocation,
        tableIdentifier.namespace().levels()[0],
        tableIdentifier.name());
  }

  @Override
  public void close() {
    clients.close();
  }
}
