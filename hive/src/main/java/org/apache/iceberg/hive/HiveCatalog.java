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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalog extends BaseMetastoreCatalog implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

  private final HiveClientPool clients;
  private final Configuration conf;
  private final StackTraceElement[] createStack;
  private final String name;
  private boolean closed;

  public HiveCatalog(Configuration conf) {
    this("hive", null, 2, conf);
  }

  public HiveCatalog(String name, String uri, int clientPoolSize, Configuration conf) {
    this.conf = new Configuration(conf);
    // before building the client pool, overwrite the configuration's URIs if the argument is non-null
    if (uri != null) {
      this.conf.set("hive.metastore.uris", uri);
    }

    this.clients = new HiveClientPool(clientPoolSize, this.conf);
    this.createStack = Thread.currentThread().getStackTrace();
    this.name = name;
    this.closed = false;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(namespace.levels().length == 1,
        "Missing database in namespace: %s", namespace);
    String database = namespace.level(0);

    try {
      List<String> tables = clients.run(client -> client.getAllTables(database));
      return tables.stream()
          .map(t -> TableIdentifier.of(namespace, t))
          .collect(Collectors.toList());

    } catch (UnknownDBException e) {
      throw new NotFoundException(e, "Unknown namespace " + namespace.toString());

    } catch (TException e) {
      throw new RuntimeException("Failed to list all tables under namespace " + namespace.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to listTables", e);
    }
  }

  @Override
  protected String name() {
    return name;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!isValidIdentifier(identifier)) {
      throw new NoSuchTableException("Invalid identifier: %s", identifier);
    }

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
    if (!isValidIdentifier(from)) {
      throw new NoSuchTableException("Invalid identifier: %s", from);
    }
    Preconditions.checkArgument(isValidIdentifier(to), "Invalid identifier: %s", to);

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

    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException("Table does not exist: %s", from);

    } catch (AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException("Table already exists: %s", to);

    } catch (TException e) {
      throw new RuntimeException("Failed to rename " + from.toString() + " to " + to.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to rename", e);
    }
  }

  @Override
  public boolean createNamespace(Namespace namespace) {
    Preconditions.checkArgument(!namespace.isEmpty(),
        "Namespace could not empty. ");
    Preconditions.checkArgument(namespace.levels().length == 1,
        "Hive MetaStore cannot support multi part namespace now: %s", namespace.toString());
    try {
      clients.run(client -> {
        client.createDatabase(nameSpaceToHiveDb(namespace));
        return null;
      });

      return true;

    } catch (AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException("namespace already exists: %s",
            namespace.toString());
    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException("NameSpace does not exist: %s", e.getMessage());

    } catch (TException e) {
      throw new RuntimeException("Failed to create namespace " + namespace.toString() + " in Hive MataStore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to createDatabase(name)", e);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    List<Namespace> namespaces = new ArrayList<>();
    List<String> dbs;
    try {
      if (namespace.isEmpty()) {
        dbs = clients.run(HiveMetaStoreClient::getAllDatabases);
      } else {
        dbs = clients.run(client -> client.getDatabases(namespace.toString()));
      }
      for (String db : dbs) {
        namespaces.add(Namespace.of(db));
      }
      return namespaces;
    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException("NameSpace does not exist: %s", e.getMessage());

    } catch (TException e) {
      throw new RuntimeException("Failed to list all namespace: " + e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getAllDatabases()", e);
    }
  }

  @Override
  public List<Namespace> listNamespaces() {
    return listNamespaces(Namespace.empty());
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    Preconditions.checkArgument(!namespace.isEmpty(),
        "Namespace could not empty. ");
    Preconditions.checkArgument(namespace.levels().length == 1,
        "Hive MetaStore cannot support multi part namespace now: %s", namespace.toString());
    try {
      clients.run(client -> {
        client.dropDatabase(namespace.toString());
        return null;
      });

      return true;

    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException("Unknown namespace %s", namespace.toString(), e.getMessage());

    } catch (TException e) {
      throw new RuntimeException("Failed to drop namespace " + namespace.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to drop dropDatabase(name)" + namespace.toString(), e);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    Preconditions.checkArgument(!namespace.isEmpty(),
        "Namespace could not empty. ");
    Preconditions.checkArgument(namespace.levels().length == 1,
        "Hive MetaStore cannot support multi part namespace now: %s", namespace.toString());
    try {
      Database database = clients.run(client -> client.getDatabase(namespace.toString()));
      return getMetafrpmhiveDb(database);

    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException("Unknown namespace %s", namespace.toString(), e.getMessage());

    } catch (UnknownDBException e) {
      throw new NoSuchNamespaceException(e, "Unknown namespace %s" + namespace.toString(), e.getMessage());

    } catch (TException e) {
      throw new RuntimeException("Failed to list namespace under namespace: %s" + namespace.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getDatabase(name)" + namespace.toString(), e);
    }
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier.namespace().levels().length == 1;
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();

    return new HiveTableOperations(conf, clients, dbName, tableName);
  }

  @Override
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

  public  Map<String, String> getMetafrpmhiveDb(Database database) {
    Map<String, String> nameMap = new HashMap<>();

    nameMap.put("name", database.getName());
    nameMap.put("location", database.getLocationUri());
    nameMap.put("comment", database.getDescription());
    database.getParameters().forEach(nameMap::put);

    return nameMap;
  }

  public  Database nameSpaceToHiveDb(Namespace namespace) {
    String warehouseLocation = conf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(warehouseLocation,
        "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    Preconditions.checkArgument(!namespace.isEmpty(),
        "Namespace could not empty. ");
    Joiner slash = Joiner.on("/");
    Database database  = new Database();

    database.setName(namespace.toString());
    database.setLocationUri(slash.join(warehouseLocation, slash.join(namespace.levels())) + ".db");
    database.setDescription(namespace.getParameters("comment"));
    database.setParameters(namespace.getParameters());

    return database;
  }

  @Override
  public void close() {
    if (!closed) {
      clients.close();
      closed = true;
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(
          Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }
}
