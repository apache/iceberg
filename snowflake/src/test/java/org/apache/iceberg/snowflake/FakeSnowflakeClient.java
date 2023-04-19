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
package org.apache.iceberg.snowflake;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class FakeSnowflakeClient implements SnowflakeClient {
  // In-memory lookup by database/schema/tableName to table metadata.
  private final Map<String, Map<String, Map<String, SnowflakeTableMetadata>>> databases =
      Maps.newTreeMap();
  private boolean closed = false;

  public FakeSnowflakeClient() {}

  /**
   * Also adds parent database/schema if they don't already exist. If the tableName already exists
   * under the given database/schema, the value is replaced with the provided metadata.
   */
  public void addTable(SnowflakeIdentifier tableIdentifier, SnowflakeTableMetadata metadata) {
    Preconditions.checkState(!closed, "Cannot call addTable after calling close()");
    if (!databases.containsKey(tableIdentifier.databaseName())) {
      databases.put(tableIdentifier.databaseName(), Maps.newTreeMap());
    }
    Map<String, Map<String, SnowflakeTableMetadata>> schemas =
        databases.get(tableIdentifier.databaseName());
    if (!schemas.containsKey(tableIdentifier.schemaName())) {
      schemas.put(tableIdentifier.schemaName(), Maps.newTreeMap());
    }
    Map<String, SnowflakeTableMetadata> tables = schemas.get(tableIdentifier.schemaName());
    tables.put(tableIdentifier.tableName(), metadata);
  }

  @Override
  public boolean databaseExists(SnowflakeIdentifier database) {
    return databases.containsKey(database.databaseName());
  }

  @Override
  public boolean schemaExists(SnowflakeIdentifier schema) {
    return databases.containsKey(schema.databaseName())
        && databases.get(schema.databaseName()).containsKey(schema.schemaName());
  }

  @Override
  public List<SnowflakeIdentifier> listDatabases() {
    Preconditions.checkState(!closed, "Cannot call listDatabases after calling close()");
    List<SnowflakeIdentifier> databaseIdentifiers = Lists.newArrayList();
    for (String databaseName : databases.keySet()) {
      databaseIdentifiers.add(SnowflakeIdentifier.ofDatabase(databaseName));
    }
    return databaseIdentifiers;
  }

  @Override
  public List<SnowflakeIdentifier> listSchemas(SnowflakeIdentifier scope) {
    Preconditions.checkState(!closed, "Cannot call listSchemas after calling close()");
    List<SnowflakeIdentifier> schemas = Lists.newArrayList();
    switch (scope.type()) {
      case ROOT:
        // "account-level" listing.
        for (Map.Entry<String, Map<String, Map<String, SnowflakeTableMetadata>>> db :
            databases.entrySet()) {
          for (String schema : db.getValue().keySet()) {
            schemas.add(SnowflakeIdentifier.ofSchema(db.getKey(), schema));
          }
        }
        break;
      case DATABASE:
        String dbName = scope.databaseName();
        if (databases.containsKey(dbName)) {
          for (String schema : databases.get(dbName).keySet()) {
            schemas.add(SnowflakeIdentifier.ofSchema(dbName, schema));
          }
        } else {
          throw new UncheckedSQLException("Object does not exist: database: '%s'", dbName);
        }
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported scope type for listSchemas: '%s'", scope));
    }
    return schemas;
  }

  @Override
  public List<SnowflakeIdentifier> listIcebergTables(SnowflakeIdentifier scope) {
    Preconditions.checkState(!closed, "Cannot call listIcebergTables after calling close()");
    List<SnowflakeIdentifier> tables = Lists.newArrayList();
    switch (scope.type()) {
      case ROOT:
        {
          // "account-level" listing.
          for (Map.Entry<String, Map<String, Map<String, SnowflakeTableMetadata>>> db :
              databases.entrySet()) {
            for (Map.Entry<String, Map<String, SnowflakeTableMetadata>> schema :
                db.getValue().entrySet()) {
              for (String tableName : schema.getValue().keySet()) {
                tables.add(SnowflakeIdentifier.ofTable(db.getKey(), schema.getKey(), tableName));
              }
            }
          }
          break;
        }
      case DATABASE:
        {
          String dbName = scope.databaseName();
          if (databases.containsKey(dbName)) {
            for (Map.Entry<String, Map<String, SnowflakeTableMetadata>> schema :
                databases.get(dbName).entrySet()) {
              for (String tableName : schema.getValue().keySet()) {
                tables.add(SnowflakeIdentifier.ofTable(dbName, schema.getKey(), tableName));
              }
            }
          } else {
            throw new UncheckedSQLException("Object does not exist: database: '%s'", dbName);
          }
          break;
        }
      case SCHEMA:
        {
          String dbName = scope.databaseName();
          if (databases.containsKey(dbName)) {
            String schemaName = scope.schemaName();
            if (databases.get(dbName).containsKey(schemaName)) {
              for (String tableName : databases.get(dbName).get(schemaName).keySet()) {
                tables.add(SnowflakeIdentifier.ofTable(dbName, schemaName, tableName));
              }
            } else {
              throw new UncheckedSQLException(
                  "Object does not exist: database.schema: '%s.%s'", dbName, schemaName);
            }
          } else {
            throw new UncheckedSQLException("Object does not exist: database: '%s'", dbName);
          }
          break;
        }
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported scope type for listing tables: %s", scope));
    }
    return tables;
  }

  @Override
  public SnowflakeTableMetadata loadTableMetadata(SnowflakeIdentifier tableIdentifier) {
    Preconditions.checkState(!closed, "Cannot call getTableMetadata after calling close()");

    Preconditions.checkArgument(
        tableIdentifier.type() == SnowflakeIdentifier.Type.TABLE,
        "tableIdentifier must be type TABLE, get: %s",
        tableIdentifier);
    String dbName = tableIdentifier.databaseName();
    String schemaName = tableIdentifier.schemaName();
    if (!databases.containsKey(dbName)
        || !databases.get(dbName).containsKey(schemaName)
        || !databases.get(dbName).get(schemaName).containsKey(tableIdentifier.tableName())) {
      throw new UncheckedSQLException("Object does not exist: object: '%s'", tableIdentifier);
    }
    return databases.get(dbName).get(schemaName).get(tableIdentifier.tableName());
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    closed = true;
  }
}
