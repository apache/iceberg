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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.snowflake.entities.SnowflakeSchema;
import org.apache.iceberg.snowflake.entities.SnowflakeTable;
import org.apache.iceberg.snowflake.entities.SnowflakeTableMetadata;

public class FakeSnowflakeClient implements SnowflakeClient {
  // In-memory lookup by database/schema/tableName to table metadata.
  private Map<String, Map<String, Map<String, SnowflakeTableMetadata>>> databases =
      Maps.newTreeMap();

  public FakeSnowflakeClient() {}

  /**
   * Also adds parent database/schema if they don't already exist. If the tableName already exists
   * under the given database/schema, the value is replaced with the provided metadata.
   */
  public void addTable(
      String database, String schema, String tableName, SnowflakeTableMetadata metadata) {
    if (!databases.containsKey(database)) {
      databases.put(database, Maps.newTreeMap());
    }
    Map<String, Map<String, SnowflakeTableMetadata>> schemas = databases.get(database);
    if (!schemas.containsKey(schema)) {
      schemas.put(schema, Maps.newTreeMap());
    }
    Map<String, SnowflakeTableMetadata> tables = schemas.get(schema);
    tables.put(tableName, metadata);
  }

  @Override
  public List<SnowflakeSchema> listSchemas(Namespace namespace) {
    Preconditions.checkArgument(
        namespace.length() <= SnowflakeResources.MAX_NAMESPACE_DEPTH,
        "Namespace {} must have namespace of length <= {}",
        namespace,
        SnowflakeResources.MAX_NAMESPACE_DEPTH);
    List<SnowflakeSchema> schemas = Lists.newArrayList();
    if (namespace.length() == 0) {
      // "account-level" listing.
      for (Map.Entry<String, Map<String, Map<String, SnowflakeTableMetadata>>> db :
          databases.entrySet()) {
        for (String schema : db.getValue().keySet()) {
          schemas.add(new SnowflakeSchema(db.getKey(), schema));
        }
      }
    } else if (namespace.length() == SnowflakeResources.NAMESPACE_DB_LEVEL) {
      String dbName = namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1);
      if (databases.containsKey(dbName)) {
        for (String schema : databases.get(dbName).keySet()) {
          schemas.add(new SnowflakeSchema(dbName, schema));
        }
      } else {
        throw new UncheckedSQLException("Nonexistent database: '%s'", dbName);
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Tried to listSchemas using a namespace with too many levels: '%s'", namespace));
    }
    return schemas;
  }

  @Override
  public List<SnowflakeTable> listIcebergTables(Namespace namespace) {
    Preconditions.checkArgument(
        namespace.length() <= SnowflakeResources.MAX_NAMESPACE_DEPTH,
        "Namespace {} must have namespace of length <= {}",
        namespace,
        SnowflakeResources.MAX_NAMESPACE_DEPTH);
    List<SnowflakeTable> tables = Lists.newArrayList();
    if (namespace.length() == 0) {
      // "account-level" listing.
      for (Map.Entry<String, Map<String, Map<String, SnowflakeTableMetadata>>> db :
          databases.entrySet()) {
        for (Map.Entry<String, Map<String, SnowflakeTableMetadata>> schema :
            db.getValue().entrySet()) {
          for (String tableName : schema.getValue().keySet()) {
            tables.add(new SnowflakeTable(db.getKey(), schema.getKey(), tableName));
          }
        }
      }
    } else if (namespace.length() == SnowflakeResources.NAMESPACE_DB_LEVEL) {
      String dbName = namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1);
      if (databases.containsKey(dbName)) {
        for (Map.Entry<String, Map<String, SnowflakeTableMetadata>> schema :
            databases.get(dbName).entrySet()) {
          for (String tableName : schema.getValue().keySet()) {
            tables.add(new SnowflakeTable(dbName, schema.getKey(), tableName));
          }
        }
      } else {
        throw new UncheckedSQLException("Nonexistent database: '%s'", dbName);
      }
    } else {
      String dbName = namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1);
      if (databases.containsKey(dbName)) {
        String schemaName = namespace.level(SnowflakeResources.NAMESPACE_SCHEMA_LEVEL - 1);
        if (databases.get(dbName).containsKey(schemaName)) {
          for (String tableName : databases.get(dbName).get(schemaName).keySet()) {
            tables.add(new SnowflakeTable(dbName, schemaName, tableName));
          }
        } else {
          throw new UncheckedSQLException(
              "Nonexistent datbase.schema: '%s.%s'", dbName, schemaName);
        }
      } else {
        throw new UncheckedSQLException("Nonexistent database: '%s'", dbName);
      }
    }
    return tables;
  }

  @Override
  public SnowflakeTableMetadata getTableMetadata(TableIdentifier tableIdentifier) {
    Namespace ns = tableIdentifier.namespace();
    Preconditions.checkArgument(
        ns.length() == SnowflakeResources.MAX_NAMESPACE_DEPTH,
        "TableIdentifier {} must have namespace of length {}",
        tableIdentifier,
        SnowflakeResources.MAX_NAMESPACE_DEPTH);
    String dbName = ns.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1);
    String schemaName = ns.level(SnowflakeResources.NAMESPACE_SCHEMA_LEVEL - 1);
    if (!databases.containsKey(dbName)
        || !databases.get(dbName).containsKey(schemaName)
        || !databases.get(dbName).get(schemaName).containsKey(tableIdentifier.name())) {
      throw new UncheckedSQLException("Nonexistent object: '%s'", tableIdentifier);
    }
    return databases.get(dbName).get(schemaName).get(tableIdentifier.name());
  }

  @Override
  public void close() {}
}
