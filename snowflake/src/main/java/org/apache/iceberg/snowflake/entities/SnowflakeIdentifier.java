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
package org.apache.iceberg.snowflake.entities;

import java.util.List;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class SnowflakeIdentifier {
  public enum Type {
    ROOT,
    DATABASE,
    SCHEMA,
    TABLE
  }

  private String databaseName;
  private String schemaName;
  private String tableName;

  protected SnowflakeIdentifier(String databaseName, String schemaName, String tableName) {
    this.databaseName = databaseName;
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public static SnowflakeIdentifier ofRoot() {
    return new SnowflakeIdentifier(null, null, null);
  }

  public static SnowflakeIdentifier ofDatabase(String databaseName) {
    Preconditions.checkArgument(null != databaseName, "databaseName must be non-null");
    return new SnowflakeIdentifier(databaseName, null, null);
  }

  public static SnowflakeIdentifier ofSchema(String databaseName, String schemaName) {
    Preconditions.checkArgument(null != databaseName, "databaseName must be non-null");
    Preconditions.checkArgument(null != schemaName, "schemaName must be non-null");
    return new SnowflakeIdentifier(databaseName, schemaName, null);
  }

  public static SnowflakeIdentifier ofTable(
      String databaseName, String schemaName, String tableName) {
    Preconditions.checkArgument(null != databaseName, "databaseName must be non-null");
    Preconditions.checkArgument(null != schemaName, "schemaName must be non-null");
    Preconditions.checkArgument(null != tableName, "tableName must be non-null");
    return new SnowflakeIdentifier(databaseName, schemaName, tableName);
  }

  /**
   * If type is TABLE, expect non-null databaseName, schemaName, and tableName. If type is SCHEMA,
   * expect non-null databaseName and schemaName. If type is DATABASE, expect non-null databaseName.
   * If type is ROOT, expect all of databaseName, schemaName, and tableName to be null.
   */
  public Type getType() {
    if (null != tableName) {
      return Type.TABLE;
    } else if (null != schemaName) {
      return Type.SCHEMA;
    } else if (null != databaseName) {
      return Type.DATABASE;
    } else {
      return Type.ROOT;
    }
  }

  public String getTableName() {
    return tableName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof SnowflakeIdentifier)) {
      return false;
    }

    SnowflakeIdentifier that = (SnowflakeIdentifier) o;
    return Objects.equal(this.databaseName, that.databaseName)
        && Objects.equal(this.schemaName, that.schemaName)
        && Objects.equal(this.tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(databaseName, schemaName, tableName);
  }

  /** Returns this identifier as a String suitable for use in a Snowflake IDENTIFIER param. */
  public String toIdentifierString() {
    switch (getType()) {
      case TABLE:
        return String.format("%s.%s.%s", databaseName, schemaName, tableName);
      case SCHEMA:
        return String.format("%s.%s", databaseName, schemaName);
      case DATABASE:
        return databaseName;
      default:
        return "";
    }
  }

  @Override
  public String toString() {
    return String.format("%s: '%s'", getType(), toIdentifierString());
  }

  /**
   * Expects to handle ResultSets representing fully-qualified Snowflake Schema identifiers,
   * containing "database_name" and "name" (representing schemaName).
   */
  public static ResultSetHandler<List<SnowflakeIdentifier>> createSchemaHandler() {
    return rs -> {
      List<SnowflakeIdentifier> schemas = Lists.newArrayList();
      while (rs.next()) {
        String databaseName = rs.getString("database_name");
        String schemaName = rs.getString("name");
        schemas.add(SnowflakeIdentifier.ofSchema(databaseName, schemaName));
      }
      return schemas;
    };
  }

  /**
   * Expects to handle ResultSets representing fully-qualified Snowflake Table identifiers,
   * containing "database_name", "schema_name", and "name" (representing tableName).
   */
  public static ResultSetHandler<List<SnowflakeIdentifier>> createTableHandler() {
    return rs -> {
      List<SnowflakeIdentifier> tables = Lists.newArrayList();
      while (rs.next()) {
        String databaseName = rs.getString("database_name");
        String schemaName = rs.getString("schema_name");
        String tableName = rs.getString("name");
        tables.add(SnowflakeIdentifier.ofTable(databaseName, schemaName, tableName));
      }
      return tables;
    };
  }
}
