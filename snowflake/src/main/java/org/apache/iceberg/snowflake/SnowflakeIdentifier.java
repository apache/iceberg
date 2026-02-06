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

import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Since the SnowflakeCatalog supports exactly two levels of Iceberg Namespaces, corresponding
 * directly to the "database" and "schema" portions of Snowflake's resource model, this class
 * represents a pre-validated and structured representation of a fully-qualified Snowflake resource
 * identifier. Snowflake-specific helper libraries should operate on this representation instead of
 * directly operating on TableIdentifiers or Namespaces wherever possible to avoid duplication of
 * parsing/validation logic for Iceberg TableIdentifier/Namespace levels.
 */
class SnowflakeIdentifier {
  public enum Type {
    ROOT,
    DATABASE,
    SCHEMA,
    TABLE
  }

  private final String databaseName;
  private final String schemaName;
  private final String tableName;
  private final Type type;

  private SnowflakeIdentifier(String databaseName, String schemaName, String tableName, Type type) {
    this.databaseName = databaseName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.type = type;
  }

  public static SnowflakeIdentifier ofRoot() {
    return new SnowflakeIdentifier(null, null, null, Type.ROOT);
  }

  public static SnowflakeIdentifier ofDatabase(String databaseName) {
    Preconditions.checkArgument(null != databaseName, "databaseName must be non-null");
    return new SnowflakeIdentifier(databaseName, null, null, Type.DATABASE);
  }

  public static SnowflakeIdentifier ofSchema(String databaseName, String schemaName) {
    Preconditions.checkArgument(null != databaseName, "databaseName must be non-null");
    Preconditions.checkArgument(null != schemaName, "schemaName must be non-null");
    return new SnowflakeIdentifier(databaseName, schemaName, null, Type.SCHEMA);
  }

  public static SnowflakeIdentifier ofTable(
      String databaseName, String schemaName, String tableName) {
    Preconditions.checkArgument(null != databaseName, "databaseName must be non-null");
    Preconditions.checkArgument(null != schemaName, "schemaName must be non-null");
    Preconditions.checkArgument(null != tableName, "tableName must be non-null");
    return new SnowflakeIdentifier(databaseName, schemaName, tableName, Type.TABLE);
  }

  /**
   * If type is TABLE, expect non-null databaseName, schemaName, and tableName. If type is SCHEMA,
   * expect non-null databaseName and schemaName. If type is DATABASE, expect non-null databaseName.
   * If type is ROOT, expect all of databaseName, schemaName, and tableName to be null.
   */
  public Type type() {
    return type;
  }

  public String tableName() {
    return tableName;
  }

  public String databaseName() {
    return databaseName;
  }

  public String schemaName() {
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
    switch (type()) {
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
    return String.format("%s: '%s'", type(), toIdentifierString());
  }
}
