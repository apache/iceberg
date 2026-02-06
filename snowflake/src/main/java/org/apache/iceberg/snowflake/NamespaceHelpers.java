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

import java.util.Locale;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class NamespaceHelpers {
  private static final int MAX_NAMESPACE_DEPTH = 2;
  private static final int NAMESPACE_ROOT_LEVEL = 0;
  private static final int NAMESPACE_DB_LEVEL = 1;
  private static final int NAMESPACE_SCHEMA_LEVEL = 2;

  private NamespaceHelpers() {}

  /**
   * Converts a Namespace into a SnowflakeIdentifier representing ROOT, a DATABASE, or a SCHEMA.
   *
   * @throws IllegalArgumentException if the namespace is not a supported depth.
   */
  public static SnowflakeIdentifier toSnowflakeIdentifier(Namespace namespace) {
    switch (namespace.length()) {
      case NAMESPACE_ROOT_LEVEL:
        return SnowflakeIdentifier.ofRoot();
      case NAMESPACE_DB_LEVEL:
        return SnowflakeIdentifier.ofDatabase(namespace.level(NAMESPACE_DB_LEVEL - 1));
      case NAMESPACE_SCHEMA_LEVEL:
        return SnowflakeIdentifier.ofSchema(
            namespace.level(NAMESPACE_DB_LEVEL - 1), namespace.level(NAMESPACE_SCHEMA_LEVEL - 1));
      default:
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "Snowflake max namespace level is %d, got namespace '%s'",
                MAX_NAMESPACE_DEPTH,
                namespace));
    }
  }

  /**
   * Converts a TableIdentifier into a SnowflakeIdentifier of type TABLE; the identifier must have
   * exactly the right namespace depth to represent a fully-qualified Snowflake table identifier.
   */
  public static SnowflakeIdentifier toSnowflakeIdentifier(TableIdentifier identifier) {
    SnowflakeIdentifier namespaceScope = toSnowflakeIdentifier(identifier.namespace());
    Preconditions.checkArgument(
        namespaceScope.type() == SnowflakeIdentifier.Type.SCHEMA,
        "Namespace portion of '%s' must be at the SCHEMA level, got namespaceScope '%s'",
        identifier,
        namespaceScope);
    return SnowflakeIdentifier.ofTable(
        namespaceScope.databaseName(), namespaceScope.schemaName(), identifier.name());
  }

  /**
   * Converts a SnowflakeIdentifier of type ROOT, DATABASE, or SCHEMA into an equivalent Iceberg
   * Namespace; throws IllegalArgumentException if not an appropriate type.
   */
  public static Namespace toIcebergNamespace(SnowflakeIdentifier identifier) {
    switch (identifier.type()) {
      case ROOT:
        return Namespace.empty();
      case DATABASE:
        return Namespace.of(identifier.databaseName());
      case SCHEMA:
        return Namespace.of(identifier.databaseName(), identifier.schemaName());
      default:
        throw new IllegalArgumentException(
            String.format("Cannot convert identifier '%s' to Namespace", identifier));
    }
  }

  /**
   * Converts a SnowflakeIdentifier to an equivalent Iceberg TableIdentifier; the identifier must be
   * of type TABLE.
   */
  public static TableIdentifier toIcebergTableIdentifier(SnowflakeIdentifier identifier) {
    Preconditions.checkArgument(
        identifier.type() == SnowflakeIdentifier.Type.TABLE,
        "SnowflakeIdentifier must be type TABLE, got '%s'",
        identifier);
    return TableIdentifier.of(
        identifier.databaseName(), identifier.schemaName(), identifier.tableName());
  }
}
