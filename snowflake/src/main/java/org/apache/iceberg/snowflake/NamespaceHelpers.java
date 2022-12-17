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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.snowflake.entities.SnowflakeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceHelpers {
  private static final int MAX_NAMESPACE_DEPTH = 2;
  private static final int NAMESPACE_ROOT_LEVEL = 0;
  private static final int NAMESPACE_DB_LEVEL = 1;
  private static final int NAMESPACE_SCHEMA_LEVEL = 2;

  private static final Logger LOG = LoggerFactory.getLogger(NamespaceHelpers.class);

  /**
   * Converts a Namespace into a SnowflakeIdentifier representing ROOT, a DATABASE, or a SCHEMA.
   *
   * @throws IllegalArgumentException if the namespace is not a supported depth.
   */
  public static SnowflakeIdentifier getSnowflakeIdentifierForNamespace(Namespace namespace) {
    SnowflakeIdentifier identifier = null;
    switch (namespace.length()) {
      case NAMESPACE_ROOT_LEVEL:
        identifier = SnowflakeIdentifier.ofRoot();
        break;
      case NAMESPACE_DB_LEVEL:
        identifier = SnowflakeIdentifier.ofDatabase(namespace.level(NAMESPACE_DB_LEVEL - 1));
        break;
      case NAMESPACE_SCHEMA_LEVEL:
        identifier =
            SnowflakeIdentifier.ofSchema(
                namespace.level(NAMESPACE_DB_LEVEL - 1),
                namespace.level(NAMESPACE_SCHEMA_LEVEL - 1));
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Snowflake max namespace level is %d, got namespace '%s'",
                MAX_NAMESPACE_DEPTH, namespace));
    }
    LOG.debug("getSnowflakeIdentifierForNamespace({}) -> {}", namespace, identifier);
    return identifier;
  }

  /**
   * Converts a TableIdentifier into a SnowflakeIdentifier of type TABLE; the identifier must have
   * exactly the right namespace depth to represent a fully-qualified Snowflake table identifier.
   */
  public static SnowflakeIdentifier getSnowflakeIdentifierForTableIdentifier(
      TableIdentifier identifier) {
    SnowflakeIdentifier namespaceScope = getSnowflakeIdentifierForNamespace(identifier.namespace());
    Preconditions.checkArgument(
        namespaceScope.getType() == SnowflakeIdentifier.Type.SCHEMA,
        "Namespace portion of '%s' must be at the SCHEMA level, got namespaceScope '%s'",
        identifier,
        namespaceScope);
    SnowflakeIdentifier ret =
        SnowflakeIdentifier.ofTable(
            namespaceScope.getDatabaseName(), namespaceScope.getSchemaName(), identifier.name());
    LOG.debug("getSnowflakeIdentifierForTableIdentifier({}) -> {}", identifier, ret);
    return ret;
  }

  private NamespaceHelpers() {}
}
