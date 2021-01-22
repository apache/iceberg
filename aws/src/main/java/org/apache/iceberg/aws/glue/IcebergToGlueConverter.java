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

package org.apache.iceberg.aws.glue;

import java.util.Map;
import java.util.regex.Pattern;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import software.amazon.awssdk.services.glue.model.DatabaseInput;

class IcebergToGlueConverter {

  private IcebergToGlueConverter() {
  }

  private static final Pattern GLUE_DB_PATTERN = Pattern.compile("^[a-z0-9_]{1,252}$");
  private static final Pattern GLUE_TABLE_PATTERN = Pattern.compile("^[a-z0-9_]{1,255}$");

  /**
   * A Glue database name cannot be longer than 252 characters.
   * The only acceptable characters are lowercase letters, numbers, and the underscore character.
   * More details: https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html
   * @param namespace namespace
   * @return if namespace can be accepted by Glue
   */
  static boolean isValidNamespace(Namespace namespace) {
    if (namespace.levels().length != 1) {
      return false;
    }
    String dbName = namespace.level(0);
    return dbName != null && GLUE_DB_PATTERN.matcher(dbName).find();
  }

  /**
   * Validate if an Iceberg namespace is valid in Glue
   * @param namespace namespace
   * @throws NoSuchNamespaceException if namespace is not valid in Glue
   */
  static void validateNamespace(Namespace namespace) {
    ValidationException.check(isValidNamespace(namespace), "Cannot convert namespace %s to Glue database name, " +
        "because it must be 1-252 chars of lowercase letters, numbers, underscore", namespace);
  }

  /**
   * Validate and convert Iceberg namespace to Glue database name
   * @param namespace Iceberg namespace
   * @return database name
   */
  static String toDatabaseName(Namespace namespace) {
    validateNamespace(namespace);
    return namespace.level(0);
  }

  /**
   * Validate and get Glue database name from Iceberg TableIdentifier
   * @param tableIdentifier Iceberg table identifier
   * @return database name
   */
  static String getDatabaseName(TableIdentifier tableIdentifier) {
    return toDatabaseName(tableIdentifier.namespace());
  }

  /**
   * Validate and convert Iceberg name to Glue DatabaseInput
   * @param namespace Iceberg namespace
   * @param metadata metadata map
   * @return Glue DatabaseInput
   */
  static DatabaseInput toDatabaseInput(Namespace namespace, Map<String, String> metadata) {
    return DatabaseInput.builder()
        .name(toDatabaseName(namespace))
        .parameters(metadata)
        .build();
  }

  /**
   * A Glue table name cannot be longer than 255 characters.
   * The only acceptable characters are lowercase letters, numbers, and the underscore character.
   * More details: https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html
   * @param tableName table name
   * @return if a table name can be accepted by Glue
   */
  static boolean isValidTableName(String tableName) {
    return tableName != null && GLUE_TABLE_PATTERN.matcher(tableName).find();
  }

  /**
   * Validate if a table name is valid in Glue
   * @param tableName table name
   * @throws NoSuchTableException if table name not valid in Glue
   */
  static void validateTableName(String tableName) {
    ValidationException.check(isValidTableName(tableName), "Cannot use %s as Glue table name, " +
        "because it must be 1-255 chars of lowercase letters, numbers, underscore", tableName);
  }

  /**
   * Validate and get Glue table name from Iceberg TableIdentifier
   * @param tableIdentifier table identifier
   * @return table name
   */
  static String getTableName(TableIdentifier tableIdentifier) {
    validateTableName(tableIdentifier.name());
    return tableIdentifier.name();
  }

  /**
   * Validate Iceberg TableIdentifier is valid in Glue
   * @param tableIdentifier Iceberg table identifier
   */
  static void validateTableIdentifier(TableIdentifier tableIdentifier) {
    validateNamespace(tableIdentifier.namespace());
    validateTableName(tableIdentifier.name());
  }
}
