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
package org.apache.iceberg.catalog;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Identifies an index instance within a catalog.
 *
 * <p>An index instance is uniquely identified by combining the {@link TableIdentifier} with the
 * index name. This ensures that index names are scoped to their respective tables.
 *
 * <p>Example: For a table "persons" in the "company" database with an index named
 * "nationality_index", the resulting IndexIdentifier would be: "company.persons.nationality_index"
 */
public class IndexIdentifier implements Serializable {

  private final TableIdentifier tableIdentifier;
  private final String name;

  private IndexIdentifier(TableIdentifier tableIdentifier, String name) {
    Preconditions.checkArgument(tableIdentifier != null, "Table identifier cannot be null");
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "Index name cannot be null or empty");
    this.tableIdentifier = tableIdentifier;
    this.name = name;
  }

  /**
   * Creates an IndexIdentifier from a table identifier and index name.
   *
   * @param tableIdentifier the table identifier
   * @param name the index name
   * @return an IndexIdentifier
   */
  public static IndexIdentifier of(TableIdentifier tableIdentifier, String name) {
    return new IndexIdentifier(tableIdentifier, name);
  }

  /**
   * Creates an IndexIdentifier from a namespace, table name, and index name.
   *
   * @param namespace the namespace
   * @param tableName the table name
   * @param indexName the index name
   * @return an IndexIdentifier
   */
  public static IndexIdentifier of(Namespace namespace, String tableName, String indexName) {
    return new IndexIdentifier(TableIdentifier.of(namespace, tableName), indexName);
  }

  /**
   * Creates an IndexIdentifier by parsing a string representation.
   *
   * <p>The string should be in the format "namespace.table.indexName" where namespace can contain
   * multiple levels separated by dots.
   *
   * @param identifier the string representation of the index identifier
   * @return an IndexIdentifier
   * @throws IllegalArgumentException if the identifier string is invalid
   */
  public static IndexIdentifier parse(String identifier) {
    Preconditions.checkArgument(
        identifier != null && !identifier.isEmpty(),
        "Cannot parse index identifier: null or empty");

    String[] parts = identifier.split("\\.");
    Preconditions.checkArgument(
        parts.length >= 3,
        "Cannot parse index identifier '%s': must contain at least namespace, table, and index name",
        identifier);

    String indexName = parts[parts.length - 1];
    String tableName = parts[parts.length - 2];
    String[] namespaceParts = Arrays.copyOfRange(parts, 0, parts.length - 2);

    return new IndexIdentifier(
        TableIdentifier.of(Namespace.of(namespaceParts), tableName), indexName);
  }

  /**
   * Returns the table identifier for this index.
   *
   * @return the table identifier
   */
  public TableIdentifier tableIdentifier() {
    return tableIdentifier;
  }

  /**
   * Returns the namespace for this index (same as the table's namespace).
   *
   * @return the namespace
   */
  public Namespace namespace() {
    return tableIdentifier.namespace();
  }

  /**
   * Returns the name of the table this index belongs to.
   *
   * @return the table name
   */
  public String tableName() {
    return tableIdentifier.name();
  }

  /**
   * Returns the name of this index.
   *
   * @return the index name
   */
  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IndexIdentifier that = (IndexIdentifier) o;
    return tableIdentifier.equals(that.tableIdentifier) && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    int result = tableIdentifier.hashCode();
    result = 31 * result + name.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return tableIdentifier.toString() + "." + name;
  }
}
