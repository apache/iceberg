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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;

/**
 * A Catalog API for table create, drop, and load operations.
 */
public interface Catalog {

  /**
   * Return all the identifiers under this namespace.
   *
   * @param namespace a namespace
   * @return a list of identifiers for tables
   * @throws  NotFoundException if the namespace is not found
   */
  List<TableIdentifier> listTables(Namespace namespace);

  /**
   * Create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param location a location for the table; leave null if unspecified
   * @param properties a string map of table properties
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties);

  /**
   * Create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param properties a string map of table properties
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  default Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return createTable(identifier, schema, spec, null, properties);
  }

  /**
   * Create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  default Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec) {
    return createTable(identifier, schema, spec, null, null);
  }

  /**
   * Create an unpartitioned table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  default Table createTable(
      TableIdentifier identifier,
      Schema schema) {
    return createTable(identifier, schema, PartitionSpec.unpartitioned(), null, null);
  }

  /**
   * Start a transaction to create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param location a location for the table; leave null if unspecified
   * @param properties a string map of table properties
   * @return a {@link Transaction} to create the table
   * @throws AlreadyExistsException if the table already exists
   */
  Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties);

  /**
   * Start a transaction to create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param properties a string map of table properties
   * @return a {@link Transaction} to create the table
   * @throws AlreadyExistsException if the table already exists
   */
  default Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return newCreateTableTransaction(identifier, schema, spec, null, properties);
  }

  /**
   * Start a transaction to create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @return a {@link Transaction} to create the table
   * @throws AlreadyExistsException if the table already exists
   */
  default Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec) {
    return newCreateTableTransaction(identifier, schema, spec, null, null);
  }

  /**
   * Start a transaction to create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @return a {@link Transaction} to create the table
   * @throws AlreadyExistsException if the table already exists
   */
  default Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema) {
    return newCreateTableTransaction(identifier, schema, PartitionSpec.unpartitioned(), null, null);
  }

  /**
   * Start a transaction to replace a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param location a location for the table; leave null if unspecified
   * @param properties a string map of table properties
   * @param orCreate whether to create the table if not exists
   * @return a {@link Transaction} to replace the table
   * @throws NoSuchTableException if the table doesn't exist and orCreate is false
   */
  Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties,
      boolean orCreate);

  /**
   * Start a transaction to replace a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param properties a string map of table properties
   * @param orCreate whether to create the table if not exists
   * @return a {@link Transaction} to replace the table
   * @throws NoSuchTableException if the table doesn't exist and orCreate is false
   */
  default Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties,
      boolean orCreate) {
    return newReplaceTableTransaction(identifier, schema, spec, null, properties, orCreate);
  }

  /**
   * Start a transaction to replace a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param orCreate whether to create the table if not exists
   * @return a {@link Transaction} to replace the table
   * @throws NoSuchTableException if the table doesn't exist and orCreate is false
   */
  default Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      boolean orCreate) {
    return newReplaceTableTransaction(identifier, schema, spec, null, null, orCreate);
  }

  /**
   * Start a transaction to replace a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param orCreate whether to create the table if not exists
   * @return a {@link Transaction} to replace the table
   * @throws NoSuchTableException if the table doesn't exist and orCreate is false
   */
  default Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      boolean orCreate) {
    return newReplaceTableTransaction(identifier, schema, PartitionSpec.unpartitioned(), null, null, orCreate);
  }

  /**
   * Check whether table exists.
   *
   * @param identifier a table identifier
   * @return true if the table exists, false otherwise
   */
  default boolean tableExists(TableIdentifier identifier) {
    try {
      loadTable(identifier);
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Drop a table and delete all data and metadata files.
   *
   * @param identifier a table identifier
   * @return true if the table was dropped, false if the table did not exist
   */
  default boolean dropTable(TableIdentifier identifier) {
    return dropTable(identifier, true /* drop data and metadata files */);
  }

  /**
   * Drop a table; optionally delete data and metadata files.
   * <p>
   * If purge is set to true the implementation should delete all data and metadata files.
   *
   * @param identifier a table identifier
   * @param purge if true, delete all data and metadata files in the table
   * @return true if the table was dropped, false if the table did not exist
   */
  boolean dropTable(TableIdentifier identifier, boolean purge);

  /**
   * Rename a table.
   *
   * @param from identifier of the table to rename
   * @param to new table name
   * @throws NoSuchTableException if the from table does not exist
   * @throws AlreadyExistsException if the to table already exists
   */
  void renameTable(TableIdentifier from, TableIdentifier to);

  /**
   * Load a table.
   *
   * @param identifier a table identifier
   * @return instance of {@link Table} implementation referred by {@code tableIdentifier}
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(TableIdentifier identifier);


  /**
   * Create a namespace in the catalog.
   *
   * @param namespace {@link Namespace}.
   */
  boolean createNamespace(Namespace namespace);

  /**
   * List top-level namespaces from the catalog.
   * <p>
   * If an object such as a table, view, or function exists, its parent namespaces must also exist
   * and must be returned by this discovery method. For example, if table a.b.t exists, this method
   * must return ["a"] in the result array.
   *
   * @return an List of namespace {@link Namespace} names
   */
  List<Namespace> listNamespaces();

  /**
   * List  namespaces from the namespace.
   * <p>
   * For example, if table a.b.t exists, use 'SELECT NAMESPACE IN a' this method
   * must return Namepace.of("b") {@link Namespace}.
   *
   * @return an List of namespace {@link Namespace} names
   */
  List<Namespace> listNamespaces(Namespace namespace);

  /**
   * Load metadata properties for a namespace.
   *
   * @param namespace a Namespace.of(name) {@link Namespace}
   * @return a string map of properties for the given namespace
   */
  Map<String, String>  loadNamespaceMetadata(Namespace namespace);

  /**
   * Load metadata properties for a namespace.
   *
   * @param namespace a Namespace.of(name) {@link Namespace}
   * @return true while drop success.
   */
  boolean dropNamespace(Namespace namespace) throws IOException;

  /**
   * Load metadata properties for a namespace.
   *
   * @param namespace a Namespace.of(name) {@link Namespace}
   * @return true while alter success.
   */
  boolean alterNamespace(Namespace namespace);

}
